"""
Pipeline 3 - TSE: Candidatos e Doações → Neo4j

Nós criados/atualizados:
  (:Pessoa)   cpf ou nr_titulo_eleitoral como chave
  (:Partido)  sigla como chave
  (:Eleicao)  cd_eleicao + ano como chave
  (:Cargo)    ds_cargo como chave
  (:Municipio) reutiliza nós IBGE (lookup por nome)
  (:Estado)   sg_uf como chave

Relacionamentos:
  (:Pessoa)-[:CANDIDATO_EM {nr_candidato, situacao, turno}]->(:Eleicao)
  (:Eleicao)-[:REALIZADA_EM]->(:Municipio | :Estado)
  (:Pessoa)-[:FILIADA_A {ano}]->(:Partido)
  (:Pessoa | :Empresa)-[:DOOU_PARA {valor, ano}]->(:Pessoa)   ← candidato
"""

import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "tse"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE",  "20000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "TSE — Tribunal Superior Eleitoral",
    "fonte_url":  "https://dadosabertos.tse.jus.br",
}

# Eleições de âmbito municipal (SG_UE = código do município)
CARGOS_MUNICIPAIS = {"PREFEITO", "VICE-PREFEITO", "VEREADOR"}
# Eleições de âmbito estadual
CARGOS_ESTADUAIS  = {"GOVERNADOR", "VICE-GOVERNADOR", "SENADOR",
                     "DEPUTADO FEDERAL", "DEPUTADO ESTADUAL",
                     "DEPUTADO DISTRITAL"}


# ── Helpers ───────────────────────────────────────────────────────────────────

# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Partido)  REQUIRE n.sigla IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Eleicao)  REQUIRE n.eleicao_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Cargo)    REQUIRE n.nome IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Estado)   REQUIRE n.sigla IS UNIQUE",
]
Q_INDEXES = [
    "CREATE INDEX pessoa_titulo IF NOT EXISTS FOR (p:Pessoa) ON (p.nr_titulo_eleitoral)",
    "CREATE INDEX eleicao_ano   IF NOT EXISTS FOR (e:Eleicao) ON (e.ano)",
    "CREATE INDEX eleicao_uf    IF NOT EXISTS FOR (e:Eleicao) ON (e.sg_uf)",
]


# ── Queries Cypher ────────────────────────────────────────────────────────────

Q_PARTIDO = """
UNWIND $rows AS r
MERGE (p:Partido {sigla: r.sg_partido})
SET p.nome       = r.nm_partido,
    p.nr_partido = r.nr_partido,
    p.fonte_nome = r.fonte_nome
"""

Q_ESTADO = """
UNWIND $rows AS r
MERGE (e:Estado {sigla: r.sg_uf})
SET e.fonte_nome = r.fonte_nome
"""

Q_ELEICAO = """
UNWIND $rows AS r
MERGE (e:Eleicao {eleicao_id: r.eleicao_id})
SET e.cd_eleicao     = r.cd_eleicao,
    e.ds_eleicao     = r.ds_eleicao,
    e.dt_eleicao     = r.dt_eleicao,
    e.ano            = toInteger(r.ano),
    e.nm_tipo        = r.nm_tipo_eleicao,
    e.sg_uf          = r.sg_uf,
    e.sg_ue          = r.sg_ue,
    e.nm_ue          = r.nm_ue,
    e.nr_turno       = toInteger(r.nr_turno),
    e.fonte_nome     = r.fonte_nome
"""

# Liga Eleição → Municipio (por código RF = SG_UE para eleições municipais)
Q_ELEICAO_MUNICIPIO = """
UNWIND $rows AS r
MATCH (e:Eleicao {eleicao_id: r.eleicao_id})
MATCH (m:Municipio {codigo_rf: r.sg_ue})
MERGE (e)-[:REALIZADA_EM]->(m)
"""

# Liga Eleição → Estado (para eleições estaduais/federais)
Q_ELEICAO_ESTADO = """
UNWIND $rows AS r
MATCH (e:Eleicao {eleicao_id: r.eleicao_id})
MERGE (est:Estado {sigla: r.sg_uf})
MERGE (e)-[:REALIZADA_EM]->(est)
"""

Q_CANDIDATO = """
UNWIND $rows AS r
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome                  = r.nm_candidato,
                p.nome_urna             = r.nm_urna,
                p.nr_titulo_eleitoral   = r.nr_titulo,
                p.dt_nascimento         = r.dt_nascimento,
                p.ds_genero             = r.ds_genero,
                p.ds_grau_instrucao     = r.ds_grau_instrucao,
                p.ds_estado_civil       = r.ds_estado_civil,
                p.ds_cor_raca           = r.ds_cor_raca,
                p.sg_uf_nascimento      = r.sg_uf_nascimento,
                p.fonte_nome            = r.fonte_nome
  ON MATCH  SET p.nome                  = coalesce(p.nome, r.nm_candidato),
                p.nome_urna             = coalesce(p.nome_urna, r.nm_urna),
                p.nr_titulo_eleitoral   = coalesce(p.nr_titulo_eleitoral, r.nr_titulo)
WITH p, r
MATCH (e:Eleicao {eleicao_id: r.eleicao_id})
MERGE (p)-[c:CANDIDATO_EM]->(e)
SET c.sq_candidato    = r.sq_candidato,
    c.nr_candidato    = r.nr_candidato,
    c.nm_urna         = r.nm_urna,
    c.ds_cargo        = r.ds_cargo,
    c.nr_turno        = toInteger(r.nr_turno),
    c.ds_situacao     = r.ds_situacao,
    c.cd_situacao     = toInteger(r.cd_situacao),
    c.cd_ocupacao     = r.cd_ocupacao,
    c.ds_ocupacao     = r.ds_ocupacao
"""

Q_FILIACAO = """
UNWIND $rows AS r
MATCH (p:Pessoa {cpf: r.cpf})
MATCH (par:Partido {sigla: r.sg_partido})
MERGE (p)-[f:FILIADA_A]->(par)
SET f.ano = toInteger(r.ano)
"""

# Doações PF → candidato
Q_DOACAO_PF = """
UNWIND $rows AS r
MERGE (doador:Pessoa {cpf: r.cpf_doador})
  ON CREATE SET doador.nome = r.nome_doador
WITH doador, r
MERGE (cand:Pessoa {cpf: r.cpf_candidato})
MERGE (doador)-[d:DOOU_PARA]->(cand)
SET d.valor = toFloat(r.valor),
    d.ano   = toInteger(r.ano),
    d.sq_candidato = r.sq_candidato,
    d.fonte_nome   = r.fonte_nome
"""

# Doações PJ → candidato
Q_DOACAO_PJ = """
UNWIND $rows AS r
MERGE (doador:Empresa {cnpj_basico: r.cnpj_basico_doador})
  ON CREATE SET doador.nome_doador = r.nome_doador
WITH doador, r
MERGE (cand:Pessoa {cpf: r.cpf_candidato})
MERGE (doador)-[d:DOOU_PARA]->(cand)
SET d.valor = toFloat(r.valor),
    d.ano   = toInteger(r.ano),
    d.sq_candidato = r.sq_candidato,
    d.fonte_nome   = r.fonte_nome
"""

Q_LINK_MUNICIPIO_TSE_IBGE = """
MATCH (m:Municipio)
WHERE m.codigo_tse IS NOT NULL AND (m.id IS NULL OR m.ibge_linked IS NULL)
WITH m, toUpper(trim(m.nome)) AS nome_upper
MATCH (ibge:Municipio)
WHERE ibge.id IS NOT NULL AND toUpper(trim(ibge.nome)) = nome_upper
  AND ibge.codigo_tse IS NULL
SET m.id = ibge.id, m.ibge_linked = true
"""


# ── Transforms ────────────────────────────────────────────────────────────────

def _strip_doc(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def _t_candidatos(chunk: list[dict]) -> dict[str, list[dict]]:
    """
    Retorna dicts separados para cada tipo de operação.
    Evita queries duplicadas fazendo dedup por chave.
    """
    partidos, estados, eleicoes = {}, {}, {}
    candidatos, filiacoes = [], []
    el_mun, el_est = [], []

    for r in chunk:
        ano      = r.get("ANO_ELEICAO", "").strip()
        sg_uf    = r.get("SG_UF", "").strip()
        sg_ue    = r.get("SG_UE", "").strip()
        cd_el    = r.get("CD_ELEICAO", "").strip()
        el_id    = f"{cd_el}_{ano}_{sg_uf}_{sg_ue}"
        cargo    = r.get("DS_CARGO", "").strip().upper()

        # CPF — pode vir mascarado (***) ou vazio
        cpf_raw  = _strip_doc(r.get("NR_TITULO_ELEITORAL_CANDIDATO", ""))
        # usamos título eleitoral como chave se CPF não disponível
        titulo   = _strip_doc(r.get("NR_TITULO_ELEITORAL_CANDIDATO", ""))
        cpf_key  = cpf_raw.zfill(11) if cpf_raw else f"TITULO_{titulo}" if titulo else ""
        if not cpf_key:
            continue

        sg_partido = r.get("SG_PARTIDO", "").strip()
        fonte_nome = r.get("fonte_nome", FONTE["fonte_nome"])

        # partidos
        if sg_partido not in partidos:
            partidos[sg_partido] = {
                "sg_partido":  sg_partido,
                "nm_partido":  r.get("NM_PARTIDO", "").strip(),
                "nr_partido":  r.get("NR_PARTIDO", "").strip(),
                "fonte_nome":  fonte_nome,
            }

        # estados
        if sg_uf and sg_uf not in estados:
            estados[sg_uf] = {"sg_uf": sg_uf, "fonte_nome": fonte_nome}

        # eleições
        if el_id not in eleicoes:
            eleicoes[el_id] = {
                "eleicao_id":     el_id,
                "cd_eleicao":     cd_el,
                "ds_eleicao":     r.get("DS_ELEICAO", "").strip(),
                "dt_eleicao":     r.get("DT_ELEICAO", "").strip(),
                "ano":            ano,
                "nm_tipo_eleicao":r.get("NM_TIPO_ELEICAO", "").strip(),
                "sg_uf":          sg_uf,
                "sg_ue":          sg_ue,
                "nm_ue":          r.get("NM_UE", "").strip(),
                "nr_turno":       r.get("NR_TURNO", "1").strip(),
                "fonte_nome":     fonte_nome,
            }
            if cargo in CARGOS_MUNICIPAIS and sg_ue:
                el_mun.append({"eleicao_id": el_id, "sg_ue": sg_ue})
            else:
                el_est.append({"eleicao_id": el_id, "sg_uf": sg_uf})

        # candidato
        candidatos.append({
            "cpf":          cpf_key,
            "nr_titulo":    titulo,
            "sq_candidato": r.get("SQ_CANDIDATO", "").strip(),
            "nr_candidato": r.get("NR_CANDIDATO", "").strip(),
            "nm_candidato": r.get("NM_CANDIDATO", "").strip(),
            "nm_urna":      r.get("NM_URNA_CANDIDATO", "").strip(),
            "dt_nascimento":r.get("DT_NASCIMENTO", "").strip(),
            "ds_genero":    r.get("DS_GENERO", "").strip(),
            "ds_grau_instrucao": r.get("DS_GRAU_INSTRUCAO", "").strip(),
            "ds_estado_civil":   r.get("DS_ESTADO_CIVIL", "").strip(),
            "ds_cor_raca":       r.get("DS_COR_RACA", "").strip(),
            "sg_uf_nascimento":  r.get("SG_UF_NASCIMENTO", "").strip(),
            "ds_cargo":          cargo,
            "nr_turno":          r.get("NR_TURNO", "1").strip(),
            "ds_situacao":       r.get("DS_SIT_TOT_TURNO", "").strip(),
            "cd_situacao":       r.get("CD_SIT_TOT_TURNO", "0").strip(),
            "cd_ocupacao":       r.get("CD_OCUPACAO", "").strip(),
            "ds_ocupacao":       r.get("DS_OCUPACAO", "").strip(),
            "eleicao_id":        el_id,
            "sg_partido":        sg_partido,
            "ano":               ano,
            "fonte_nome":        fonte_nome,
        })

        # filiação
        if cpf_key and sg_partido:
            filiacoes.append({"cpf": cpf_key, "sg_partido": sg_partido,
                              "ano": ano})

    return {
        "partidos":   list(partidos.values()),
        "estados":    list(estados.values()),
        "eleicoes":   list(eleicoes.values()),
        "el_mun":     el_mun,
        "el_est":     el_est,
        "candidatos": candidatos,
        "filiacoes":  filiacoes,
    }


def _t_doacoes(chunk: list[dict],
               sq_cpf: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """
    Separa doações em PF e PJ.
    sq_cpf: dicionário sq_candidato → cpf, construído dos CSVs de candidatos.
    Doações sem CPF resolvido são descartadas silenciosamente.
    """
    pf_rows, pj_rows = [], []
    for r in chunk:
        doc_raw   = _strip_doc(r.get("cpf_cnpj_doador", ""))
        valor_raw = r.get("valor", "0").strip().replace(",", ".")
        sq_cand   = r.get("sq_candidato", "").strip()
        nome_d    = r.get("nome_doador", "").strip()
        ano       = r.get("ano", "").strip()
        fonte_nome = r.get("fonte_nome", FONTE["fonte_nome"])

        try:
            float(valor_raw)
        except ValueError:
            valor_raw = "0"

        # resolve CPF do candidato via dicionário Python — O(1), sem round-trip Neo4j
        cpf_cand = sq_cpf.get(sq_cand, "")
        if not cpf_cand:
            continue   # descarta doação sem candidato identificável

        base = {
            "sq_candidato":  sq_cand,
            "cpf_candidato": cpf_cand,
            "nome_doador":   nome_d,
            "valor":         valor_raw,
            "ano":           ano,
            "fonte_nome":    fonte_nome,
        }

        if len(doc_raw) == 11:
            pf_rows.append({**base, "cpf_doador": doc_raw.zfill(11)})
        elif len(doc_raw) in (14, 8):
            pj_rows.append({**base, "cnpj_basico_doador": doc_raw[:8].zfill(8)})

    return pf_rows, pj_rows


def _build_sq_cpf_map(cand_dir: Path,
                      eleicoes: list[int] | None = None) -> dict[str, str]:
    """
    Lê os CSVs de candidatos e constrói sq_candidato → cpf em memória.
    Muito mais rápido que resolver via Neo4j depois da carga.
    """
    sq_cpf: dict[str, str] = {}
    todos = sorted(cand_dir.glob("candidatos_*.csv"), key=lambda p: p.stem)
    if eleicoes:
        todos = [p for p in todos
                 if any(f'_{ano}' in p.stem or p.stem.endswith(str(ano))
                        for ano in eleicoes)]
    for path in todos:
        for chunk in iter_csv(path):
            for r in chunk:
                sq  = r.get("SQ_CANDIDATO", "").strip()
                cpf = _strip_doc(r.get("NR_CPF_CANDIDATO", ""))
                if sq and cpf and len(cpf) == 11:
                    sq_cpf[sq] = cpf.zfill(11)
    log.info(f"  Mapa sq→cpf: {len(sq_cpf):,} candidatos")
    return sq_cpf

# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_candidatos(driver, data_dir: Path,
                     eleicoes: list[int] | None = None) -> None:
    todos = sorted(data_dir.glob("candidatos_*.csv"), key=lambda p: p.stem)
    if eleicoes:
        todos = [p for p in todos if any(f'_{ano}' in p.stem or p.stem.endswith(str(ano)) for ano in eleicoes)]
    if not todos:
        log.warning("  Nenhum arquivo candidatos_*.csv encontrado (verifique --eleicao)")
        return

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        total = 0
        with driver.session() as session:
            for chunk in iter_csv(path):
                t = _t_candidatos(chunk)

                if t["partidos"]:
                    run_batches(session, Q_PARTIDO, t["partidos"])
                if t["estados"]:
                    run_batches(session, Q_ESTADO, t["estados"])
                if t["eleicoes"]:
                    run_batches(session, Q_ELEICAO, t["eleicoes"])
                if t["el_mun"]:
                    run_batches(session, Q_ELEICAO_MUNICIPIO, t["el_mun"])
                if t["el_est"]:
                    run_batches(session, Q_ELEICAO_ESTADO, t["el_est"])
                if t["candidatos"]:
                    run_batches(session, Q_CANDIDATO, t["candidatos"])
                    total += len(t["candidatos"])
                if t["filiacoes"]:
                    run_batches(session, Q_FILIACAO, t["filiacoes"])

        log.info(f"    ✓ {path.name}  {total:,} candidatos")


def _load_doacoes(driver, data_dir: Path,
                  sq_cpf: dict[str, str],
                  eleicoes: list[int] | None = None) -> None:
    todos = sorted(data_dir.glob("doacoes_*.csv"))
    if eleicoes:
        todos = [p for p in todos
                 if any(f'_{ano}' in p.stem or p.stem.endswith(str(ano))
                        for ano in eleicoes)]
    if not todos:
        log.warning("  Nenhum arquivo doacoes_*.csv encontrado (verifique --eleicao)")
        return

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        pf_t = pj_t = skip_t = 0
        with driver.session() as session:
            for chunk in iter_csv(path):
                pf, pj = _t_doacoes(chunk, sq_cpf)
                skip_t += len(chunk) - len(pf) - len(pj)
                if pf:
                    run_batches(session, Q_DOACAO_PF, pf)
                    pf_t += len(pf)
                if pj:
                    run_batches(session, Q_DOACAO_PJ, pj)
                    pj_t += len(pj)
        log.info(f"    ✓ {path.name}  PF={pf_t:,}  PJ={pj_t:,}  sem_cpf={skip_t:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str,
        eleicoes: list[int] | None = None):
    """
    eleicoes: lista de anos a carregar. None = todos os disponíveis.
    Passado pelo CLI via --eleicao 2024 --eleicao 2022.
    """
    log.info(
        f"[tse] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}"
        + (f"  eleicoes={eleicoes}" if eleicoes else "  eleicoes=todas")
    )

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "tse"):
        cand_dir = DATA_DIR / "candidatos"
        doac_dir = DATA_DIR / "doacoes"

        log.info("  [1/2] Candidatos → Pessoa, Eleição, Partido, FILIADA_A, CANDIDATO_EM...")
        _load_candidatos(driver, cand_dir, eleicoes)

        log.info("  Construindo mapa sq_candidato → cpf...")
        sq_cpf = _build_sq_cpf_map(cand_dir, eleicoes)

        log.info("  [2/2] Doações → DOOU_PARA...")
        _load_doacoes(driver, doac_dir, sq_cpf, eleicoes)

        log.info("  Linkando municípios TSE → IBGE...")
        with driver.session() as session:
            session.run(Q_LINK_MUNICIPIO_TSE_IBGE)
        log.info("  ✓ municípios linkados")

    driver.close()
    log.info("[tse] Pipeline concluído")