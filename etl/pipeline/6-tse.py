"""
Pipeline 3 - TSE: Candidatos, Doações e Bens Declarados → Neo4j

Nós criados/atualizados:
  (:Pessoa)        cpf ou nr_titulo_eleitoral como chave
  (:Partido)       sigla como chave
  (:Eleicao)       cd_eleicao + ano como chave
  (:Cargo)         ds_cargo como chave
  (:Municipio)     reutiliza nós IBGE (lookup por nome)
  (:Estado)        sg_uf como chave
  (:BemDeclarado)  bem_id = sq_candidato_nr_ordem_ano como chave

Relacionamentos:
  (:Pessoa)-[:CANDIDATO_EM {nr_candidato, situacao, turno}]->(:Eleicao)
  (:Eleicao)-[:REALIZADA_EM]->(:Municipio | :Estado)
  (:Pessoa)-[:FILIADA_A {ano}]->(:Partido)
  (:Pessoa | :Empresa)-[:DOOU_PARA {valor, ano}]->(:Pessoa)   ← candidato
  (:Pessoa)-[:DECLAROU_BEM {ano_eleicao}]->(:BemDeclarado)
"""

import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, apply_schema, setup_schema

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
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Partido)       REQUIRE n.sigla IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Eleicao)       REQUIRE n.eleicao_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Cargo)         REQUIRE n.nome IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Estado)        REQUIRE n.sigla IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:BemDeclarado)  REQUIRE n.bem_id IS UNIQUE",
]
Q_INDEXES = [
    "CREATE INDEX pessoa_titulo IF NOT EXISTS FOR (p:Pessoa)       ON (p.nr_titulo_eleitoral)",
    "CREATE INDEX eleicao_ano   IF NOT EXISTS FOR (e:Eleicao)      ON (e.ano)",
    "CREATE INDEX eleicao_uf    IF NOT EXISTS FOR (e:Eleicao)      ON (e.sg_uf)",
    "CREATE INDEX bem_tipo      IF NOT EXISTS FOR (b:BemDeclarado) ON (b.tipo_bem)",
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

Q_BEM = """
UNWIND $rows AS r
MERGE (b:BemDeclarado {bem_id: r.bem_id})
SET b.tipo_bem   = r.tipo_bem,
    b.descricao  = r.descricao,
    b.valor      = toFloat(r.valor),
    b.ano        = toInteger(r.ano),
    b.fonte_nome = r.fonte_nome
"""

Q_DECLAROU_BEM = """
UNWIND $rows AS r
MATCH (p:Pessoa {cpf: r.cpf})
MATCH (b:BemDeclarado {bem_id: r.bem_id})
MERGE (p)-[rel:DECLAROU_BEM]->(b)
SET rel.ano_eleicao = toInteger(r.ano)
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
                sq      = r.get("SQ_CANDIDATO", "").strip()
                titulo  = _strip_doc(r.get("NR_TITULO_ELEITORAL_CANDIDATO", ""))
                cpf_key = titulo.zfill(11) if titulo else ""
                if sq and cpf_key:
                    sq_cpf[sq] = cpf_key
    log.info(f"  Mapa sq→cpf: {len(sq_cpf):,} candidatos")
    return sq_cpf

# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_candidatos(driver, data_dir: Path,
                     eleicoes: list[int] | None = None,
                     limite: int | None = None,
                     stats: dict = None) -> None:
    todos = sorted(data_dir.glob("candidatos_*.csv"), key=lambda p: p.stem)
    if eleicoes:
        todos = [p for p in todos if any(f'_{ano}' in p.stem or p.stem.endswith(str(ano)) for ano in eleicoes)]
    if not todos:
        log.warning("  Nenhum arquivo candidatos_*.csv encontrado (verifique --eleicao)")
        return
    if stats is None:
        stats = {'total': 0}

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        total = 0
        with driver.session() as session:
            for chunk in iter_csv(path):
                if limite is not None and stats['total'] >= limite:
                    log.info(f"    [candidatos] Limite de {limite:,} atingido. Parando.")
                    return
                if limite is not None:
                    restante = limite - stats['total']
                    if restante <= 0:
                        return
                    if len(chunk) > restante:
                        chunk = chunk[:restante]

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
                    stats['total'] += len(t["candidatos"])
                if t["filiacoes"]:
                    run_batches(session, Q_FILIACAO, t["filiacoes"])

        log.info(f"    ✓ {path.name}  {total:,} candidatos")


def _load_doacoes(driver, data_dir: Path,
                  sq_cpf: dict[str, str],
                  eleicoes: list[int] | None = None,
                  limite: int | None = None,
                  stats: dict = None) -> None:
    todos = sorted(data_dir.glob("doacoes_*.csv"))
    if eleicoes:
        todos = [p for p in todos
                 if any(f'_{ano}' in p.stem or p.stem.endswith(str(ano))
                        for ano in eleicoes)]
    if not todos:
        log.warning("  Nenhum arquivo doacoes_*.csv encontrado (verifique --eleicao)")
        return
    if stats is None:
        stats = {'total': 0}

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        pf_t = pj_t = skip_t = 0
        with driver.session() as session:
            for chunk in iter_csv(path):
                if limite is not None and stats['total'] >= limite:
                    log.info(f"    [doacoes] Limite de {limite:,} atingido. Parando.")
                    return
                if limite is not None:
                    restante = limite - stats['total']
                    if restante <= 0:
                        return
                    if len(chunk) > restante:
                        chunk = chunk[:restante]

                pf, pj = _t_doacoes(chunk, sq_cpf)
                skip_t += len(chunk) - len(pf) - len(pj)
                if pf:
                    run_batches(session, Q_DOACAO_PF, pf)
                    pf_t += len(pf)
                    stats['total'] += len(pf)
                if pj:
                    run_batches(session, Q_DOACAO_PJ, pj)
                    pj_t += len(pj)
                    stats['total'] += len(pj)
        log.info(f"    ✓ {path.name}  PF={pf_t:,}  PJ={pj_t:,}  sem_cpf={skip_t:,}")


def _safe_valor_bem(s: str) -> float:
    """'1.234.567,89' → 1234567.89  (formato PT-BR com separador de milhar)"""
    try:
        return float(s.strip().replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0


def _t_bens(chunk: list[dict], sq_cpf: dict[str, str]) -> list[dict]:
    rows = []
    for r in chunk:
        sq  = r.get("SQ_CANDIDATO", "").strip()
        cpf = sq_cpf.get(sq, "")
        if not cpf:
            continue
        nr  = r.get("NR_ORDEM_BEM", "0").strip()
        ano = r.get("ANO_ELEICAO", "").strip()
        rows.append({
            "bem_id":    f"{sq}_{nr}_{ano}",
            "cpf":       cpf,
            "tipo_bem":  r.get("DS_TIPO_BEM", "").strip(),
            "descricao": r.get("DS_BEM_CANDIDATO", "").strip(),
            "valor":     _safe_valor_bem(r.get("VR_BEM_CANDIDATO", "0")),
            "ano":       ano,
            "fonte_nome": r.get("fonte_nome", FONTE["fonte_nome"]),
        })
    return rows


def _load_bens(driver, data_dir: Path,
               sq_cpf: dict[str, str],
               eleicoes: list[int] | None = None,
               limite: int | None = None,
               stats: dict = None) -> None:
    todos = sorted(data_dir.glob("bens_*.csv"))
    if eleicoes:
        todos = [p for p in todos
                 if any(f'_{ano}' in p.stem or p.stem.endswith(str(ano))
                        for ano in eleicoes)]
    if not todos:
        log.warning("  Nenhum arquivo bens_*.csv encontrado (execute download tse primeiro)")
        return
    if stats is None:
        stats = {'total': 0}

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        total = skip = 0
        with driver.session() as session:
            for chunk in iter_csv(path):
                if limite is not None and stats['total'] >= limite:
                    log.info(f"    [bens] Limite de {limite:,} atingido. Parando.")
                    return
                if limite is not None:
                    restante = limite - stats['total']
                    if restante <= 0:
                        return
                    if len(chunk) > restante:
                        chunk = chunk[:restante]

                rows = _t_bens(chunk, sq_cpf)
                skip += len(chunk) - len(rows)
                if rows:
                    run_batches(session, Q_BEM, rows)
                    run_batches(session, Q_DECLAROU_BEM, rows)
                    total += len(rows)
                    stats['total'] += len(rows)
        log.info(f"    ✓ {path.name}  {total:,} bens  sem_cpf={skip:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str,
        eleicoes: list[int] | None = None, limite: int | None = None):
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
        apply_schema(session, Q_CONSTRAINTS, Q_INDEXES)

    with IngestionRun(driver, "tse") as run_ctx:
        cand_dir = DATA_DIR
        doac_dir = DATA_DIR
        bens_dir = DATA_DIR

        log.info("  [1/3] Candidatos → Pessoa, Eleição, Partido, FILIADA_A, CANDIDATO_EM...")
        stats = {'total': 0}
        _load_candidatos(driver, cand_dir, eleicoes, limite, stats)
        if limite is not None and stats['total'] >= limite:
            log.info(f"  Limite de {limite:,} linhas atingido após candidatos. Parando.")
            run_ctx.add(rows_in=stats['total'], rows_out=stats['total'])
            driver.close()
            return

        log.info("  Construindo mapa sq_candidato → cpf...")
        sq_cpf = _build_sq_cpf_map(cand_dir, eleicoes)

        log.info("  [2/3] Doações → DOOU_PARA...")
        _load_doacoes(driver, doac_dir, sq_cpf, eleicoes, limite, stats)
        if limite is not None and stats['total'] >= limite:
            log.info(f"  Limite de {limite:,} linhas atingido após doacoes. Parando.")
            run_ctx.add(rows_in=stats['total'], rows_out=stats['total'])
            driver.close()
            return

        log.info("  [3/3] Bens declarados → BemDeclarado, DECLAROU_BEM...")
        _load_bens(driver, bens_dir, sq_cpf, eleicoes, limite, stats)

        run_ctx.add(rows_in=stats['total'], rows_out=stats['total'])

        log.info("  Linkando municípios TSE → IBGE...")
        with driver.session() as session:
            session.run(Q_LINK_MUNICIPIO_TSE_IBGE)
        log.info("  ✓ municípios linkados")

    driver.close()
    log.info("[tse] Pipeline concluído")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline TSE — carrega candidatos, doações e bens no Neo4j")
    parser.add_argument("--eleicao", type=int, action="append", default=None, help="Ano(s) da eleição (ex: --eleicao 2024 --eleicao 2022)")
    parser.add_argument("--limite", type=int, default=None, help="Número máximo de linhas a inserir (carga parcial)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    run(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "senha"),
        eleicoes=args.eleicao,
        limite=args.limite,
    )
