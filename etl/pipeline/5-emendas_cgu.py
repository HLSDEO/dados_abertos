"""
Pipeline 5 - Emendas Parlamentares CGU → Neo4j

Lê os CSVs de data/emendas_cgu/ e carrega no grafo.

Arquivos:
  emendas.csv        → nós :Emenda + vínculos geográficos e orçamentários
  convenios.csv      → rel :Emenda -[:TEM_CONVENIO]-> :Convenio
  por_favorecido.csv → rel :Emenda -[:TEM_DESPESA]-> :Despesa

Nós criados/atualizados:
  (:Emenda)               — codigo_emenda como chave
  (:Parlamentar)          — codigo_autor como chave → merge com :Pessoa pelo nome
  (:FuncaoOrcamentaria)   — codigo_funcao
  (:Programa)             — codigo_programa
  (:Despesa)              — chave composta emenda+cnpj
  (:Empresa)              — merge pelo cnpj_basico (8 dígitos)

Relacionamentos:
  (:Parlamentar)-[:AUTORA_DE]->(:Emenda)
  (:Emenda)-[:DESTINADA_A]->(:Municipio|:Estado)        ← pelo Código IBGE
  (:Emenda)-[:CLASSIFICADA_EM]->(:FuncaoOrcamentaria)
  (:Emenda)-[:TEM_DESPESA]->(:Despesa)
  (:Emenda)-[:TEM_CONVENIO]->(:Convenio)
  (:Despesa)-[:PAGO_A]->(:Empresa)
  (:Emenda)-[:BENEFICIOU {valor_total}]->(:Empresa)     ← agregado de todas as despesas
"""

import csv
import logging
import os
import re
import unicodedata
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "emendas_cgu"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "10000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "CGU — Portal da Transparência",
    "fonte_url":  "https://portaldatransparencia.gov.br",
}


# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Emenda)            REQUIRE n.codigo_emenda    IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Parlamentar)       REQUIRE n.codigo_autor     IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:FuncaoOrcamentaria) REQUIRE n.codigo_funcao   IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Programa)          REQUIRE n.codigo_programa  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Convenio)          REQUIRE n.numero_convenio  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Despesa)           REQUIRE n.codigo_despesa   IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX emenda_ano        IF NOT EXISTS FOR (e:Emenda)    ON (e.ano_emenda)",
    "CREATE INDEX emenda_tipo       IF NOT EXISTS FOR (e:Emenda)    ON (e.tipo_emenda)",
    "CREATE INDEX parlamentar_nome  IF NOT EXISTS FOR (p:Parlamentar) ON (p.nome_autor)",
    "CREATE INDEX municipio_cod_ibge IF NOT EXISTS FOR (m:Municipio) ON (m.codigo_ibge)",
    "CREATE INDEX pessoa_nome_urna  IF NOT EXISTS FOR (p:Pessoa)    ON (p.nome_urna)",
    "CREATE INDEX empresa_cnpj_bas  IF NOT EXISTS FOR (emp:Empresa) ON (emp.cnpj_basico)",
]


# ── Queries ───────────────────────────────────────────────────────────────────

Q_PARLAMENTAR = """
UNWIND $rows AS r
MERGE (p:Parlamentar {codigo_autor: r.codigo_autor})
SET p.nome_autor = r.nome_autor,
    p.fonte_nome = r.fonte_nome,
    p.fonte_url  = r.fonte_url
"""

Q_FUNCAO = """
UNWIND $rows AS r
MERGE (f:FuncaoOrcamentaria {codigo_funcao: r.codigo_funcao})
SET f.nome_funcao    = r.nome_funcao,
    f.codigo_subfuncao = r.codigo_subfuncao,
    f.nome_subfuncao  = r.nome_subfuncao
"""

Q_PROGRAMA = """
UNWIND $rows AS r
MERGE (p:Programa {codigo_programa: r.codigo_programa})
SET p.nome_programa = r.nome_programa
"""

Q_EMENDA = """
UNWIND $rows AS r
MERGE (e:Emenda {codigo_emenda: r.codigo_emenda})
SET e.ano_emenda           = r.ano_emenda,
    e.tipo_emenda          = r.tipo_emenda,
    e.numero_emenda        = r.numero_emenda,
    e.localidade_gasto     = r.localidade_gasto,
    e.regiao               = r.regiao,
    e.valor_empenhado      = toFloat(r.valor_empenhado),
    e.valor_liquidado      = toFloat(r.valor_liquidado),
    e.valor_pago           = toFloat(r.valor_pago),
    e.valor_rp_inscrito    = toFloat(r.valor_rp_inscrito),
    e.valor_rp_cancelado   = toFloat(r.valor_rp_cancelado),
    e.valor_rp_pago        = toFloat(r.valor_rp_pago),
    e.fonte_nome           = r.fonte_nome,
    e.fonte_url            = r.fonte_url
"""

Q_AUTORIA = """
UNWIND $rows AS r
MATCH (e:Emenda {codigo_emenda: r.codigo_emenda})
MATCH (p:Parlamentar {codigo_autor: r.codigo_autor})
MERGE (p)-[:AUTORA_DE]->(e)
"""

Q_EMENDA_MUNICIPIO = """
UNWIND $rows AS r
MATCH (e:Emenda {codigo_emenda: r.codigo_emenda})
MATCH (m:Municipio {id: r.codigo_ibge})
MERGE (e)-[:DESTINADA_A]->(m)
"""

Q_EMENDA_MUNICIPIO_RF = """
UNWIND $rows AS r
MATCH (e:Emenda {codigo_emenda: r.codigo_emenda})
WHERE NOT (e)-[:DESTINADA_A]->()
MATCH (m:Municipio {codigo_ibge: r.codigo_ibge})
MERGE (e)-[:DESTINADA_A]->(m)
"""

Q_EMENDA_ESTADO = """
UNWIND $rows AS r
MATCH (e:Emenda {codigo_emenda: r.codigo_emenda})
MATCH (est:Estado {sigla: r.uf})
MERGE (e)-[:DESTINADA_A]->(est)
"""

Q_EMENDA_FUNCAO = """
UNWIND $rows AS r
MATCH (e:Emenda {codigo_emenda: r.codigo_emenda})
MATCH (f:FuncaoOrcamentaria {codigo_funcao: r.codigo_funcao})
MERGE (e)-[:CLASSIFICADA_EM]->(f)
"""

Q_CONVENIO_NOS = """
UNWIND $rows AS r
MERGE (c:Convenio {numero_convenio: r.numero_convenio})
SET c.objeto       = r.objeto,
    c.valor_emenda = toFloat(r.valor_emenda),
    c.situacao     = r.situacao,
    c.fonte_nome   = r.fonte_nome
"""

Q_CONVENIO_REL = """
UNWIND $rows AS r
MATCH (e:Emenda  {codigo_emenda:   r.codigo_emenda})
MATCH (c:Convenio {numero_convenio: r.numero_convenio})
MERGE (e)-[:TEM_CONVENIO]->(c)
"""

Q_DESPESA_NOS = """
UNWIND $rows AS r
MERGE (d:Despesa {codigo_despesa: r.codigo_despesa})
SET d.valor_recebido  = toFloat(r.valor_recebido),
    d.nome_favorecido = r.nome_favorecido,
    d.tipo_favorecido = r.tipo_favorecido,
    d.cnpj_favorecido = r.cnpj_favorecido,
    d.fonte_nome      = r.fonte_nome
"""

Q_DESPESA_REL = """
UNWIND $rows AS r
MATCH (e:Emenda  {codigo_emenda:  r.codigo_emenda})
MATCH (d:Despesa {codigo_despesa: r.codigo_despesa})
MERGE (e)-[:TEM_DESPESA]->(d)
"""

Q_DESPESA_EMPRESA = """
UNWIND $rows AS r
MATCH (d:Despesa {codigo_despesa: r.codigo_despesa})
WHERE r.cnpj_favorecido <> "" AND r.tipo_favorecido = "Pessoa Jurídica"
MERGE (emp:Empresa {cnpj_basico: r.cnpj_favorecido})
MERGE (d)-[:PAGO_A]->(emp)
"""

# Atalho direto Emenda → Empresa agregando valor_recebido de todas as despesas vinculadas.
# Derivado do grafo já carregado (idempotente: SET sobrescreve em re-run).
Q_EMENDA_BENEFICIOU = """
MATCH (e:Emenda)-[:TEM_DESPESA]->(d:Despesa)-[:PAGO_A]->(emp:Empresa)
WITH e, emp, sum(d.valor_recebido) AS total
MERGE (e)-[rel:BENEFICIOU]->(emp)
SET rel.valor_total = total
"""

# ── Resolução fuzzy de nomes ──────────────────────────────────────────────────
# Parlamentares no CGU usam abreviações ("PR.", "DEP.") e nomes de urna que
# diferem do nome civil registrado no TSE.  Estratégias em ordem de confiança:
#   1. Exact match (nome completo ou nome de urna)
#   2. Normalized exact (sem acentos e sem títulos)
#   3. Token subset: todos os tokens do parlamentar estão nos tokens do candidato
#   4. Reverse subset: todos os tokens do candidato estão nos tokens do parlamentar
# Em qualquer caso, só vincula quando há exatamente um CPF correspondente.

_TITULOS_RE = re.compile(
    r'\b(PR|DEP|SEN|VER|DR|DRA|ENG|PROF|CEL|GEN|BRG|CAP|TEN|SGT|SD)\b\.?',
    re.IGNORECASE,
)


def _norm_tokens(name: str) -> frozenset[str]:
    """Remove acentos, títulos parlamentares e pontuação; retorna frozenset de tokens ≥ 3 chars."""
    sem_acento = unicodedata.normalize("NFD", name)
    sem_acento = "".join(c for c in sem_acento if unicodedata.category(c) != "Mn")
    sem_titulo = _TITULOS_RE.sub("", sem_acento).upper()
    sem_pontos = re.sub(r"[^A-Z\s]", "", sem_titulo)
    return frozenset(t for t in sem_pontos.split() if len(t) >= 3)


def _resolve_cpf(nome_autor: str,
                 exact_map: dict[str, str],
                 cand_tokens: list[tuple[frozenset, str]]) -> str:
    """
    Resolve nome de parlamentar → CPF usando match em cascata.
    Retorna "" se não encontrar ou se ambíguo.
    """
    nome_up = nome_autor.strip().upper()

    # 1. exact
    if nome_up in exact_map:
        return exact_map[nome_up]

    # 2. normalized exact (só acentos e títulos removidos)
    parl_tokens = _norm_tokens(nome_autor)
    if not parl_tokens:
        return ""
    norm_key = " ".join(sorted(parl_tokens))
    if norm_key in exact_map:
        return exact_map[norm_key]

    # 3. subset: tokens do parlamentar ⊆ tokens do candidato  (nome abreviado CGU)
    matches = {cpf for toks, cpf in cand_tokens if parl_tokens <= toks}
    if len(matches) == 1:
        return next(iter(matches))

    # 4. reverse subset: tokens do candidato ⊆ tokens do parlamentar  (nome urna curto)
    if not matches:
        matches = {cpf for toks, cpf in cand_tokens
                   if toks <= parl_tokens and len(toks) >= 2}
        if len(matches) == 1:
            return next(iter(matches))

    if len(matches) > 1:
        log.debug(f"  Ambíguo: '{nome_autor}' → {len(matches)} candidatos, pulando")
    return ""


# Link parlamentar → pessoa resolvido em Python via dicionário nome→cpf
# carregado dos CSVs de candidatos TSE — evita full scan em 30M :Pessoa
Q_LINK_PARLAMENTAR_PESSOA = """
UNWIND $rows AS r
MATCH (par:Parlamentar {codigo_autor: r.codigo_autor})
MATCH (p:Pessoa {cpf: r.cpf})
MERGE (par)-[:MESMO_QUE]->(p)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_nome_cpf_map(driver) -> tuple[dict[str, str], list[tuple[frozenset, str]]]:
    """
    Constrói dois índices de resolução nome → CPF a partir dos candidatos TSE no grafo.
    Retorna (exact_map, cand_tokens_list).
      exact_map       : nome_upper → cpf  (match exato)
      cand_tokens_list: [(frozenset_tokens, cpf), ...]  (match por subconjunto)
    Ambos vazios se TSE não estiver carregado.
    """
    exact_map:   dict[str, str]              = {}
    cand_tokens: list[tuple[frozenset, str]] = []
    try:
        with driver.session() as session:
            # coalesce garante que nome_urna funcione mesmo em grafos gerados
            # antes do fix que passou a salvar o campo no nó :Pessoa
            result = session.run("""
                MATCH (p:Pessoa)-[c:CANDIDATO_EM]->()
                WHERE p.cpf IS NOT NULL AND p.nome IS NOT NULL
                WITH p, collect(c.nm_urna)[0] AS nm_urna_rel
                RETURN p.cpf      AS cpf,
                       p.nome     AS nome,
                       coalesce(p.nome_urna, nm_urna_rel) AS nome_urna
            """)
            for rec in result:
                cpf = rec["cpf"]
                if not cpf or len(cpf) != 11:
                    continue
                for campo in (rec["nome"], rec["nome_urna"]):
                    if not campo:
                        continue
                    exact_map.setdefault(campo.strip().upper(), cpf)
                    toks = _norm_tokens(campo)
                    if toks:
                        cand_tokens.append((toks, cpf))
        log.info(f"  Índice TSE: {len(exact_map):,} exact  {len(cand_tokens):,} token-sets")
    except Exception as exc:
        log.warning(f"  Não foi possível construir índice nome→cpf: {exc}")
    return exact_map, cand_tokens


def _safe_float(s: str) -> str:
    # CGU usa vírgula como separador decimal em vários arquivos
    s = (s or "").strip().replace(",", ".")
    if not s:
        return "0"
    try:
        float(s)
        return s
    except ValueError:
        return "0"



Q_EMENDA_PROGRAMA = """
UNWIND $rows AS r
MATCH (e:Emenda {codigo_emenda: r.codigo_emenda})
MATCH (p:Programa {codigo_programa: r.codigo_programa})
MERGE (e)-[:CLASSIFICADA_EM_PROGRAMA]->(p)
"""

# ── Transforms ────────────────────────────────────────────────────────────────

def _t_emendas(chunk: list[dict]) -> dict:
    parlamentares, funcoes, programas = {}, {}, {}
    emendas, autoriais = [], []
    dest_mun, dest_est, dest_funcao = [], [], []

    for r in chunk:
        cod_e   = r.get("Código da Emenda", "").strip()
        cod_aut = r.get("Código do Autor da Emenda", "").strip()
        cod_f   = r.get("Código Função", "").strip()
        cod_p   = r.get("Código Programa", "").strip()
        cod_ibge = r.get("Código Município IBGE", "").strip()
        uf       = r.get("UF", "").strip()

        if not cod_e:
            continue

        if cod_aut and cod_aut not in parlamentares:
            parlamentares[cod_aut] = {
                "codigo_autor": cod_aut,
                "nome_autor":   r.get("Nome do Autor da Emenda", "").strip(),
                **FONTE,
            }

        if cod_f and cod_f not in funcoes:
            funcoes[cod_f] = {
                "codigo_funcao":    cod_f,
                "nome_funcao":      r.get("Nome Função", "").strip(),
                "codigo_subfuncao": r.get("Código Subfunção", "").strip(),
                "nome_subfuncao":   r.get("Nome Subfunção", "").strip(),
            }

        if cod_p and cod_p not in programas:
            programas[cod_p] = {
                "codigo_programa": cod_p,
                "nome_programa":   r.get("Nome Programa", "").strip(),
            }

        emendas.append({
            "codigo_emenda":     cod_e,
            "ano_emenda":        r.get("Ano da Emenda", "").strip(),
            "tipo_emenda":       r.get("Tipo de Emenda", r.get("Tipo da Emenda", "")).strip(),
            "numero_emenda":     r.get("Número da Emenda", r.get("Número da emenda", "")).strip(),
            "localidade_gasto":  r.get("Localidade do Gasto",
                                       r.get("Localidade de aplicação do recurso", "")).strip(),
            "regiao":            r.get("Região", "").strip(),
            "valor_empenhado":   _safe_float(r.get("Valor Empenhado", "")),
            "valor_liquidado":   _safe_float(r.get("Valor Liquidado", "")),
            "valor_pago":        _safe_float(r.get("Valor Pago", "")),
            "valor_rp_inscrito": _safe_float(r.get("Valor Restos A Pagar Inscritos", "")),
            "valor_rp_cancelado":_safe_float(r.get("Valor Restos A Pagar Cancelados", "")),
            "valor_rp_pago":     _safe_float(r.get("Valor Restos A Pagar Pagos", "")),
            **FONTE,
        })

        if cod_aut:
            autoriais.append({"codigo_emenda": cod_e, "codigo_autor": cod_aut})

        if cod_ibge:
            dest_mun.append({"codigo_emenda": cod_e, "codigo_ibge": cod_ibge})
        elif uf:
            dest_est.append({"codigo_emenda": cod_e, "uf": uf})

        if cod_f:
            dest_funcao.append({"codigo_emenda": cod_e, "codigo_funcao": cod_f})

    return {
        "parlamentares": list(parlamentares.values()),
        "funcoes":       list(funcoes.values()),
        "programas":     list(programas.values()),
        "emendas":       emendas,
        "autoriais":     autoriais,
        "dest_mun":      dest_mun,
        "dest_est":      dest_est,
        "dest_funcao":   dest_funcao,
    }


def _t_convenios(chunk: list[dict]) -> list[dict]:
    rows = []
    for r in chunk:
        cod_e = r.get("Código da Emenda", "").strip()
        num_c = r.get("Número Convênio", r.get("Número do Convênio", "")).strip()
        if not cod_e or not num_c:
            continue
        rows.append({
            "codigo_emenda":   cod_e,
            "numero_convenio": num_c,
            "objeto":          r.get("Objeto Convênio", r.get("Objeto do Convênio", "")).strip(),
            "convenente":      r.get("Convenente", "").strip(),
            "valor_emenda":    _safe_float(r.get("Valor Convênio",
                                                  r.get("Valor Repassado", r.get("Valor", "0")))),
            "situacao":        r.get("Situação", "").strip(),
            **FONTE,
        })
    return rows


def _t_despesas(chunk: list[dict]) -> list[dict]:
    rows = []
    for r in chunk:
        cod_e = r.get("Código da Emenda", "").strip()
        if not cod_e:
            continue
        # Código do Favorecido: CNPJ (14 dígitos) para PJ, CPF para PF
        cod_fav      = "".join(c for c in r.get("Código do Favorecido", "") if c.isdigit())
        tipo_fav     = r.get("Tipo Favorecido", "").strip()
        nome_fav     = r.get("Favorecido", "").strip()
        # chave única por emenda+favorecido (uma linha por beneficiário por emenda por mês)
        cod_d        = f"{cod_e}_{cod_fav or nome_fav[:20]}"
        cnpj_basico  = cod_fav[:8].zfill(8) if len(cod_fav) >= 8 and tipo_fav == "Pessoa Jurídica" else ""
        rows.append({
            "codigo_emenda":   cod_e,
            "codigo_despesa":  cod_d,
            "valor_recebido":  _safe_float(r.get("Valor Recebido", "0")),
            "nome_favorecido": nome_fav,
            "tipo_favorecido": tipo_fav,
            "cnpj_favorecido": cnpj_basico,
            **FONTE,
        })
    return rows


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_emendas(driver, limite: int | None = None, stats: dict = None) -> None:
    path = DATA_DIR / "emendas.csv"
    parl_t = func_t = prog_t = em_t = 0
    if stats is None:
        stats = {'total': 0}

    with driver.session() as session:
        for chunk in iter_csv(path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [emendas] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]

            t = _t_emendas(chunk)
            if t["parlamentares"]:
                run_batches(session, Q_PARLAMENTAR, t["parlamentares"])
                parl_t += len(t["parlamentares"])
            if t["funcoes"]:
                run_batches(session, Q_FUNCAO, t["funcoes"])
                func_t += len(t["funcoes"])
            if t["programas"]:
                run_batches(session, Q_PROGRAMA, t["programas"])
                prog_t += len(t["programas"])
            if t["emendas"]:
                run_batches(session, Q_EMENDA, t["emendas"])
                em_t += len(t["emendas"])
                stats['total'] += len(t["emendas"])
            if t["autoriais"]:
                run_batches(session, Q_AUTORIA, t["autoriais"])
            if t["dest_mun"]:
                run_batches(session, Q_EMENDA_MUNICIPIO, t["dest_mun"])
                run_batches(session, Q_EMENDA_MUNICIPIO_RF, t["dest_mun"])
            if t["dest_est"]:
                run_batches(session, Q_EMENDA_ESTADO, t["dest_est"])
            if t["dest_funcao"]:
                run_batches(session, Q_EMENDA_FUNCAO, t["dest_funcao"])

    log.info(f"    ✓ emendas={em_t:,}  parlamentares={parl_t:,}  funções={func_t:,}")


def _load_convenios(driver, limite: int | None = None, stats: dict = None) -> None:
    path = DATA_DIR / "convenios.csv"
    total = 0
    if stats is None:
        stats = {'total': 0}
    batch_conv = min(BATCH, 500)
    with driver.session() as session:
        for chunk in iter_csv(path, chunk_size=10000):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [convenios] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]
            rows = _t_convenios(chunk)
            if not rows:
                continue
            run_batches(session, Q_CONVENIO_NOS, rows, batch=batch_conv)
            run_batches(session, Q_CONVENIO_REL, rows, batch=batch_conv)
            total += len(rows)
            stats['total'] += len(rows)
    log.info(f"    ✓ convênios={total:,}")


def _load_por_favorecido(driver, limite: int | None = None, stats: dict = None) -> None:
    path = DATA_DIR / "por_favorecido.csv"
    if not path.exists():
        path = DATA_DIR / "despesas.csv"
    total = 0
    if stats is None:
        stats = {'total': 0}
    batch_desp = min(BATCH, 500)
    with driver.session() as session:
        for chunk in iter_csv(path, chunk_size=10000):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [por_favorecido] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]
            rows = _t_despesas(chunk)
            if not rows:
                continue
            run_batches(session, Q_DESPESA_NOS, rows, batch=batch_desp)
            run_batches(session, Q_DESPESA_REL, rows, batch=batch_desp)
            # vincula empresa apenas para rows com cnpj
            com_cnpj = [r for r in rows if r.get("cnpj_favorecido")]
            if com_cnpj:
                run_batches(session, Q_DESPESA_EMPRESA, com_cnpj, batch=batch_desp)
            total += len(rows)
            stats['total'] += len(rows)
    log.info(f"    ✓ por_favorecido={total:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, limite: int | None = None):
    log.info(f"[emendas_cgu] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "emendas_cgu"):
        stats = {'total': 0}
        log.info("  [1/5] Emendas, parlamentares, funções...")
        _load_emendas(driver, limite, stats)
        if limite is not None and stats['total'] >= limite:
            log.info(f"  Limite de {limite:,} linhas atingido após emendas. Parando.")
            driver.close()
            return

        log.info("  [2/5] Convênios...")
        _load_convenios(driver, limite, stats)
        if limite is not None and stats['total'] >= limite:
            log.info(f"  Limite de {limite:,} linhas atingido após convênios. Parando.")
            driver.close()
            return

        log.info("  [3/5] Por favorecido...")
        _load_por_favorecido(driver, limite, stats)
        if limite is not None and stats['total'] >= limite:
            log.info(f"  Limite de {limite:,} linhas atingido após por_favorecido. Parando.")
            driver.close()
            return

        log.info("  [4/5] Emenda → BENEFICIOU → Empresa (agregado)...")
        with driver.session() as session:
            result = session.run(Q_EMENDA_BENEFICIOU)
            summary = result.consume()
            log.info(f"    ✓ {summary.counters.relationships_created:,} rels BENEFICIOU criadas  "
                     f"({summary.counters.properties_set:,} props)")

        log.info("  [5/5] Linkando Parlamentar → Pessoa (TSE)...")
        exact_map, cand_tokens = _build_nome_cpf_map(driver)
        if exact_map or cand_tokens:
            link_rows = []
            ambiguous = missed = 0
            with driver.session() as session:
                result = session.run(
                    "MATCH (p:Parlamentar) RETURN p.codigo_autor AS cod, p.nome_autor AS nome"
                )
                for rec in result:
                    cpf = _resolve_cpf(rec["nome"] or "", exact_map, cand_tokens)
                    if cpf:
                        link_rows.append({"codigo_autor": rec["cod"], "cpf": cpf})
                    else:
                        tokens = _norm_tokens(rec["nome"] or "")
                        if any(tokens & toks for toks, _ in cand_tokens):
                            ambiguous += 1
                        else:
                            missed += 1
            if link_rows:
                with driver.session() as session:
                    run_batches(session, Q_LINK_PARLAMENTAR_PESSOA, link_rows)
                log.info(
                    f"    ✓ {len(link_rows):,} parlamentares linkados  "
                    f"ambíguos={ambiguous}  sem_match={missed}"
                )
            else:
                log.info("    Nenhum parlamentar encontrado nos candidatos TSE")
        else:
            log.info("    Pulado — dados TSE não disponíveis")

    driver.close()
    log.info("[emendas_cgu] Pipeline concluído")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Emendas CGU — carrega emendas parlamentares no Neo4j")
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
        limite=args.limite,
    )