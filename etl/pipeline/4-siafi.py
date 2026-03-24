"""
Pipeline 4 - SIAFI: Unidades Gestoras → Neo4j

Lê o arquivo criado manualmente:
  data/siafi/unidades.xlsx

Colunas:
  CD_UASG  SG_UASG  NO_UASG  ID_ORGAO  NO_ORGAO
  ID_ESFERA_ADMINISTRATIVA  NO_ESFERA_ADMINISTRATIVA

Nós criados/atualizados:
  (:UnidadeGestora) — CD_UASG como chave
  (:Orgao)          — ID_ORGAO como chave
  (:Esfera)         — ID_ESFERA_ADMINISTRATIVA como chave

Relacionamentos:
  (:UnidadeGestora)-[:PERTENCE_A]->(:Orgao)
  (:Orgao)-[:PERTENCE_A]->(:Esfera)

O arquivo é opcional — se não existir o pipeline pula sem erro.
"""

import logging
import os
from pathlib import Path

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data"))
XLSX_PATH  = DATA_DIR / "siafi" / "unidades.xlsx"
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "SIAFI — Sistema Integrado de Administração Financeira",
    "fonte_url":  "https://www.tesourotransparente.gov.br",
}


# ── Normalização de esfera administrativa ────────────────────────────────────
# O campo NO_ESFERA_ADMINISTRATIVA no SIAFI mistura esferas reais com
# classificações de porte de município e valores inválidos.
# Mapeamento: valor original → esfera canônica

ESFERA_MAP = {
    "FEDERAL":                "FEDERAL",
    "ESTADUAL":               "ESTADUAL",
    "MUNICIPIO CAPITAL":      "MUNICIPAL",
    "MUNICIPIO SIGNIFICATIVO": "MUNICIPAL",
    "DEMAIS MUNICIPIOS":      "MUNICIPAL",
    "UG-TV PORTAL-SICONV":    "MUNICIPAL",   # UGs de convênio, essencialmente municipal
    "CODIGO INVALIDO":        "NAO CLASSIFICADO",
    "NAO SE APLICA":          "NAO CLASSIFICADO",
    "SEM INFORMACAO":         "NAO CLASSIFICADO",
}

def _normalize_esfera(no_esfera: str) -> dict:
    """Retorna esfera canônica e subtipo original."""
    raw = (no_esfera or "").strip().upper()
    canonica = ESFERA_MAP.get(raw, "NAO CLASSIFICADO")
    return {
        "no_esfera":         canonica,
        "no_esfera_original": raw,
    }

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:UnidadeGestora) REQUIRE n.cd_uasg  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Orgao)          REQUIRE n.id_orgao IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Esfera)         REQUIRE n.id_esfera IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX uasg_nome   IF NOT EXISTS FOR (u:UnidadeGestora) ON (u.no_uasg)",
    "CREATE INDEX orgao_nome  IF NOT EXISTS FOR (o:Orgao)          ON (o.no_orgao)",
]


# ── Queries ───────────────────────────────────────────────────────────────────

Q_ESFERA = """
UNWIND $rows AS r
MERGE (e:Esfera {id_esfera: r.id_esfera})
SET e.no_esfera          = r.no_esfera,
    e.no_esfera_original = r.no_esfera_original,
    e.fonte_nome         = r.fonte_nome,
    e.fonte_url          = r.fonte_url
"""

Q_ORGAO = """
UNWIND $rows AS r
MERGE (o:Orgao {id_orgao: r.id_orgao})
SET o.no_orgao   = r.no_orgao,
    o.fonte_nome = r.fonte_nome,
    o.fonte_url  = r.fonte_url
WITH o, r
MERGE (e:Esfera {id_esfera: r.id_esfera})
MERGE (o)-[:PERTENCE_A]->(e)
"""

Q_UASG = """
UNWIND $rows AS r
MERGE (u:UnidadeGestora {cd_uasg: r.cd_uasg})
SET u.sg_uasg    = r.sg_uasg,
    u.no_uasg    = r.no_uasg,
    u.fonte_nome = r.fonte_nome,
    u.fonte_url  = r.fonte_url
WITH u, r
MERGE (o:Orgao {id_orgao: r.id_orgao})
MERGE (u)-[:PERTENCE_A]->(o)
"""

# Liga órgãos estaduais → nó :Estado já existente (carregado pelo IBGE/TSE)
# NO_ORGAO = "ESTADO DE GOIAS" → busca :Estado {sigla: "GO"} ou pelo nome
Q_ORGAO_ESTADO = """
UNWIND $rows AS r
MATCH (o:Orgao {id_orgao: r.id_orgao})
MATCH (est:Estado)
WHERE toUpper(trim(est.nome)) = r.nome_estado
   OR toUpper(trim(est.sigla)) = r.sigla_estado
MERGE (o)-[:LOCALIZADO_EM]->(est)
"""

# Liga UASGs municipais → nó :Municipio já existente (carregado pelo IBGE)
# NO_UASG = "DIADEMA" → busca :Municipio pelo nome
Q_UASG_MUNICIPIO = """
UNWIND $rows AS r
MATCH (u:UnidadeGestora {cd_uasg: r.cd_uasg})
MATCH (m:Municipio)
WHERE toUpper(trim(m.nome)) = r.nome_municipio
MERGE (u)-[:LOCALIZADO_EM]->(m)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_batches(session, query: str, rows: list[dict],
                 retries: int = 5) -> None:
    import time
    from neo4j.exceptions import TransientError
    for i in range(0, len(rows), BATCH):
        batch = rows[i : i + BATCH]
        for attempt in range(1, retries + 1):
            try:
                session.run(query, rows=batch)
                break
            except TransientError as exc:
                if "DeadlockDetected" in str(exc) and attempt < retries:
                    time.sleep(attempt * 0.5)
                else:
                    raise


def _wait_for_neo4j(uri, user, password, retries=20, delay=5.0):
    import time
    from neo4j.exceptions import ServiceUnavailable
    driver = GraphDatabase.driver(uri, auth=(user, password))
    for attempt in range(1, retries + 1):
        try:
            with driver.session() as s:
                s.run("RETURN 1")
            return driver
        except ServiceUnavailable:
            log.warning(f"  Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Neo4j não disponível")


def _extract_estado(no_orgao: str, no_esfera: str) -> str:
    """
    Extrai o nome do estado a partir de NO_ORGAO quando esfera = ESTADUAL.
    Ex: "ESTADO DE GOIAS" → "GOIAS"
        "ESTADO DE SAO PAULO" → "SAO PAULO"
    """
    if no_esfera != "ESTADUAL":
        return ""
    s = no_orgao.strip().upper()
    if s.startswith("ESTADO DE "):
        return s[len("ESTADO DE "):]
    if s.startswith("ESTADO DO "):
        return s[len("ESTADO DO "):]
    if s.startswith("ESTADO DA "):
        return s[len("ESTADO DA "):]
    return s


def _extract_municipio(no_uasg: str, no_esfera: str) -> str:
    """
    Extrai o nome do município a partir de NO_UASG quando esfera = MUNICIPAL.
    O campo já vem com o nome direto: "DIADEMA", "DORMENTES", etc.
    """
    if no_esfera != "MUNICIPAL":
        return ""
    return no_uasg.strip().upper()


def _read_xlsx(path: Path) -> list[dict]:
    """Lê o XLSX com pandas, normaliza colunas e retorna lista de dicts."""
    try:
        import pandas as pd
    except ImportError:
        log.error("  pandas não instalado — rode: pip install pandas openpyxl")
        return []

    df = pd.read_excel(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df = df.fillna("")

    col_map = {
        "CD_UASG":                    "cd_uasg",
        "SG_UASG":                    "sg_uasg",
        "NO_UASG":                    "no_uasg",
        "ID_ORGAO":                   "id_orgao",
        "NO_ORGAO":                   "no_orgao",
        "ID_ESFERA_ADMINISTRATIVA":   "id_esfera",
        "NO_ESFERA_ADMINISTRATIVA":   "no_esfera",
    }

    # aceita variações de nome com espaços extras ou case diferente
    rename = {}
    for col in df.columns:
        col_clean = col.strip().upper()
        for src, dest in col_map.items():
            if col_clean == src:
                rename[col] = dest
                break

    df = df.rename(columns=rename)

    # verifica colunas obrigatórias
    required = {"cd_uasg", "no_uasg", "id_orgao", "no_orgao"}
    missing  = required - set(df.columns)
    if missing:
        log.error(f"  Colunas ausentes no XLSX: {missing}")
        log.error(f"  Colunas encontradas: {list(df.columns)}")
        return []

    # adiciona metadados de fonte
    for k, v in FONTE.items():
        df[k] = v

    # normaliza cd_uasg e id_orgao como string sem zeros à esquerda removidos
    for col in ("cd_uasg", "id_orgao", "id_esfera"):
        if col in df.columns:
            df[col] = df[col].str.strip()

    # normaliza esfera administrativa
    rows = df.to_dict("records")
    for row in rows:
        esfera_norm = _normalize_esfera(row.get("no_esfera", ""))
        row.update(esfera_norm)
        # extrai nome de estado (para órgãos estaduais)
        row["nome_estado"]    = _extract_estado(row.get("no_orgao", ""), row["no_esfera"])
        row["sigla_estado"]   = ""   # preenchido só se vier no formato "ESTADO DE XX"
        # extrai nome de município (para UASGs municipais)
        row["nome_municipio"] = _extract_municipio(row.get("no_uasg", ""), row["no_esfera"])

    # loga distribuição de esferas para conferência
    from collections import Counter
    dist = Counter(r["no_esfera"] for r in rows)
    log.info(f"  Distribuição de esferas: {dict(dist)}")

    return rows


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info("[siafi] Pipeline unidades gestoras")

    if not XLSX_PATH.exists():
        log.warning(f"  {XLSX_PATH} não encontrado — pulando pipeline siafi")
        log.warning("  Crie manualmente: data/siafi/unidades.xlsx")
        return

    rows = _read_xlsx(XLSX_PATH)
    if not rows:
        return

    driver = _wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    # ordem: Esfera → Orgao → UnidadeGestora → vínculos geográficos
    log.info("  [1/5] Esferas administrativas...")
    with driver.session() as session:
        _run_batches(session, Q_ESFERA, rows)
    log.info(f"    ✓ {len(set(r.get('id_esfera','') for r in rows))} esferas")

    log.info("  [2/5] Órgãos...")
    with driver.session() as session:
        _run_batches(session, Q_ORGAO, rows)
    log.info(f"    ✓ {len(set(r.get('id_orgao','') for r in rows))} órgãos")

    log.info("  [3/5] Unidades gestoras...")
    with driver.session() as session:
        _run_batches(session, Q_UASG, rows)
    log.info(f"    ✓ {len(rows):,} unidades gestoras")

    # vínculos geográficos — só para linhas com nome extraído
    rows_estaduais = [r for r in rows if r.get("nome_estado")]
    if rows_estaduais:
        log.info(f"  [4/5] Vinculando {len(rows_estaduais):,} órgãos → :Estado...")
        with driver.session() as session:
            _run_batches(session, Q_ORGAO_ESTADO, rows_estaduais)
    else:
        log.info("  [4/5] Nenhum órgão estadual para vincular")

    rows_municipais = [r for r in rows if r.get("nome_municipio")]
    if rows_municipais:
        log.info(f"  [5/5] Vinculando {len(rows_municipais):,} UASGs → :Municipio...")
        with driver.session() as session:
            _run_batches(session, Q_UASG_MUNICIPIO, rows_municipais)
    else:
        log.info("  [5/5] Nenhuma UASG municipal para vincular")

    driver.close()
    log.info("[siafi] Pipeline concluído")