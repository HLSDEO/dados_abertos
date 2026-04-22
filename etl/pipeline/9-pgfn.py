"""
Pipeline 9 - PGFN: Dívida Ativa da União → Neo4j

Nós criados/atualizados:
  (:DividaAtiva {divida_id, tipo_credito, receita_principal, valor_consolidado,
                 situacao, situacao_juridica, data_inscricao, indicador_ajuizado,
                 uf_devedor, municipio_devedor})

Relacionamentos:
  (:Empresa)-[:POSSUI_DIVIDA]->(:DividaAtiva)   ← CNPJ (14 dígitos)
  (:Pessoa)-[:POSSUI_DIVIDA]->(:DividaAtiva)    ← CPF  (11 dígitos)

Nota: CPF vem mascarado (***) no arquivo — vincula apenas quando há dígitos suficientes.
"""

import logging
import os
import re
from pathlib import Path

from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "pgfn"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE",  "50000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "2000"))

FONTE = {
    "fonte_nome": "PGFN — Procuradoria-Geral da Fazenda Nacional",
    "fonte_url":  "https://portaldatransparencia.gov.br/download-de-dados/pgfn",
}

# ── Constraints / índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:DividaAtiva) REQUIRE n.divida_id IS UNIQUE",
]
Q_INDEXES = [
    "CREATE INDEX divida_situacao IF NOT EXISTS FOR (d:DividaAtiva) ON (d.situacao)",
    "CREATE INDEX divida_tipo     IF NOT EXISTS FOR (d:DividaAtiva) ON (d.tipo_credito)",
]

# ── Queries Cypher ────────────────────────────────────────────────────────────

Q_DIVIDA_EMPRESA = """
UNWIND $rows AS r
MERGE (d:DividaAtiva {divida_id: r.divida_id})
SET d.tipo_credito       = r.tipo_credito,
    d.receita_principal  = r.receita_principal,
    d.valor_consolidado  = toFloat(r.valor_consolidado),
    d.situacao           = r.situacao,
    d.situacao_juridica  = r.situacao_juridica,
    d.data_inscricao     = r.data_inscricao,
    d.indicador_ajuizado = r.indicador_ajuizado,
    d.uf_devedor         = r.uf_devedor,
    d.municipio_devedor  = r.municipio_devedor,
    d.nome_devedor       = r.nome_devedor,
    d.fonte_nome         = r.fonte_nome
WITH d, r
MERGE (emp:Empresa {cnpj_basico: r.cnpj_basico})
  ON CREATE SET emp.fonte_nome = r.fonte_nome
MERGE (emp)-[:POSSUI_DIVIDA]->(d)
"""

Q_DIVIDA_PESSOA = """
UNWIND $rows AS r
MERGE (d:DividaAtiva {divida_id: r.divida_id})
SET d.tipo_credito       = r.tipo_credito,
    d.receita_principal  = r.receita_principal,
    d.valor_consolidado  = toFloat(r.valor_consolidado),
    d.situacao           = r.situacao,
    d.situacao_juridica  = r.situacao_juridica,
    d.data_inscricao     = r.data_inscricao,
    d.indicador_ajuizado = r.indicador_ajuizado,
    d.uf_devedor         = r.uf_devedor,
    d.municipio_devedor  = r.municipio_devedor,
    d.nome_devedor       = r.nome_devedor,
    d.fonte_nome         = r.fonte_nome
WITH d, r
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome = r.nome_devedor, p.fonte_nome = r.fonte_nome
MERGE (p)-[:POSSUI_DIVIDA]->(d)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

_RE_DIGITS = re.compile(r"\d")


def _extract_doc(raw: str) -> tuple[str, str]:
    """
    Retorna (tipo, doc_limpo): tipo = 'pj' | 'pf' | ''.
    CPF vem mascarado: '***123456**' — usa dígitos disponíveis como heurística.
    CNPJ vem completo (14 dígitos).
    """
    digits = "".join(c for c in raw if c.isdigit())
    stars  = raw.count("*")
    total  = len(digits) + stars

    if total == 14 and stars == 0:
        return "pj", digits[:8]           # CNPJ → cnpj_basico (8 dígitos)
    if total == 11 and len(digits) >= 8:  # CPF com máscara parcial
        return "pf", digits.zfill(11)     # melhor esforço
    return "", ""


def _safe_float(s: str) -> str:
    """'1.234.567,89' → '1234567.89'"""
    try:
        return str(float(s.strip().replace(".", "").replace(",", ".")))
    except (ValueError, AttributeError):
        return "0"


def _t_dividas(chunk: list[dict]) -> tuple[list[dict], list[dict]]:
    """Separa registros em PJ (cnpj_basico) e PF (cpf)."""
    pj_rows: list[dict] = []
    pf_rows: list[dict] = []

    for r in chunk:
        tipo_doc, doc = _extract_doc(r.get("cpf_cnpj", ""))
        if not tipo_doc:
            continue

        num_inscricao = r.get("numero_inscricao", "").strip()
        if not num_inscricao:
            continue

        base = {
            "divida_id":          num_inscricao,
            "tipo_credito":       r.get("tipo_credito", "").strip(),
            "receita_principal":  r.get("receita_principal", "").strip(),
            "valor_consolidado":  _safe_float(r.get("valor_consolidado", "0")),
            "situacao":           r.get("situacao", "").strip(),
            "situacao_juridica":  r.get("situacao_juridica", "").strip(),
            "data_inscricao":     r.get("data_inscricao", "").strip(),
            "indicador_ajuizado": r.get("indicador_ajuizado", "").strip(),
            "uf_devedor":         r.get("uf_devedor", "").strip(),
            "municipio_devedor":  r.get("municipio_devedor", "").strip(),
            "nome_devedor":       r.get("nome_devedor", "").strip(),
            "fonte_nome":         r.get("fonte_nome", FONTE["fonte_nome"]),
        }

        if tipo_doc == "pj":
            pj_rows.append({**base, "cnpj_basico": doc})
        else:
            pf_rows.append({**base, "cpf": doc})

    return pj_rows, pf_rows


# ── Loader ────────────────────────────────────────────────────────────────────

def _load_dividas(driver) -> None:
    todos = sorted(DATA_DIR.glob("*.csv"))
    if not todos:
        log.warning("  Nenhum arquivo *.csv encontrado — execute download pgfn primeiro")
        return

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        pj_t = pf_t = skip_t = 0
        with driver.session() as session:
            for chunk in iter_csv(path):
                pj, pf = _t_dividas(chunk)
                skip_t += len(chunk) - len(pj) - len(pf)
                if pj:
                    run_batches(session, Q_DIVIDA_EMPRESA, pj)
                    pj_t += len(pj)
                if pf:
                    run_batches(session, Q_DIVIDA_PESSOA, pf)
                    pf_t += len(pf)
        log.info(f"    ✓ {path.name}  PJ={pj_t:,}  PF={pf_t:,}  sem_doc={skip_t:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info(f"[pgfn] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "pgfn"):
        log.info("  [1/1] Dívidas → DividaAtiva, POSSUI_DIVIDA...")
        _load_dividas(driver)

    driver.close()
    log.info("[pgfn] Pipeline concluído")
