"""
Pipeline 11 - Câmara dos Deputados: Despesas CEAP → Neo4j

Nós criados/atualizados:
  (:Parlamentar {id_camara}) — criado/atualizado via ID da Câmara
  (:Despesa {despesa_id, tipo_despesa, valor_liquido, data_emissao, ano, mes,
              nome_fornecedor, partido, uf})

Relacionamentos:
  (:Parlamentar)-[:GASTOU]->(:Despesa)
  (:Empresa {cnpj})-[:FORNECEU]->(:Despesa) via CNPJ do fornecedor
"""

import logging
import os
from pathlib import Path

from pipeline.lib import (wait_for_neo4j, run_batches, iter_csv,
                        IngestionRun, setup_schema, strip_doc)

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "camara"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "50000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "2000"))

FONTE = {
    "fonte_nome": "Câmara dos Deputados",
    "fonte_url":  "https://dadosabertos.camara.leg.br",
}


# ── Constraints / índices ──────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Despesa) REQUIRE d.despesa_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Parlamentar) REQUIRE p.id_camara IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX despesa_ano         IF NOT EXISTS FOR (d:Despesa) ON (d.ano)",
    "CREATE INDEX despesa_mes         IF NOT EXISTS FOR (d:Despesa) ON (d.mes)",
    "CREATE INDEX despesa_tipo        IF NOT EXISTS FOR (d:Despesa) ON (d.tipo_despesa)",
    "CREATE INDEX parlamentar_id_camara IF NOT EXISTS FOR (p:Parlamentar) ON (p.id_camara)",
]


# ── Queries Cypher ─────────────────────────────────────────────────────

Q_PARLAMENTAR = """
UNWIND $rows AS r
MERGE (p:Parlamentar {id_camara: r.id_camara})
SET p.nome_parlamentar = r.nome_parlamentar,
    p.partido = r.partido,
    p.uf = r.uf,
    p.fonte_nome = r.fonte_nome
"""

Q_DESPESA = """
UNWIND $rows AS r
MATCH (p:Parlamentar {id_camara: r.id_camara})
MERGE (d:Despesa {despesa_id: r.despesa_id})
SET d.tipo_despesa    = r.tipo_despesa,
    d.valor_liquido    = toFloat(r.valor_liquido),
    d.data_emissao    = r.data_emissao,
    d.ano             = toInteger(r.ano),
    d.mes             = toInteger(r.mes),
    d.nome_fornecedor = r.nome_fornecedor,
    d.partido         = r.partido,
    d.uf              = r.uf,
    d.fonte_nome      = r.fonte_nome
MERGE (p)-[:GASTOU]->(d)
"""

Q_DESPESA_EMPRESA = """
UNWIND $rows AS r
MATCH (e:Empresa {cnpj: r.cnpj_fornecedor})
MATCH (d:Despesa {despesa_id: r.despesa_id})
MERGE (e)-[:FORNECEU]->(d)
"""


# ── Helpers ────────────────────────────────────────────────────────────

def _clean_cnpj(raw: str) -> str:
    """Remove pontuação e retorna CNPJ com 14 dígitos ou vazio."""
    digits = strip_doc(raw)
    if len(digits) == 14:
        return digits
    return ""


def _transform_chunk(chunk: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Separa registros em:
      - parlamentar_rows: para criar/atualizar Parlamentar e criar Despesa com GASTOU
      - empresa_rows: para criar relação FORNECEU (se tiver CNPJ válido)
    """
    parlamentar_rows: list[dict] = []
    empresa_rows: list[dict] = []

    for r in chunk:
        despesa_id      = r.get("despesa_id", "").strip()
        id_camara       = r.get("despesa_id", "").strip()  # usa despesa_id como id_camara
        cnpj_fornecedor = _clean_cnpj(r.get("cnpj_fornecedor", ""))
        valor           = r.get("valor_liquido", "0").replace(",", ".")
        ano            = r.get("ano", "").strip()
        mes            = r.get("mes", "").strip()

        if not despesa_id:
            continue

        base = {
            "despesa_id":       despesa_id,
            "id_camara":       id_camara,
            "tipo_despesa":    r.get("tipo_despesa", "").strip(),
            "valor_liquido":   valor,
            "data_emissao":   r.get("data_emissao", "").strip(),
            "ano":             ano,
            "mes":             mes,
            "nome_fornecedor": r.get("nome_fornecedor", "").strip(),
            "partido":         r.get("partido", "").strip(),
            "uf":              r.get("uf", "").strip(),
            "fonte_nome":      r.get("fonte_nome", FONTE["fonte_nome"]),
            "cnpj_fornecedor": cnpj_fornecedor,
            "nome_parlamentar": r.get("nome_parlamentar", "").strip(),
        }

        parlamentar_rows.append(base)

        if cnpj_fornecedor:
            empresa_rows.append(base)

    return parlamentar_rows, empresa_rows


def _load_despesas(driver) -> None:
    """Carrega todos os CSVs de despesas."""
    todos = sorted(DATA_DIR.glob("despesas_*.csv"))
    if not todos:
        log.warning("  Nenhum arquivo despesas_*.csv encontrado — execute download camara primeiro")
        return

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        parlamentar_t = empresa_t = skip_t = 0

        with driver.session() as session:
            # Primeiro passo: cria/atualiza Parlamentares
            for chunk in iter_csv(path):
                parlamentar_rows, empresa_rows = _transform_chunk(chunk)
                skip_t += len(chunk) - len(parlamentar_rows) - len(empresa_rows)

                if parlamentar_rows:
                    run_batches(session, Q_PARLAMENTAR, parlamentar_rows)
                    run_batches(session, Q_DESPESA, parlamentar_rows)
                    parlamentar_t += len(parlamentar_rows)

                if empresa_rows:
                    run_batches(session, Q_DESPESA_EMPRESA, empresa_rows)
                    empresa_t += len(empresa_rows)

        log.info(f"    ✓ {path.name}  Parlamentar={parlamentar_t:,}  Empresa={empresa_t:,}  sem_id={skip_t:,}")


# ── Entry-point ────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info(f"[camara] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "camara"):
        log.info("  [1/1] Parlamentar + Despesa → GASTOU, FORNECEU...")
        _load_despesas(driver)

    driver.close()
    log.info("[camara] Pipeline concluído")
