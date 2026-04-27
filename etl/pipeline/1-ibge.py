"""
Pipeline 1 - IBGE Localidades
Lê os CSVs brutos de data/ibge/ e sobe os nós/relações no Neo4j.
As propriedades de fonte (fonte_nome, fonte_url, fonte_coletado_em, ...)
são gravadas em cada nó para rastreabilidade completa.
"""

import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "ibge"


# ── helpers ───────────────────────────────────────────────────────────────────

def _read_csv(name: str) -> list[dict]:
    path = DATA_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"CSV não encontrado: {path}\n"
            "Execute primeiro: etl/main.py download ibge"
        )
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    log.info(f"  Lido {path.name}  ({len(rows):,} registros)")
    return rows


def _run_batch(session, query: str, rows: list[dict], batch: int = 500):
    for i in range(0, len(rows), batch):
        session.run(query, rows=rows[i : i + batch])


# ── queries Cypher ────────────────────────────────────────────────────────────
# Todas as queries gravam as propriedades de fonte em cada nó:
#   fonte_nome, fonte_descricao, fonte_licenca, fonte_url, fonte_coletado_em

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Regiao)        REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Estado)        REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Mesorregiao)   REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Microrregiao)  REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Municipio)     REQUIRE n.id IS UNIQUE",
]

Q_REGIOES = """
UNWIND $rows AS r
MERGE (n:Regiao {id: toInteger(r.id)})
SET n.sigla              = r.sigla,
    n.nome               = r.nome,
    n.fonte_nome         = r.fonte_nome,
    n.fonte_descricao    = r.fonte_descricao,
    n.fonte_licenca      = r.fonte_licenca,
    n.fonte_url          = r.fonte_url,
    n.fonte_coletado_em  = r.fonte_coletado_em
"""

Q_ESTADOS = """
UNWIND $rows AS r
MERGE (e:Estado {id: toInteger(r.id)})
SET e.sigla              = r.sigla,
    e.nome               = r.nome,
    e.fonte_nome         = r.fonte_nome,
    e.fonte_descricao    = r.fonte_descricao,
    e.fonte_licenca      = r.fonte_licenca,
    e.fonte_url          = r.fonte_url,
    e.fonte_coletado_em  = r.fonte_coletado_em
WITH e, r
MATCH (reg:Regiao {id: toInteger(r.regiao_id)})
MERGE (e)-[:PERTENCE_A]->(reg)
"""

Q_MESORREGIOES = """
UNWIND $rows AS r
MERGE (m:Mesorregiao {id: toInteger(r.id)})
SET m.nome               = r.nome,
    m.fonte_nome         = r.fonte_nome,
    m.fonte_descricao    = r.fonte_descricao,
    m.fonte_licenca      = r.fonte_licenca,
    m.fonte_url          = r.fonte_url,
    m.fonte_coletado_em  = r.fonte_coletado_em
WITH m, r
MATCH (e:Estado {id: toInteger(r.UF_id)})
MERGE (m)-[:PERTENCE_A]->(e)
"""

Q_MICRORREGIOES = """
UNWIND $rows AS r
MERGE (mi:Microrregiao {id: toInteger(r.id)})
SET mi.nome              = r.nome,
    mi.fonte_nome        = r.fonte_nome,
    mi.fonte_descricao   = r.fonte_descricao,
    mi.fonte_licenca     = r.fonte_licenca,
    mi.fonte_url         = r.fonte_url,
    mi.fonte_coletado_em = r.fonte_coletado_em
WITH mi, r
MATCH (m:Mesorregiao {id: toInteger(r.mesorregiao_id)})
MERGE (mi)-[:PERTENCE_A]->(m)
"""

Q_MUNICIPIOS = """
UNWIND $rows AS r
MERGE (c:Municipio {id: toInteger(r.id)})
SET c.nome               = r.nome,
    c.fonte_nome         = r.fonte_nome,
    c.fonte_descricao    = r.fonte_descricao,
    c.fonte_licenca      = r.fonte_licenca,
    c.fonte_url          = r.fonte_url,
    c.fonte_coletado_em  = r.fonte_coletado_em
WITH c, r
MATCH (mi:Microrregiao {id: toInteger(r.microrregiao_id)})
MERGE (c)-[:PERTENCE_A]->(mi)
"""


# ── entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, limite: int | None = None):
    log.info("[ibge] Iniciando pipeline → Neo4j")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    setup_schema(driver)

    with IngestionRun(driver, "ibge") as run_ctx:
        with driver.session() as session:
            log.info("  Criando constraints...")
            for q in Q_CONSTRAINTS:
                session.run(q)

        steps = [
            ("regioes",       Q_REGIOES),
            ("estados",       Q_ESTADOS),
            ("mesorregioes",  Q_MESORREGIOES),
            ("microrregioes", Q_MICRORREGIOES),
            ("municipios",    Q_MUNICIPIOS),
        ]

        total_global = 0
        for name, query in steps:
            rows = _read_csv(name)
            if limite is not None:
                restante = limite - total_global
                if restante <= 0:
                    log.info(f"    Limite de {limite:,} linhas atingido. Parando.")
                    break
                if len(rows) > restante:
                    rows = rows[:restante]
                    log.info(f"    Limitando {name} para {restante:,} linhas (de {len(rows):,})")

            log.info(f"  Carregando {name}...")
            with driver.session() as session:
                _run_batch(session, query, rows)
            total_global += len(rows)
            run_ctx.add(len(rows))

            if limite is not None and total_global >= limite:
                log.info(f"    Limite de {limite:,} linhas atingido após {name}. Parando.")
                break

    driver.close()
    log.info("[ibge] Pipeline concluído")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline IBGE — carrega localidades no Neo4j")
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