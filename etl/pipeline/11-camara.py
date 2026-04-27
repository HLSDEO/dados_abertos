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


def _load_despesas(
    driver,
    limite: int | None = None,
    stats: dict = None,
    anos: list[int] | None = None,
) -> None:
    """Carrega CSVs de despesas (todos ou apenas anos selecionados)."""
    if anos:
        todos = [DATA_DIR / f"despesas_{int(ano)}.csv" for ano in sorted(set(anos))]
    else:
        todos = sorted(DATA_DIR.glob("despesas_*.csv"))

    todos = [p for p in todos if p.exists()]
    if not todos:
        if anos:
            anos_str = ", ".join(str(a) for a in sorted(set(anos)))
            log.warning(
                f"  Nenhum arquivo despesas_<ano>.csv encontrado para ano(s): {anos_str} "
                "— execute download camara primeiro"
            )
        else:
            log.warning("  Nenhum arquivo despesas_*.csv encontrado — execute download camara primeiro")
        return
    if stats is None:
        stats = {'total': 0}

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        parlamentar_t = empresa_t = skip_t = 0

        with driver.session() as session:
            for chunk in iter_csv(path):
                if limite is not None and stats['total'] >= limite:
                    log.info(f"    [despesas] Limite de {limite:,} atingido. Parando.")
                    return
                if limite is not None:
                    restante = limite - stats['total']
                    if restante <= 0:
                        return
                    if len(chunk) > restante:
                        chunk = chunk[:restante]

                parlamentar_rows, empresa_rows = _transform_chunk(chunk)
                skip_t += len(chunk) - len(parlamentar_rows) - len(empresa_rows)

                if parlamentar_rows:
                    run_batches(session, Q_PARLAMENTAR, parlamentar_rows)
                    run_batches(session, Q_DESPESA, parlamentar_rows)
                    parlamentar_t += len(parlamentar_rows)
                    stats['total'] += len(parlamentar_rows)

                if empresa_rows:
                    run_batches(session, Q_DESPESA_EMPRESA, empresa_rows)
                    empresa_t += len(empresa_rows)
                    # Não contar empresas no limite global (só despesas)

        log.info(f"    ✓ {path.name}  Parlamentar={parlamentar_t:,}  Empresa={empresa_t:,}  sem_id={skip_t:,}")


# ── Entry-point ────────────────────────────────────────────────────────

def run(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    limite: int | None = None,
    anos: list[int] | None = None,
):
    log.info(f"[camara] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")
    if anos:
        log.info(f"  Filtrando por ano(s): {sorted(set(anos))}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "camara"):
        log.info("  [1/1] Parlamentar + Despesa → GASTOU, FORNECEU...")
        stats = {'total': 0}
        _load_despesas(driver, limite, stats, anos)

    driver.close()
    log.info("[camara] Pipeline concluído")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Câmara — carrega despesas CEAP no Neo4j")
    parser.add_argument("--limite", type=int, default=None, help="Número máximo de linhas a inserir (carga parcial)")
    parser.add_argument("--ano", type=int, action="append", default=None, help="Ano(s) dos CSVs a carregar (repetível)")
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
        anos=args.ano,
    )
