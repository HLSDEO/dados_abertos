"""
Analytics 2 - Deduplicação probabilística de :Pessoa com Splink

Lê todos os nós :Pessoa do Neo4j, treina um modelo de linkage usando:
  - Jaro-Winkler no nome (thresholds 0.9 e 0.8)
  - Exact match em cpf
  - Exact match em dt_nascimento
  - Blocking rules: cpf exato OU nome exato

Cria relacionamentos:
  (:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)
  confianca: 'alta' (>=0.9) | 'media' (>=0.7) | 'baixa' (<0.7)

Threshold padrão para criar a relação: 0.8

Requisito extra (não incluído em requirements.txt para não impor a todos):
  pip install splink duckdb

Uso:
  python main.py analytics splink
  python main.py run --full   # download + pipeline + analytics (inclui splink se instalado)
"""

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

NEO4J_URI      = os.environ.get("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER     = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "changeme")

MATCH_THRESHOLD = float(os.environ.get("SPLINK_THRESHOLD", "0.8"))
MAX_PESSOAS     = int(os.environ.get("SPLINK_MAX_PESSOAS", "2000000"))
BATCH           = int(os.environ.get("NEO4J_BATCH", "500"))


# ── Configuração Splink ───────────────────────────────────────────────────────

def _get_splink_settings() -> dict:
    try:
        import splink.comparison_library as cl
        from splink import SettingsCreator
    except ImportError as exc:
        raise ImportError(
            "Splink não instalado. Execute: pip install splink duckdb\n"
            "Ou adicione ao requirements.txt e reconstrua a imagem Docker."
        ) from exc

    creator = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.JaroWinklerAtThresholds("nome", score_threshold_or_thresholds=[0.9, 0.8]),
            cl.ExactMatch("cpf"),
            cl.ExactMatch("dt_nascimento"),
        ],
        blocking_rules_to_generate_predictions=[
            "l.cpf = r.cpf",
            "l.nome = r.nome",
        ],
        retain_matching_columns=True,
        retain_intermediate_calculation_columns=False,
    )
    return creator.get_settings("duckdb")


def _classify_score(score: float) -> str:
    if score >= 0.9:
        return "alta"
    if score >= 0.7:
        return "media"
    return "baixa"


# ── Carga de dados do Neo4j ───────────────────────────────────────────────────

def _load_pessoas(driver) -> "pd.DataFrame":
    import pandas as pd
    log.info(f"  Carregando :Pessoa do Neo4j (limite {MAX_PESSOAS:,})...")
    with driver.session() as session:
        result = session.run(
            """
            MATCH (p:Pessoa)
            WHERE p.cpf IS NOT NULL AND p.nome IS NOT NULL
            RETURN p.cpf AS cpf, p.nome AS nome,
                   coalesce(p.dt_nascimento, '') AS dt_nascimento
            LIMIT $lim
            """,
            lim=MAX_PESSOAS,
        )
        rows = [dict(r) for r in result]
    df = pd.DataFrame(rows)
    log.info(f"  {len(df):,} pessoas carregadas  colunas={list(df.columns)}")
    return df


# ── Treinamento e predição ────────────────────────────────────────────────────

def _run_splink(df: "pd.DataFrame") -> "pd.DataFrame":
    try:
        import duckdb
        from splink import Linker
    except ImportError as exc:
        raise ImportError("pip install splink duckdb") from exc

    settings = _get_splink_settings()
    db_api = duckdb.connect()
    linker  = Linker(df, settings, db_api=db_api)

    log.info("  Estimando u-values (random sampling, max 1M pares)...")
    linker.training.estimate_u_using_random_sampling(max_pairs=1_000_000)

    log.info("  Estimando m-values (EM com bloqueio em cpf)...")
    linker.training.estimate_parameters_using_expectation_maximisation("l.cpf = r.cpf")

    log.info(f"  Predizendo pares (threshold={MATCH_THRESHOLD})...")
    results = linker.inference.predict(threshold_match_probability=MATCH_THRESHOLD)
    df_out: "pd.DataFrame" = results.as_pandas_dataframe()
    log.info(f"  {len(df_out):,} pares encontrados acima do threshold")
    return df_out


# ── Escrita no Neo4j ──────────────────────────────────────────────────────────

Q_MESMO_QUE = """
UNWIND $rows AS r
MATCH (a:Pessoa {cpf: r.cpf_l})
MATCH (b:Pessoa {cpf: r.cpf_r})
WHERE a <> b
MERGE (a)-[rel:MESMO_QUE]->(b)
SET rel.score      = r.score,
    rel.confianca  = r.confianca,
    rel.fonte      = 'splink'
"""


def _write_links(driver, df_pares: "pd.DataFrame") -> int:
    if df_pares.empty:
        return 0
    rows = [
        {
            "cpf_l":     row["cpf_l"],
            "cpf_r":     row["cpf_r"],
            "score":     float(row["match_probability"]),
            "confianca": _classify_score(float(row["match_probability"])),
        }
        for _, row in df_pares.iterrows()
        if row.get("cpf_l") and row.get("cpf_r")
    ]
    total = 0
    with driver.session() as session:
        for i in range(0, len(rows), BATCH):
            chunk = rows[i : i + BATCH]
            with session.begin_transaction() as tx:
                tx.run(Q_MESMO_QUE, rows=chunk)
                tx.commit()
            total += len(chunk)
    return total


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info(
        f"[splink] Deduplicação probabilística  "
        f"threshold={MATCH_THRESHOLD}  max_pessoas={MAX_PESSOAS:,}"
    )

    try:
        import splink  # noqa: F401
        import duckdb  # noqa: F401
    except ImportError:
        log.error(
            "[splink] Dependências ausentes. Execute:\n"
            "  pip install splink duckdb\n"
            "  docker compose build --no-cache etl  # se estiver em container"
        )
        return

    from pipeline.lib import wait_for_neo4j, IngestionRun
    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    with IngestionRun(driver, "splink") as run_ctx:
        df = _load_pessoas(driver)
        run_ctx.add(rows_in=len(df))

        if len(df) < 2:
            log.warning("  Menos de 2 pessoas no grafo — nada a deduplicar")
            return

        df_pares = _run_splink(df)
        n = _write_links(driver, df_pares)
        run_ctx.add(rows_out=n)
        log.info(f"  ✓ {n:,} relações MESMO_QUE criadas/atualizadas")

    driver.close()
    log.info("[splink] Concluído")
