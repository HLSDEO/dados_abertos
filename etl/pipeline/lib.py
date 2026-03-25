"""
etl/pipeline/lib.py — Utilitários compartilhados entre pipelines

Centraliza funções duplicadas em todos os 7 pipelines:
  - _wait_for_neo4j()
  - _run_batches()
  - _iter_csv()
"""

import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "20000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))


def wait_for_neo4j(uri: str, user: str, password: str,
                   retries: int = 20, delay: float = 5.0,
                   max_pool: int = 8):
    """Cria driver Neo4j e aguarda Bolt ficar disponível."""
    import time
    from neo4j.exceptions import ServiceUnavailable
    driver = GraphDatabase.driver(uri, auth=(user, password),
                                  max_connection_pool_size=max_pool)
    for attempt in range(1, retries + 1):
        try:
            with driver.session() as s:
                s.run("RETURN 1")
            log.info(f"  Neo4j pronto (tentativa {attempt})")
            return driver
        except ServiceUnavailable:
            log.warning(f"  Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError(f"Neo4j não ficou disponível após {retries} tentativas")


def run_batches(session, query: str, rows: list[dict],
                extra_params: dict | None = None,
                batch: int = BATCH,
                retries: int = 5) -> None:
    """Executa query em batches com retry automático em DeadlockDetected."""
    import time
    from neo4j.exceptions import TransientError
    params = extra_params or {}
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        for attempt in range(1, retries + 1):
            try:
                with session.begin_transaction() as tx:
                    tx.run(query, rows=chunk, **params)
                    tx.commit()
                break
            except TransientError as exc:
                if "DeadlockDetected" in str(exc) and attempt < retries:
                    wait = attempt * 0.5
                    log.warning(f"    Deadlock — retry {attempt}/{retries} em {wait}s")
                    time.sleep(wait)
                else:
                    raise


def iter_csv(path: Path, chunk_size: int = CHUNK_SIZE,
             encoding: str = "utf-8-sig", delimiter: str = ","):
    """
    Lê CSV em chunks sem carregar tudo em memória.
    Auto-detecta delimitador se delimiter='auto'.
    """
    if not path.exists():
        log.warning(f"  CSV ausente: {path.name} — pulando")
        return
    total = 0
    with open(path, encoding=encoding, newline="") as f:
        # auto-detecção de delimitador
        if delimiter == "auto":
            sample = f.read(4096)
            f.seek(0)
            delimiter = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        chunk: list[dict] = []
        for row in reader:
            row = {k: (v or "").strip() for k, v in row.items()
                   if k is not None}
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                total += len(chunk)
                chunk = []
        if chunk:
            total += len(chunk)
            yield chunk
    log.info(f"    {path.name}: {total:,} linhas lidas")