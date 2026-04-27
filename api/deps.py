import os
import time
from neo4j import GraphDatabase, Driver

from observability import (
    NEO4J_QUERY_COUNT,
    NEO4J_QUERY_LATENCY,
    NEO4J_SLOW_QUERY_COUNT,
    SLOW_QUERY_THRESHOLD_SECONDS,
)

_driver: Driver | None = None
_QUERY_TIMEOUT_SECONDS = int(os.environ.get("NEO4J_QUERY_TIMEOUT_SECONDS", "120"))
_POOL_SIZE = int(os.environ.get("NEO4J_MAX_CONNECTION_POOL_SIZE", "20"))


def get_driver() -> Driver:
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
            auth=(
                os.environ.get("NEO4J_USER", "neo4j"),
                os.environ.get("NEO4J_PASSWORD", "changeme"),
            ),
            max_connection_pool_size=_POOL_SIZE,
            connection_timeout=30,
        )
    return _driver


def run_query(session, query: str, **params):
    start = time.perf_counter()
    try:
        result = session.run(query, timeout=_QUERY_TIMEOUT_SECONDS, **params)
        elapsed = time.perf_counter() - start
        NEO4J_QUERY_COUNT.labels(status="ok").inc()
        NEO4J_QUERY_LATENCY.observe(elapsed)
        if elapsed >= SLOW_QUERY_THRESHOLD_SECONDS:
            NEO4J_SLOW_QUERY_COUNT.inc()
        return result
    except Exception:
        elapsed = time.perf_counter() - start
        NEO4J_QUERY_COUNT.labels(status="error").inc()
        NEO4J_QUERY_LATENCY.observe(elapsed)
        if elapsed >= SLOW_QUERY_THRESHOLD_SECONDS:
            NEO4J_SLOW_QUERY_COUNT.inc()
        raise


def close_driver():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
