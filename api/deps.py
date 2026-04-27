import os
from neo4j import GraphDatabase, Driver

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
    return session.run(query, timeout=_QUERY_TIMEOUT_SECONDS, **params)


def close_driver():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
