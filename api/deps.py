import os
from neo4j import GraphDatabase, Driver

_driver: Driver | None = None


def get_driver() -> Driver:
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
            auth=(
                os.environ.get("NEO4J_USER", "neo4j"),
                os.environ.get("NEO4J_PASSWORD", "changeme"),
            ),
            max_connection_pool_size=20,
        )
    return _driver


def close_driver():
    global _driver
    if _driver:
        _driver.close()
        _driver = None
