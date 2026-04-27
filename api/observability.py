import os
import time

from fastapi import Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware

REQUEST_COUNT = Counter(
    "daberto_http_requests_total",
    "Total de requests HTTP",
    ["method", "path", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "daberto_http_request_duration_seconds",
    "Latencia HTTP por endpoint",
    ["method", "path"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

REQUEST_EXCEPTIONS = Counter(
    "daberto_http_exceptions_total",
    "Total de excecoes em requests HTTP",
    ["method", "path", "exception_type"],
)

NEO4J_QUERY_COUNT = Counter(
    "daberto_neo4j_queries_total",
    "Total de queries Neo4j executadas",
    ["status"],
)

NEO4J_QUERY_LATENCY = Histogram(
    "daberto_neo4j_query_duration_seconds",
    "Latencia de queries Neo4j",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

NEO4J_SLOW_QUERY_COUNT = Counter(
    "daberto_neo4j_slow_queries_total",
    "Total de queries Neo4j consideradas lentas",
)

SLOW_QUERY_THRESHOLD_SECONDS = float(os.environ.get("SLOW_QUERY_THRESHOLD_SECONDS", "2.0"))


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path == "/metrics":
            return await call_next(request)

        method = request.method
        start = time.perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as exc:
            REQUEST_EXCEPTIONS.labels(
                method=method,
                path=path,
                exception_type=exc.__class__.__name__,
            ).inc()
            raise
        finally:
            elapsed = time.perf_counter() - start
            REQUEST_COUNT.labels(
                method=method,
                path=path,
                status_code=str(status_code),
            ).inc()
            REQUEST_LATENCY.labels(method=method, path=path).observe(elapsed)


def metrics_response() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
