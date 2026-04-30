from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from deps import get_driver, close_driver
from observability import MetricsMiddleware, metrics_response
from routers import search, pessoa, empresa, parlamentar, graph, patterns, pipelines, contrato, sancao, emprestimo, despesa

@asynccontextmanager
async def lifespan(app: FastAPI):
    get_driver()   # inicializa pool na subida
    yield
    close_driver()

app = FastAPI(
    title="DABERTO API",
    description="Inteligência cívica sobre bases públicas brasileiras em grafo Neo4j",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(MetricsMiddleware)

app.include_router(search.router)
app.include_router(pessoa.router)
app.include_router(empresa.router)
app.include_router(parlamentar.router)
app.include_router(graph.router)
app.include_router(patterns.router)
app.include_router(pipelines.router)
app.include_router(contrato.router)
app.include_router(sancao.router)
app.include_router(despesa.router)
app.include_router(emprestimo.router)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return metrics_response()
