from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from deps import get_driver, close_driver
from routers import search, pessoa, empresa, parlamentar, graph, patterns


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

app.include_router(search.router)
app.include_router(pessoa.router)
app.include_router(empresa.router)
app.include_router(parlamentar.router)
app.include_router(graph.router)
app.include_router(patterns.router)


@app.get("/health")
def health():
    return {"status": "ok"}
