# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Visão Geral

DABERTO é uma infraestrutura open-source que cruza bases públicas brasileiras em um grafo Neo4j para gerar inteligência cívica. Stack: **Neo4j 5 + GDS** (grafo), **FastAPI** (API), **Redis 7** (cache), **Nginx** (frontend), **Python 3.12 ETL** (pandas/requests/neo4j-driver), orquestrado via **Docker Compose**.

## Estrutura do Projeto

```
dados_abertos/
  api/           — FastAPI: main.py, deps.py, routers/, cache.py, observability.py
  etl/           — orquestrador main.py + download/ + pipeline/ + analytics/
  etl/data/      — dados baixados (gitignored, exceto .keep)
  neo4j/         — Dockerfile customizado (instala GDS) + conf/
  frontend/      — interface web estática: index.html, nginx.conf, assets/, Dockerfile
```

## Comandos Principais

```bash
# Subir todos os serviços (Neo4j + API + Redis + Frontend)
docker compose up -d

# URLs
# Frontend:       http://localhost:8080
# API (Swagger):  http://localhost:8000/docs
# Métricas:       http://localhost:8000/metrics
# Neo4j Browser:  http://localhost:7474 (neo4j/changeme)
# Redis:          redis://localhost:6379

# Perfis de ambiente
cp .env.dev  .env   # desenvolvimento (8 cores / 24 GB RAM)
cp .env.prod .env   # produção (24 cores / 120 GB RAM)

# ETL
docker compose run --rm etl                                  # completo
docker compose run --rm etl download cnpj
docker compose run --rm etl pipeline emendas_cgu
docker compose run --rm etl download cnpj --chunk 100000 --workers 4
docker compose run --rm etl download tse --eleicao 2024 --eleicao 2022
docker compose run --rm etl pipeline cnpj --history
docker compose run --rm etl pipeline camara --ano 2024 --ano 2025
docker compose run --rm etl run cnpj --full                  # download + pipeline + analytics
docker compose run --rm etl analytics gds
docker compose run --rm etl analytics splink
docker compose run --rm etl schema
docker compose run --rm etl ingestion-status

# Rebuild
docker compose build --no-cache etl
docker compose build --no-cache api
docker compose build --no-cache frontend
```

## Arquitetura API (`api/`)

| Arquivo | Responsabilidade |
|---|---|
| `main.py` | App FastAPI, CORS, lifespan, middleware de métricas e endpoint `/metrics` |
| `deps.py` | Singleton do driver Neo4j (pool configurável) + timeout por query + métricas de query |
| `cache.py` | Cache Redis com fallback seguro + key builder/hash + TTL |
| `observability.py` | Métricas Prometheus HTTP e Neo4j (`Counter`/`Histogram`) |
| `routers/search.py` | `GET /search?q=` — fulltext com paginação (`limit`/`offset`) + cache Redis |
| `routers/pessoa.py` | `GET /pessoa/{cpf}` — perfil com paginação (`limit`/`offset`) + cache Redis |
| `routers/empresa.py` | `GET /empresa/{cnpj_basico}` — perfil com paginação (`limit`/`offset`) + cache Redis |
| `routers/parlamentar.py` | `GET /parlamentar/{id}` — perfil com paginação (`limit`/`offset`) |
| `routers/graph.py` | `GET /graph/expand?label=&id=&hops=` — subgrafo com paginação (`offset`) |
| `routers/patterns.py` | `GET /patterns/empresa/{cnpj}` e `GET /patterns/estado/{uf}` — padrões de irregularidade |

O endpoint `/graph/expand` retorna `{nodes, edges}` com `uid` no formato `Label:valor_chave` — pronto para Sigma.js/Cytoscape.js.

### Paginação

Todos os endpoints de leitura aceitam `limit` (padrão `20`) e `offset` (padrão `0`). Aplicar sempre nos parâmetros de query Cypher com `SKIP $offset LIMIT $limit` para evitar varreduras completas.

### Cache Redis (`cache.py`)

- `get_cached(key)` / `set_cached(key, value, ttl)` com fallback silencioso.
- Chave construída via `build_cache_key(prefix, **params)` → SHA-256[:16] dos params.
- `CACHE_ENABLED=0` desabilita completamente sem alterar código dos routers.
- Endpoints com cache: `search`, `pessoa`, `empresa`.

### Observabilidade (`observability.py`)

Métricas Prometheus expostas em `GET /metrics`:

| Métrica | Tipo | Label(s) |
|---|---|---|
| `http_requests_total` | Counter | `method`, `endpoint`, `status` |
| `http_request_duration_seconds` | Histogram | `endpoint` |
| `http_exceptions_total` | Counter | `endpoint`, `exception` |
| `neo4j_query_duration_seconds` | Histogram | `router` |
| `neo4j_queries_total` | Counter | `router`, `status` (`ok`/`error`) |
| `neo4j_slow_queries_total` | Counter | `router` |

Limiar de query lenta: `SLOW_QUERY_THRESHOLD_SECONDS` (padrão `2.0`).

## Arquitetura Frontend (`frontend/`)

| Arquivo | Responsabilidade |
|---|---|
| `index.html` | Página principal: busca, perfis, visualização de grafo |
| `nginx.conf` | Proxy `/api/` → `api:8000`; serve estáticos em `/usr/share/nginx/html` |
| `Dockerfile` | `nginx:alpine` com injeção de `nginx.conf` e arquivos estáticos |
| `assets/` | CSS, JavaScript (Sigma.js/Cytoscape.js), ícones |

O frontend consome diretamente os endpoints da API. O proxy Nginx elimina CORS em produção:
```nginx
location /api/ {
    proxy_pass http://api:8000/;
}
```

Nós do grafo usam `uid` no formato `Label:chave`, compatível nativamente com Sigma.js e Cytoscape.js. A paginação de expansão de grafo usa o parâmetro `offset` para carregamento incremental de vizinhos.

## Arquitetura ETL (`etl/`)

### Fluxo de dados
```
Fontes públicas (HTTP) → download/*.py → etl/data/<base>/
                                                ↓
                         pipeline/*.py → Neo4j (grafo)
                                                ↓
               analytics/1-gds.py   → GDS (Louvain, PageRank, Betweenness)
               analytics/2-splink.py → dedup probabilístico (:MESMO_QUE)
```

### Orquestrador: `etl/main.py`
Carrega módulos dinamicamente via `importlib`. Os registros `DOWNLOADS`, `PIPELINES` e `ANALYTICS` mapeiam nome de base → arquivo. Cada módulo expõe `run(**kwargs)` — kwargs aceitos detectados por `inspect.signature`.

Comandos: `download | pipeline | analytics | run | schema | ingestion-status`

### `etl/pipeline/lib.py` — utilitários centrais

| Função / Classe | Descrição |
|---|---|
| `wait_for_neo4j()` | Cria driver e aguarda Bolt disponível (retry) |
| `run_batches()` | UNWIND em lotes com retry automático em deadlock |
| `iter_csv()` | Lê CSV em chunks sem carregar em memória; auto-detecta delimitador |
| `setup_schema()` | Cria constraints, índices e fulltext index (`entidade_busca`) de forma idempotente |
| `IngestionRun` | Context manager de auditoria — grava nó `:IngestionRun` com status, timestamps e contagens. Use `run_ctx.add(rows_in=X, rows_out=Y)` para reportar progresso. |
| `classify_doc()` | Classifica CPF/CNPJ: `cpf_valid` \| `cpf_partial` \| `cnpj_valid` \| `invalid` |
| `make_partner_id()` | SHA-256[:16] de `nome\|doc_digits\|doc_raw\|tipo\|fonte` para identidades parciais |

### Padrões de pipeline
- Cada pipeline chama `setup_schema(driver)` antes de inserir dados.
- Todo `run()` é envolvido em `with IngestionRun(driver, "source_id") as run_ctx:`.
- **Auditoria Obrigatória**: Sempre chame `run_ctx.add(rows_in=N, rows_out=N)` ao final ou durante o loop para evitar contadores zerados no painel.
- Carga em chunks via `iter_csv()` + `run_batches()` com retry em deadlock.
- CNPJ paralleliza simples + estabelecimentos + sócios com `ThreadPoolExecutor` após empresas.
- `DATA_DIR` usa `Path(__file__).resolve().parents[1] / "data"` como fallback local (em Docker é `/app/data` via volume).

## Variáveis de Ambiente (`.env` na raiz)

### Neo4j

| Variável | Dev | Prod | Descrição |
|---|---|---|---|
| `NEO4J_USER` | `neo4j` | `neo4j` | Usuário |
| `NEO4J_PASSWORD` | `changeme` | `<forte>` | Senha |
| `NEO4J_HEAP_INITIAL_SIZE` | `1g` | `4g` | Heap inicial |
| `NEO4J_HEAP_MAX_SIZE` | `2g` | `8g` | Heap máximo |
| `NEO4J_PAGECACHE_SIZE` | `2g` | `16g` | Page cache |
| `NEO4J_TRANSACTION_TIMEOUT` | `120s` | `300s` | Timeout de transação |

### API

| Variável | Dev | Prod | Descrição |
|---|---|---|---|
| `API_PORT` | `8000` | `8000` | Porta |
| `API_WORKERS` | `4` | `16` | Workers Gunicorn |
| `API_THREADS` | `2` | `4` | Threads por worker |
| `API_TIMEOUT_SECONDS` | `120` | `300` | Timeout HTTP |
| `NEO4J_QUERY_TIMEOUT_SECONDS` | `120` | `300` | Timeout de query Neo4j |
| `NEO4J_MAX_CONNECTION_POOL_SIZE` | `20` | `50` | Pool de conexões |
| `SLOW_QUERY_THRESHOLD_SECONDS` | `2.0` | `5.0` | Limiar de query lenta |

### Redis / Cache

| Variável | Dev | Prod | Descrição |
|---|---|---|---|
| `CACHE_ENABLED` | `1` | `1` | Liga/desliga cache |
| `REDIS_URL` | `redis://redis:6379/0` | `redis://redis:6379/0` | Endereço Redis |
| `CACHE_DEFAULT_TTL_SECONDS` | `60` | `300` | TTL padrão |

### ETL

| Variável | Dev | Prod | Descrição |
|---|---|---|---|
| `CHUNK_SIZE` | `150000` | `200000` | Linhas por chunk de leitura |
| `WORKERS` | `4` | `8` | ZIPs processados em paralelo |
| `NEO4J_BATCH` | `2000` | `5000` | Linhas por UNWIND |
| `PIPELINE_WORKERS` | `2` | `4` | Sessões Neo4j paralelas |
| `SPLINK_THRESHOLD` | `0.8` | `0.8` | Threshold de match Splink |
| `SPLINK_MAX_PESSOAS` | `2000000` | `2000000` | Limite de `:Pessoa` no Splink |

## Bases de Dados

| Base | Download | Pipeline | Nós principais |
|---|---|---|---|
| ibge | `1-ibge.py` | `1-ibge.py` | Regiao, Estado, Municipio |
| cnpj | `2-cnpj.py` | `2-cnpj.py` | Empresa, Pessoa, **Partner**, Municipio |
| tse | `3-tse.py` | `6-tse.py` | Pessoa (candidato/doador), Partido, Eleicao |
| emendas_cgu | `4-emendas_cgu.py` | `5-emendas_cgu.py` | Emenda, Parlamentar |
| tesouro_transparente | `5-tesouro_transparente.py` | — | — |
| servidores_cgu | `6-servidores_cgu.py` | `4-servidores_cgu.py` | Servidor |
| sancoes_cgu | `7-sancoes_cgu.py` | `7-sancoes_cgu.py` | Sancao |
| pncp | `8-pncp.py` | `8-pncp.py` | ItemResultado, Fornecedor, ContratoComprasNet, Empenho, Orgao |
| siafi | — | `3-siafi.py` | UnidadeGestora, Orgao, Esfera |

## Features Avançadas

### Frontend com Visualização de Grafo
Interface estática (Nginx) que consome a API. Renderiza subgrafos interativos com Sigma.js/Cytoscape.js usando `uid` no formato `Label:chave`. Proxy Nginx em `/api/` elimina CORS.

### Paginação e Proteção de Carga
`limit`/`offset` em todos os endpoints de leitura. No Cypher: `SKIP $offset LIMIT $limit`. Impede varreduras acidentais de grandes subgrafos.

### Cache Redis com Fallback Seguro (`cache.py`)
`search`, `pessoa`, `empresa` armazenam respostas no Redis. Fallback transparente se Redis cair. Chave = SHA-256[:16] dos parâmetros. TTL e enable/disable via `.env`.

### Observabilidade Prometheus (`observability.py`)
`GET /metrics` expõe counters e histogramas de latência HTTP e Neo4j. Pronto para Grafana. Queries lentas rastreadas via `neo4j_slow_queries_total`.

### Identidade Parcial (`:Partner`)
Sócios CNPJ com CPF mascarado ou inválido viram nós `:Partner` com `partner_id` estável (SHA-256[:16] de `nome|doc_digits|doc_raw|tipo|rfb`). Relação `(:Partner)-[:SOCIO_DE]->(:Empresa)`. Permite resolução futura via `MESMO_QUE` quando CPF completo aparecer em outra fonte.

### IngestionRun (auditoria)
Todo pipeline cria `:IngestionRun {run_id}` com `status`, `rows_in/out`, timestamps UTC e `error`. Consultável via `ingestion-status` ou diretamente no Neo4j Browser.

### Fulltext Search (`entidade_busca`)
Criado por `setup_schema()`. Cobre 13 labels: `Pessoa | Empresa | Partner | Servidor | Parlamentar | Emenda | Contrato | Sancao | Municipio | Estado | Partido | Eleicao | Licitacao`

### Deduplicação Probabilística (Splink)
`analytics/2-splink.py` — Jaro-Winkler no `nome` + exact match em `cpf`/`dt_nascimento`. Cria `(:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)`. Requer `pip install splink duckdb`.

### Linking Fuzzy Parlamentar ↔ TSE
`5-emendas_cgu.py` resolve nomes como "PR. MARCO FELICIANO" → CPF via cascata: exact → normalized → token subset → reverse subset. Só vincula com CPF único.

### GDS Analytics
`analytics/1-gds.py` projeta o grafo para algoritmos de centralidade e comunidade. Suporta perfis de memória (`full`, `lean`, `core`, `tiny`). O perfil `tiny` é otimizado para ambientes com 2-4GB de RAM, focando apenas em Empresas e Sanções. Grava `gds_comunidade`, `gds_pagerank`, `gds_betweenness` em cada nó. Cria `(:Empresa)-[:SIMILAR_A {score}]->(:Empresa)`.

### Motor de Padrões (`routers/patterns.py`)
Detecta e retorna padrões de corrupção/irregularidade por empresa (`/patterns/empresa/{cnpj}`) ou por estado (`/patterns/estado/{uf}`). Retorna apenas padrões com `triggered=true` e suas evidências. Ver tabela completa no `readme.md`.