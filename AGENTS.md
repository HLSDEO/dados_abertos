# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Visão Geral

DABERTO é uma infraestrutura open-source que cruza bases públicas brasileiras em um grafo Neo4j para gerar inteligência cívica. Stack: **Neo4j 5 + GDS** (grafo), **FastAPI** (API), **Python 3.12 ETL** (pandas/requests/neo4j-driver), orquestrado via **Docker Compose**.

## Estrutura do Projeto

```
dados_abertos/
  api/           — FastAPI: main.py, deps.py, routers/, cache.py, observability.py
  etl/           — orquestrador main.py + download/ + pipeline/ + analytics/
  etl/data/      — dados baixados (gitignored, exceto .keep)
  neo4j/         — Dockerfile customizado (instala GDS) + conf/
```

## Comandos Principais

```bash
# Subir Neo4j + API
docker compose up -d

# API disponível em http://localhost:8000/docs
# Métricas em http://localhost:8000/metrics
# Neo4j Browser em http://localhost:7474 (neo4j/changeme)

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
```

## Arquitetura API (`api/`)

| Arquivo | Responsabilidade |
|---|---|
| `main.py` | App FastAPI, CORS, lifespan, middleware de métricas e endpoint `/metrics` |
| `deps.py` | Singleton do driver Neo4j (pool configurável) + timeout por query + métricas de query |
| `cache.py` | Cache Redis com fallback seguro + key builder/hash + TTL |
| `observability.py` | Métricas Prometheus HTTP e Neo4j (`Counter`/`Histogram`) |
| `routers/search.py` | `GET /search?q=` — fulltext com paginação (`limit`/`offset`) + cache |
| `routers/pessoa.py` | `GET /pessoa/{cpf}` — perfil com paginação (`limit`/`offset`) + cache |
| `routers/empresa.py` | `GET /empresa/{cnpj_basico}` — perfil com paginação (`limit`/`offset`) + cache |
| `routers/parlamentar.py` | `GET /parlamentar/{id}` — perfil com paginação (`limit`/`offset`) |
| `routers/graph.py` | `GET /graph/expand?label=&id=&hops=` — subgrafo com paginação (`offset`) |

O endpoint `/graph/expand` retorna `{nodes, edges}` com `uid` no formato `Label:valor_chave` — pronto para Sigma.js/Cytoscape.js.

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
| `IngestionRun` | Context manager de auditoria — grava nó `:IngestionRun` com status, timestamps e contagens |
| `classify_doc()` | Classifica CPF/CNPJ: `cpf_valid` \| `cpf_partial` \| `cnpj_valid` \| `invalid` |
| `make_partner_id()` | SHA-256[:16] de `nome\|doc_digits\|doc_raw\|tipo\|fonte` para identidades parciais |

### Padrões de pipeline
- Cada pipeline chama `setup_schema(driver)` antes de inserir dados.
- Todo `run()` é envolvido em `with IngestionRun(driver, "source_id"):`.
- Carga em chunks via `iter_csv()` + `run_batches()` com retry em deadlock.
- CNPJ paralleliza simples + estabelecimentos + sócios com `ThreadPoolExecutor` após empresas.
- `DATA_DIR` usa `Path(__file__).resolve().parents[1] / "data"` como fallback local (em Docker é `/app/data` via volume).

### Variáveis de ambiente (`.env` na raiz)
| Variável | Padrão | Descrição |
|---|---|---|
| `NEO4J_URI` | `bolt://neo4j:7687` | URI Bolt |
| `NEO4J_USER` | `neo4j` | Usuário |
| `NEO4J_PASSWORD` | `changeme` | Senha |
| `NEO4J_TRANSACTION_TIMEOUT` | `120s` | Timeout de transação no Neo4j |
| `NEO4J_QUERY_TIMEOUT_SECONDS` | `120` | Timeout por query na API |
| `NEO4J_MAX_CONNECTION_POOL_SIZE` | `20` | Pool de conexões Neo4j da API |
| `API_WORKERS` | `4` | Workers do Gunicorn |
| `API_THREADS` | `2` | Threads por worker do Gunicorn |
| `API_TIMEOUT_SECONDS` | `120` | Timeout HTTP do Gunicorn |
| `CACHE_ENABLED` | `1` | Liga/desliga cache Redis |
| `REDIS_URL` | `redis://redis:6379/0` | Endereço do Redis |
| `CACHE_DEFAULT_TTL_SECONDS` | `60` | TTL padrão do cache |
| `SLOW_QUERY_THRESHOLD_SECONDS` | `2.0` | Limiar de query lenta para métrica |
| `CHUNK_SIZE` | `200000` | Linhas por chunk de leitura (CNPJ usa 50000) |
| `WORKERS` | `4` | ZIPs processados em paralelo |
| `NEO4J_BATCH` | `2000` | Linhas por UNWIND (CNPJ usa 5000) |
| `PIPELINE_WORKERS` | `2` | Sessões Neo4j paralelas (CNPJ) |
| `SPLINK_THRESHOLD` | `0.8` | Threshold de match para Splink |
| `SPLINK_MAX_PESSOAS` | `2000000` | Limite de `:Pessoa` carregados no Splink |

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

### Identidade Parcial (`:Partner`)
Sócios CNPJ com CPF mascarado ou inválido viram nós `:Partner` com `partner_id` estável (SHA-256[:16] de `nome|doc_digits|doc_raw|tipo|rfb`). Relação `(:Partner)-[:SOCIO_DE]->(:Empresa)`. Permite resolução futura via `MESMO_QUE` quando CPF completo aparecer em outra fonte.

### IngestionRun (auditoria)
Todo pipeline cria `:IngestionRun {run_id}` com `status`, `rows_in/out`, timestamps UTC e `error`. Consultável via `ingestion-status` ou diretamente no Neo4j Browser.

### Fulltext Search (`entidade_busca`)
Criado por `setup_schema()`. Cobre 13 labels: `Pessoa | Empresa | Partner | Servidor | Parlamentar | Emenda | Contrato | Sancao | Municipio | Estado | Partido | Eleicao | Licitacao`

### Paginação e proteção de carga na API
Endpoints de leitura aceitam `limit`/`offset` para reduzir payload e cardinalidade por chamada.

### Cache Redis (endpoints quentes)
`search`, `pessoa` e `empresa` usam cache Redis com fallback transparente quando indisponível.

### Observabilidade básica
- `GET /metrics` expõe métricas Prometheus.
- HTTP: latência por endpoint, total por status e exceções.
- Neo4j: latência de query, total `ok/error` e contador de queries lentas.

### Índices base para API
`setup_schema()` aplica índices centrais para acelerar lookups/filtros de `Pessoa`, `Empresa`, `Parlamentar`, `Municipio`, `Emenda` e `Sancao`.

### Deduplicação Probabilística (Splink)
`analytics/2-splink.py` — Jaro-Winkler no `nome` + exact match em `cpf`/`dt_nascimento`. Cria `(:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)`. Requer `pip install splink duckdb`.

### Linking Fuzzy Parlamentar ↔ TSE
`5-emendas_cgu.py` resolve nomes como "PR. MARCO FELICIANO" → CPF via cascata: exact → normalized → token subset → reverse subset. Só vincula com CPF único.

### GDS Analytics
`analytics/1-gds.py` projeta: Empresa, Pessoa, Partner, Municipio, Estado, Parlamentar, Emenda, Sancao, Servidor, UnidadeGestora, Partido, Contrato, Licitacao. Grava `gds_comunidade`, `gds_pagerank`, `gds_betweenness` em cada nó. Cria `(:Empresa)-[:SIMILAR_A {score}]->(:Empresa)`.
