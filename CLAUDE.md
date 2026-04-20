# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Visão Geral

DABERTO é uma infraestrutura open-source que cruza bases públicas brasileiras em um grafo Neo4j para gerar inteligência cívica. O stack é: **Neo4j 5 + GDS** (grafo), **Python 3.12 ETL** (pandas/requests/neo4j-driver), tudo orquestrado via **Docker Compose**.

## Comandos Principais

```bash
# Subir o Neo4j (background)
docker compose up -d

# ETL completo (download + pipeline)
docker compose run --rm --profile etl etl

# Bases específicas
docker compose run --rm --profile etl etl download cnpj
docker compose run --rm --profile etl etl pipeline emendas_cgu

# Flags disponíveis
docker compose run --rm --profile etl etl download cnpj --chunk 100000 --workers 4
docker compose run --rm --profile etl etl download tse --eleicao 2024 --eleicao 2022
docker compose run --rm --profile etl etl pipeline cnpj --history   # todos os snapshots
docker compose run --rm --profile etl etl run cnpj --full           # download + pipeline + analytics

# Analytics
docker compose run --rm --profile etl etl analytics gds             # GDS (Louvain, PageRank, etc.)
docker compose run --rm --profile etl etl analytics splink          # deduplicação probabilística

# Operações de schema e status
docker compose run --rm --profile etl etl schema                    # aplica constraints + índices + fulltext
docker compose run --rm --profile etl etl ingestion-status          # status dos últimos runs por pipeline

# Rebuild forçado do ETL (limpa cache)
docker compose build --no-cache etl
```

**Neo4j Browser:** http://localhost:7474 (usuário: `neo4j` / senha: `changeme`)

## Arquitetura ETL

### Fluxo de dados
```
Fontes públicas (HTTP) → download/*.py → data/<base>/ (CSV/ZIP)
                                                ↓
                        pipeline/*.py → Neo4j (grafo)
                                                ↓
              analytics/1-gds.py → GDS (Louvain, PageRank, Betweenness)
              analytics/2-splink.py → dedup probabilístico (:MESMO_QUE)
```

### Orquestrador: `etl/main.py`
Carrega módulos dinamicamente via `importlib`. Os registros `DOWNLOADS`, `PIPELINES` e `ANALYTICS` mapeiam nome de base → arquivo. Cada módulo expõe uma função `run(**kwargs)` — os kwargs aceitos são detectados por `inspect.signature` e passados automaticamente.

Comandos disponíveis: `download | pipeline | analytics | run | schema | ingestion-status`

### Estrutura dos módulos
- `etl/download/<n>-<base>.py` — baixa e extrai arquivos brutos para `data/<base>/`
- `etl/pipeline/<n>-<base>.py` — lê CSVs e carrega nós/relações no Neo4j via Cypher UNWIND
- `etl/pipeline/lib.py` — utilitários compartilhados (ver seção abaixo)
- `etl/analytics/1-gds.py` — projeção GDS + algoritmos (Louvain, PageRank, Betweenness, NodeSimilarity)
- `etl/analytics/2-splink.py` — deduplicação probabilística de `:Pessoa` com Splink

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
- Cada pipeline chama `setup_schema(driver)` antes de inserir dados — garante fulltext index idempotente.
- Todo `run()` é envolvido em `with IngestionRun(driver, "source_id"):` para rastreabilidade.
- Carga em chunks via `iter_csv()` + `run_batches()` com retry em deadlock.
- CNPJ usa sessões Neo4j paralelas (`PIPELINE_WORKERS`) por volume (~70+ GB).
- Todo nó recebe propriedades de rastreabilidade: `fonte_nome`, `fonte_url`, `fonte_coletado_em`.

### Variáveis de ambiente (`.env` na raiz)
| Variável | Padrão | Descrição |
|---|---|---|
| `NEO4J_URI` | `bolt://neo4j:7687` | URI Bolt |
| `NEO4J_USER` | `neo4j` | Usuário |
| `NEO4J_PASSWORD` | `changeme` | Senha |
| `CHUNK_SIZE` | `200000` | Linhas por chunk de leitura |
| `WORKERS` | `4` | ZIPs processados em paralelo |
| `NEO4J_BATCH` | `2000` | Linhas por UNWIND no Neo4j |
| `PIPELINE_WORKERS` | `2` | Sessões Neo4j paralelas (CNPJ) |
| `SPLINK_THRESHOLD` | `0.8` | Threshold de match para Splink |
| `SPLINK_MAX_PESSOAS` | `2000000` | Limite de `:Pessoa` carregados no Splink |

## Bases de Dados

| Base | Download | Pipeline | Nós principais |
|---|---|---|---|
| ibge | `1-ibge.py` | `1-ibge.py` | Regiao, Estado, Municipio |
| cnpj | `2-cnpj.py` | `2-cnpj.py` | Empresa, Pessoa, **Partner**, Municipio |
| tse | `3-tse.py` | `6-tse.py` | Pessoa (candidato/doador), Partido, Eleicao |
| emendas_cgu | `4-emendas_cgu.py` | `5-emendas_cgu.py` | Emenda, Parlamentar, Despesa, Convenio |
| tesouro_transparente | `5-tesouro_transparente.py` | — | — |
| servidores_cgu | `6-servidores_cgu.py` | `4-servidores_cgu.py` | Servidor |
| sancoes_cgu | `7-sancoes_cgu.py` | `7-sancoes_cgu.py` | Sancao |
| pncp | `8-pncp.py` | `8-pncp.py` | Contrato, Licitacao |
| siafi | — | `3-siafi.py` | UnidadeGestora, Orgao, Esfera |

## Features Avançadas

### Identidade Parcial (`:Partner`)
Sócios CNPJ com CPF mascarado (ex: `***.039.886-**`) ou inválido não são descartados nem criam `:Pessoa` com CPF falso. Em vez disso, cria-se um nó `:Partner` com:
- `partner_id`: SHA-256[:16] de `nome|doc_digits|doc_raw|tipo|rfb` — chave estável
- `doc_partial`: 6+ dígitos visíveis (útil para cruzamento futuro quando CPF for revelado)
- `doc_tipo`: `cpf_partial` | `invalid`
- `qualidade_id`: `partial` | `unknown`

Relação: `(:Partner)-[:SOCIO_DE]->(:Empresa)` — mesma relação das `:Pessoa` PF.

### IngestionRun (auditoria)
Todo pipeline cria/atualiza um nó `:IngestionRun {run_id}` com:
- `status`: `running` → `loaded` | `quality_fail`
- `rows_in` / `rows_out`: contagens de linhas processadas
- `started_at` / `finished_at`: timestamps ISO 8601 UTC
- `error`: mensagem de erro (primeiros 1000 chars) se falhar

Consultar via browser: `MATCH (r:IngestionRun) RETURN r ORDER BY r.started_at DESC LIMIT 20`
Ou via CLI: `docker compose run --rm --profile etl etl ingestion-status`

### Fulltext Search (`entidade_busca`)
Criado automaticamente por `setup_schema()`. Cobre labels:
`Pessoa | Empresa | Partner | Servidor | Parlamentar | Emenda | Contrato | Sancao | Municipio | Estado | Partido | Eleicao`

Propriedades: `nome, razao_social, cpf, cnpj, doc_partial, nome_urna, nome_autor, objeto, codigo_emenda, numero_contrato, motivo_sancao, nome_favorecido`

Uso no Neo4j:
```cypher
CALL db.index.fulltext.queryNodes('entidade_busca', 'Marco Feliciano')
YIELD node, score
RETURN labels(node), node.nome, score ORDER BY score DESC LIMIT 10
```

### Deduplicação Probabilística (Splink)
`analytics/2-splink.py` detecta `:Pessoa` duplicadas entre fontes (TSE, servidores, etc.) usando:
- Jaro-Winkler no `nome` (thresholds 0.9 e 0.8)
- Exact match em `cpf` e `dt_nascimento`
- Blocking em CPF exato OU nome exato

Cria: `(:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)`
`confianca`: `alta` (≥0.9) | `media` (≥0.7) | `baixa` (<0.7)

**Requer instalação extra** (não incluído na imagem Docker por padrão):
```bash
pip install splink duckdb
# ou descomente as linhas em etl/requirements.txt e rebuild
```

### Linking Fuzzy Parlamentar ↔ TSE
`5-emendas_cgu.py` resolve nomes como "PR. MARCO FELICIANO" → CPF de "MARCO ANTONIO FELICIANO" com estratégias em cascata: exact → normalized (sem acentos/títulos) → token subset → reverse subset. Só vincula quando há CPF único (descarta ambíguos).

## Neo4j GDS (Analytics)

Após carga completa, `analytics/1-gds.py` projeta todos os nós/rels em memória (`_GRAPH_NAME = "dados_abertos"`) e executa:
- **Louvain** → `gds_comunidade` em cada nó
- **PageRank** → `gds_pagerank` em cada nó
- **Betweenness** → `gds_betweenness` em cada nó
- **NodeSimilarity** → rel `(:Empresa)-[:SIMILAR_A {score}]->(:Empresa)` (threshold 0.8)

O Neo4j custom Dockerfile (`neo4j/Dockerfile`) instala o plugin GDS automaticamente detectando a versão do Neo4j em runtime.
