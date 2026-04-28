# DADOS ABERTOS — DABERTO
**Infraestrutura open-source em grafo que cruza bases públicas brasileiras para gerar inteligência acionável para melhoria cívica.**

## Estrutura
```
dados_abertos/
  api/           — FastAPI (endpoints de busca, perfil, grafo, padrões, métricas)
  etl/           — scripts de download + pipeline + analytics
  etl/data/      — dados baixados (gitignored)
  neo4j/         — configuração e Dockerfile do banco
  frontend/      — interface web estática (Nginx), visualização de grafos e busca
```

## Comandos

### 1. Subir todos os serviços
```bash
docker compose up -d
```

| Serviço | URL |
| :--- | :--- |
| Frontend | http://localhost:8080 |
| API (Swagger) | http://localhost:8000/docs |
| Métricas Prometheus | http://localhost:8000/metrics |
| Neo4j Browser | http://localhost:7474 (`neo4j` / `changeme`) |
| Redis | `redis://localhost:6379` |

---

### 1.1 Perfis de ambiente (dev / prod)

Copie o arquivo de ambiente adequado para `.env` antes de subir os serviços:

```bash
# Linux / macOS
cp .env.dev  .env   # perfil de desenvolvimento
cp .env.prod .env   # perfil de produção

# Windows
copy .env.dev  .env
copy .env.prod .env
```

#### Referência de variáveis de ambiente

| Variável | Dev | Prod | Descrição |
| :--- | :--- | :--- | :--- |
| `NEO4J_USER` | `neo4j` | `neo4j` | Usuário do banco |
| `NEO4J_PASSWORD` | `changeme` | `<senha forte>` | Senha do banco |
| `NEO4J_HEAP_INITIAL_SIZE` | `1g` | `4g` | Heap inicial do Neo4j |
| `NEO4J_HEAP_MAX_SIZE` | `2g` | `8g` | Heap máximo do Neo4j |
| `NEO4J_PAGECACHE_SIZE` | `2g` | `16g` | Page cache do Neo4j |
| `NEO4J_TRANSACTION_TIMEOUT` | `120s` | `300s` | Timeout de transação |
| `NEO4J_QUERY_TIMEOUT_SECONDS` | `120` | `300` | Timeout de query na API |
| `NEO4J_MAX_CONNECTION_POOL_SIZE` | `20` | `50` | Pool de conexões Neo4j |
| `API_PORT` | `8000` | `8000` | Porta da API |
| `API_WORKERS` | `4` | `16` | Workers Gunicorn |
| `API_THREADS` | `2` | `4` | Threads por worker |
| `API_TIMEOUT_SECONDS` | `120` | `300` | Timeout HTTP Gunicorn |
| `CACHE_ENABLED` | `1` | `1` | Liga/desliga cache Redis |
| `REDIS_URL` | `redis://redis:6379/0` | `redis://redis:6379/0` | Endereço Redis |
| `CACHE_DEFAULT_TTL_SECONDS` | `60` | `300` | TTL padrão do cache |
| `SLOW_QUERY_THRESHOLD_SECONDS` | `2.0` | `5.0` | Limiar para query lenta |
| `CHUNK_SIZE` | `150000` | `200000` | Linhas por chunk (ETL) |
| `WORKERS` | `4` | `8` | ZIPs paralelos (ETL) |
| `NEO4J_BATCH` | `2000` | `5000` | Linhas por UNWIND (ETL) |
| `PIPELINE_WORKERS` | `2` | `4` | Sessões Neo4j paralelas (ETL) |
| `GDS_MAX_MEMORY_GB` | `4.0` | `32.0` | Limite de memória para projeção GDS |
| `GDS_PROFILE` | `auto` | `full` | Perfil GDS (`auto`, `full`, `lean`, `core`, `tiny`) |

> **Dev**: 8 cores / 24 GB RAM  
> **Prod**: 24 cores / 120 GB RAM

---

### 2. ETL

#### Completo (download + pipeline)
```bash
docker compose run --rm etl
```

#### Bases específicas
```bash
docker compose run --rm etl download [nome_da_base]
docker compose run --rm etl pipeline [nome_da_base]
```

#### Flags disponíveis
```bash
docker compose run --rm etl download cnpj --chunk 100000 --workers 4
docker compose run --rm etl download cnpj --limite 10000   # limita linhas carregadas
docker compose run --rm etl download tse --eleicao 2024 --eleicao 2022
docker compose run --rm etl pipeline cnpj --history        # todos os snapshots
docker compose run --rm etl pipeline camara --ano 2024 --ano 2025
docker compose run --rm etl run cnpj --full                # download + pipeline + analytics
```

#### Analytics
```bash
docker compose run --rm etl analytics gds       # Louvain, PageRank, Betweenness, NodeSimilarity
docker compose run --rm etl analytics splink    # deduplicação probabilística de pessoas
```

> **GDS**: Se o ambiente tiver pouca RAM, use `GDS_PROFILE=tiny` no `.env` para focar apenas em Empresas e Sanções.

#### Schema e status
```bash
docker compose run --rm etl schema              # aplica constraints + índices + fulltext
docker compose run --rm etl ingestion-status    # mostra status dos últimos runs
```

#### Rebuild
```bash
docker compose build --no-cache etl
docker compose build --no-cache api
docker compose build --no-cache frontend
```

---

## Frontend

O frontend é uma aplicação web estática servida via **Nginx** na porta `8080`. Ele se comunica diretamente com a API em `http://localhost:8000`.

### Funcionalidades
- **Busca fulltext** — campo de busca livre que consulta `GET /search` e exibe resultados paginados por tipo de entidade.
- **Perfil de Pessoa** — exibe vínculos societários, candidaturas, cargos públicos e sanções indiretas.
- **Perfil de Empresa** — exibe sócios, sanções, contratos, emendas recebidas e empresas similares.
- **Perfil de Parlamentar** — emendas destinadas, empresas beneficiadas, doadores, padrões detectados.
- **Visualização de grafo** — expansão interativa de subgrafos via `GET /graph/expand`, renderizado com **Sigma.js / Cytoscape.js**. Nós identificados por `uid` no formato `Label:chave`.
- **Motor de padrões** — exibe padrões de corrupção e irregularidade detectados (`/patterns/empresa` e `/patterns/estado`).

### Estrutura de arquivos
```
frontend/
  index.html        — página principal / busca
  nginx.conf        — configuração do Nginx (proxy para API em /api/)
  Dockerfile        — imagem Nginx Alpine
  assets/           — CSS, JS, ícones
```

### Proxy Nginx
O Nginx está configurado para repassar requisições `/api/*` para o serviço `api:8000`, eliminando CORS em produção:
```nginx
location /api/ {
    proxy_pass http://api:8000/;
}
```

---

## Bases de Dados

| Base | Descrição | Nós principais |
| :--- | :--- | :--- |
| ibge | IBGE — municípios e estados | Regiao, Estado, Municipio |
| cnpj | Receita Federal — empresas, sócios, estabelecimentos | Empresa, Pessoa, **Partner**, Municipio |
| siafi | SIAFI — órgãos e unidades com código SIAFI | UnidadeGestora, Orgao, Esfera |
| servidores_cgu | CGU — servidores SIAPE e militares | Servidor |
| emendas_cgu | CGU — emendas parlamentares | Emenda, Parlamentar |
| tse | TSE — candidatos a eleições e doadores | Pessoa, Partido, Eleicao |
| sancoes_cgu | CGU — sanções aplicadas a empresas | Sancao |
| pncp | Portal Nacional de Contratações Públicas | ItemResultado, Fornecedor, ContratoComprasNet, Empenho, Orgao |
| tesouro_transparente | Ordens bancárias de emendas parlamentares | — |
| pgfn | PGFN — dívida ativa (não previdenciária e previdenciária) | DividaAtiva |
| camara | Câmara dos Deputados — despesas CEAP | Despesa, Parlamentar |
| bndes | BNDES — operações de financiamento | Emprestimo |
| senado | Senado Federal — despesas CEAP | Despesa, Parlamentar (id_senado) |

---

## API

Documentação interativa: http://localhost:8000/docs

| Endpoint | Descrição |
| :--- | :--- |
| `GET /search?q=texto` | Busca fulltext em todos os tipos de entidade |
| `GET /pessoa/{cpf}` | Perfil: sócios, servidor, candidaturas, sanções indiretas, parlamentar |
| `GET /empresa/{cnpj_basico}` | Perfil: sócios, sanções, contratos, emendas, similares |
| `GET /parlamentar/{parlamentar_id}` | Perfil: emendas, empresas beneficiadas, doadores, sanções indiretas |
| `GET /graph/expand?label=&id=&hops=` | Subgrafo para visualização (`nodes` + `edges`), uid no formato `Label:chave` |
| `GET /patterns/empresa/{cnpj_basico}` | Padrões de corrupção/irregularidade de uma empresa |
| `GET /patterns/estado/{uf}` | Padrões de corrupção/irregularidade de empresas de um estado |
| `GET /metrics` | Métricas Prometheus (latência HTTP, erros, queries Neo4j, queries lentas) |
| `GET /docs` | Swagger UI |

### Paginação

Todos os endpoints de leitura suportam os parâmetros `limit` e `offset` para controle de payload e proteção de carga:

| Parâmetro | Padrão | Descrição |
| :--- | :--- | :--- |
| `limit` | `20` | Máximo de itens retornados por chamada |
| `offset` | `0` | Deslocamento para navegação entre páginas |

Endpoints com paginação: `search`, `pessoa`, `empresa`, `parlamentar`, `graph/expand`.

### Cache Redis

Cache em memória com **fallback seguro** — se o Redis ficar indisponível, a API continua respondendo sem cache, sem erros.

| Endpoint | Cache | TTL padrão |
| :--- | :--- | :--- |
| `GET /search` | ✅ | 60s (configurável) |
| `GET /pessoa/{cpf}` | ✅ | 60s |
| `GET /empresa/{cnpj_basico}` | ✅ | 60s |

- Chave de cache gerada via hash dos parâmetros da requisição.
- TTL configurável via `CACHE_DEFAULT_TTL_SECONDS`.
- Cache pode ser desabilitado com `CACHE_ENABLED=0`.

### Observabilidade (`/metrics`)

Métricas expostas em formato **Prometheus** no endpoint `GET /metrics`:

| Métrica | Tipo | Descrição |
| :--- | :--- | :--- |
| `http_requests_total` | Counter | Total de requisições por endpoint e status HTTP |
| `http_request_duration_seconds` | Histogram | Latência por endpoint |
| `http_exceptions_total` | Counter | Total de exceções não tratadas |
| `neo4j_query_duration_seconds` | Histogram | Latência de queries Neo4j |
| `neo4j_queries_total` | Counter | Total de queries (`ok` / `error`) |
| `neo4j_slow_queries_total` | Counter | Queries acima do limiar `SLOW_QUERY_THRESHOLD_SECONDS` |

### Motor de Padrões (`/patterns/`)

Retorna apenas padrões disparados (`triggered=true`) com evidências. Útil para dashboards e alertas automáticos.

| ID | Nome | O que detecta | Dados usados |
|---|---|---|---|
| `sanctioned_contract` | Empresa sancionada recebendo contrato | Sanção vigente sobreposta à data do contrato | `Sancao` × `Contrato` |
| `sanctioned_bid` | Empresa sancionada com licitação publicada | Sanção vigente sobreposta à data da licitação | `Sancao` × `Licitacao` |
| `amendment_owner` | Parlamentar destina emenda para empresa onde é sócio | Parlamentar → Emenda → Empresa ← Sócio | `Emenda` × `MESMO_QUE` |
| `contract_concentration` | Concentração ≥60% de contratos num único órgão | Agregação de `FIRMOU_CONTRATO` por órgão | `Contrato` |
| `split_contracts` | Fracionamento de contratos < R$80k | Múltiplos contratos no mesmo órgão abaixo do limite | `Contrato` |
| `inexigibility_recurrence` | Inexigibilidade recorrente ≥3 contratações | Múltiplos contratos diretos via inexigibilidade | `Contrato` × `Licitacao` |
| `servant_company` | Servidor público ativo sócio da empresa contratada | Servidor ativo vinculado à empresa contratada | `Servidor` × `SOCIO_DE` × `FIRMOU_CONTRATO` |
| `donation_contract` | Empresa doadora com contratos públicos | Doação de campanha seguida de contrato no mesmo ano | `DOOU_PARA` × `FIRMOU_CONTRATO` |
| `debtor_contracts` | Inadimplente recebendo contrato público | Dívida ativa (PGFN) vigente sobreposta ao contrato | `DividaAtiva` × `FIRMOU_CONTRATO` |
| `expense_supplier_overlap` | Parlamentar gasta CEAP com empresa que recebeu emenda | Deputado gasta com fornecedor que recebeu sua emenda | `Despesa` × `BENEFICIOU` |
| `bndes_sanction_overlap` | Empresa recebe BNDES e está sancionada | Empréstimo BNDES + Sanção vigente | `Emprestimo` × `Sancao` |
| `enrichment_signal` | Servidor com patrimônio declarado suspeito | Bens declarados > R$500k (TSE) | `BemDeclarado` × `EH_SERVIDOR` |

---

## Arquitetura

| Camada | Tecnologia |
| :--- | :--- |
| Banco de Grafo | Neo4j 5 Community + GDS |
| API | FastAPI (Python 3.12+) |
| Servidor API | Gunicorn + Uvicorn Worker |
| Cache | Redis 7 (Alpine) |
| Observabilidade | Prometheus metrics em `/metrics` |
| Frontend | HTML/JS estático servido por Nginx (Alpine) |
| Visualização de Grafo | Sigma.js / Cytoscape.js |
| ETL | Python 3.12 (pandas, splink opcional) |
| Infra | Docker Compose |

---

## Consultas úteis no Neo4j

### Busca fulltext
```cypher
CALL db.index.fulltext.queryNodes('entidade_busca', 'Marco Feliciano')
YIELD node, score
RETURN labels(node), node.nome, score ORDER BY score DESC LIMIT 10
```

### Status dos pipelines
```cypher
MATCH (r:IngestionRun)
WITH r.source_id AS source, max(r.started_at) AS last
MATCH (r:IngestionRun {source_id: source, started_at: last})
RETURN source, r.status, r.rows_in, r.rows_out, r.started_at
ORDER BY source
```

---

## Features

### Frontend com Visualização de Grafo
Interface web estática servida pelo Nginx, que consome a API e renderiza subgrafos interativos com Sigma.js/Cytoscape.js. Nós retornados com `uid` no formato `Label:chave` para compatibilidade direta com os renderers.

### Paginação e Proteção de Carga
Todos os endpoints de leitura aceitam `limit` e `offset`, evitando payloads excessivos e protegendo o Neo4j de varreduras completas acidentais.

### Cache Redis com Fallback Seguro
Endpoints quentes (`search`, `pessoa`, `empresa`) armazenam respostas no Redis. Se o Redis estiver indisponível, o fallback é transparente — a API continua funcionando sem cache e sem erros.

### Observabilidade com Prometheus
`GET /metrics` expõe contadores e histogramas de latência HTTP e Neo4j. Pronto para integração com Grafana.

### Identidade Parcial (`:Partner`)
Sócios CNPJ com CPF mascarado (ex: `***.039.886-**`) viram nós `:Partner` com `partner_id` estável (SHA-256 do nome+dígitos visíveis) e relação `SOCIO_DE → :Empresa`. Resolução futura via `MESMO_QUE` quando CPF completo aparecer em outra fonte.

### Auditoria (`:IngestionRun`)
Todo pipeline registra um nó de auditoria com status (`running` / `loaded` / `quality_fail`), timestamps, contagem de linhas e erro. Consultável no Neo4j Browser ou via `ingestion-status`.

### Fulltext Search nativo
Índice `entidade_busca` cobre 13 labels e 14 propriedades — busca livre sem necessidade de Elasticsearch.

### Deduplicação probabilística (Splink)
Detecta pessoas duplicadas entre fontes usando Jaro-Winkler + exact match em CPF/data de nascimento. Cria `(:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)` para pares acima de 0.8 de probabilidade.

### Linking fuzzy Parlamentar ↔ TSE
Resolve nomes abreviados como "PR. MARCO FELICIANO" → CPF do candidato TSE via cascata: exact → normalized → token subset → reverse subset. Vincula somente com CPF único.

### GDS Analytics
`analytics gds` projeta o grafo em memória e executa Louvain (`gds_comunidade`), PageRank (`gds_pagerank`), Betweenness (`gds_betweenness`) e NodeSimilarity (`(:Empresa)-[:SIMILAR_A]->(:Empresa)`).