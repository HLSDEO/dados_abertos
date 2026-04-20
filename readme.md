# DADOS ABERTOS -DABERTO
**Infraestrutura open-source em grafo que cruza bases publicas brasileiras para gerar inteligencia acionavel para melhoria civica.**

## Comandos
### 1. sobe o Neo4j (fica em background)
```bash
    docker compose up -d
```
### 2. executa o ETL
#### 2.1. completo (download + pipeline)
```bash
    docker compose run --rm etl
```
#### 2.2. Ou bases específicas:
```bash
    docker compose run --rm etl download
    docker compose run --rm etl pipeline
```
#### 2.3. Analytics
```bash
    docker compose run --rm etl analytics gds       # GDS: comunidades, PageRank, betweenness
    docker compose run --rm etl analytics splink    # deduplicação probabilística de pessoas *
```
> \* Requer `pip install splink duckdb` (ou descomente em `etl/requirements.txt` e rebuilde a imagem)

#### 2.4. Schema e status
```bash
    docker compose run --rm etl schema              # aplica constraints + índices + fulltext
    docker compose run --rm etl ingestion-status    # mostra status dos últimos runs de cada pipeline
```

#### 100. Limpar o cache do ETL:
```bash
    docker compose build --no-cache etl
```

#### BASES
| Nome | Descrição |
| :--- | :---: |
| ibge | Dados do IBGE relacionados a munícipios, estados. |
| cnpj | Dados da receita federal relacionados a empresas, sócios e estabelecimentos. |
| siafi | Dados de órgãos e unidades com seu código SIAFI. |
| servidores_cgu | Dados obtidos do CGU, relacionados aos servidores SIAPE, militares. |
| emendas_cgu | Dados obtidos do CGU, relacionados a emendas parlamentares. |
| tse | Dados do TSE relacionados ao candidatos a eleições e doadores. |
| sancoes_cgu | Dados obtidos do CGU, relacionados a sanções aplicadas a empresas. |
| tesouro_transparente | Dados do Tesouro Transparente relacionados a dados de ordem bancária de emendas parlamentares individuais e de bancada. |

## 4. acessa o browser do Neo4j
* http://localhost:7474   (usuário: neo4j / senha: changeme)

### Busca fulltext
Após a carga, use o índice `entidade_busca` para busca livre sobre todos os tipos de entidade:
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

## Arquitetura
| Camada | Tecnologia |
| :--- | :---: |
| Banco de Grafo | Neo4j 5 Community + GDS |
| Backend | FastAPI (Python 3.12+, async) |
| Frontend | * |
| ETL | Python (pandas, splink opcional) |
| Infra | Docker Compose |

## Features

### Identidade Parcial (`:Partner`)
Sócios CNPJ com CPF mascarado (ex: `***.039.886-**`) não são descartados. Viram nós `:Partner` com `partner_id` estável (SHA-256 do nome+dígitos visíveis) e relação `SOCIO_DE → :Empresa`. Quando o CPF completo estiver disponível em outra fonte, basta criar `MESMO_QUE` entre `:Partner` e `:Pessoa`.

### Auditoria (`:IngestionRun`)
Todo pipeline registra um nó de auditoria com status (`running` / `loaded` / `quality_fail`), timestamps, contagem de linhas e erro (se houver). Visível no browser do Neo4j ou via `ingestion-status`.

### Fulltext Search nativo
Índice `entidade_busca` cobre 12 labels e 14 propriedades — busca livre sem Elasticsearch.

### Deduplicação probabilística (Splink)
Detecta pessoas duplicadas entre fontes usando Jaro-Winkler + exact match em CPF/data de nascimento. Cria `(:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)` para pares acima de 0.8 de probabilidade.

### Linking fuzzy Parlamentar ↔ TSE
Resolve nomes abreviados como "PR. MARCO FELICIANO" → CPF do candidato TSE usando match em cascata: exact → normalized → token subset. Só vincula quando há CPF único (sem ambiguidade).
