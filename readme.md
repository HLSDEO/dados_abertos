# DADOS ABERTOS — DABERTO
**Infraestrutura open-source em grafo que cruza bases públicas brasileiras para gerar inteligência acionável para melhoria cívica.**

## Estrutura
```
dados_abertos/
  api/       — FastAPI (endpoints de busca, perfil, grafo)
  etl/       — scripts de download + pipeline + analytics
  etl/data/  — dados baixados (gitignored)
  neo4j/     — configuração e Dockerfile do banco
```

## Comandos
### 1. Neo4j + API
```bash
docker compose up -d
```
- Neo4j Browser: http://localhost:7474 (usuário: `neo4j` / senha: `changeme`)
- API: http://localhost:8000/docs

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
docker compose run --rm etl download cnpj --limite 10000 # limita o numero de linhas carregadas
docker compose run --rm etl download tse --eleicao 2024 --eleicao 2022
docker compose run --rm etl pipeline cnpj --history   # todos os snapshots
docker compose run --rm etl run cnpj --full           # download + pipeline + analytics
```

#### Analytics
```bash
docker compose run --rm etl analytics gds       # Louvain, PageRank, Betweenness, NodeSimilarity
docker compose run --rm etl analytics splink    # deduplicação probabilística de pessoas *
```

#### Schema e status
```bash
docker compose run --rm etl schema              # aplica constraints + índices + fulltext
docker compose run --rm etl ingestion-status    # mostra status dos últimos runs
```

#### Rebuild do ETL
```bash
docker compose build --no-cache etl
```

## Bases

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

## API

Documentação interativa: http://localhost:8000/docs

| Endpoint | Descrição |
| :--- | :--- |
| `GET /search?q=texto` | Busca fulltext em todos os tipos de entidade |
| `GET /pessoa/{cpf}` | Perfil: sócios, servidor, candidaturas, sanções indiretas, parlamentar (se houver) |
| `GET /empresa/{cnpj_basico}` | Perfil: sócios, sanções, contratos, emendas, similares |
| `GET /parlamentar/{parlamentar_id}` | Perfil: emendas, empresas beneficiadas, doadores, sanções indiretas. Busca por `id_camara`, `id_senado`, `id` ou `cpf` |
| `GET /graph/expand?label=Pessoa&id=...&hops=1` | Subgrafo para visualização (nodes + edges). Retorna `nome` e `razao_social` proeminentes |
| `GET /patterns/empresa/{cnpj_basico}` | Padrões de corrupção/irregularidade de uma empresa. 
| `GET /patterns/estado/{uf}` | Padrões de corrupção/irregularidade de empresas de um estado. 
| `GET /docs` | Swagger UI |

#### PATTERNS

Os Endpoint's `GET /patterns/` — motor de padrões de corrupção/irregularidade. Retorna apenas padrões disparados (`triggered=true`) com evidências.

| ID | Nome | O que detecta | Dados usados | Status |
|---|---|---|---|---|
| `sanctioned_contract` | Empresa sancionada recebendo contrato | Sanção vigente se sobrepõe à data do contrato | `Sancao` × `Contrato` | ✅ |
| `sanctioned_bid` | Empresa sancionada com licitação publicada | Sanção vigente se sobrepõe à data da licitação | `Sancao` × `Licitacao` | ✅ |
| `amendment_owner` | Parlamentar destina emenda para empresa onde é sócio | Parlamentar → Emenda → Empresa ← Sócio ← Pessoa (parlamentar) | `Emenda` × `MESMO_QUE` | ✅ |
| `contract_concentration` | Concentração de contratos ≥60% num órgão | Agregação de `FIRMOU_CONTRATO` por órgão | `Contrato` | ✅ |
| `split_contracts` | Fracionamento de contratos < R$80k | Múltiplos contratos no mesmo órgão abaixo do limite | `Contrato` | ✅ |
| `inexigibility_recurrence` | Inexigibilidade recorrente ≥3 contratações | Múltiplos contratos diretos via inexigibilidade | `Contrato` × `Licitacao` | ✅ |
| `servant_company` | Servidor público ativo sócio da empresa contratada | Servidor ativo vinculado à empresa que recebe contrato | `Servidor` × `SOCIO_DE` × `FIRMOU_CONTRATO` | ✅ |
| `donation_contract` | Empresa doadora com contratos públicos (correlação) | Doação de campanha seguida de contrato no mesmo ano | `DOOU_PARA` × `FIRMOU_CONTRATO` | ✅ |
| `debtor_contracts` | Inadimplente recebendo contrato público | Dívida ativa (PGFN) vigente se sobrepõe ao contrato | `DividaAtiva` × `FIRMOU_CONTRATO` | ✅ |
| `expense_supplier_overlap` | Parlamentar gasta CEAP com empresa que recebeu emenda | Deputado gasta com fornecedor que recebeu sua emenda | `Despesa` × `BENEFICIOU` (Câmara) | ✅ |
| `bndes_sanction_overlap` | Empresa recebe BNDES e está sancionada | Empréstimo BNDES + Sanção vigente na mesma empresa | `Emprestimo` × `Sancao` | ✅ |
| `enrichment_signal` | Sócio servidor com patrimônio declarado suspeito | Servidor público com bens declarados > R$ 500k (TSE) | `BemDeclarado` × `EH_SERVIDOR` × `DECLAROU_BEM` | ✅ |

## Arquitetura

| Camada | Tecnologia |
| :--- | :--- |
| Banco de Grafo | Neo4j 5 Community + GDS |
| API | FastAPI (Python 3.12+) |
| Frontend | — (a implementar) |
| ETL | Python 3.12 (pandas, splink opcional) |
| Infra | Docker Compose |


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

## Features

### Identidade Parcial (`:Partner`)
Sócios CNPJ com CPF mascarado (ex: `***.039.886-**`) não são descartados. Viram nós `:Partner` com `partner_id` estável (SHA-256 do nome+dígitos visíveis) e relação `SOCIO_DE → :Empresa`. Quando o CPF completo estiver disponível em outra fonte, basta criar `MESMO_QUE` entre `:Partner` e `:Pessoa`.

### Auditoria (`:IngestionRun`)
Todo pipeline registra um nó de auditoria com status (`running` / `loaded` / `quality_fail`), timestamps, contagem de linhas e erro (se houver). Visível no browser do Neo4j ou via `ingestion-status`.

### Fulltext Search nativo
Índice `entidade_busca` cobre 13 labels e 14 propriedades — busca livre sem Elasticsearch.

### Deduplicação probabilística (Splink)
Detecta pessoas duplicadas entre fontes usando Jaro-Winkler + exact match em CPF/data de nascimento. Cria `(:Pessoa)-[:MESMO_QUE {score, confianca}]->(:Pessoa)` para pares acima de 0.8 de probabilidade.

### Linking fuzzy Parlamentar ↔ TSE
Resolve nomes abreviados como "PR. MARCO FELICIANO" → CPF do candidato TSE usando match em cascata: exact → normalized → token subset. Só vincula quando há CPF único (sem ambiguidade).

### GDS Analytics
Após carga completa, `analytics gds` projeta o grafo em memória e executa Louvain (`gds_comunidade`), PageRank (`gds_pagerank`), Betweenness (`gds_betweenness`) e NodeSimilarity (`(:Empresa)-[:SIMILAR_A]->(:Empresa)`).
