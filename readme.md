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

## Arquitetura
| Camada | Tecnologia |
| :--- | :---: |
| Banco de Grafo | Neo4j 5 Community |
| Backend | FastAPI (Python 3.12+, async) |
| Frontend | * |
| ETL | Python (pandas) |
| Infra | Docker Compose |