# 1. sobe o Neo4j (fica em background)
docker compose up -d

# 2. executa o ETL completo (download + pipeline)
docker compose run --rm etl

# 3. ou comandos específicos:
docker compose run --rm etl download
docker compose run --rm etl download ibge
docker compose run --rm etl pipeline
docker compose run --rm etl pipeline ibge
docker compose run --rm etl run ibge

# 4. acessa o browser do Neo4j
#    http://localhost:7474   (usuário: neo4j / senha: changeme)