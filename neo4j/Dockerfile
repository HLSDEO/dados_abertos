# Usamos a imagem oficial estável
FROM neo4j:5.26.0

# Instalamos apenas utilitários básicos se necessário
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# No Neo4j 5, é preferível deixar o NEO4J_PLUGINS no docker-compose lidar com o download
# ou colocar os JARs manualmente. Para garantir estabilidade, vamos apenas preparar a pasta.
RUN mkdir -p /plugins && chown neo4j:neo4j /plugins
