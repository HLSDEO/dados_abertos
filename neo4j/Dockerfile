# Neo4j com GDS instalado automaticamente na versão correta
FROM neo4j:5

RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends curl jq \
    && rm -rf /var/lib/apt/lists/*

# Cria /plugins se não existir, descobre versão do Neo4j e baixa GDS compatível
RUN mkdir -p /plugins \
    && NEO4J_VERSION=$(neo4j version | grep -oP '\d+\.\d+') \
    && echo "Neo4j detectado: $NEO4J_VERSION" \
    && GDS_URL=$(curl -sf "https://api.github.com/repos/neo4j/graph-data-science/releases" \
        | jq -r --arg v "$NEO4J_VERSION" \
          '[.[] | select(.prerelease == false) | .assets[] | select(.name | test("neo4j-graph-data-science-.*\\.jar$"))] | first | .browser_download_url') \
    && echo "Baixando GDS: $GDS_URL" \
    && curl -fL "$GDS_URL" -o /plugins/graph-data-science.jar \
    && echo "GDS instalado: $(ls -lh /plugins/graph-data-science.jar)"