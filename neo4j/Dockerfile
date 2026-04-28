# Neo4j com GDS instalado automaticamente na versão correta
# Usamos uma versão estável específica para evitar quebras por atualizações automáticas
FROM neo4j:5.26.0

RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends curl jq \
    && rm -rf /var/lib/apt/lists/*

# Instalação manual do GDS 2.13.0 (compatível com Neo4j 5.26.0)
# Isso evita o erro ClassNotFoundException: org.neo4j.lang.AutoCloseablePlus
RUN mkdir -p /plugins \
    && GDS_VERSION="2.13.0" \
    && GDS_URL="https://github.com/neo4j/graph-data-science/releases/download/v${GDS_VERSION}/neo4j-graph-data-science-${GDS_VERSION}.jar" \
    && echo "Baixando GDS v${GDS_VERSION} para Neo4j 5.26.0..." \
    && curl -fL "$GDS_URL" -o /plugins/graph-data-science.jar \
    && echo "GDS instalado: $(ls -lh /plugins/graph-data-science.jar)"