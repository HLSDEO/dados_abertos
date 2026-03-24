"""
Analytics 1 - Neo4j GDS
Roda algoritmos de Graph Data Science sobre o grafo populado.

IMPORTANTE: execute SOMENTE após todas as cargas (pipeline ibge + cnpj).
Os algoritmos precisam do grafo completo para produzir resultados corretos.

Algoritmos executados (em ordem):
  1. Projeção do grafo em memória GDS
  2. Louvain      → detecta comunidades (empresas/pessoas interconectadas)
  3. PageRank     → identifica nós mais influentes na rede
  4. Betweenness  → identifica intermediários (ponte entre comunidades)
  5. Node Similarity → detecta empresas estruturalmente similares (laranjas)
  6. Limpeza da projeção

Propriedades gravadas nos nós:
  gds_comunidade      (Louvain)
  gds_pagerank        (PageRank)
  gds_betweenness     (Betweenness)

Relacionamentos criados:
  (:Empresa)-[:SIMILAR_A {score}]->(:Empresa)   (NodeSimilarity, score >= 0.8)

Uso:
  python main.py analytics
  python main.py analytics gds
  python main.py run --full          # download + pipeline + analytics
"""

import logging

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

# Nome da projeção em memória — pode ser qualquer string única
_GRAPH_NAME = "dados_abertos"

# Score mínimo para criar relacionamento SIMILAR_A entre empresas
_SIMILARITY_CUTOFF = 0.8


# ── Queries de projeção ───────────────────────────────────────────────────────

# Projeta Empresa, Pessoa e os relacionamentos SOCIO_DE e LOCALIZADA_EM
# em um grafo nativo GDS em memória para os algoritmos rodarem
Q_PROJECT = """
CALL gds.graph.project(
  $graph_name,
  ['Empresa', 'Pessoa', 'Municipio'],
  {
    SOCIO_DE:      { orientation: 'UNDIRECTED' },
    LOCALIZADA_EM: { orientation: 'NATURAL'    }
  }
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount
"""

Q_DROP_IF_EXISTS = """
CALL gds.graph.exists($graph_name)
YIELD exists
WITH exists WHERE exists = true
CALL gds.graph.drop($graph_name) YIELD graphName
RETURN graphName
"""


# ── Louvain — detecção de comunidades ────────────────────────────────────────
# Agrupa nós em comunidades baseado na densidade de conexões.
# Comunidades grandes e densas com muitas empresas = rede suspeita.

Q_LOUVAIN = """
CALL gds.louvain.write($graph_name, {
  writeProperty:    'gds_comunidade',
  maxIterations:    10,
  maxLevels:        5,
  tolerance:        0.0001,
  includeIntermediateCommunities: false
})
YIELD communityCount, modularity, ranLevels
RETURN communityCount, modularity, ranLevels
"""


# ── PageRank — nós mais influentes ───────────────────────────────────────────
# Nós com alto PageRank são "hubs" da rede — pessoas ou empresas que
# conectam muitos outros. Alto PageRank + muitos contratos = suspeito.

Q_PAGERANK = """
CALL gds.pageRank.write($graph_name, {
  writeProperty:       'gds_pagerank',
  maxIterations:       20,
  dampingFactor:       0.85,
  tolerance:           0.0000001
})
YIELD nodePropertiesWritten, ranIterations, didConverge
RETURN nodePropertiesWritten, ranIterations, didConverge
"""


# ── Betweenness Centrality — intermediários ───────────────────────────────────
# Nós com alta betweenness são pontes entre comunidades diferentes.
# Clássico perfil de laranja/intermediário em esquemas de desvio.

Q_BETWEENNESS = """
CALL gds.betweenness.write($graph_name, {
  writeProperty: 'gds_betweenness',
  samplingSize:  1000
})
YIELD nodePropertiesWritten, minimumScore, maximumScore, scoreSum
RETURN nodePropertiesWritten, minimumScore, maximumScore, scoreSum
"""


# ── Node Similarity — empresas estruturalmente iguais ────────────────────────
# Duas empresas com os mesmos sócios e mesma estrutura = possíveis laranjas.
# Cria relacionamento SIMILAR_A com score de similaridade.

Q_SIMILARITY = """
CALL gds.nodeSimilarity.write($graph_name, {
  writeRelationshipType: 'SIMILAR_A',
  writeProperty:         'score',
  similarityCutoff:      $cutoff,
  topK:                  10,
  nodeLabels:            ['Empresa']
})
YIELD nodesCompared, relationshipsWritten, similarityDistribution
RETURN nodesCompared, relationshipsWritten, similarityDistribution
"""


# ── Constraint para SIMILAR_A ─────────────────────────────────────────────────
Q_INDEX_PAGERANK = """
CREATE INDEX empresa_pagerank IF NOT EXISTS
FOR (e:Empresa) ON (e.gds_pagerank)
"""
Q_INDEX_COMUNIDADE = """
CREATE INDEX empresa_comunidade IF NOT EXISTS
FOR (e:Empresa) ON (e.gds_comunidade)
"""
Q_INDEX_BETWEENNESS = """
CREATE INDEX empresa_betweenness IF NOT EXISTS
FOR (e:Empresa) ON (e.gds_betweenness)
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def _run(session, query: str, params: dict = None, label: str = ""):
    result = session.run(query, **(params or {}))
    record = result.single()
    if record and label:
        log.info(f"    {label}: {dict(record)}")
    return record


# ── entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info("[gds] Iniciando análise GDS")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with driver.session() as session:

        # 1. Remove projeção anterior se existir
        log.info("  Verificando projeção anterior...")
        session.run(Q_DROP_IF_EXISTS, graph_name=_GRAPH_NAME)

        # 2. Cria projeção em memória
        log.info(f"  Projetando grafo '{_GRAPH_NAME}' em memória...")
        record = _run(session, Q_PROJECT, {"graph_name": _GRAPH_NAME})
        if record:
            log.info(
                f"    Nós: {record['nodeCount']:,}  |  "
                f"Relacionamentos: {record['relationshipCount']:,}"
            )

        # 3. Louvain — comunidades
        log.info("  [1/4] Louvain — detectando comunidades...")
        _run(session, Q_LOUVAIN, {"graph_name": _GRAPH_NAME}, "resultado")

        # 4. PageRank — influência
        log.info("  [2/4] PageRank — calculando influência...")
        _run(session, Q_PAGERANK, {"graph_name": _GRAPH_NAME}, "resultado")

        # 5. Betweenness — intermediários
        log.info("  [3/4] Betweenness — identificando intermediários...")
        _run(session, Q_BETWEENNESS, {"graph_name": _GRAPH_NAME}, "resultado")

        # 6. Node Similarity — empresas similares
        log.info(f"  [4/4] Node Similarity — score >= {_SIMILARITY_CUTOFF}...")
        _run(
            session, Q_SIMILARITY,
            {"graph_name": _GRAPH_NAME, "cutoff": _SIMILARITY_CUTOFF},
            "resultado",
        )

        # 7. Índices para consultas rápidas por score
        log.info("  Criando índices nos scores GDS...")
        for q in (Q_INDEX_PAGERANK, Q_INDEX_COMUNIDADE, Q_INDEX_BETWEENNESS):
            session.run(q)

        # 8. Remove projeção da memória
        log.info("  Liberando projeção da memória...")
        session.run(
            "CALL gds.graph.drop($graph_name) YIELD graphName",
            graph_name=_GRAPH_NAME,
        )

    driver.close()
    log.info("[gds] Análise concluída")
    log.info("")
    log.info("  Consultas úteis no Neo4j Browser:")
    log.info("  -- Top 20 mais influentes (PageRank):")
    log.info("     MATCH (e:Empresa) RETURN e.razao_social, e.gds_pagerank")
    log.info("     ORDER BY e.gds_pagerank DESC LIMIT 20")
    log.info("")
    log.info("  -- Comunidades com mais de 10 empresas:")
    log.info("     MATCH (e:Empresa)")
    log.info("     WITH e.gds_comunidade AS com, count(*) AS total")
    log.info("     WHERE total > 10")
    log.info("     RETURN com, total ORDER BY total DESC")
    log.info("")
    log.info("  -- Empresas similares (possíveis laranjas):")
    log.info("     MATCH (a:Empresa)-[r:SIMILAR_A]->(b:Empresa)")
    log.info("     WHERE r.score >= 0.95")
    log.info("     RETURN a.razao_social, b.razao_social, r.score")
    log.info("     ORDER BY r.score DESC LIMIT 50")