"""
Analytics 1 - Neo4j GDS
Roda algoritmos de Graph Data Science sobre o grafo populado.

IMPORTANTE: execute SOMENTE após as cargas principais estarem completas.
Os algoritmos precisam do grafo completo para produzir resultados corretos.

Algoritmos executados (em ordem):
  1. Projeção do grafo em memória GDS (todos os nós e rels relevantes)
  2. Louvain      → detecta comunidades (redes interconectadas)
  3. PageRank     → identifica nós mais influentes
  4. Betweenness  → identifica intermediários (pontes entre comunidades)
  5. Node Similarity → detecta empresas estruturalmente similares
  6. Limpeza da projeção

Labels projetados:
  Empresa, Pessoa, Partner, Municipio, Estado, Parlamentar,
  Emenda, Sancao, Servidor, UnidadeGestora, Partido, Contrato, Licitacao

Propriedades gravadas nos nós:
  gds_comunidade   (Louvain)
  gds_pagerank     (PageRank)
  gds_betweenness  (Betweenness)

Relacionamentos criados:
  (:Empresa)-[:SIMILAR_A {score}]->(:Empresa)   (NodeSimilarity, score >= 0.8)

Uso:
  python main.py analytics
  python main.py analytics gds
  python main.py run --full   # download + pipeline + analytics
"""

import logging
from neo4j import GraphDatabase
from pipeline.lib import IngestionRun

log = logging.getLogger(__name__)

_GRAPH_NAME        = "dados_abertos"
_SIMILARITY_CUTOFF = 0.8


# ── Projeção ──────────────────────────────────────────────────────────────────
# Inclui todos os labels e relacionamentos relevantes para investigação.
# Nós estruturais (Regiao, Mesorregiao etc) são excluídos — não agregam
# valor para detecção de irregularidades.

Q_DROP_IF_EXISTS = """
CALL gds.graph.exists($graph_name)
YIELD exists
WITH exists WHERE exists = true
CALL gds.graph.drop($graph_name) YIELD graphName
RETURN graphName
"""

Q_PROJECT = """
CALL gds.graph.project(
  $graph_name,
  [
    'Empresa',
    'Pessoa',
    'Partner',
    'Municipio',
    'Estado',
    'Parlamentar',
    'Emenda',
    'Sancao',
    'Servidor',
    'UnidadeGestora',
    'Partido',
    'Contrato',
    'Licitacao'
  ],
  {
    SOCIO_DE:          { orientation: 'NATURAL'    },
    LOCALIZADA_EM:     { orientation: 'NATURAL'    },
    POSSUI_SANCAO:     { orientation: 'NATURAL'    },
    AUTORA_DE:         { orientation: 'NATURAL'    },
    DESTINADA_A:       { orientation: 'NATURAL'    },
    BENEFICIOU:        { orientation: 'NATURAL'    },
    DOOU_PARA:         { orientation: 'NATURAL'    },
    EH_SERVIDOR:       { orientation: 'NATURAL'    },
    LOTADO_EM:         { orientation: 'NATURAL'    },
    CANDIDATO_EM:      { orientation: 'NATURAL'    },
    POSSUI_DIVIDA:      { orientation: 'NATURAL'    },
    RECEBEU_EMPRESTIMO: { orientation: 'NATURAL'    },
    GASTOU:            { orientation: 'NATURAL'    },
    FORNECEU:          { orientation: 'NATURAL'    },
    DECLAROU_BEM:     { orientation: 'NATURAL'    },
    MESMO_QUE:         { orientation: 'UNDIRECTED' },
    PUBLICOU_LICITACAO:{ orientation: 'NATURAL'    },
    FIRMOU_CONTRATO:   { orientation: 'NATURAL'    },
    CONTRATOU:         { orientation: 'NATURAL'    },
    VINCULADO_A:        { orientation: 'NATURAL'    }
  }
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount
"""


# ── Algoritmos ────────────────────────────────────────────────────────────────

Q_LOUVAIN = """
CALL gds.louvain.write($graph_name, {
  writeProperty:   'gds_comunidade',
  maxIterations:   10,
  maxLevels:       5,
  tolerance:       0.0001,
  includeIntermediateCommunities: false
})
YIELD communityCount, modularity, ranLevels
RETURN communityCount, modularity, ranLevels
"""

Q_PAGERANK = """
CALL gds.pageRank.write($graph_name, {
  writeProperty:  'gds_pagerank',
  maxIterations:  20,
  dampingFactor:  0.85,
  tolerance:      0.0000001
})
YIELD nodePropertiesWritten, ranIterations, didConverge
RETURN nodePropertiesWritten, ranIterations, didConverge
"""

Q_BETWEENNESS = """
CALL gds.betweenness.write($graph_name, {
  writeProperty: 'gds_betweenness',
  samplingSize:  1000
})
YIELD nodePropertiesWritten, minimumScore, maximumScore, scoreSum
RETURN nodePropertiesWritten, minimumScore, maximumScore, scoreSum
"""

# NodeSimilarity só sobre Empresa — compara vizinhança de sócios/localização
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


# ── Índices nos scores GDS ────────────────────────────────────────────────────

Q_INDEXES = [
    "CREATE INDEX empresa_pagerank     IF NOT EXISTS FOR (e:Empresa)     ON (e.gds_pagerank)",
    "CREATE INDEX empresa_comunidade   IF NOT EXISTS FOR (e:Empresa)     ON (e.gds_comunidade)",
    "CREATE INDEX empresa_betweenness  IF NOT EXISTS FOR (e:Empresa)     ON (e.gds_betweenness)",
    "CREATE INDEX pessoa_pagerank      IF NOT EXISTS FOR (p:Pessoa)      ON (p.gds_pagerank)",
    "CREATE INDEX pessoa_comunidade    IF NOT EXISTS FOR (p:Pessoa)      ON (p.gds_comunidade)",
    "CREATE INDEX pessoa_betweenness   IF NOT EXISTS FOR (p:Pessoa)      ON (p.gds_betweenness)",
    "CREATE INDEX partner_pagerank     IF NOT EXISTS FOR (p:Partner)     ON (p.gds_pagerank)",
    "CREATE INDEX partner_comunidade   IF NOT EXISTS FOR (p:Partner)     ON (p.gds_comunidade)",
    "CREATE INDEX parlamentar_pagerank IF NOT EXISTS FOR (p:Parlamentar) ON (p.gds_pagerank)",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run(session, query: str, params: dict = None, label: str = ""):
    result = session.run(query, **(params or {}))
    record = result.single()
    if record and label:
        log.info(f"    {label}: {dict(record)}")
    return record


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info("[gds] Iniciando análise GDS")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with IngestionRun(driver, "gds") as run_ctx:
        with driver.session() as session:

            log.info("  Removendo projeção anterior (se existir)...")
            session.run(Q_DROP_IF_EXISTS, graph_name=_GRAPH_NAME)

            log.info(f"  Projetando grafo '{_GRAPH_NAME}' em memória...")
            record = _run(session, Q_PROJECT, {"graph_name": _GRAPH_NAME})
            if record:
                node_count = record["nodeCount"]
                log.info(
                    f"    Nós: {node_count:,}  |  "
                    f"Relacionamentos: {record['relationshipCount']:,}"
                )
                run_ctx.add(rows_in=node_count)

            log.info("  [1/4] Louvain — detectando comunidades...")
            _run(session, Q_LOUVAIN, {"graph_name": _GRAPH_NAME}, "resultado")

            log.info("  [2/4] PageRank — calculando influência...")
            _run(session, Q_PAGERANK, {"graph_name": _GRAPH_NAME}, "resultado")

            log.info("  [3/4] Betweenness — identificando intermediários...")
            _run(session, Q_BETWEENNESS, {"graph_name": _GRAPH_NAME}, "resultado")

            log.info(f"  [4/4] Node Similarity — score >= {_SIMILARITY_CUTOFF}...")
            r = _run(session, Q_SIMILARITY,
                     {"graph_name": _GRAPH_NAME, "cutoff": _SIMILARITY_CUTOFF},
                     "resultado")
            if r:
                run_ctx.add(rows_out=r["relationshipsWritten"])

            log.info("  Criando índices nos scores GDS...")
            for q in Q_INDEXES:
                session.run(q)

            log.info("  Liberando projeção da memória...")
            session.run(
                "CALL gds.graph.drop($graph_name) YIELD graphName",
                graph_name=_GRAPH_NAME,
            )

    driver.close()
    log.info("[gds] Análise concluída")
    _print_queries()


def _print_queries():
    log.info("")
    log.info("  ── Consultas úteis no Neo4j Browser ──────────────────────────")
    log.info("")
    log.info("  Top 20 empresas mais influentes (PageRank):")
    log.info("    MATCH (e:Empresa)")
    log.info("    RETURN e.razao_social, e.gds_pagerank, e.gds_comunidade")
    log.info("    ORDER BY e.gds_pagerank DESC LIMIT 20")
    log.info("")
    log.info("  Parlamentares mais influentes na rede:")
    log.info("    MATCH (p:Parlamentar)")
    log.info("    RETURN p.nome_autor, p.gds_pagerank, p.gds_betweenness")
    log.info("    ORDER BY p.gds_pagerank DESC LIMIT 20")
    log.info("")
    log.info("  Comunidades suspeitas (empresa + sanção + emenda):")
    log.info("    MATCH (e:Empresa)-[:POSSUI_SANCAO]->(s:Sancao)")
    log.info("    MATCH (e2:Empresa)")
    log.info("    WHERE e2.gds_comunidade = e.gds_comunidade AND e2 <> e")
    log.info("    MATCH (p:Parlamentar)-[:AUTORA_DE]->(em:Emenda)-[:DESTINADA_A]->(m:Municipio)")
    log.info("    MATCH (e2)-[:LOCALIZADA_EM]->(m)")
    log.info("    RETURN e.razao_social, e2.razao_social, p.nome_autor, m.nome")
    log.info("    LIMIT 50")
    log.info("")
    log.info("  Empresas similares com sanção (possíveis laranjas):")
    log.info("    MATCH (a:Empresa)-[r:SIMILAR_A]->(b:Empresa)")
    log.info("    WHERE r.score >= 0.95")
    log.info("    AND (EXISTS { (a)-[:POSSUI_SANCAO]->() }")
    log.info("     OR EXISTS { (b)-[:POSSUI_SANCAO]->() })")
    log.info("    RETURN a.razao_social, b.razao_social, r.score")
    log.info("    ORDER BY r.score DESC LIMIT 50")
    log.info("")
    log.info("  Servidores que são sócios de empresas sancionadas:")
    log.info("    MATCH (p:Pessoa)-[:EH_SERVIDOR]->(s:Servidor)")
    log.info("    MATCH (p)-[:SOCIO_DE]->(e:Empresa)-[:POSSUI_SANCAO]->(san:Sancao)")
    log.info("    RETURN p.nome, s.org_exercicio, e.razao_social, san.tipo_sancao")
    log.info("    LIMIT 50")
    log.info("")
    log.info("  Intermediários de alta betweenness (possíveis laranjas):")
    log.info("    MATCH (p:Pessoa)")
    log.info("    WHERE p.gds_betweenness > 1000")
    log.info("    RETURN p.nome, p.gds_betweenness, p.gds_comunidade")
    log.info("    ORDER BY p.gds_betweenness DESC LIMIT 30")