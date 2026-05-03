from fastapi import APIRouter, Depends
from typing import Any
from deps import get_driver
from neo4j import Driver

router = APIRouter(prefix="/insights", tags=["insights"])


@router.get("/top-empresas")
def top_empresas(limit: int = 20, driver: Driver = Depends(get_driver)):
    """
    Top empresas por PageRank.
    """
    query = """
    MATCH (e:Empresa)
    WHERE e.gds_pagerank IS NOT NULL
    RETURN e.razao_social AS razao_social, e.gds_pagerank AS gds_pagerank, e.gds_comunidade AS gds_comunidade, e.cnpj_basico AS id
    ORDER BY e.gds_pagerank DESC
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, limit=limit)
        items = [record.data() for record in result]
    return {"items": items}


@router.get("/top-parlamentares")
def top_parlamentares(limit: int = 20, driver: Driver = Depends(get_driver)):
    """
    Parlamentares mais influentes por PageRank e Betweenness.
    """
    query = """
    MATCH (p:Parlamentar)
    WHERE exists(p.gds_pagerank) OR exists(p.gds_betweenness)
    RETURN p.nome_autor AS nome_autor, p.gds_pagerank AS gds_pagerank, p.gds_betweenness AS gds_betweenness, p.id_parlamentar AS id
    ORDER BY p.gds_pagerank DESC
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, limit=limit)
        items = [record.data() for record in result]
    return {"items": items}


@router.get("/comunidades-suspeitas")
def comunidades_suspeitas(limit: int = 50, driver: Driver = Depends(get_driver)):
    """
    Empresas na mesma comunidade que possuem sanção e são vinculadas a emendas de parlamentares.
    """
    query = """
    MATCH (e:Empresa)-[:POSSUI_SANCAO]->(s:Sancao)
    MATCH (e2:Empresa)
    WHERE e2.gds_comunidade = e.gds_comunidade AND e2 <> e
    MATCH (p:Parlamentar)-[:AUTORA_DE]->(em:Emenda)-[:DESTINADA_A]->(m:Municipio)
    MATCH (e2)-[:LOCALIZADA_EM]->(m)
    RETURN e.razao_social   AS empresa_razao_social,
           e2.razao_social  AS empresa2_razao_social,
           p.nome_autor     AS parlamentar_nome,
           m.nome           AS municipio_nome,
           e.cnpj_basico    AS empresa,
           e2.cnpj_basico   AS empresa2,
           p.id_parlamentar AS parlamentar,
           m.id_municipio   AS municipio
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, limit=limit)
        items = [record.data() for record in result]
    return {"items": items}


@router.get("/empresas-similares")
def empresas_similares(score_min: float = 0.95, limit: int = 50, driver: Driver = Depends(get_driver)):
    """
    Empresas altamente similares (SIMILAR_A) onde pelo menos uma possui sanção.
    """
    query = """
    MATCH (a:Empresa)-[r:SIMILAR_A]->(b:Empresa)
    WHERE r.score >= $score_min
      AND (EXISTS { (a)-[:POSSUI_SANCAO]->() } OR EXISTS { (b)-[:POSSUI_SANCAO]->() })
    RETURN a.razao_social AS empresa_a_razao_social,
           b.razao_social AS empresa_b_razao_social,
           r.score AS score,
           a.cnpj_basico AS empresa_a,
           b.cnpj_basico AS empresa_b
    ORDER BY r.score DESC
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, score_min=score_min, limit=limit)
        items = [record.data() for record in result]
    return {"items": items}


@router.get("/servidores-socios")
def servidores_socios(limit: int = 50, driver: Driver = Depends(get_driver)):
    """
    Servidores públicos que são sócios de empresas sancionadas.
    """
    query = """
    MATCH (p:Pessoa)-[:EH_SERVIDOR]->(s:Servidor)
    MATCH (p)-[:SOCIO_DE]->(e:Empresa)-[:POSSUI_SANCAO]->(san:Sancao)
    RETURN p.nome          AS pessoa_nome,
           s.org_exercicio AS org_exercicio,
           e.razao_social  AS empresa_razao_social,
           san.tipo_sancao AS tipo_sancao,
           p.cpf           AS pessoa,
           e.cnpj_basico   AS empresa,
           san.id_sancao   AS sancao
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, limit=limit)
        items = [record.data() for record in result]
    return {"items": items}


@router.get("/intermediarios")
def intermediarios(threshold: float = 1000, limit: int = 30, driver: Driver = Depends(get_driver)):
    """
    Pessoas com betweenness elevado (potenciais intermediários / laranjas).
    """
    query = """
    MATCH (p:Pessoa)
    WHERE p.gds_betweenness > $threshold
    RETURN p.nome         AS pessoa_nome,
           p.gds_betweenness AS gds_betweenness,
           p.gds_comunidade   AS gds_comunidade,
           p.cpf              AS pessoa
    ORDER BY p.gds_betweenness DESC
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, threshold=threshold, limit=limit)
        items = [record.data() for record in result]
    return {"items": items}
