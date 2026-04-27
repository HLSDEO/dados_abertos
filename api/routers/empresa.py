from fastapi import APIRouter, HTTPException, Query
from deps import get_driver, run_query

router = APIRouter(prefix="/empresa", tags=["empresa"])


@router.get("/{cnpj_basico}")
def get_empresa(
    cnpj_basico: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
):
    driver = get_driver()
    with driver.session() as s:

        node = run_query(
            s,
            "MATCH (e:Empresa {cnpj_basico: $cnpj}) RETURN e", cnpj=cnpj_basico
        ).single()
        if not node:
            raise HTTPException(404, f"Empresa não encontrada: {cnpj_basico}")
        empresa = dict(node["e"])

        socios_pf = run_query(
            s,
            """
            MATCH (p:Pessoa)-[r:SOCIO_DE]->(e:Empresa {cnpj_basico: $cnpj})
            RETURN p.cpf AS cpf, p.nome AS nome,
                   r.qualificacao AS qualificacao, r.data_entrada AS data_entrada
            ORDER BY r.data_entrada DESC
            SKIP $offset
            LIMIT $limit
            """,
            cnpj=cnpj_basico, offset=offset, limit=limit,
        ).data()

        socios_pj = run_query(
            s,
            """
            MATCH (soc:Empresa)-[r:SOCIO_DE]->(e:Empresa {cnpj_basico: $cnpj})
            RETURN soc.cnpj_basico AS cnpj_basico, soc.razao_social AS razao_social,
                   r.qualificacao AS qualificacao, r.data_entrada AS data_entrada
            ORDER BY r.data_entrada DESC
            SKIP $offset
            LIMIT $limit
            """,
            cnpj=cnpj_basico, offset=offset, limit=limit,
        ).data()

        sancoes = run_query(
            s,
            """
            MATCH (e:Empresa {cnpj_basico: $cnpj})-[:POSSUI_SANCAO]->(san:Sancao)
            RETURN san.tipo_sancao AS tipo, san.data_inicio_sancao AS inicio,
                   san.data_fim_sancao AS fim, san.motivo_sancao AS motivo,
                   san.orgao_sancionador AS orgao
            ORDER BY san.data_inicio_sancao DESC
            SKIP $offset
            LIMIT $limit
            """,
            cnpj=cnpj_basico, offset=offset, limit=limit,
        ).data()

        contratos = run_query(
            s,
            """
            MATCH (e:Empresa {cnpj_basico: $cnpj})-[:FIRMOU_CONTRATO|CONTRATOU]-(c:Contrato)
            RETURN c.contrato_id AS id, c.objeto AS objeto,
                   c.valor_global AS valor, c.data_assinatura AS data,
                   c.ano AS ano
            ORDER BY c.data_assinatura DESC
            SKIP $offset
            LIMIT $limit
            """,
            cnpj=cnpj_basico, offset=offset, limit=limit,
        ).data()

        emendas = run_query(
            s,
            """
            MATCH (e:Empresa {cnpj_basico: $cnpj})-[:LOCALIZADA_EM]->(m:Municipio)
            MATCH (em:Emenda)-[:DESTINADA_A]->(m)
            MATCH (parl:Parlamentar)-[:AUTORA_DE]->(em)
            RETURN parl.nome_autor AS parlamentar, em.codigo_emenda AS codigo,
                   em.valor_empenhado AS valor, em.ano AS ano, m.nome AS municipio
            ORDER BY em.ano DESC
            SKIP $offset
            LIMIT $limit
            """,
            cnpj=cnpj_basico, offset=offset, limit=limit,
        ).data()

        similares = run_query(
            s,
            """
            MATCH (e:Empresa {cnpj_basico: $cnpj})-[r:SIMILAR_A]-(e2:Empresa)
            RETURN e2.cnpj_basico AS cnpj_basico, e2.razao_social AS razao_social,
                   r.score AS score
            ORDER BY r.score DESC
            SKIP $offset
            LIMIT $limit
            """,
            cnpj=cnpj_basico, offset=offset, limit=limit,
        ).data()

    return {
        "pagination": {"limit": limit, "offset": offset},
        "empresa":   empresa,
        "socios_pf": socios_pf,
        "socios_pj": socios_pj,
        "sancoes":   sancoes,
        "contratos": contratos,
        "emendas":   emendas,
        "similares": similares,
    }
