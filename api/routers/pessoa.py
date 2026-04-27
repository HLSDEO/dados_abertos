from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/pessoa", tags=["pessoa"])
_PESSOA_CACHE_TTL = 120


@router.get("/{cpf}")
def get_pessoa(
    cpf: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
):
    cache_key = make_cache_key("pessoa", cpf=cpf, limit=limit, offset=offset)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:

        node = run_query(
            s,
            "MATCH (p:Pessoa {cpf: $cpf}) RETURN p", cpf=cpf
        ).single()
        if not node:
            raise HTTPException(404, f"Pessoa não encontrada: {cpf}")
        pessoa = dict(node["p"])

        socios = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:SOCIO_DE]->(e:Empresa)
            RETURN e.cnpj_basico AS cnpj_basico, e.razao_social AS razao_social,
                   e.situacao_cadastral AS situacao, e.uf AS uf,
                   r.qualificacao AS qualificacao, r.data_entrada AS data_entrada
            ORDER BY r.data_entrada DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        servidor = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[:EH_SERVIDOR]->(srv:Servidor)
            RETURN srv
            LIMIT 1
            """,
            cpf=cpf,
        ).single()

        candidaturas = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[c:CANDIDATO_EM]->(el:Eleicao)
            RETURN el.ano AS ano, el.cargo AS cargo, el.uf AS uf,
                   c.situacao AS situacao, c.nome_urna AS nome_urna,
                   c.partido AS partido
            ORDER BY el.ano DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        sancoes_indir = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:SOCIO_DE]->(e:Empresa)-[:POSSUI_SANCAO]->(san:Sancao)
            RETURN e.razao_social AS empresa, san.tipo_sancao AS tipo,
                   san.data_inicio AS inicio, san.motivo_sancao AS motivo
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        duplicatas = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:MESMO_QUE]-(p2:Pessoa)
            RETURN p2.cpf AS cpf, p2.nome AS nome,
                   r.score AS score, r.confianca AS confianca
            ORDER BY r.score DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        # Verifica se a pessoa é parlamentar (via MESMO_QUE com Parlamentar)
        parlamentar = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:MESMO_QUE]-(par:Parlamentar)
            RETURN par.codigo_autor AS parlamentar_id,
                   par.nome_autor AS nome_parlamentar
            LIMIT 1
            """,
            cpf=cpf,
        ).single()

        # Se não encontrou via MESMO_QUE, tenta buscar Parlamentar com mesmo CPF
        if not parlamentar:
            parlamentar = run_query(
                s,
                """
                MATCH (par:Parlamentar {cpf: $cpf})
                RETURN par.codigo_autor AS parlamentar_id,
                       par.nome_autor AS nome_parlamentar
                LIMIT 1
                """,
                cpf=cpf,
            ).single()

    payload = {
        "pagination": {"limit": limit, "offset": offset},
        "pessoa":           pessoa,
        "socios":           socios,
        "servidor":         dict(servidor["srv"]) if servidor else None,
        "candidaturas":     candidaturas,
        "sancoes_indiretas": sancoes_indir,
        "duplicatas":       duplicatas,
        "parlamentar":      dict(parlamentar) if parlamentar else None,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_PESSOA_CACHE_TTL)
    return payload
