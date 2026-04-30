from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/sancao", tags=["sancao"])
_SANCAO_CACHE_TTL = 120


@router.get("/{sancao_id}")
def get_sancao(
    sancao_id: str,
):
    cache_key = make_cache_key("sancao", sancao_id=sancao_id)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:
        # No Neo4j, o ID da sanção pode ser o ID interno ou uma propriedade específica.
        # Como o frontend passa o nodeId (que no Neo4j é o uid), vamos buscar por uid.
        node = run_query(
            s,
            "MATCH (s:Sancao) WHERE s.uid = $sancao_id RETURN s", sancao_id=sancao_id
        ).single()
        
        if not node:
            # Tenta buscar por id se uid não funcionar
            node = run_query(
                s,
                "MATCH (s:Sancao) WHERE s.id = $sancao_id RETURN s", sancao_id=sancao_id
            ).single()

        if not node:
            raise HTTPException(404, f"Sanção não encontrada: {sancao_id}")
        
        sancao = dict(node["s"])

        empresa = run_query(
            s,
            """
            MATCH (e:Empresa)-[:POSSUI_SANCAO]->(s:Sancao)
            WHERE s.uid = $sancao_id OR s.id = $sancao_id
            RETURN e.cnpj_basico AS cnpj, e.razao_social AS razao_social
            """,
            sancao_id=sancao_id,
        ).single()
        
        if empresa:
            empresa = dict(empresa)
        else:
            empresa = {}

    payload = {
        "sancao": sancao,
        "empresa": empresa,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_SANCAO_CACHE_TTL)
    return payload
