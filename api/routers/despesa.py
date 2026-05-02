from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/despesa", tags=["despesa"])
_DESPESA_CACHE_TTL = 120


@router.get("/{despesa_id}")
def get_despesa(
    despesa_id: str,
):
    cache_key = make_cache_key("despesa", despesa_id=despesa_id)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:
        node = run_query(
            s,
            "MATCH (d:Despesa) WHERE d.despesa_id = $despesa_id RETURN d", despesa_id=despesa_id
        ).single()

        if not node:
            raise HTTPException(404, f"Despesa não encontrada: {despesa_id}")
        
        despesa = dict(node["d"])

        parlamentar = run_query(
            s,
            """
            MATCH (p:Parlamentar)-[:GASTOU]->(d:Despesa)
            WHERE d.despesa_id = $despesa_id
            RETURN p.codigo_autor AS codigo_autor, p.nome_autor AS nome, p.partido AS partido, p.uf AS uf
            """,
            despesa_id=despesa_id,
        ).single()
        
        if parlamentar:
            parlamentar = dict(parlamentar)
        else:
            parlamentar = {}

        fornecedor = run_query(
            s,
            """
            MATCH (e:Empresa)-[:FORNECEU]->(d:Despesa)
            WHERE d.despesa_id = $despesa_id
            RETURN e.cnpj_basico AS cnpj_basico, e.razao_social AS razao_social
            """,
            despesa_id=despesa_id,
        ).single()
        
        if fornecedor:
            fornecedor = dict(fornecedor)
        else:
            fornecedor = {}

    payload = {
        "despesa": despesa,
        "parlamentar": parlamentar,
        "fornecedor": fornecedor,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_DESPESA_CACHE_TTL)
    return payload
