from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/emenda", tags=["emenda"])
_EMENDA_CACHE_TTL = 120


@router.get("/{codigo_emenda}")
def get_emenda(
    codigo_emenda: str,
):
    cache_key = make_cache_key("emenda", codigo_emenda=codigo_emenda)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:
        node = run_query(
            s,
            "MATCH (e:Emenda) WHERE e.codigo_emenda = $codigo_emenda RETURN e", codigo_emenda=codigo_emenda
        ).single()

        if not node:
            raise HTTPException(404, f"Emenda não encontrada: {codigo_emenda}")
        
        emenda = dict(node["e"])

        parlamentar = run_query(
            s,
            """
            MATCH (p:Parlamentar)-[:AUTORA_DE]->(e:Emenda)
            WHERE e.codigo_emenda = $codigo_emenda
            RETURN p.id_camara AS id, p.nome_autor AS nome, p.partido AS partido, p.uf AS uf
            """,
            codigo_emenda=codigo_emenda,
        ).single()
        
        if parlamentar:
            parlamentar = dict(parlamentar)
        else:
            parlamentar = {}

        empresas = run_query(
            s,
            """
            MATCH (e:Emenda)-[:BENEFICIOU]->(emp:Empresa)
            WHERE e.codigo_emenda = $codigo_emenda
            RETURN emp.cnpj_basico AS cnpj, emp.razao_social AS razao_social
            LIMIT 10
            """,
            codigo_emenda=codigo_emenda,
        ).data()

    payload = {
        "emenda": emenda,
        "parlamentar": parlamentar,
        "empresas": empresas,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_EMENDA_CACHE_TTL)
    return payload
