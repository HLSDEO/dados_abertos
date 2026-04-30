from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/emprestimo", tags=["emprestimo"])
_EMPRESTIMO_CACHE_TTL = 120


@router.get("/{emprestimo_id}")
def get_emprestimo(
    emprestimo_id: str,
):
    cache_key = make_cache_key("emprestimo", emprestimo_id=emprestimo_id)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:
        node = run_query(
            s,
            "MATCH (e:Emprestimo {emprestimo_id: $emprestimo_id}) RETURN e", 
            emprestimo_id=emprestimo_id
        ).single()

        if not node:
            raise HTTPException(404, f"Empréstimo não encontrado: {emprestimo_id}")
        
        emprestimo = dict(node["e"])

        empresa = run_query(
            s,
            """
            MATCH (emp:Empresa)-[:RECEBEU_EMPRESTIMO]->(e:Emprestimo {emprestimo_id: $emprestimo_id})
            RETURN emp.cnpj_basico AS cnpj, emp.razao_social AS razao_social
            """,
            emprestimo_id=emprestimo_id,
        ).single()
        
        if empresa:
            empresa = dict(empresa)
        else:
            empresa = {}

    payload = {
        "emprestimo": emprestimo,
        "empresa": empresa,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_EMPRESTIMO_CACHE_TTL)
    return payload
