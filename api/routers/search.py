from fastapi import APIRouter, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/search", tags=["search"])
_SEARCH_CACHE_TTL = 30

_LABEL_ID = {
    "Pessoa":      "cpf",
    "Empresa":     "cnpj_basico",
    "Partner":     "partner_id",
    "Parlamentar": "codigo_autor",
    "Servidor":    "id",
    "Municipio":   "id",
    "Regiao":      "id",
    "Mesorregiao": "id",
    "Microrregiao":"id",
    "Estado":      "sigla",
    "Partido":     "sigla",
    "Emenda":      "codigo_emenda",
    "ContratoComprasNet": "contrato_id",
    "Sancao":      "sancao_id",
    "Eleicao":     "eleicao_id",
    "BemDeclarado": "bem_id",
    "Emprestimo":  "emprestimo_id",
    "Fornecedor":  "ni_fornecedor",
    "Despesa":     "despesa_id",
    "Licitacao":   "numero_controle",
    "ItemResultado":"item_id",
    "Orgao":       "orgao_cnpj",
    "UnidadeGestora":"ug_codigo",
    "DividaAtiva": "divida_id",
}


def _node_to_dict(node) -> dict:
    label = list(node.labels)[0] if node.labels else "Node"
    id_prop = _LABEL_ID.get(label)
    return {
        "id":    node.get(id_prop) if id_prop else None,
        "label": label,
        "nome":  node.get("nome") or node.get("razao_social") or node.get("nome_autor") or "",
        **{k: v for k, v in node.items() if k not in ("nome", "razao_social", "nome_autor")},
    }


@router.get("")
def search(
    q: str = Query(..., min_length=2, description="Texto livre"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
):
    cache_key = make_cache_key("search", q=q, limit=limit, offset=offset)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:
        result = run_query(
            s,
            """
            CALL db.index.fulltext.queryNodes('entidade_busca', $q)
            YIELD node, score
            RETURN node, score, labels(node) AS lbls
            ORDER BY score DESC
            SKIP $offset
            LIMIT $limit
            """,
            q=q, offset=offset, limit=limit,
        )
        items = [
            {"score": round(r["score"], 4), **_node_to_dict(r["node"])}
            for r in result
        ]
    payload = {
        "q": q,
        "offset": offset,
        "limit": limit,
        "returned": len(items),
        "items": items,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_SEARCH_CACHE_TTL)
    return payload
