from fastapi import APIRouter, Query
from deps import get_driver

router = APIRouter(prefix="/search", tags=["search"])

_LABEL_ID = {
    "Pessoa":      "cpf",
    "Empresa":     "cnpj_basico",
    "Partner":     "partner_id",
    "Parlamentar": "id",
    "Servidor":    "id",
    "Municipio":   "id",
    "Estado":      "sigla",
    "Partido":     "sigla",
    "Emenda":      "codigo_emenda",
    "Contrato":    "contrato_id",
    "Sancao":      "id",
    "Eleicao":     "id",
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
):
    driver = get_driver()
    with driver.session() as s:
        result = s.run(
            """
            CALL db.index.fulltext.queryNodes('entidade_busca', $q)
            YIELD node, score
            RETURN node, score, labels(node) AS lbls
            ORDER BY score DESC
            LIMIT $limit
            """,
            q=q, limit=limit,
        )
        items = [
            {"score": round(r["score"], 4), **_node_to_dict(r["node"])}
            for r in result
        ]
    return {"q": q, "total": len(items), "items": items}
