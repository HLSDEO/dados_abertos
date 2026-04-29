from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/contrato", tags=["contrato"])
_CONTRATO_CACHE_TTL = 120


@router.get("/{contrato_id}")
def get_contrato(
    contrato_id: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
):
    cache_key = make_cache_key("contrato", contrato_id=contrato_id, limit=limit, offset=offset)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:

        node = run_query(
            s,
            "MATCH (c:ContratoComprasNet {contrato_id: $contrato_id}) RETURN c", contrato_id=contrato_id
        ).single()
        if not node:
            raise HTTPException(404, f"Contrato não encontrado: {contrato_id}")
        contrato = dict(node["c"])

        fornecedor = run_query(
            s,
            "MATCH (c:ContratoComprasNet {contrato_id: $contrato_id})-[:CELEBRADO_COM]->(f:Fornecedor) RETURN f.ni_fornecedor AS ni_fornecedor, f.nome AS nome, f.tipo_pessoa AS tipo_pessoa",
            contrato_id=contrato_id,
        ).single()
        fornecedor = dict(fornecedor) if fornecedor else None

        orgao = run_query(
            s,
            "MATCH (o:Orgao)-[:CELEBRA]->(c:ContratoComprasNet {contrato_id: $contrato_id}) RETURN o.cnpj AS cnpj, o.nome AS nome, o.uf AS uf",
            contrato_id=contrato_id,
        ).single()
        orgao = dict(orgao) if orgao else None

        empenhos = run_query(
            s,
            "MATCH (c:ContratoComprasNet {contrato_id: $contrato_id})-[:PAGO_POR]->(e:Empenho) RETURN e.empenho_id AS id, e.numero_empenho AS numero, e.ano_empenho AS ano, e.valor_empenho AS valor ORDER BY e.ano_empenho DESC, e.numero_empenho DESC SKIP $offset LIMIT $limit",
            contrato_id=contrato_id, offset=offset, limit=limit,
        ).data()

    payload = {
        "pagination": {"limit": limit, "offset": offset},
        "contrato": contrato,
        "fornecedor": fornecedor,
        "orgao": orgao,
        "empenhos": empenhos,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_CONTRATO_CACHE_TTL)
    return payload</content>
<parameter name="filePath">C:\Git\dados_abertos\api\routers\contrato.py