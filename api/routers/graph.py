from fastapi import APIRouter, Query, HTTPException
from deps import get_driver

router = APIRouter(prefix="/graph", tags=["graph"])

_LABEL_KEY = {
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

_NODE_DISPLAY = ["nome", "razao_social", "nome_autor", "sigla", "codigo_emenda",
                 "cpf", "cnpj_basico", "cnpj", "uf", "situacao_cadastral",
                 "gds_pagerank", "gds_comunidade", "gds_betweenness"]


def _serialize_node(node) -> dict:
    label = list(node.labels)[0] if node.labels else "Node"
    key   = _LABEL_KEY.get(label)
    uid   = f"{label}:{node.get(key)}" if key and node.get(key) else f"eid:{node.element_id}"
    nome  = (node.get("nome") or node.get("razao_social")
             or node.get("nome_autor") or node.get("sigla") or "")
    props = {k: node.get(k) for k in _NODE_DISPLAY if node.get(k) is not None}
    return {"uid": uid, "label": label, "nome": nome, "props": props}


def _serialize_rel(rel, src_uid: str, tgt_uid: str) -> dict:
    return {
        "source": src_uid,
        "target": tgt_uid,
        "type":   rel.type,
        "props":  dict(rel),
    }

# altered by CÉLIO in 24/04/2026 (Proteção contra Supernós)
@router.get("/expand")
def expand(
    label: str = Query(..., description="Label do nó inicial (ex: Pessoa)"),
    id: str    = Query(..., description="Valor da chave única (ex: CPF)"),
    hops: int  = Query(1, ge=1, le=2, description="Profundidade de expansão"),
    max_nodes: int = Query(200, ge=1, le=1000), # Limite ajustado para permitir redes maiores
):
    key = _LABEL_KEY.get(label)
    if not key:
        raise HTTPException(400, f"Label desconhecido: {label}")

    driver = get_driver()
    with driver.session() as s:
        # 1. Verifica existência e conta o grau (total de conexões) em uma única query
        start = s.run(
            f"MATCH (n:{label} {{{key}: $id}}) RETURN n, COUNT {{ (n)--() }} as degree LIMIT 1", id=id
        ).single()
        
        if not start:
            raise HTTPException(404, f"{label} não encontrado: {id}")

        degree = start["degree"]
        
        # 2. Lógica de proteção contra supernós
        is_supernode = degree > 500
        applied_limit = 50 if is_supernode else max_nodes

        # 3. Executa a expansão aplicando o limite seguro
        if hops == 1:
            result = s.run(
                f"""
                MATCH (n:{label} {{{key}: $id}})
                MATCH (n)-[r]-(m)
                RETURN n, r, m
                LIMIT $max
                """,
                id=id, max=applied_limit,
            )
        else:
            result = s.run(
                f"""
                MATCH (n:{label} {{{key}: $id}})
                MATCH path = (n)-[*1..2]-(m)
                UNWIND relationships(path) AS r
                WITH startNode(r) AS src, endNode(r) AS tgt, r
                RETURN DISTINCT src AS n, r, tgt AS m
                LIMIT $max
                """,
                id=id, max=applied_limit,
            )

        nodes_map: dict[str, dict] = {}
        edges: list[dict] = []

        for record in result:
            sn = _serialize_node(record["n"])
            tn = _serialize_node(record["m"])
            nodes_map[sn["uid"]] = sn
            nodes_map[tn["uid"]] = tn
            edges.append(_serialize_rel(record["r"], sn["uid"], tn["uid"]))

    return {
        "nodes": list(nodes_map.values()),
        "edges": edges,
        "meta": {
            "label": label, 
            "id": id, 
            "hops": hops,
            "degree": degree,
            "is_supernode": is_supernode,
            "limit_applied": applied_limit
        },
    }