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
                 "gds_pagerank", "gds_comunidade", "gds_betweenness",
                 "tipo_despesa", "valor_liquido", "data_emissao", "ano", "mes",
                 "tipo_sancao", "data_inicio", "numero_contrato", "valor_contratado_reais",
                 "produto", "setor_bndes"]


def _serialize_node(node) -> dict:
    label = list(node.labels)[0] if node.labels else "Node"
    key   = _LABEL_KEY.get(label)
    uid   = f"{label}:{node.get(key)}" if key and node.get(key) else f"eid:{node.element_id}"

    # Prioriza nome/razão social conforme o tipo de nó
    nome          = (node.get("nome") or node.get("razao_social")
                       or node.get("nome_autor") or node.get("sigla") or ""
    razao_social  = node.get("razao_social") or ""
    cpf_cnpj       = (node.get("cpf") or node.get("cnpj") or node.get("cnpj_basico") or "")

    # Propriedades completas (todas as que existem no nó)
    props = {k: node.get(k) for k in _NODE_DISPLAY if node.get(k) is not None}
    # Garante que nome e razão social estejam em props
    if nome and "nome" not in props:
        props["nome"] = nome
    if razao_social and "razao_social" not in props:
        props["razao_social"] = razao_social

    return {
        "uid":          uid,
        "label":        label,
        "nome":         nome,
        "razao_social": razao_social,
        "cpf_cnpj":     cpf_cnpj,
        "props":        props,
    }


def _serialize_rel(rel, src_uid: str, tgt_uid: str) -> dict:
    return {
        "source": src_uid,
        "target": tgt_uid,
        "type":   rel.type,
        "props":  dict(rel),
    }


@router.get("/expand")
def expand(
    label: str = Query(..., description="Label do nó inicial (ex: Pessoa)"),
    id: str    = Query(..., description="Valor da chave única (ex: CPF)"),
    hops: int  = Query(1, ge=1, le=2, description="Profundidade de expansão"),
    max_nodes: int = Query(80, ge=1, le=300),
):
    key = _LABEL_KEY.get(label)
    if not key:
        raise HTTPException(400, f"Label desconhecido: {label}")

    driver = get_driver()
    with driver.session() as s:
        # verifica existência
        start = s.run(
            f"MATCH (n:{label} {{{key}: $id}}) RETURN n LIMIT 1", id=id
        ).single()
        if not start:
            raise HTTPException(404, f"{label} não encontrado: {id}")

        if hops == 1:
            result = s.run(
                f"""
                MATCH (n:{label} {{{key}: $id}})
                MATCH (n)-[r]-(m)
                RETURN n, r, m
                LIMIT $max
                """,
                id=id, max=max_nodes,
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
                id=id, max=max_nodes,
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
        "meta": {"label": label, "id": id, "hops": hops},
    }
