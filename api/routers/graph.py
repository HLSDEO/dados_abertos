from fastapi import APIRouter, Query, HTTPException
from deps import get_driver, run_query

router = APIRouter(prefix="/graph", tags=["graph"])


_LABEL_KEY = {
    "Pessoa":      "cpf",
    "Empresa":     "cnpj_basico",
    "Partner":     "partner_id",
    "Parlamentar": "id",
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


_NODE_DISPLAY = ["nome", "razao_social", "nome_autor", "sigla", "codigo_emenda",
                 "cpf", "cnpj_basico", "cnpj", "uf", "situacao_cadastral",
                 "gds_pagerank", "gds_comunidade", "gds_betweenness",
                 "tipo_despesa", "valor_liquido", "data_emissao", "ano", "mes",
                 "tipo_sancao", "data_inicio", "numero_contrato", "valor_contratado_reais",
                 "produto", "setor_bndes", "descricao", "ds_eleicao", "tipo", "nome_urna",
                 "bem_id", "eleicao_id"]


def _serialize_node(node) -> dict:
    label = list(node.labels)[0] if node.labels else "Node"
    key   = _LABEL_KEY.get(label)
    uid   = f"{label}:{node.get(key)}" if key and node.get(key) else f"eid:{node.element_id}"

    # Prioriza nome/razão social conforme o tipo de nó
    if label == "Eleicao":
        nome = node.get("ds_eleicao") or ""
    elif label == "BemDeclarado":
        nome = node.get("descricao") or ""
    elif label == "ContratoComprasNet":
        nome = node.get("objeto") or ""
    else:
        nome = node.get("nome") or node.get("razao_social")
        if not nome:
            nome = node.get("nome_autor") or node.get("sigla") or ""
    
    razao_social = node.get("razao_social") or ""
    cpf_cnpj = (node.get("cpf") or node.get("cnpj") or node.get("cnpj_basico") or "")

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

# altered by CÉLIO in 24/04/2026 (Proteção contra Supernós)
@router.get("/expand")
def expand(
    label: str = Query(..., description="Label do nó inicial (ex: Pessoa)"),
    id: str    = Query(..., description="Valor da chave única (ex: CPF)"),
    hops: int  = Query(1, ge=1, le=2, description="Profundidade de expansão"),
    max_nodes: int = Query(200, ge=1, le=1000), # Limite ajustado para permitir redes maiores
    offset: int = Query(0, ge=0, le=10000),
):
    key = _LABEL_KEY.get(label)
    if not key:
        raise HTTPException(400, f"Label desconhecido: {label}")

    driver = get_driver()

    with driver.session() as s:
        # 1. Verifica existência e conta o grau (total de conexões) em uma única query
        start = run_query(
            s,
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
            result = run_query(
                s,
                f"""
                MATCH (n:{label} {{{key}: $id}})
                MATCH (n)-[r]-(m)
                RETURN n, r, m
                SKIP $offset
                LIMIT $max
                """,
                id=id, offset=offset, max=applied_limit,
            )
        else:
            result = run_query(
                s,
                f"""
                MATCH (n:{label} {{{key}: $id}})
                MATCH path = (n)-[*1..2]-(m)
                UNWIND relationships(path) AS r
                WITH startNode(r) AS src, endNode(r) AS tgt, r
                RETURN DISTINCT src AS n, r, tgt AS m
                SKIP $offset
                LIMIT $max
                """,
                id=id, offset=offset, max=applied_limit,
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
            "offset": offset,
            "returned_edges": len(edges),
            "returned_nodes": len(nodes_map),
            "degree": degree,
            "is_supernode": is_supernode,
            "limit_applied": applied_limit
        },
    }