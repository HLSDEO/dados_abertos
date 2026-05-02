from fastapi import APIRouter, Query, HTTPException
from deps import get_driver, run_query

router = APIRouter(prefix="/graph", tags=["graph"])


_LABEL_KEY = {
    "Pessoa":      "cpf",
    "Empresa":     "cnpj_basico",
    "Partner":     "partner_id",
    "Parlamentar": "codigo_autor",
    "Servidor":    "id",
    "Municipio":   "id",
    "Regiao":      "id",
    "Mesorregiao": "id",
    "Microrregiao":"id",
    "Esfera":"no_esfera",
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
    "Orgao":       "id_orgao",
    "UnidadeGestora":"cd_uasg",
    "DividaAtiva": "divida_id",
    "Convenio": "numero_convenio",
    "FuncaoOrcamentaria": "codigo_funcao"
}


_NODE_DISPLAY = ["nome", "razao_social", "nome_autor", "sigla", "codigo_emenda", "codigo_autor", "nome_autor",
                 "cpf", "cnpj_basico", "cnpj", "uf", "situacao_cadastral",
                 "gds_pagerank", "gds_comunidade", "gds_betweenness", 
                 "tipo_despesa", "valor_liquido", "data_emissao", "ano", "mes",
                 "tipo_sancao", "data_inicio", "numero_contrato", "valor_contratado_reais",
                 "produto", "setor_bndes", "descricao", "ds_eleicao", "tipo", "nome_urna",
                 "bem_id", "eleicao_id", "nome_funcao", "objeto"]


def _serialize_node(node) -> dict:
    label = list(node.labels)[0] if node.labels else "Node"
    key   = _LABEL_KEY.get(label)
    
    # 1. PRIMEIRO: TENTAMOS DESCOBRIR O NOME DO NÓ
    if label == "Eleicao":
        nome = node.get("ds_eleicao") or ""
    elif label == "BemDeclarado":
        nome = node.get("descricao") or ""
    elif label == "Parlamentar":
        nome = node.get("nome_autor") or node.get("nome_parlamentar") or ""
    elif label == "ContratoComprasNet":
        nome = node.get("objeto") or ""
    elif label == "Despesa":
        nome = node.get("tipo_despesa") or node.get("nome_favorecido") or "Despesa"
    elif label == "Emprestimo":
        nome = node.get("produto") or node.get("descricao_do_projeto") or "Empréstimo"
    else:
        nome = node.get("nome") or node.get("razao_social")
        if not nome:
            nome = node.get("nome_autor") or node.get("sigla") or ""

    # 2. TENTAMOS ACHAR O ID OFICIAL NO BANCO
    node_key_value = node.get(key) if key else None
    if not node_key_value:
        if label == "Parlamentar":
            node_key_value = node.get("id_camara") or node.get("cpf") or node.get("id_senado")

    # 3. A MÁGICA DO FALLBACK (ONDE ENTRA O SEU AJUSTE)
    if node_key_value:
        # Cenário A: Achou um ID oficial (Ex: "Parlamentar:4606")
        uid = f"{label}:{node_key_value}"
    elif nome:
        # Cenário B: Não tem ID, mas TEM NOME! Usamos o nome como ID.
        # Higienizamos o nome trocando espaços por underline e removendo ':'
        nome_limpo = str(nome).replace(" ", "_").replace(":", "-")
        uid = f"{label}:{nome_limpo}"
    else:
        # Cenário C: Falha catastrófica (Sem ID e Sem Nome). Usa o interno do Neo4j.
        safe_eid = str(node.element_id).replace(":", "-")
        uid = f"eid:{safe_eid}"
    
    # 4. PREENCHIMENTO DO RESTO DAS PROPRIEDADES
    razao_social = node.get("razao_social") or ""
    cpf_cnpj = (node.get("cpf") or node.get("cnpj") or node.get("cnpj_basico") or "")

    props = {k: node.get(k) for k in _NODE_DISPLAY if node.get(k) is not None}
    if nome and "nome" not in props:
        props["nome"] = nome
    if razao_social and "razao_social" not in props:
        props["razao_social"] = razao_social

    # Se por acaso o nome ficou vazio, tenta usar o próprio valor da chave como quebra-galho visual
    nome_final = nome or str(node_key_value) if node_key_value else nome

    return {
        "uid":          uid,
        "label":        label,
        "nome":         nome_final,
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
            # ---> ADICIONE ESTE PRINT PARA DEPURAÇÃO <---
            print("Nó Fonte (n):", dict(record["n"]))
            print("Nó Alvo (m):", dict(record["m"]))
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