from fastapi import APIRouter, HTTPException
from deps import get_driver

router = APIRouter(prefix="/parlamentar", tags=["parlamentar"])


@router.get("/{parlamentar_id}")
def get_parlamentar(parlamentar_id: str):
    driver = get_driver()
    with driver.session() as s:

        # Tenta primeiro por id_camara (Câmara), depois por id, depois por cpf
        node = s.run(
            "MATCH (p:Parlamentar {id_camara: $id}) RETURN p", id=parlamentar_id
        ).single()
        if not node:
            node = s.run(
                "MATCH (p:Parlamentar {id: $id}) RETURN p", id=parlamentar_id
            ).single()
        if not node:
            node = s.run(
                "MATCH (p:Parlamentar {cpf: $id}) RETURN p", id=parlamentar_id
            ).single()
        if not node:
            raise HTTPException(404, f"Parlamentar não encontrado: {parlamentar_id}")
        parlamentar = dict(node["p"])

        emendas = s.run(
            """
            MATCH (p:Parlamentar {id: $id})-[:AUTORA_DE]->(em:Emenda)
            OPTIONAL MATCH (em)-[:DESTINADA_A]->(m:Municipio)
            RETURN em.codigo_emenda AS codigo, em.ano AS ano,
                   em.valor_empenhado AS valor_empenhado,
                   em.valor_pago AS valor_pago,
                   em.nome_favorecido AS favorecido,
                   em.cnpj_favorecido AS cnpj_favorecido,
                   m.nome AS municipio
            ORDER BY em.ano DESC, em.valor_empenhado DESC
            LIMIT 200
            """,
            id=parlamentar_id,
        ).data()

        empresas_beneficiadas = s.run(
            """
            MATCH (p:Parlamentar {id: $id})-[:AUTORA_DE]->(em:Emenda)
            MATCH (em)-[:DESTINADA_A]->(m:Municipio)
            MATCH (e:Empresa)-[:LOCALIZADA_EM]->(m)
            RETURN e.cnpj_basico AS cnpj_basico, e.razao_social AS razao_social,
                   e.situacao_cadastral AS situacao,
                   count(em) AS qtd_emendas,
                   sum(em.valor_empenhado) AS total_empenhado
            ORDER BY total_empenhado DESC
            LIMIT 50
            """,
            id=parlamentar_id,
        ).data()

        doadores = s.run(
            """
            MATCH (p:Parlamentar {id: $id})<-[:DOADOR_A]-(doador)
            RETURN labels(doador) AS tipo, doador.nome AS nome,
                   doador.cpf AS cpf, doador.cnpj_basico AS cnpj,
                   count(*) AS qtd_doacoes
            ORDER BY qtd_doacoes DESC
            LIMIT 50
            """,
            id=parlamentar_id,
        ).data()

        empresas_com_sancao = s.run(
            """
            MATCH (p:Parlamentar {id: $id})-[:AUTORA_DE]->(em:Emenda)
            MATCH (em)-[:DESTINADA_A]->(m:Municipio)
            MATCH (e:Empresa)-[:LOCALIZADA_EM]->(m)
            MATCH (e)-[:POSSUI_SANCAO]->(san:Sancao)
            RETURN e.cnpj_basico AS cnpj_basico, e.razao_social AS razao_social,
                   san.tipo_sancao AS tipo_sancao, san.data_inicio_sancao AS inicio
            ORDER BY san.data_inicio_sancao DESC
            LIMIT 30
            """,
            id=parlamentar_id,
        ).data()

    return {
        "parlamentar":          parlamentar,
        "emendas":              emendas,
        "empresas_beneficiadas": empresas_beneficiadas,
        "doadores":             doadores,
        "empresas_com_sancao":  empresas_com_sancao,
    }
