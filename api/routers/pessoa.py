from fastapi import APIRouter, HTTPException
from deps import get_driver

router = APIRouter(prefix="/pessoa", tags=["pessoa"])


@router.get("/{cpf}")
def get_pessoa(cpf: str):
    driver = get_driver()
    with driver.session() as s:

        node = s.run(
            "MATCH (p:Pessoa {cpf: $cpf}) RETURN p", cpf=cpf
        ).single()
        if not node:
            raise HTTPException(404, f"Pessoa não encontrada: {cpf}")
        pessoa = dict(node["p"])

        socios = s.run(
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:SOCIO_DE]->(e:Empresa)
            RETURN e.cnpj_basico AS cnpj_basico, e.razao_social AS razao_social,
                   e.situacao_cadastral AS situacao, e.uf AS uf,
                   r.qualificacao AS qualificacao, r.data_entrada AS data_entrada
            ORDER BY r.data_entrada DESC
            """,
            cpf=cpf,
        ).data()

        servidor = s.run(
            """
            MATCH (p:Pessoa {cpf: $cpf})-[:EH_SERVIDOR]->(srv:Servidor)
            RETURN srv
            LIMIT 1
            """,
            cpf=cpf,
        ).single()

        candidaturas = s.run(
            """
            MATCH (p:Pessoa {cpf: $cpf})-[c:CANDIDATO_EM]->(el:Eleicao)
            RETURN el.ano AS ano, el.cargo AS cargo, el.uf AS uf,
                   c.situacao AS situacao, c.nome_urna AS nome_urna,
                   c.partido AS partido
            ORDER BY el.ano DESC
            """,
            cpf=cpf,
        ).data()

        sancoes_indir = s.run(
            """
            MATCH (p:Pessoa {cpf: $cpf})-[:SOCIO_DE]->(e:Empresa)-[:POSSUI_SANCAO]->(san:Sancao)
            RETURN e.razao_social AS empresa, san.tipo_sancao AS tipo,
                   san.data_inicio_sancao AS inicio, san.motivo_sancao AS motivo
            LIMIT 50
            """,
            cpf=cpf,
        ).data()

        duplicatas = s.run(
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:MESMO_QUE]-(p2:Pessoa)
            RETURN p2.cpf AS cpf, p2.nome AS nome,
                   r.score AS score, r.confianca AS confianca
            ORDER BY r.score DESC
            """,
            cpf=cpf,
        ).data()

    return {
        "pessoa":           pessoa,
        "socios":           socios,
        "servidor":         dict(servidor["srv"]) if servidor else None,
        "candidaturas":     candidaturas,
        "sancoes_indiretas": sancoes_indir,
        "duplicatas":       duplicatas,
    }
