from fastapi import APIRouter, HTTPException, Query
from cache import cache_get_json, cache_set_json, make_cache_key
from deps import get_driver, run_query

router = APIRouter(prefix="/pessoa", tags=["pessoa"])
_PESSOA_CACHE_TTL = 120


@router.get("/{cpf}")
def get_pessoa(
    cpf: str,
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
):
    cache_key = make_cache_key("pessoa", cpf=cpf, limit=limit, offset=offset)
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    driver = get_driver()
    with driver.session() as s:

        node = run_query(
            s,
            "MATCH (p:Pessoa {cpf: $cpf}) RETURN p", cpf=cpf
        ).single()
        if not node:
            raise HTTPException(404, f"Pessoa não encontrada: {cpf}")
        pessoa = dict(node["p"])

        socios = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:SOCIO_DE]->(e:Empresa)
            RETURN e.cnpj_basico AS cnpj_basico, e.razao_social AS razao_social,
                   e.situacao_cadastral AS situacao, e.uf AS uf,
                   r.qualificacao AS qualificacao, r.data_entrada AS data_entrada
            ORDER BY r.data_entrada DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        servidor = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[:EH_SERVIDOR]->(srv:Servidor)
            RETURN srv
            LIMIT 1
            """,
            cpf=cpf,
        ).single()

        candidaturas = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[c:CANDIDATO_EM]->(e:Eleicao)
            OPTIONAL MATCH (p)-[f:FILIADA_A {ano: e.ano}]->(part:Partido)
            RETURN e.ano AS ano,
                   c.ds_cargo AS cargo,
                   e.sg_uf AS uf,
                   c.ds_situacao AS situacao,
                   c.nm_urna AS nome_urna,
                   part.sigla AS partido
            ORDER BY e.ano DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        sancoes_indir = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:SOCIO_DE]->(e:Empresa)-[:POSSUI_SANCAO]->(san:Sancao)
            RETURN e.razao_social AS empresa, san.tipo_sancao AS tipo,
                   san.data_inicio AS inicio, san.motivo_sancao AS motivo
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        duplicatas = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:MESMO_QUE]-(p2:Pessoa)
            RETURN p2.cpf AS cpf, p2.nome AS nome,
                   r.score AS score, r.confianca AS confianca
            ORDER BY r.score DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        # Busca parlamentar vinculado (pode ser a mesma pessoa se for senador/deputado)
        # Estratégias: 1) MESMO_QUE, 2) CPF direto, 3) Título eleitoral, 4) Nome similar via token intersection
        parlamentar = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[r:MESMO_QUE]-(par:Parlamentar)
            RETURN par.codigo_autor AS parlamentar_id,
                   par.nome_autor AS nome_parlamentar
            LIMIT 1
            """,
            cpf=cpf,
        ).single()

        if not parlamentar:
            parlamentar = run_query(
                s,
                """
                MATCH (par:Parlamentar {cpf: $cpf})
                RETURN par.codigo_autor AS parlamentar_id,
                       par.nome_autor AS nome_parlamentar
                LIMIT 1
                """,
                cpf=cpf,
            ).single()

        if not parlamentar:
            parlamentar = run_query(
                s,
                """
                MATCH (p:Pessoa {cpf: $cpf})
                MATCH (par:Parlamentar {nr_titulo_eleitoral: p.nr_titulo_eleitoral})
                RETURN par.codigo_autor AS parlamentar_id,
                       par.nome_autor AS nome_parlamentar
                LIMIT 1
                """,
                cpf=cpf,
            ).single()

        if not parlamentar:
            parlamentar = run_query(
                s,
                """
                MATCH (p:Pessoa {cpf: $cpf})
                MATCH (par:Parlamentar)
                WITH p, par, size(apoc.coll.intersection(
                  [w IN split(toLower(p.nome), ' ') WHERE w > ''],
                  [w IN split(toLower(par.nome_autor), ' ') WHERE w > '']
                )) AS common
                WHERE common >= 1
                RETURN par.codigo_autor AS parlamentar_id,
                       par.nome_autor AS nome_parlamentar
                ORDER BY common DESC
                LIMIT 1
                """,
                cpf=cpf,
            ).single()

        sancoes_diretas = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[:POSSUI_SANCAO]->(san:Sancao)
            RETURN san.tipo_sancao AS tipo, san.data_inicio AS inicio,
                   san.motivo_sancao AS motivo, san.orgao_sancionador AS orgao
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        bens_declarados = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[rel:DECLAROU_BEM]->(b:BemDeclarado)
            RETURN b.tipo_bem AS tipo, b.descricao AS descricao,
                   b.valor AS valor, b.ano AS ano
            ORDER BY b.ano DESC, b.valor DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        doacoes_feitas = run_query(
            s,
            """
            MATCH (p:Pessoa {cpf: $cpf})-[d:DOOU_PARA]->(cand:Pessoa)
            RETURN cand.nome AS candidato, cand.cpf AS cpf_candidato,
                   d.valor AS valor, d.ano AS ano
            ORDER BY d.ano DESC, d.valor DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

        doacoes_recebidas = run_query(
            s,
            """
            MATCH (doador)-[d:DOOU_PARA]->(p:Pessoa {cpf: $cpf})
            RETURN coalesce(doador.nome, doador.nome_doador) AS doador,
                   labels(doador)[0] AS tipo_doador, d.valor AS valor, d.ano AS ano
            ORDER BY d.ano DESC, d.valor DESC
            SKIP $offset
            LIMIT $limit
            """,
            cpf=cpf, offset=offset, limit=limit,
        ).data()

    payload = {
        "pagination": {"limit": limit, "offset": offset},
        "pessoa":           pessoa,
        "socios":           socios,
        "servidor":         dict(servidor["srv"]) if servidor else None,
        "candidaturas":     candidaturas,
        "sancoes_diretas":  sancoes_diretas,
        "sancoes_indiretas": sancoes_indir,
        "duplicatas":       duplicatas,
        "parlamentar":      dict(parlamentar) if parlamentar else None,
        "bens_declarados":  bens_declarados,
        "doacoes_feitas":   doacoes_feitas,
        "doacoes_recebidas": doacoes_recebidas,
    }
    cache_set_json(cache_key, payload, ttl_seconds=_PESSOA_CACHE_TTL)
    return payload
