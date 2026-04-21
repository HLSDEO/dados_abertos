"""
Registro de padrões de corrupção/irregularidade.

Cada padrão tem:
  id          — identificador único
  name_pt     — nome em português
  risk_level  — "high" | "medium" | "low"
  cypher      — query parametrizada com $cnpj
                Deve retornar: count (int), valor_total (float|null), evidence (list<map>)

Os patterns são executados pelo router /patterns/{cnpj_basico}.
"""

PATTERNS: list[dict] = [

    # ── HIGH RISK ────────────────────────────────────────────────────────────

    {
        "id": "sanctioned_contract",
        "name_pt": "Empresa sancionada recebendo contrato",
        "risk_level": "high",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:POSSUI_SANCAO]->(s:Sancao)
            MATCH (emp)-[:FIRMOU_CONTRATO]->(c:Contrato)
            WHERE s.data_inicio IS NOT NULL
              AND c.data_assinatura IS NOT NULL
              AND c.data_assinatura >= s.data_inicio
              AND (s.data_fim IS NULL OR c.data_assinatura <= s.data_fim)
            WITH count(DISTINCT c) AS count,
                 sum(c.valor_global)  AS valor_total,
                 collect(DISTINCT {
                     tipo:  "Sancao",
                     id:    s.sancao_id,
                     label: s.tipo_sancao + " (" + s.data_inicio + "→" + coalesce(s.data_fim,"vigente") + ")"
                 })[..5] AS ev_sancoes,
                 collect(DISTINCT {
                     tipo:  "Contrato",
                     id:    c.contrato_id,
                     label: c.nome_orgao + " — R$ " + toString(c.valor_global) + " (" + c.data_assinatura + ")"
                 })[..5] AS ev_contratos
            RETURN count, valor_total, ev_sancoes + ev_contratos AS evidence
        """,
    },

    {
        "id": "sanctioned_bid",
        "name_pt": "Empresa sancionada com licitação publicada",
        "risk_level": "high",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:POSSUI_SANCAO]->(s:Sancao)
            WHERE s.data_fim IS NULL
            MATCH (emp)-[:PUBLICOU_LICITACAO]->(l:Licitacao)
            WHERE l.data_publicacao >= s.data_inicio
            WITH count(DISTINCT l) AS count,
                 sum(l.valor_total_estimado) AS valor_total,
                 collect(DISTINCT {
                     tipo:  "Sancao",
                     id:    s.sancao_id,
                     label: s.tipo_sancao + " (em vigor desde " + s.data_inicio + ")"
                 })[..3] AS ev_sancoes,
                 collect(DISTINCT {
                     tipo:  "Licitacao",
                     id:    l.numero_controle,
                     label: l.nome_orgao + " — " + l.objeto
                 })[..5] AS ev_licitacoes
            RETURN count, valor_total, ev_sancoes + ev_licitacoes AS evidence
        """,
    },

    {
        "id": "amendment_owner",
        "name_pt": "Parlamentar destina emenda para empresa onde é sócio",
        "risk_level": "high",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})<-[:BENEFICIOU]-(em:Emenda)<-[:AUTORA_DE]-(par:Parlamentar)
            MATCH (par)-[:MESMO_QUE]->(p:Pessoa)-[:SOCIO_DE]->(emp)
            WITH count(DISTINCT em)   AS count,
                 sum(em.valor_pago)   AS valor_total,
                 collect(DISTINCT {
                     tipo:  "Parlamentar",
                     id:    par.codigo_autor,
                     label: par.nome_autor
                 })[..3] AS ev_parl,
                 collect(DISTINCT {
                     tipo:  "Emenda",
                     id:    em.codigo_emenda,
                     label: "R$ " + toString(em.valor_pago) + " — " + em.tipo_emenda + " " + em.ano_emenda
                 })[..5] AS ev_emendas
            RETURN count, valor_total, ev_parl + ev_emendas AS evidence
        """,
    },

    # ── MEDIUM RISK ──────────────────────────────────────────────────────────

    {
        "id": "contract_concentration",
        "name_pt": "Concentração de contratos em único órgão (≥ 60%)",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:FIRMOU_CONTRATO]->(c:Contrato)
            WHERE c.cnpj_orgao IS NOT NULL AND c.valor_global > 0
            WITH c.cnpj_orgao AS orgao, c.nome_orgao AS nome_orgao,
                 count(c) AS cnt, sum(c.valor_global) AS valor
            WITH collect({orgao: nome_orgao, cnt: cnt, valor: valor}) AS por_orgao,
                 sum(valor) AS total
            WHERE total > 0
            UNWIND por_orgao AS o
            WITH o, total, toFloat(o.valor) / total AS share
            WHERE share >= 0.6
            RETURN count(o)     AS count,
                   sum(o.valor) AS valor_total,
                   collect({
                       tipo:  "Contrato",
                       id:    o.orgao,
                       label: o.orgao + " — " + toString(round(share * 100)) + "% dos contratos (R$ " + toString(o.valor) + ")"
                   }) AS evidence
        """,
    },

    {
        "id": "split_contracts",
        "name_pt": "Possível fracionamento de contratos (múltiplos abaixo do limite de dispensa)",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:FIRMOU_CONTRATO]->(c:Contrato)
            WHERE c.cnpj_orgao IS NOT NULL
              AND c.valor_global IS NOT NULL
              AND c.valor_global > 0
              AND c.valor_global < 80000
            WITH c.cnpj_orgao AS orgao, c.nome_orgao AS nome_orgao,
                 count(c) AS cnt, sum(c.valor_global) AS total_valor, avg(c.valor_global) AS media
            WHERE cnt >= 5
            RETURN count(orgao) AS count,
                   sum(total_valor) AS valor_total,
                   collect({
                       tipo:  "Contrato",
                       id:    orgao,
                       label: nome_orgao + " — " + toString(cnt) + " contratos (média R$ " + toString(round(media)) + ")"
                   })[..5] AS evidence
        """,
    },

    {
        "id": "inexigibility_recurrence",
        "name_pt": "Inexigibilidade recorrente (≥ 3 contratações diretas)",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:FIRMOU_CONTRATO]->(c:Contrato)-[:VINCULADO_A]->(l:Licitacao)
            WHERE toLower(l.amparo_legal_nome) CONTAINS 'inexig'
               OR toLower(l.modalidade_nome)   CONTAINS 'inexig'
            WITH count(DISTINCT l) AS count,
                 sum(c.valor_global) AS valor_total,
                 collect(DISTINCT {
                     tipo:  "Licitacao",
                     id:    l.numero_controle,
                     label: l.nome_orgao + " — " + l.objeto
                 })[..5] AS evidence
            WHERE count >= 3
            RETURN count, valor_total, evidence
        """,
    },

    {
        "id": "servant_company",
        "name_pt": "Servidor público ativo sócio da empresa contratada",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:FIRMOU_CONTRATO]->(:Contrato)
            MATCH (p:Pessoa)-[:SOCIO_DE]->(emp)
            WHERE p.cpf IS NOT NULL
            MATCH (srv:Servidor)
            WHERE srv.cpf = p.cpf
              AND toLower(srv.situacao_vinculo) CONTAINS 'ativo'
            WITH count(DISTINCT srv) AS count,
                 collect(DISTINCT {
                     tipo:  "Servidor",
                     id:    srv.id_servidor,
                     label: srv.nome + " — " + srv.cargo + " / " + srv.org_exercicio
                 })[..5] AS evidence
            WHERE count > 0
            RETURN count, null AS valor_total, evidence
        """,
    },

    # ── LOW RISK ─────────────────────────────────────────────────────────────

    {
        "id": "donation_contract",
        "name_pt": "Empresa doadora de campanha com contratos públicos (correlação)",
        "risk_level": "low",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[d:DOOU_PARA]->(cand:Pessoa)
            MATCH (emp)-[:FIRMOU_CONTRATO]->(c:Contrato)
            WHERE c.data_assinatura >= toString(toInteger(d.ano))
            WITH count(DISTINCT cand) AS count,
                 sum(c.valor_global)  AS valor_total,
                 collect(DISTINCT {
                     tipo:  "Pessoa",
                     id:    cand.cpf,
                     label: cand.nome + " (doação " + toString(d.ano) + " — R$ " + toString(d.valor) + ")"
                 })[..5] AS evidence
            WHERE count > 0
            RETURN count, valor_total, evidence
        """,
    },
]

PATTERN_INDEX: dict[str, dict] = {p["id"]: p for p in PATTERNS}
