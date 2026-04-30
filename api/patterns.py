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
            MATCH (f:Fornecedor) WHERE f.ni_fornecedor STARTS WITH emp.cnpj_basico
            MATCH (c:ContratoComprasNet)-[:CELEBRADO_COM]->(f)
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
                      tipo:  "ContratoComprasNet",
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
            MATCH (emp:Empresa {cnpj_basico: $cnpj})
            MATCH (f:Fornecedor) WHERE f.ni_fornecedor STARTS WITH emp.cnpj_basico
            MATCH (c:ContratoComprasNet)-[:CELEBRADO_COM]->(f)
            WHERE c.orgao_codigo IS NOT NULL AND c.valor_global > 0
            WITH c.orgao_codigo AS orgao, c.orgao_nome AS nome_orgao,
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
                        tipo:  "ContratoComprasNet",
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
            MATCH (emp:Empresa {cnpj_basico: $cnpj})
            MATCH (f:Fornecedor) WHERE f.ni_fornecedor STARTS WITH emp.cnpj_basico
            MATCH (c:ContratoComprasNet)-[:CELEBRADO_COM]->(f)
            WHERE c.orgao_codigo IS NOT NULL
              AND c.valor_global IS NOT NULL
              AND c.valor_global > 0
              AND c.valor_global < 80000
            WITH c.orgao_codigo AS orgao, c.orgao_nome AS nome_orgao,
                  count(c) AS cnt, sum(c.valor_global) AS total_valor, avg(c.valor_global) AS media
            WHERE cnt >= 5
            RETURN count(orgao) AS count,
                    sum(total_valor) AS valor_total,
                    collect({
                        tipo:  "ContratoComprasNet",
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
            MATCH (emp:Empresa {cnpj_basico: $cnpj})
            MATCH (f:Fornecedor) WHERE f.ni_fornecedor STARTS WITH emp.cnpj_basico
            MATCH (:ContratoComprasNet)-[:CELEBRADO_COM]->(f)
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

    {
        "id": "debtor_contracts",
        "name_pt": "Inadimplente recebendo contrato público",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:POSSUI_DIVIDA]->(d:DividaAtiva)
            WHERE d.situacao = 'Ativa' OR d.situacao = 'Aberta'
            MATCH (f:Fornecedor) WHERE f.ni_fornecedor STARTS WITH emp.cnpj_basico
            MATCH (c:ContratoComprasNet)-[:CELEBRADO_COM]->(f)
            WHERE c.data_assinatura >= d.data_inscricao
            WITH count(DISTINCT c) AS count,
                   sum(c.valor_global) AS valor_total,
                   collect(DISTINCT {
                       tipo:  "DividaAtiva",
                       id:    d.divida_id,
                       label: d.tipo_credito + " (R$ " + toString(d.valor_consolidado) + ")"
                   })[..3] AS ev_divida,
                   collect(DISTINCT {
                       tipo:  "ContratoComprasNet",
                       id:    c.contrato_id,
                       label: c.orgao_nome + " — R$ " + toString(c.valor_global) + " (" + c.data_assinatura + ")"
                   })[..5] AS ev_contratos
            WHERE count > 0
            RETURN count, valor_total, ev_divida + ev_contratos AS evidence
        """,
    },

    {
        "id": "expense_supplier_overlap",
        "name_pt": "Parlamentar gasta CEAP com empresa que recebe emenda",
        "risk_level": "medium",
        "cypher": """
            MATCH (par:Parlamentar)-[:AUTORA_DE]->(em:Emenda)-[:DESTINADA_A]->(m:Municipio)
            MATCH (emp:Empresa)-[:LOCALIZADA_EM]->(m)
            MATCH (par)-[:GASTOU]->(d:Despesa)
            WHERE (d.nome_fornecedor CONTAINS emp.razao_social
                  OR (d.cnpj_fornecedor IS NOT NULL AND emp.cnpj_basico = d.cnpj_fornecedor))
            WITH par, emp, count(DISTINCT em) AS emendas_count,
                  sum(em.valor_pago) AS valor_emendas,
                  collect(DISTINCT {
                      tipo:  "Emenda",
                      id:    em.codigo_emenda,
                      label: "R$ " + toString(em.valor_pago) + " (" + em.ano + ")"
                  })[..3] AS ev_emendas,
                  collect(DISTINCT {
                      tipo:  "Despesa",
                      id:    d.despesa_id,
                      label: d.tipo_despesa + " — R$ " + toString(d.valor_liquido) + " (" + toString(d.mes) + "/" + toString(d.ano) + ")"
                  })[..5] AS ev_despesas
            WHERE emendas_count > 0
            RETURN count(DISTINCT par) AS count,
                   valor_emendas AS valor_total,
                   ev_emendas + ev_despesas AS evidence
        """,
    },

    {
        "id": "bndes_sanction_overlap",
        "name_pt": "Empresa recebe BNDES e está sancionada",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[:RECEBEU_EMPRESTIMO]->(e:Emprestimo)
            MATCH (emp)-[:POSSUI_SANCAO]->(s:Sancao)
            WITH count(DISTINCT e) AS count,
                   sum(e.valor_contratado_reais) AS valor_total,
                   collect(DISTINCT {
                       tipo:  "Emprestimo",
                       id:    e.emprestimo_id,
                       label: e.produto + " — R$ " + toString(e.valor_contratado_reais) + " (" + e.data_da_contratacao + ")"
                   })[..3] AS ev_emprestimos,
                   collect(DISTINCT {
                       tipo:  "Sancao",
                       id:    s.sancao_id,
                       label: s.tipo_sancao + " (" + s.data_inicio + "→" + coalesce(s.data_fim,"vigente") + ")"
                   })[..3] AS ev_sancoes
            WHERE count > 0
            RETURN count, valor_total, ev_emprestimos + ev_sancoes AS evidence
        """,
    },

    {
        "id": "enrichment_signal",
        "name_pt": "Sócio servidor com patrimônio declarado suspeito",
        "risk_level": "medium",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})<-[:SOCIO_DE]-(p:Pessoa)
            WHERE EXISTS { (p)-[:EH_SERVIDOR]->(:Servidor) }
              AND EXISTS { (p)-[:DECLAROU_BEM]->(:BemDeclarado) }
            MATCH (p)-[:EH_SERVIDOR]->(srv:Servidor)
            MATCH (p)-[:DECLAROU_BEM]->(b:BemDeclarado)
            WITH p, srv, sum(b.valor) AS total_bens,
                 collect(DISTINCT {
                     tipo:  "Servidor",
                     id:    srv.id_servidor,
                     label: srv.nome + " — " + srv.cargo + " (" + srv.org_exercicio + ")"
                 })[..3] AS ev_servidor,
                 collect(DISTINCT {
                     tipo:  "BemDeclarado",
                     id:    b.bem_id,
                     label: b.tipo_bem + " — R$ " + toString(b.valor) + " (" + toString(b.ano_eleicao) + ")"
                 })[..5] AS ev_bens
            WHERE total_bens > 1500000
            RETURN count(DISTINCT p) AS count,
                   total_bens AS valor_total,
                   ev_servidor + ev_bens AS evidence
        """,
    },

    # ── LOW RISK ─────────────────────────────────────
    {
        "id": "donation_contract",
        "name_pt": "Empresa doadora de campanha com contratos públicos (correlação)",
        "risk_level": "low",
        "cypher": """
            MATCH (emp:Empresa {cnpj_basico: $cnpj})-[d:DOOU_PARA]->(cand:Pessoa)
            MATCH (f:Fornecedor) WHERE f.ni_fornecedor STARTS WITH emp.cnpj_basico
            MATCH (c:ContratoComprasNet)-[:CELEBRADO_COM]->(f)
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
