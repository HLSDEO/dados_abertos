# Features Roadmap — DABERTO

Itens organizados por prioridade e dependência. Marcar com `[x]` ao concluir.

---

## Fase 1 — Padrões com dados existentes (~1–2 dias)

### 1.1 Fix pipeline de emendas — `BENEFICIOU → Empresa` (30min)
- [x] Em `etl/pipeline/5-emendas_cgu.py`, criar relação `(:Emenda)-[:BENEFICIOU]->(:Empresa)` usando `cnpj_favorecido` (já presente no CSV)
- Desbloqueia o padrão mais crítico: parlamentar que destina emenda para empresa onde é sócio

### 1.2 Motor de padrões — endpoint `/patterns/{cnpj}` (~1 dia)
- [x] Criar `api/routers/patterns.py` com `GET /patterns/{cnpj}`
- [x] Criar registro de patterns: dict `{id, name_pt, cypher, params}` → `api/patterns.py`
- [x] Retornar `evidence_refs` (IDs dos nós que dispararam o alerta)
- [x] Registrar no `main.py`

#### Padrões implementáveis agora (dados já existem):

| ID | Nome | Cruza |
|---|---|---|
| `sanctioned_contract` | Empresa sancionada recebendo contrato | `Sancao.data_inicio/fim` × `Contrato.data_assinatura` | ✅ |
| `contract_concentration` | Concentração de contratos | `FIRMOU_CONTRATO` agregado por Empresa | ✅ |
| `sanctioned_bid` | Empresa sancionada vencendo licitação | `Sancao` × `Licitacao` | ✅ |
| `amendment_owner` | Parlamentar destina emenda para empresa própria | `AUTORA_DE → Emenda → BENEFICIOU → Empresa ← SOCIO_DE ← Pessoa ← (parlamentar)` | ✅ |
| `split_contracts` | Fracionamento de contratos | Múltiplos contratos < R$80k no mesmo órgão | ✅ |
| `inexigibility_recurrence` | Inexigibilidade recorrente | ≥ 3 contratos diretos via inexigibilidade | ✅ |
| `servant_company` | Servidor ativo sócio da empresa contratada | `Servidor.cpf = Pessoa.cpf` × `SOCIO_DE` × `FIRMOU_CONTRATO` | ✅ |
| `donation_contract` | Empresa doadora com contratos (correlação) | `DOOU_PARA` × `FIRMOU_CONTRATO` | ✅ |

---

## Fase 2 — Novas fontes de dados

### 2.1 Bens declarados TSE (~2–3h)
- [ ] Em `etl/download/3-tse.py`, adicionar download do arquivo `BEM_CANDIDATO_*.zip`
- [ ] Em `etl/pipeline/6-tse.py`, carregar bens:
  - Nó: `(:BemDeclarado {bem_id, tipo_bem, descricao, valor})`
  - Relação: `(:Pessoa)-[:DECLAROU_BEM {ano_eleicao}]->(:BemDeclarado)`
- Habilita: detecção de enriquecimento ilícito (patrimônio declarado × salário público)

### 2.2 PGFN — Dívida Ativa (~4–5h)
- [x] Criar `etl/download/9-pgfn.py`
  - Fonte: https://portaldatransparencia.gov.br/download/pgfn/
  - Formato: CSV ~2GB
- [x] Criar `etl/pipeline/9-pgfn.py`
  - Nó: `(:DividaAtiva {divida_id, tipo, valor_consolidado, situacao, data_inscricao})`
  - Relações:
    - `(:Empresa)-[:POSSUI_DIVIDA]->(:DividaAtiva)`
    - `(:Pessoa)-[:POSSUI_DIVIDA]->(:DividaAtiva)`
- Habilita: padrão `debtor_contracts` — inadimplente recebendo contrato público

### 2.3 BNDES — Empréstimos (~3–4h)
- [x] Criar `etl/download/12-bndes.py`
   - Fonte: https://dadosabertos.bndes.gov.br
   - Formato: CSV via CKAN datastore API
   - Recursos: operações não automáticas + operações indiretas automáticas
- [x] Criar `etl/pipeline/12-bndes.py`
   - Nó: `(:Emprestimo {emprestimo_id, valor_contratado_reais, data_da_contratacao, produto, setor_bndes})`
   - Relação: `(:Empresa {cnpj_basico})-[:RECEBEU_EMPRESTIMO]->(:Emprestimo)`
- [x] Atualizar `etl/main.py` com novos módulos
- Habilita: fluxo completo de dinheiro público (contrato + emenda + empréstimo)

### 2.4 Despesas Câmara dos Deputados (~6–8h)
- [x] Criar `etl/download/11-camara.py`
   - Fonte: https://dadosabertos.camara.leg.br (arquivos ZIP por ano)
   - Formato: CSV
- [x] Criar `etl/pipeline/11-camara.py`
   - Nó: `(:Despesa {despesa_id, tipo_despesa, valor_liquido, data_emissao, ano, mes})`
   - Relações:
     - `(:Parlamentar)-[:GASTOU]->(:Despesa)`
     - `(:Empresa)-[:FORNECEU]->(:Despesa)` — via CNPJ do fornecedor
- [x] Atualizar rota `/pessoa/{cpf}` para incluir ID do parlamentar (se houver)
- Habilita: padrão `parlamentar × fornecedor` — deputado gasta com empresa que recebe emenda dele

### 2.5 Despesas Senado Federal (~4–6h)
- [ ] Criar `etl/download/12-senado.py`
  - Fonte: https://legis.senado.leg.br/dadosabertos (API REST diferente)
- [ ] Criar `etl/pipeline/12-senado.py`
  - Mesmo esquema da Câmara
- Habilita: cobertura completa do Congresso

---

## Fase 3 — Motor de padrões completo (8 patterns, ~1 dia adicional)

Depende das fases 1 e 2. Padrões adicionais:

| ID | Nome | Depende de |
|---|---|---|
| `debtor_contracts` | Inadimplente recebendo contrato | PGFN (2.2) |
| `enrichment_signal` | Patrimônio declarado inconsistente com salário | TSE bens (2.1) + servidores já existente |
| `expense_supplier_overlap` | Parlamentar gasta CEAP com empresa que recebe emenda | Despesas Câmara (2.4) |
| `bndes_sanction_overlap` | Empresa recebe BNDES e está sancionada | BNDES (2.3) |

---

## Fase 4 — Workbench de investigação

### 4.1 Timeline por entidade
- [ ] Endpoint `GET /timeline/{entity_id}` — eventos ordenados por data
  - Contratos, sanções, emendas, candidaturas, cargos, dívidas
  - Cursor-based pagination

### 4.2 Investigações (autenticadas)
- [ ] Auth JWT simples (registro + login)
- [ ] Nó `(:Investigation)` no Neo4j
- [ ] `POST /investigations` — criar investigação
- [ ] `POST /investigations/{id}/entities/{entity_id}` — adicionar entidade
- [ ] `POST /investigations/{id}/annotations` — anotar descoberta
- [ ] `GET /investigations/{id}/export` — exportar como JSON/PDF

### 4.3 Supernode protection
- [ ] No endpoint `/graph/expand`, detectar nós com grau > 500
- [ ] Limitar automaticamente `hops=1` para supernós
- [ ] Retornar `meta.supernode: true` no response

---

## Fase 5 — Fontes complementares (baixa prioridade)

| Fonte | Valor investigativo | Esforço |
|---|---|---|
| TransfereGov convênios | Repasses federais para municípios/ONGs | ~4h |
| Filiação partidária TSE | Histórico de partido por pessoa | ~2h |
| IBAMA embargos ambientais | Empresa embargada × contrato | ~3h |
| ICIJ Offshore Leaks | Estrutura societária offshore | ~6h (dados estáticos) |
| PEP CGU | Lista oficial de expostos politicamente | ~2h |
| CVM processos | Infrações no mercado de capitais | ~4h |

---

## Dependências entre fases

```
1.1 fix emendas
  └→ 1.2 padrão amendment_owner

2.1 bens TSE
  └→ 3 enrichment_signal

2.2 PGFN
  └→ 3 debtor_contracts

2.3 BNDES
  └→ 3 bndes_sanction_overlap

2.4 despesas câmara
  └→ 3 expense_supplier_overlap
  └→ 4.1 timeline (adicionar evento Despesa)
```
