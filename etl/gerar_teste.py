import pandas as pd
import os
import random
import shutil
from datetime import datetime, timezone

DATA_DIR = "data"

# IDs base
CPF_POLITICO = "99988877766"
CPF_JOAO     = "11111111111"
CPF_MARIA    = "22222222222"

MUNICIPIO_BRASILIA = "5300108"
UF_DF = "DF"

TOTAL_EMPRESAS = 120

# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def rand_cnpj():
    return "".join([str(random.randint(0, 9)) for _ in range(14)])

def rand_cpf():
    return "".join([str(random.randint(0, 9)) for _ in range(11)])

def generate_empresas():
    empresas = []

    for i in range(TOTAL_EMPRESAS):
        cnpj = rand_cnpj()
        empresas.append({
            "cnpj": cnpj,
            "cnpj_basico": cnpj[:8],
            "razao_social": f"EMPRESA_{i}",
            "cenario": i % 6
        })

    return empresas

# ─────────────────────────────────────────
# CNPJ
# ─────────────────────────────────────────

def generate_cnpj_data(empresas):
    print("Gerando CNPJ...")

    base_dir = os.path.join(DATA_DIR, "cnpj/2024-01/csv")
    os.makedirs(base_dir, exist_ok=True)

    emp_rows = []
    est_rows = []
    socio_rows = []
    simples_rows = []

    for e in empresas:

        emp_rows.append({
            "cnpj_basico": e["cnpj_basico"],
            "razao_social": e["razao_social"],
            "natureza_juridica": "2062",
            "capital_social": "100000.00"
        })

        est_rows.append({
            "cnpj_basico": e["cnpj_basico"],
            "cnpj": e["cnpj"],
            "nome_fantasia": e["razao_social"],
            "uf": "DF",
            "municipio_cod": MUNICIPIO_BRASILIA,
            "municipio_nome": "Brasília"
        })

        cpf = rand_cpf()

        if e["cenario"] == 2:
            cpf = CPF_MARIA  # servidor como sócio

        socio_rows.append({
            "cnpj_basico": e["cnpj_basico"],
            "nome_socio": "SOCIO TESTE",
            "cpf_cnpj_socio": cpf
        })

        simples_rows.append({
            "cnpj_basico": e["cnpj_basico"],
            "opcao_simples": "S"
        })

    pd.DataFrame(emp_rows).to_csv(os.path.join(base_dir, "empresas.csv"), index=False)
    pd.DataFrame(est_rows).to_csv(os.path.join(base_dir, "estabelecimentos.csv"), index=False)
    pd.DataFrame(socio_rows).to_csv(os.path.join(base_dir, "socios.csv"), index=False)
    pd.DataFrame(simples_rows).to_csv(os.path.join(base_dir, "simples.csv"), index=False)

# ─────────────────────────────────────────
# IBGE (mantido)
# ─────────────────────────────────────────

def generate_ibge_data():
    print("Gerando dados do IBGE...")
    ibge_dir = os.path.join(DATA_DIR, "ibge")
    os.makedirs(ibge_dir, exist_ok=True)
    
    meta = {
        'fonte_nome': 'IBGE',
        'fonte_descricao': 'Dados Abertos IBGE',
        'fonte_licenca': 'CC-BY 4.0',
        'fonte_url': 'https://www.ibge.gov.br',
        'fonte_coletado_em': datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    # Regiões
    pd.DataFrame([{
        'id': '5', 'sigla': 'CO', 'nome': 'Centro-Oeste', **meta
    }]).to_csv(os.path.join(ibge_dir, "regioes.csv"), index=False)
    
    # Estados
    pd.DataFrame([{
        'id': '53', 'sigla': 'DF', 'nome': 'Distrito Federal', 'regiao_id': '5', **meta
    }]).to_csv(os.path.join(ibge_dir, "estados.csv"), index=False)
    
    # Mesorregiões
    pd.DataFrame([{
        'id': '5301', 'nome': 'Distrito Federal', 'UF_id': '53', **meta
    }]).to_csv(os.path.join(ibge_dir, "mesorregioes.csv"), index=False)

    # Microrregiões
    pd.DataFrame([{
        'id': '53001', 'nome': 'Brasília', 'mesorregiao_id': '5301', **meta
    }]).to_csv(os.path.join(ibge_dir, "microrregioes.csv"), index=False)

    # Municípios
    pd.DataFrame([{
        'id': '5300108', 'nome': 'Brasília', 'uf': 'DF', 'microrregiao_id': '53001', **meta
    }]).to_csv(os.path.join(ibge_dir, "municipios.csv"), index=False)

# ─────────────────────────────────────────
# PNCP
# ─────────────────────────────────────────

def generate_pncp_data(empresas):
    print("Gerando dados do PNCP...")

    pncp_dir = os.path.join(DATA_DIR, "pncp_csv")
    os.makedirs(pncp_dir, exist_ok=True)

    itens = []
    contratos = []
    empenhos = []

    for idx, e in enumerate(empresas):

        contrato_id = f"C{idx}"
        empenho_id = f"E{idx}"

        # ─────────────────────────────────────────
        # CONTRATOS (varia por cenário)
        # ─────────────────────────────────────────

        # cenário 0 → contrato alto (suspeito)
        if e["cenario"] == 0:
            valor = "500000"

        # cenário 1 → fracionamento
        elif e["cenario"] == 1:
            valor = str(random.randint(10000, 70000))

        else:
            valor = str(random.randint(50000, 200000))

        contratos.append({
            "id": contrato_id,
            "receita_despesa": "Despesa",
            "numero": f"{idx:05d}/2025",
            "orgao_codigo": "14000",
            "orgao_nome": "MINISTERIO DA SAUDE",
            "unidade_codigo": "070009",
            "esfera": "Federal",
            "poder": "Executivo",
            "sisg": "Nao",
            "gestao": "00001",
            "unidade_nome_resumido": "MS",
            "unidade_nome": "MINISTERIO DA SAUDE",
            "unidade_origem_codigo": "070009",
            "unidade_origem_nome": "MINISTERIO DA SAUDE",
            "fornecedor_tipo": "JURIDICA",
            "fonecedor_cnpj_cpf_idgener": e["cnpj"],
            "fornecedor_nome": e["razao_social"],
            "codigo_tipo": "50",
            "tipo": "Contrato",
            "categoria": "Compras",
            "processo": f"PROC-{idx}",
            "objeto": "AQUISICAO DE EQUIPAMENTOS",
            "fundamento_legal": "",
            "informacao_complementar": "",
            "codigo_modalidade": "05",
            "modalidade": "Pregao",
            "unidade_compra": "070009",
            "licitacao_numero": f"{90000+idx}/2025",
            "data_assinatura": "2026-01-01",
            "data_publicacao": "2026-01-02",
            "vigencia_inicio": "2026-01-01",
            "vigencia_fim": "2030-01-01",
            "valor_inicial": valor,
            "valor_global": valor,
            "num_parcelas": "1",
            "valor_parcela": valor,
            "valor_acumulado": "0.0",
            "situacao": "Ativo"
        })

        # ─────────────────────────────────────────
        # ITENS (sempre gera)
        # ─────────────────────────────────────────

        itens.append({
            'id_contratacao_pncp': contrato_id,
            'numero_item': '1',
            'ni_fornecedor': e["cnpj"],
            'nome_razao_social_fornecedor': e["razao_social"],
            'quantidade_homologada': '100',
            'valor_unitario_homologado': str(round(float(valor)/100, 2)),
            'orgao_entidade_cnpj': '00000000000191'
        })

        # ─────────────────────────────────────────
        # EMPENHOS (ligados ao contrato)
        # ─────────────────────────────────────────

        empenhos.append({
            "id": empenho_id,
            "unidade": "580003",
            "unidade_nome": "SUBSECRETARIA DE GESTAO",
            "gestao": "00001",
            "numero_empenho": f"2026NE{idx:06d}",
            "data_emissao": "2026-03-01",
            "cpf_cnpj_credor": e["cnpj"],
            "credor": e["razao_social"],
            "fonte_recurso": "1000000000",
            "ptres": "",
            "modalidade_licitacao_siafi": "",
            "naturezadespesa": "339040",
            "naturezadespesa_descricao": "SERVICOS DE TI",
            "planointerno": "ADMPA",
            "planointerno_descricao": "OPERACAO ADMINISTRATIVA",
            "valor_empenhado": valor,
            "valor_aliquidar": "0",
            "valor_liquidado": "0",
            "valor_pago": "0",
            "valor_rpinscrito": "0",
            "valor_rpaliquidar": "0",
            "valor_rpaliquidado": "0",
            "valor_rppago": "0",
            "informacao_complementar": "",
            "sistema_origem": "COMPRASNET",
            "contrato_id": contrato_id,
            "created_at": "2026-03-01",
            "updated_at": "2026-03-01",
            "id_cipi": ""
        })

    # ─────────────────────────────────────────
    # SALVAR CSVs
    # ─────────────────────────────────────────

    pd.DataFrame(itens).to_csv(
        os.path.join(pncp_dir, "itens.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

    pd.DataFrame(contratos).to_csv(
        os.path.join(pncp_dir, "contratos.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

    pd.DataFrame(empenhos).to_csv(
        os.path.join(pncp_dir, "empenhos.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

# ─────────────────────────────────────────
# SANÇÕES
# ─────────────────────────────────────────

def generate_sancoes_data(empresas):
    print("Gerando sanções...")

    dir_ = os.path.join(DATA_DIR, "sancoes_cgu")
    os.makedirs(dir_, exist_ok=True)

    rows = []

    for e in empresas:
        if e["cenario"] == 0:
            rows.append({
                "cpf_cnpj": e["cnpj"],
                "tipo_sancao": "Inidoneidade",
                "data_inicio": "2020-01-01",
                "data_fim": ""
            })

    pd.DataFrame(rows).to_csv(os.path.join(dir_, "ceis.csv"), index=False)

# ─────────────────────────────────────────
# BNDES
# ─────────────────────────────────────────

def generate_bndes_data(empresas):
    print("Gerando dados do BNDES...")

    bndes_dir = os.path.join(DATA_DIR, "bndes")
    os.makedirs(bndes_dir, exist_ok=True)

    rows = []

    for i, e in enumerate(empresas):

        # ─────────────────────────────────────────
        # CENÁRIO 0 → financiamento alto suspeito
        # ─────────────────────────────────────────
        if e["cenario"] == 0:
            rows.append({
                "_id": f"BNDES_{i}",
                "cliente": e["razao_social"],
                "cnpj": e["cnpj"],
                "valor_contratado_reais": "9000000,00",
                "data_da_contratacao": "2024-01-01",
                "produto": "FINEM",
                "uf": "DF"
            })

        # ─────────────────────────────────────────
        # CENÁRIO 1 → múltiplos contratos médios (fragmentação)
        # ─────────────────────────────────────────
        elif e["cenario"] == 1:
            for j in range(3):
                rows.append({
                    "_id": f"BNDES_{i}_{j}",
                    "cliente": e["razao_social"],
                    "cnpj": e["cnpj"],
                    "valor_contratado_reais": f"{random.randint(500000, 2000000)},00",
                    "data_da_contratacao": f"2024-0{j+1}-01",
                    "produto": "FINAME",
                    "uf": "DF"
                })

        # ─────────────────────────────────────────
        # CENÁRIO 2 → empresa com sócio servidor (baixo valor)
        # ─────────────────────────────────────────
        elif e["cenario"] == 2:
            rows.append({
                "_id": f"BNDES_{i}",
                "cliente": e["razao_social"],
                "cnpj": e["cnpj"],
                "valor_contratado_reais": "300000,00",
                "data_da_contratacao": "2024-03-01",
                "produto": "MICROCREDITO",
                "uf": "DF"
            })

        # ─────────────────────────────────────────
        # OUTROS CENÁRIOS → eventualmente sem financiamento
        # ─────────────────────────────────────────
        else:
            if random.random() < 0.3:
                rows.append({
                    "_id": f"BNDES_{i}",
                    "cliente": e["razao_social"],
                    "cnpj": e["cnpj"],
                    "valor_contratado_reais": f"{random.randint(100000, 800000)},00",
                    "data_da_contratacao": "2024-05-01",
                    "produto": "CAPITAL DE GIRO",
                    "uf": "DF"
                })

    pd.DataFrame(rows).to_csv(
        os.path.join(bndes_dir, "operacoes_2024.csv"),
        index=False,
        sep=';',
        encoding='utf-8'
    )

    print(f"✓ BNDES registros: {len(rows)}")

# ─────────────────────────────────────────
# SERVIDORES
# ─────────────────────────────────────────

def generate_servidores_data(empresas):
    print("Gerando dados de Servidores...")

    serv_dir = os.path.join(DATA_DIR, "servidores", "2024", "01")
    os.makedirs(serv_dir, exist_ok=True)

    rows = []

    # ─────────────────────────────────────────
    # BASE FIXA (mantém compatibilidade)
    # ─────────────────────────────────────────
    rows.append({
        'id_servidor': 'S_BASE',
        'cpf': CPF_MARIA,
        'nome': 'MARIA SERVIDORA',
        'cargo': 'ANALISTA',
        'org_exercicio': 'MINISTERIO DA SAUDE',
        'uf_exercicio': 'DF'
    })

    # ─────────────────────────────────────────
    # GERAR VARIAÇÕES (ligação com cenários)
    # ─────────────────────────────────────────
    for i, e in enumerate(empresas):

        # cenário 2 → empresa com sócio servidor
        if e["cenario"] == 2:
            rows.append({
                'id_servidor': f'S{i}',
                'cpf': '11111111111',  # mesmo CPF usado em sócio
                'nome': f'SERVIDOR_VINCULADO_{i}',
                'cargo': random.choice(['ANALISTA', 'GESTOR', 'DIRETOR']),
                'org_exercicio': 'MINISTERIO DA SAUDE',
                'uf_exercicio': 'DF'
            })

        # outros cenários → ruído (dados normais)
        elif random.random() < 0.2:
            rows.append({
                'id_servidor': f'S{i}',
                'cpf': "".join([str(random.randint(0, 9)) for _ in range(11)]),
                'nome': f'SERVIDOR_{i}',
                'cargo': random.choice(['TECNICO', 'ANALISTA']),
                'org_exercicio': random.choice([
                    'MINISTERIO DA SAUDE',
                    'MINISTERIO DA EDUCACAO',
                    'MINISTERIO DA JUSTICA'
                ]),
                'uf_exercicio': random.choice(['DF', 'SP', 'RJ'])
            })

    # ─────────────────────────────────────────
    # SALVAR (mantendo padrão original)
    # ─────────────────────────────────────────
    pd.DataFrame(rows).to_csv(
        os.path.join(serv_dir, "cadastro.csv"),
        index=False,
        sep=',',
        encoding='utf-8-sig'
    )

    print(f"✓ Servidores gerados: {len(rows)}")

# ─────────────────────────────────────────
# EMENDAS
# ─────────────────────────────────────────

def generate_emendas_cgu_data(empresas):
    print("Gerando dados de emendas CGU...")

    base_dir = os.path.join(DATA_DIR, "emendas_cgu")
    os.makedirs(base_dir, exist_ok=True)

    emendas = []
    convenios = []
    despesas = []

    for i, e in enumerate(empresas):

        # gerar apenas para cenários relevantes
        if e["cenario"] not in [3, 4]:
            continue

        cod_emenda = f"2026{str(i).zfill(6)}"
        cod_autor = str(random.randint(1000, 9999))
        nome_autor = "POLITICO TESTE"

        cod_funcao = random.choice(["10", "12", "15"])
        nome_funcao = {
            "10": "Saúde",
            "12": "Educação",
            "15": "Urbanismo"
        }[cod_funcao]

        cod_programa = str(random.randint(5000, 5999))

        # ─────────────────────────────────────────
        # EMENDAS
        # ─────────────────────────────────────────
        emendas.append({
            "Código da Emenda": cod_emenda,
            "Ano da Emenda": "2026",
            "Tipo de Emenda": "Emenda Individual - Transferências com Finalidade Definida",
            "Código do Autor da Emenda": cod_autor,
            "Nome do Autor da Emenda": nome_autor,
            "Número da emenda": str(i).zfill(4),
            "Localidade de aplicação do recurso": "MÚLTIPLO",
            "Código Município IBGE": "",
            "Município": "Múltiplo",
            "Código UF IBGE": "32",
            "UF": "ES",
            "Região": "Sudeste",
            "Código Função": cod_funcao,
            "Nome Função": nome_funcao,
            "Código Subfunção": "302",
            "Nome Subfunção": "Assistência hospitalar",
            "Código Programa": cod_programa,
            "Nome Programa": "PROGRAMA TESTE",
            "Código Ação": "2E90",
            "Nome Ação": "AÇÃO TESTE",
            "Código Plano Orçamentário": "0000",
            "Nome Plano Orçamentário": "PLANO TESTE",
            "Valor Empenhado": str(round(random.uniform(10000, 500000), 2)).replace(".", ","),
            "Valor Liquidado": "0,00",
            "Valor Pago": "0,00",
            "Valor Restos A Pagar Inscritos": "0,00",
            "Valor Restos A Pagar Cancelados": "0,00",
            "Valor Restos A Pagar Pagos": "0,00",
        })

        # ─────────────────────────────────────────
        # CONVÊNIOS (mais frequente em cenário 4)
        # ─────────────────────────────────────────
        if e["cenario"] == 4 or random.random() < 0.5:
            num_conv = str(random.randint(800000, 999999))

            convenios.append({
                "Código da Emenda": cod_emenda,
                "Código Função": cod_funcao,
                "Nome Função": nome_funcao,
                "Código Subfunção": "302",
                "Nome Subfunção": "Assistência hospitalar",
                "Localidade do gasto": "SERRA (ES)",
                "Tipo de Emenda": "Emenda Individual",
                "Data Publicação Convênio": "01/01/2026",
                "Convenente": "PREFEITURA DE SERRA",
                "Objeto Convênio": "AQUISIÇÃO DE EQUIPAMENTOS",
                "Número Convênio": num_conv,
                "Valor Convênio": str(round(random.uniform(10000, 300000), 2)).replace(".", ","),
            })

        # ─────────────────────────────────────────
        # DESPESAS (ligadas à empresa do cenário)
        # ─────────────────────────────────────────
        for _ in range(random.randint(2, 5)):
            despesas.append({
                "Código da Emenda": cod_emenda,
                "Código do Autor da Emenda": cod_autor,
                "Nome do Autor da Emenda": nome_autor,
                "Número da emenda": str(i).zfill(4),
                "Tipo de Emenda": "Emenda Individual",
                "Ano/Mês": "202604",
                "Código do Favorecido": e["cnpj"],  # 🔗 vínculo direto com empresa
                "Favorecido": e["razao_social"],
                "Natureza Jurídica": "Sociedade Empresária Limitada",
                "Tipo Favorecido": "Pessoa Jurídica",
                "UF Favorecido": "ES",
                "Município Favorecido": "SERRA",
                "Valor Recebido": str(round(random.uniform(1000, 20000), 2)).replace(".", ","),
            })

    # ─────────────────────────────────────────
    # SALVAR (mantendo padrão original)
    # ─────────────────────────────────────────
    pd.DataFrame(emendas).to_csv(os.path.join(base_dir, "emendas.csv"), index=False)
    pd.DataFrame(convenios).to_csv(os.path.join(base_dir, "convenios.csv"), index=False)
    pd.DataFrame(despesas).to_csv(os.path.join(base_dir, "por_favorecido.csv"), index=False)

    print(f"✓ Emendas: {len(emendas)}")
    print(f"✓ Convênios: {len(convenios)}")
    print(f"✓ Despesas: {len(despesas)}")

# ─────────────────────────────────────────
# CAMARA
# ─────────────────────────────────────────

def generate_camara_data(empresas):
    print("Gerando dados da Câmara (CSV)...")

    camara_dir = os.path.join(DATA_DIR, "camara")
    os.makedirs(camara_dir, exist_ok=True)

    rows = []

    for i, e in enumerate(empresas):

        # ─────────────────────────────────────────
        # CENÁRIO 4 → uso de verba parlamentar (suspeito)
        # ─────────────────────────────────────────
        if e["cenario"] == 4:
            for j in range(random.randint(3, 8)):
                rows.append({
                    "despesa_id": f"CAM_{i}_{j}",
                    "tipo_despesa": "DIVULGACAO",
                    "valor_liquido": str(random.randint(2000, 8000)) + ".00",
                    "data_emissao": f"2024-{(j%12)+1:02d}-01",
                    "ano": "2024",
                    "mes": str((j % 12) + 1),
                    "cnpj_fornecedor": e["cnpj"],  # 🔗 vínculo com empresa
                    "nome_fornecedor": e["razao_social"],
                    "partido": "PTST",
                    "uf": "DF",
                    "nome_parlamentar": "DEPUTADO INFLUENTE",
                    "fonte_nome": "Câmara dos Deputados"
                })

        # ─────────────────────────────────────────
        # OUTROS CENÁRIOS → ruído (dados normais)
        # ─────────────────────────────────────────
        elif random.random() < 0.15:
            rows.append({
                "despesa_id": f"CAM_{i}",
                "tipo_despesa": random.choice(["DIVULGACAO", "COMBUSTIVEL", "CONSULTORIA"]),
                "valor_liquido": str(random.randint(500, 4000)) + ".00",
                "data_emissao": "2024-01-01",
                "ano": "2024",
                "mes": "1",
                "cnpj_fornecedor": "".join([str(random.randint(0, 9)) for _ in range(14)]),
                "nome_fornecedor": "FORNECEDOR GENERICO",
                "partido": random.choice(["PT", "PL", "UNI", "PSD"]),
                "uf": random.choice(["DF", "SP", "RJ"]),
                "nome_parlamentar": random.choice([
                    "DEPUTADO TESTE",
                    "PARLAMENTAR X",
                    "POLITICO Y"
                ]),
                "fonte_nome": "Câmara dos Deputados"
            })

    # ─────────────────────────────────────────
    # SALVAR (padrão original)
    # ─────────────────────────────────────────
    pd.DataFrame(rows).to_csv(
        os.path.join(camara_dir, "despesas_2024.csv"),
        index=False,
        sep=',',
        encoding='utf-8-sig'
    )

    print(f"✓ Despesas Câmara: {len(rows)}")

# ─────────────────────────────────────────
# TSE
# ─────────────────────────────────────────
def generate_tse_data(empresas):
    print("Gerando dados do TSE...")

    doacoes_dir = os.path.join(DATA_DIR, "tse", "doacoes")
    candidatos_dir = os.path.join(DATA_DIR, "tse", "candidatos")
    os.makedirs(doacoes_dir, exist_ok=True)
    os.makedirs(candidatos_dir, exist_ok=True)

    # ─────────────────────────────────────────
    # CANDIDATOS (mantém estrutura original)
    # ─────────────────────────────────────────
    candidatos = [{
        'ANO_ELEICAO': '2022',
        'SG_UF': 'DF',
        'SQ_CANDIDATO': '10001',
        'NR_CPF_CANDIDATO': CPF_POLITICO,
        'NM_CANDIDATO': 'POLITICO INFLUENTE',
        'SG_PARTIDO': 'PTST',
        'DS_CARGO': 'SENADOR'
    }]

    pd.DataFrame(candidatos).to_csv(
        os.path.join(candidatos_dir, "candidatos_2022.csv"),
        index=False,
        sep=';'
    )

    # ─────────────────────────────────────────
    # DOAÇÕES (agora com empresas por cenário)
    # ─────────────────────────────────────────
    doacoes = []

    for e in empresas:

        # cenário 5 → empresa doando (suspeito)
        if e["cenario"] == 5:
            doacoes.append({
                'ANO_ELEICAO': '2022',
                'SQ_CANDIDATO': '10001',
                'NR_CPF_DOADOR': e["cnpj"],  # empresa como doadora (intencional)
                'NM_DOADOR': e["razao_social"],
                'VR_RECEITA': '50000,00',
                'DT_RECEITA': '01/09/2022'
            })

    # mantém também 1 doador "normal" (baseline)
    doacoes.append({
        'ANO_ELEICAO': '2022',
        'SQ_CANDIDATO': '10001',
        'NR_CPF_DOADOR': CPF_JOAO,
        'NM_DOADOR': 'JOAO DOADOR',
        'VR_RECEITA': '50000,00',
        'DT_RECEITA': '01/09/2022'
    })

    pd.DataFrame(doacoes).to_csv(
        os.path.join(doacoes_dir, "doacoes_2022.csv"),
        index=False,
        sep=';'
    )

def generate_tse_bens_data(empresas):
    print("Gerando dados de bens de candidatos (TSE)...")

    bens_dir = os.path.join(DATA_DIR, "tse", "bens")
    os.makedirs(bens_dir, exist_ok=True)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    meta = {
        "fonte_nome": "TSE — Tribunal Superior Eleitoral",
        "fonte_descricao": "Dados Eleitorais Abertos",
        "fonte_url": "https://dadosabertos.tse.jus.br",
        "fonte_licenca": "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
        "fonte_url_origem": "https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2022.zip",
        "fonte_ano": "2022",
        "fonte_coletado_em": now
    }

    bens = []

    # ─────────────────────────────────────────
    # CANDIDATO PRINCIPAL (já existente)
    # ─────────────────────────────────────────
    SQ_CANDIDATO_BASE = "10001"

    bens_base = [
        ("TRX 420 FOURTRAX MARCA HONDA", "27999,00"),
        ("DINHEIRO EM ESPÉCIE", "20000,00"),
        ("SALDO EM CONTAS CORRENTES", "46103,97"),
        ("SALDO EM CONTAS POUPANÇA", "734,89"),
        ("SALDO EM CONTAS INVESTIMENTO", "8095,58"),
    ]

    for desc, val in bens_base:
        bens.append({
            "ANO_ELEICAO": "2022",
            "CD_TIPO_ELEICAO": "2",
            "NM_TIPO_ELEICAO": "Eleição Ordinária",
            "SQ_CANDIDATO": SQ_CANDIDATO_BASE,
            "DS_BEM_CANDIDATO": desc,
            "VR_BEM_CANDIDATO": val,
            **meta
        })

    # ─────────────────────────────────────────
    # GERAR OUTROS CANDIDATOS SINTÉTICOS
    # ─────────────────────────────────────────
    for i, e in enumerate(empresas[:20]):  # limita volume

        sq = f"20000{i:06d}"

        # Cenário 5 → candidato com patrimônio alto (cruzável com doações)
        if e["cenario"] == 5:
            bens.append({
                "ANO_ELEICAO": "2022",
                "CD_TIPO_ELEICAO": "2",
                "NM_TIPO_ELEICAO": "Eleição Ordinária",
                "SQ_CANDIDATO": sq,
                "DS_BEM_CANDIDATO": "APARTAMENTO DE ALTO PADRÃO",
                "VR_BEM_CANDIDATO": "850000,00",
                **meta
            })

            bens.append({
                "ANO_ELEICAO": "2022",
                "CD_TIPO_ELEICAO": "2",
                "NM_TIPO_ELEICAO": "Eleição Ordinária",
                "SQ_CANDIDATO": sq,
                "DS_BEM_CANDIDATO": "SUV",
                "VR_BEM_CANDIDATO": "180000,00",
                **meta
            })

        # Cenário 4 → bens médios + vínculo político
        elif e["cenario"] == 4:
            bens.append({
                "ANO_ELEICAO": "2022",
                "CD_TIPO_ELEICAO": "2",
                "NM_TIPO_ELEICAO": "Eleição Ordinária",
                "SQ_CANDIDATO": sq,
                "DS_BEM_CANDIDATO": "CASA RESIDENCIAL",
                "VR_BEM_CANDIDATO": "320000,00",
                **meta
            })

            bens.append({
                "ANO_ELEICAO": "2022",
                "CD_TIPO_ELEICAO": "2",
                "NM_TIPO_ELEICAO": "Eleição Ordinária",
                "SQ_CANDIDATO": sq,
                "DS_BEM_CANDIDATO": "SALDO EM CONTA CORRENTE",
                "VR_BEM_CANDIDATO": "45000,00",
                **meta
            })

        # Outros cenários → ruído controlado
        else:
            if random.random() < 0.3:
                bens.append({
                    "ANO_ELEICAO": "2022",
                    "CD_TIPO_ELEICAO": "2",
                    "NM_TIPO_ELEICAO": "Eleição Ordinária",
                    "SQ_CANDIDATO": sq,
                    "DS_BEM_CANDIDATO": random.choice([
                        "MOTOCICLETA",
                        "VEÍCULO AUTOMOTOR",
                        "SALDO EM POUPANÇA",
                        "APLICAÇÃO FINANCEIRA"
                    ]),
                    "VR_BEM_CANDIDATO": f"{random.randint(5000, 80000)},00",
                    **meta
                })

    # ─────────────────────────────────────────
    # SALVAR CSV
    # ─────────────────────────────────────────
    pd.DataFrame(bens).to_csv(
        os.path.join(bens_dir, "bens_2022.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

    print(f"✓ Bens gerados: {len(bens)}")

def generate_senado_data(empresas):
    print("Gerando dados do Senado Federal (CSV)...")

    senado_dir = os.path.join(DATA_DIR, "senado")
    os.makedirs(senado_dir, exist_ok=True)

    data = []

    # ─────────────────────────────────────────
    # GARANTE EMPRESAS BASE SEGURAS
    # ─────────────────────────────────────────
    emp_base_0 = next((e for e in empresas if e["cenario"] == 0), empresas[0])
    emp_base_1 = next((e for e in empresas if e["cenario"] == 0 and e != emp_base_0), empresas[1] if len(empresas) > 1 else empresas[0])

    # ─────────────────────────────────────────
    # BASE FIXA (compatível com layout original)
    # ─────────────────────────────────────────
    base_senado = [
        {
            "COD_SENADOR": "5967",
            "NOME_SENADOR": "ANGELO CORONEL",
            "TIPO_DESPESA": "Combustíveis",
            "CPF_CNPJ_FORNECEDOR": emp_base_0["cnpj"],
            "NOME_FORNECEDOR": "AUTO POSTO BRASIL LTDA",
            "VALOR_REEMBOLSADO": "261,03"
        },
        {
            "COD_SENADOR": "1234",
            "NOME_SENADOR": "SENADOR TESTE",
            "TIPO_DESPESA": "Hospedagem",
            "CPF_CNPJ_FORNECEDOR": emp_base_1["cnpj"],
            "NOME_FORNECEDOR": "HOTEL CENTRAL",
            "VALOR_REEMBOLSADO": "1.200,50"
        }
    ]

    # ─────────────────────────────────────────
    # DADOS BASE (ruído)
    # ─────────────────────────────────────────
    for i in range(TOTAL_EMPRESAS * 2):
        for sen in base_senado:
            row = sen.copy()
            row.update({
                "ID": f"999{i}{random.randint(100,999)}",
                "ANO": "2024",
                "MÊS": str((i % 12) + 1),
                "DATA": f"2024-{(i % 12)+1:02d}-01",
                "fonte_nome": "Senado Federal"
            })
            data.append(row)

    # ─────────────────────────────────────────
    # CENÁRIO 4 → vínculo forte
    # ─────────────────────────────────────────
    for i, e in enumerate(empresas):
        if e["cenario"] == 4:
            for j in range(random.randint(3, 6)):
                data.append({
                    "COD_SENADOR": "9999",
                    "NOME_SENADOR": "SENADOR INFLUENTE",
                    "TIPO_DESPESA": random.choice(["Divulgação", "Consultoria", "Serviços"]),
                    "CPF_CNPJ_FORNECEDOR": e["cnpj"],
                    "NOME_FORNECEDOR": e["razao_social"],
                    "VALOR_REEMBOLSADO": f"{random.randint(1000, 8000)},00",
                    "ID": f"SEN_{i}_{j}",
                    "ANO": "2024",
                    "MÊS": str((j % 12) + 1),
                    "DATA": f"2024-{(j % 12)+1:02d}-01",
                    "fonte_nome": "Senado Federal"
                })

    # ─────────────────────────────────────────
    # OUTROS CENÁRIOS → ruído leve
    # ─────────────────────────────────────────
    for i, e in enumerate(empresas):
        if e["cenario"] != 4 and random.random() < 0.1:
            data.append({
                "COD_SENADOR": str(random.randint(1000, 9999)),
                "NOME_SENADOR": "SENADOR ALEATORIO",
                "TIPO_DESPESA": "Combustíveis",
                "CPF_CNPJ_FORNECEDOR": rand_cnpj(),
                "NOME_FORNECEDOR": "FORNECEDOR GENERICO",
                "VALOR_REEMBOLSADO": f"{random.randint(100, 1000)},00",
                "ID": f"SEN_R_{i}",
                "ANO": "2024",
                "MÊS": "1",
                "DATA": "2024-01-01",
                "fonte_nome": "Senado Federal"
            })

    # ─────────────────────────────────────────
    # SALVAR (padrão original)
    # ─────────────────────────────────────────
    pd.DataFrame(data).to_csv(
        os.path.join(senado_dir, "despesas_2024.csv"),
        index=False,
        sep=',',
        encoding='utf-8-sig'
    )

    print(f"✓ Despesas Senado: {len(data)}")

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

if __name__ == "__main__":

    if os.path.exists(DATA_DIR):
        for item in os.listdir(DATA_DIR):
            path = os.path.join(DATA_DIR, item)
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except Exception as e:
                print(f"[WARN] {e}")

    empresas = generate_empresas()

    generate_cnpj_data(empresas)
    generate_ibge_data()
    generate_pncp_data(empresas)
    generate_sancoes_data(empresas)
    generate_bndes_data(empresas)
    generate_servidores_data(empresas)
    generate_emendas_cgu_data(empresas)
    generate_camara_data(empresas)
    generate_senado_data(empresas)
    generate_tse_data(empresas)
    generate_tse_bens_data(empresas)

    print("✓ Dados sintéticos com cenários gerados!")