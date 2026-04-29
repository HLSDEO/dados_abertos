import pandas as pd
import os
import json
import random
import shutil
import csv
from datetime import datetime, timezone

# Configuração de pastas
DATA_DIR = "data"

# IDs padronizados para interconexões (base para padrões de corrupção)
CPF_POLITICO = "99988877766"
CPF_JOAO     = "11111111111"
CPF_MARIA    = "22222222222"

CNPJ_FACHADA   = "11111111000100"
CNPJ_AMIGA     = "22222222000100"
CNPJ_SUSPEITOS = "33333333000199"
CNPJ_XYZ       = "12345678000199"
CNPJ_HOTEL     = "22345678000100"
CNPJ_GRAFICA   = "32345678000100"

MUNICIPIO_BRASILIA = "9701"
UF_DF = "DF"

# Multiplicador para volume de dados
MULT = 5

def generate_cnpj_data():
    print("Gerando dados de CNPJ (Receita Federal)...")

    snapshot = "2024-01"
    base_dir = os.path.join(DATA_DIR, "cnpj", snapshot, "csv")
    os.makedirs(base_dir, exist_ok=True)

    # ─────────────────────────────────────────
    # EMPRESAS
    # ─────────────────────────────────────────
    empresas = [{
        "cnpj_basico": CNPJ_FACHADA[:8],
        "razao_social": "EMPRESA FACHADA LTDA",
        "natureza_juridica": "2062",
        "qualificacao_responsavel": "49",
        "capital_social": "100000.00",
        "porte_empresa": "01",
        "ente_federativo": "",
        "fonte_snapshot": snapshot
    }]

    pd.DataFrame(empresas).to_csv(
        os.path.join(base_dir, "empresas.csv"),
        index=False,
        encoding="utf-8"
    )

    # ─────────────────────────────────────────
    # ESTABELECIMENTOS
    # ─────────────────────────────────────────
    estabelecimentos = [{
        "cnpj_basico": CNPJ_FACHADA[:8],
        "cnpj": CNPJ_FACHADA,
        "nome_fantasia": "FACHADA TECH",
        "situacao_cadastral": "02",
        "data_situacao_cadastral": "20240101",
        "data_inicio_atividade": "20200101",
        "cnae_principal": "6201501",
        "uf": "DF",
        "cep": "70000000",
        "logradouro": "RUA FICTICIA",
        "numero": "100",
        "bairro": "CENTRO",
        "email": "contato@fachada.com",
        "municipio": MUNICIPIO_BRASILIA
    }]

    pd.DataFrame(estabelecimentos).to_csv(
        os.path.join(base_dir, "estabelecimentos.csv"),
        index=False,
        encoding="utf-8"
    )

    # ─────────────────────────────────────────
    # SOCIOS
    # ─────────────────────────────────────────
    socios = [
        {
            "cnpj_basico": CNPJ_FACHADA[:8],
            "identificador_socio": "2",  # PF
            "nome_socio": "JOAO SOCIO",
            "cpf_cnpj_socio": CPF_JOAO,
            "qualificacao_socio": "49",
            "data_entrada": "20200101",
            "faixa_etaria": "4"
        },
        {
            "cnpj_basico": CNPJ_FACHADA[:8],
            "identificador_socio": "2",
            "nome_socio": "CPF MASCARADO",
            "cpf_cnpj_socio": "***123***00",
            "qualificacao_socio": "49",
            "data_entrada": "20200101",
            "faixa_etaria": "3"
        }
    ]

    pd.DataFrame(socios).to_csv(
        os.path.join(base_dir, "socios.csv"),
        index=False,
        encoding="utf-8"
    )

    # ─────────────────────────────────────────
    # SIMPLES
    # ─────────────────────────────────────────
    simples = [{
        "cnpj_basico": CNPJ_FACHADA[:8],
        "opcao_simples": "S",
        "data_opcao_simples": "20200101",
        "data_exclusao_simples": "",
        "opcao_mei": "N",
        "data_opcao_mei": "",
        "data_exclusao_mei": ""
    }]

    pd.DataFrame(simples).to_csv(
        os.path.join(base_dir, "simples.csv"),
        index=False,
        encoding="utf-8"
    )

    # ─────────────────────────────────────────
    # TABELAS DE DOMÍNIO (OBRIGATÓRIAS!)
    # ─────────────────────────────────────────
    pd.DataFrame([{
        "codigo_cnae": "6201501",
        "descricao_cnae": "Desenvolvimento de software"
    }]).to_csv(os.path.join(base_dir, "cnaes.csv"), index=False)

    pd.DataFrame([{
        "codigo_natureza": "2062",
        "descricao_natureza": "Sociedade Empresária Limitada"
    }]).to_csv(os.path.join(base_dir, "naturezas.csv"), index=False)

    pd.DataFrame([{
        "codigo_qualificacao": "49",
        "descricao_qualificacao": "Sócio-Administrador"
    }]).to_csv(os.path.join(base_dir, "qualificacoes.csv"), index=False)

    pd.DataFrame([{
        "codigo_municipio_rf": MUNICIPIO_BRASILIA,
        "nome_municipio": "BRASILIA"
    }]).to_csv(os.path.join(base_dir, "municipios_rf.csv"), index=False)

    pd.DataFrame([{
        "codigo_pais": "105",
        "nome_pais": "BRASIL"
    }]).to_csv(os.path.join(base_dir, "paises.csv"), index=False)

# Funções auxiliares para variações
def vary_name(name):
    variations = [
        name,
        name.replace('A', 'Ã'),
        name.replace('O', 'Ô'),
        name.upper(),
        name.lower(),
        name.split()[0] + ' ' + name.split()[-1][0] + '.',
    ]
    return random.choice(variations)

def vary_cpf(cpf):
    cpf_list = list(cpf)
    pos = random.randint(0, len(cpf_list)-1)
    cpf_list[pos] = str((int(cpf_list[pos]) + 1) % 10)
    return ''.join(cpf_list)

def vary_cnpj(cnpj):
    cnpj_list = list(cnpj)
    pos = random.randint(0, len(cnpj_list)-1)
    if cnpj_list[pos].isdigit():
        cnpj_list[pos] = str((int(cnpj_list[pos]) + 1) % 10)
    return ''.join(cnpj_list)

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
        'id': '5300108', 'nome': 'Brasília', 'microrregiao_id': '53001', **meta
    }]).to_csv(os.path.join(ibge_dir, "municipios.csv"), index=False)

def generate_senado_data():
    print("Gerando dados do Senado Federal (CSV)...")
    senado_dir = os.path.join(DATA_DIR, "senado")
    os.makedirs(senado_dir, exist_ok=True)
    
    data = []
    base_senado = [
        {"COD_SENADOR": "5967", "NOME_SENADOR": "ANGELO CORONEL", "TIPO_DESPESA": "Combustíveis", "CPF_CNPJ_FORNECEDOR": CNPJ_XYZ, "NOME_FORNECEDOR": "AUTO POSTO BRASIL LTDA", "VALOR_REEMBOLSADO": "261,03"},
        {"COD_SENADOR": "1234", "NOME_SENADOR": "SENADOR TESTE", "TIPO_DESPESA": "Hospedagem", "CPF_CNPJ_FORNECEDOR": CNPJ_HOTEL, "NOME_FORNECEDOR": "HOTEL CENTRAL", "VALOR_REEMBOLSADO": "1.200,50"},
    ]
    
    for i in range(MULT * 2):
        for sen in base_senado:
            row = sen.copy()
            row["ID"] = f"999{i}{random.randint(100,999)}"
            row["ANO"] = "2024"
            row["MÊS"] = str((i % 12) + 1)
            row["DATA"] = f"2024-{(i % 12)+1:02d}-01"
            row["fonte_nome"] = "Senado Federal"
            data.append(row)
            
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(senado_dir, "despesas_2024.csv"), index=False, sep=',', encoding='utf-8-sig')

def generate_camara_data():
    print("Gerando dados da Câmara (CSV)...")
    camara_dir = os.path.join(DATA_DIR, "camara")
    os.makedirs(camara_dir, exist_ok=True)
    
    data = []
    for i in range(MULT * 5):
        data.append({
            "despesa_id": f"CAM{i}",
            "tipo_despesa": "DIVULGACAO",
            "valor_liquido": str(random.randint(1000, 5000)) + ".00",
            "data_emissao": "2024-01-01",
            "ano": "2024",
            "mes": "1",
            "cnpj_fornecedor": CNPJ_GRAFICA,
            "nome_fornecedor": "GRAFICA RAPIDA",
            "partido": "PTST",
            "uf": "DF",
            "nome_parlamentar": "DEPUTADO INFLUENTE",
            "fonte_nome": "Câmara dos Deputados"
        })
    pd.DataFrame(data).to_csv(os.path.join(camara_dir, "despesas_2024.csv"), index=False, sep=',', encoding='utf-8-sig')

def generate_tse_data():
    print("Gerando dados do TSE...")
    tse_dir = os.path.join(DATA_DIR, "tse", "2022")
    os.makedirs(tse_dir, exist_ok=True)
    
    # Candidatos
    cand = [{
        'ANO_ELEICAO': '2022', 'SG_UF': 'DF', 'SQ_CANDIDATO': '10001', 'NR_CPF_CANDIDATO': CPF_POLITICO,
        'NM_CANDIDATO': 'POLITICO INFLUENTE', 'SG_PARTIDO': 'PTST', 'DS_CARGO': 'SENADOR'
    }]
    pd.DataFrame(cand).to_csv(os.path.join(tse_dir, "candidatos_2022.csv"), index=False, sep=';')
    
    # Doações
    doacoes = [{
        'ANO_ELEICAO': '2022', 'SQ_CANDIDATO': '10001', 'NR_CPF_DOADOR': CPF_JOAO,
        'NM_DOADOR': 'JOAO DOADOR', 'VR_RECEITA': '50000,00', 'DT_RECEITA': '01/09/2022'
    }]
    pd.DataFrame(doacoes).to_csv(os.path.join(tse_dir, "doacoes_2022.csv"), index=False, sep=';')

def generate_sancoes_data():
    print("Gerando dados de Sanções (CEIS/CNEP)...")
    sancoes_dir = os.path.join(DATA_DIR, "sancoes_cgu")
    os.makedirs(sancoes_dir, exist_ok=True)
    
    sancoes = [{
        'cpf_cnpj': CNPJ_SUSPEITOS, 'nome': 'SUPRIMENTOS SUSPEITOS ME',
        'tipo_sancao': 'Inidoneidade', 'data_inicio': '2023-01-01', 'orgao_sancionador': 'MINISTERIO DA SAUDE'
    }]
    pd.DataFrame(sancoes).to_csv(os.path.join(sancoes_dir, "ceis.csv"), index=False, sep=';', encoding='utf-8-sig')

def generate_bndes_data():
    print("Gerando dados do BNDES...")
    bndes_dir = os.path.join(DATA_DIR, "bndes")
    os.makedirs(bndes_dir, exist_ok=True)
    
    bndes = [{
        '_id': 'BNDES1', 'cliente': 'CONSTRUTORA AMIGA SA', 'cnpj': CNPJ_AMIGA,
        'valor_contratado_reais': '10000000,00', 'data_da_contratacao': '2024-01-01',
        'produto': 'FINEM', 'uf': 'DF'
    }]
    pd.DataFrame(bndes).to_csv(os.path.join(bndes_dir, "operacoes_2024.csv"), index=False, sep=';')

def generate_pncp_data():
    print("Gerando dados do PNCP...")
    pncp_dir = os.path.join(DATA_DIR, "pncp_csv")
    os.makedirs(pncp_dir, exist_ok=True)

    # ─────────────────────────────────────────
    # ITENS (já existia)
    # ─────────────────────────────────────────
    itens = [{
        'id_contratacao_pncp': '2024-1',
        'numero_item': '1',
        'ni_fornecedor': CNPJ_FACHADA,
        'nome_razao_social_fornecedor': 'EMPRESA FACHADA LTDA',
        'quantidade_homologada': '100',
        'valor_unitario_homologado': '1000.00',
        'orgao_entidade_cnpj': '00394460000141'
    }]

    pd.DataFrame(itens).to_csv(
        os.path.join(pncp_dir, "itens.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

    # ─────────────────────────────────────────
    # CONTRATOS
    # ─────────────────────────────────────────
    contratos = [{
        "id": "C1",
        "receita_despesa": "Despesa",
        "numero": "00065/2025",
        "orgao_codigo": "14000",
        "orgao_nome": "JUSTICA ELEITORAL",
        "unidade_codigo": "070009",
        "esfera": "Federal",
        "poder": "Judiciario",
        "sisg": "Nao",
        "gestao": "00001",
        "unidade_nome_resumido": "TRE/PB",
        "unidade_nome": "TRIBUNAL REGIONAL ELEITORAL DA PARAIBA",
        "unidade_origem_codigo": "070009",
        "unidade_origem_nome": "TRIBUNAL REGIONAL ELEITORAL DA PARAIBA",
        "fornecedor_tipo": "JURIDICA",
        "fonecedor_cnpj_cpf_idgener": CNPJ_FACHADA,
        "fornecedor_nome": "EMPRESA FACHADA LTDA",
        "codigo_tipo": "50",
        "tipo": "Contrato",
        "categoria": "Compras",
        "processo": "0008512-65.2024.6.15.8000",
        "objeto": "AQUISICAO DE EQUIPAMENTOS DE TI",
        "fundamento_legal": "",
        "informacao_complementar": "",
        "codigo_modalidade": "05",
        "modalidade": "Pregao",
        "unidade_compra": "070009",
        "licitacao_numero": "90001/2025",
        "data_assinatura": "2026-01-14",
        "data_publicacao": "2026-01-15",
        "vigencia_inicio": "2026-01-14",
        "vigencia_fim": "2031-01-14",
        "valor_inicial": "466441.8",
        "valor_global": "466441.8",
        "num_parcelas": "1",
        "valor_parcela": "466441.8",
        "valor_acumulado": "0.0",
        "situacao": "Ativo"
    }]

    pd.DataFrame(contratos).to_csv(
        os.path.join(pncp_dir, "contratos.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

    # ─────────────────────────────────────────
    # EMPENHOS
    # ─────────────────────────────────────────
    empenhos = [{
        "id": "E1",
        "unidade": "580003",
        "unidade_nome": "SUBSECRETARIA DE GESTAO E ADMINISTRACAO",
        "gestao": "00001",
        "numero_empenho": "2026NE000083",
        "data_emissao": "2026-03-11",
        "cpf_cnpj_credor": CNPJ_FACHADA,
        "credor": "EMPRESA FACHADA LTDA",
        "fonte_recurso": "1000000000",
        "ptres": "",
        "modalidade_licitacao_siafi": "",
        "naturezadespesa": "339040",
        "naturezadespesa_descricao": "SERVICOS DE TI",
        "planointerno": "ADMPA",
        "planointerno_descricao": "OPERACAO ADMINISTRATIVA",
        "valor_empenhado": "22242.63",
        "valor_aliquidar": "16048.7",
        "valor_liquidado": "1521.32",
        "valor_pago": "4672.61",
        "valor_rpinscrito": "0",
        "valor_rpaliquidar": "0",
        "valor_rpaliquidado": "0",
        "valor_rppago": "0",
        "informacao_complementar": "",
        "sistema_origem": "COMPRASNET",
        "contrato_id": "C1",  # ligação com contratos
        "created_at": "2026-03-11",
        "updated_at": "2026-03-19",
        "id_cipi": ""
    }]

    pd.DataFrame(empenhos).to_csv(
        os.path.join(pncp_dir, "empenhos.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

def generate_servidores_data():
    print("Gerando dados de Servidores...")
    serv_dir = os.path.join(DATA_DIR, "servidores", "2024", "01")
    os.makedirs(serv_dir, exist_ok=True)
    
    cad = [{
        'id_servidor': 'S1', 'cpf': CPF_MARIA, 'nome': 'MARIA SERVIDORA',
        'cargo': 'ANALISTA', 'org_exercicio': 'MINISTERIO DA SAUDE', 'uf_exercicio': 'DF'
    }]
    pd.DataFrame(cad).to_csv(os.path.join(serv_dir, "cadastro.csv"), index=False, sep=',', encoding='utf-8-sig')

def generate_emendas_cgu_data(qtd=1000):
    print("Gerando dados de emendas CGU...")

    base_dir = os.path.join(DATA_DIR, "emendas_cgu")
    os.makedirs(base_dir, exist_ok=True)

    emendas = []
    convenios = []
    despesas = []

    for i in range(qtd):
        cod_emenda = f"2026{str(i).zfill(6)}"
        cod_autor = str(random.randint(1000, 9999))
        nome_autor = random.choice([
            "DELEGADO EDER MAURO",
            "JOAO SILVA",
            "MARIA SOUZA",
            "CARLOS OLIVEIRA"
        ])

        cod_funcao = random.choice(["10", "12", "15"])
        nome_funcao = {
            "10": "Saúde",
            "12": "Educação",
            "15": "Urbanismo"
        }[cod_funcao]

        cod_programa = str(random.randint(5000, 5999))

        # ── EMENDAS ─────────────────────
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
            "Valor Empenhado": str(round(random.uniform(1000, 1000000), 2)).replace(".", ","),
            "Valor Liquidado": "0,00",
            "Valor Pago": "0,00",
            "Valor Restos A Pagar Inscritos": "0,00",
            "Valor Restos A Pagar Cancelados": "0,00",
            "Valor Restos A Pagar Pagos": "0,00",
        })

        # ── CONVÊNIO (50% das emendas) ──
        if random.random() < 0.5:
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
                "Valor Convênio": str(round(random.uniform(10000, 500000), 2)).replace(".", ","),
            })

        # ── DESPESAS ────────────────────
        for _ in range(random.randint(1, 3)):
            cnpj = "".join([str(random.randint(0, 9)) for _ in range(14)])

            despesas.append({
                "Código da Emenda": cod_emenda,
                "Código do Autor da Emenda": cod_autor,
                "Nome do Autor da Emenda": nome_autor,
                "Número da emenda": str(i).zfill(4),
                "Tipo de Emenda": "Emenda Individual",
                "Ano/Mês": "202604",
                "Código do Favorecido": cnpj,
                "Favorecido": f"EMPRESA {cnpj[:4]} LTDA",
                "Natureza Jurídica": "Sociedade Empresária Limitada",
                "Tipo Favorecido": "Pessoa Jurídica",
                "UF Favorecido": "ES",
                "Município Favorecido": "SERRA",
                "Valor Recebido": str(round(random.uniform(100, 10000), 2)).replace(".", ","),
            })

    # ── SALVAR CSVs ────────────────────
    pd.DataFrame(emendas).to_csv(os.path.join(base_dir, "emendas.csv"), index=False)
    pd.DataFrame(convenios).to_csv(os.path.join(base_dir, "convenios.csv"), index=False)
    pd.DataFrame(despesas).to_csv(os.path.join(base_dir, "por_favorecido.csv"), index=False)

    print(f"✓ Emendas: {len(emendas)}")
    print(f"✓ Convênios: {len(convenios)}")
    print(f"✓ Despesas: {len(despesas)}")

if __name__ == "__main__":
    # Limpar e recriar
    if os.path.exists(DATA_DIR):
        for item in os.listdir(DATA_DIR):
            if item not in ["cnpj", "siafi"]: # Preserva o que o usuário pediu
                shutil.rmtree(os.path.join(DATA_DIR, item), ignore_errors=True)
                
    generate_cnpj_data()
    generate_ibge_data()
    generate_senado_data()
    generate_camara_data()
    generate_tse_data()
    generate_sancoes_data()
    generate_bndes_data()
    generate_pncp_data()
    generate_servidores_data()
    generate_emendas_cgu_data()
    
    print("\nDados sintéticos realistas gerados com sucesso!")
