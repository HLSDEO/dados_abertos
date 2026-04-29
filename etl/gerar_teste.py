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
    
    itens = [{
        'id_contratacao_pncp': '2024-1', 'numero_item': '1', 'ni_fornecedor': CNPJ_FACHADA,
        'nome_razao_social_fornecedor': 'EMPRESA FACHADA LTDA', 'quantidade_homologada': '100',
        'valor_unitario_homologado': '1000,00', 'orgao_entidade_cnpj': '00394460000141'
    }]
    pd.DataFrame(itens).to_csv(os.path.join(pncp_dir, "itens.csv"), index=False, sep=';')

def generate_servidores_data():
    print("Gerando dados de Servidores...")
    serv_dir = os.path.join(DATA_DIR, "servidores", "2024", "01")
    os.makedirs(serv_dir, exist_ok=True)
    
    cad = [{
        'id_servidor': 'S1', 'cpf': CPF_MARIA, 'nome': 'MARIA SERVIDORA',
        'cargo': 'ANALISTA', 'org_exercicio': 'MINISTERIO DA SAUDE', 'uf_exercicio': 'DF'
    }]
    pd.DataFrame(cad).to_csv(os.path.join(serv_dir, "cadastro.csv"), index=False, sep=',', encoding='utf-8-sig')

if __name__ == "__main__":
    # Limpar e recriar
    if os.path.exists(DATA_DIR):
        for item in os.listdir(DATA_DIR):
            if item not in ["cnpj", "siafi"]: # Preserva o que o usuário pediu
                shutil.rmtree(os.path.join(DATA_DIR, item), ignore_errors=True)
    
    generate_ibge_data()
    generate_senado_data()
    generate_camara_data()
    generate_tse_data()
    generate_sancoes_data()
    generate_bndes_data()
    generate_pncp_data()
    generate_servidores_data()
    
    print("\nDados sintéticos realistas gerados com sucesso!")
