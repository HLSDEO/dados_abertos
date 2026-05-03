import pandas as pd
import os
import random
from datetime import datetime, timedelta, timezone
from faker import Faker

# ─────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────
DATA_DIR = "data"
TOTAL_EMPRESAS = 200

# Inicializa Faker para pt_BR
fake = Faker('pt_BR')
Faker.seed(42)
random.seed(42)

# Lista de todos os padrões suportados
AVAILABLE_PATTERNS = [
    "sanctioned_contract", "amendment_owner", "contract_concentration", 
    "split_contracts", "inexigibility_recurrence", "servant_company", 
    "debtor_contracts", "expense_supplier_overlap", "bndes_sanction_overlap", 
    "enrichment_signal", "donation_contract"
]

# ─────────────────────────────────────────
# HELPERS GLOBAIS (Armazenamento de Dados)
# ─────────────────────────────────────────
db = {
    "empresas": [], "socios": [], "estabelecimentos": [], "simples": [],
    "uasgs": [], "contratos": [], "itens": [], "empenhos": [],
    "sancoes": [], "bndes": [], "servidores": [],
    "emendas": [], "convenios": [], "despesas_emendas": [],
    "camara": [], "senado": [], "tse_candidatos": [],
    "tse_doacoes": [], "tse_bens": [], "pgfn": [], "cpgf": []
}

def clean_doc(doc):
    return ''.join(filter(str.isdigit, doc))

# ─────────────────────────────────────────
# MOTOR DE GERAÇÃO (TIMELINE E PADRÕES)
# ─────────────────────────────────────────

def distribute_patterns():
    """Distribui as empresas nas faixas percentuais solicitadas."""
    distribution = []
    
    # 10% -> 0 padrões
    for _ in range(int(TOTAL_EMPRESAS * 0.10)):
        distribution.append([])
        
    # 40% -> 1 padrão (garante cobertura de todos)
    qtd_1_padrao = int(TOTAL_EMPRESAS * 0.40)
    for i in range(qtd_1_padrao):
        distribution.append([AVAILABLE_PATTERNS[i % len(AVAILABLE_PATTERNS)]])
        
    # 30% -> 2 padrões
    for _ in range(int(TOTAL_EMPRESAS * 0.30)):
        distribution.append(random.sample(AVAILABLE_PATTERNS, 2))
        
    # 20% -> 3 ou mais padrões (3 a 5)
    for _ in range(int(TOTAL_EMPRESAS * 0.20)):
        num_patterns = random.randint(3, 5)
        distribution.append(random.sample(AVAILABLE_PATTERNS, min(num_patterns, len(AVAILABLE_PATTERNS))))
        
    # Ajusta caso o arredondamento falhe
    while len(distribution) < TOTAL_EMPRESAS:
        distribution.append([])
        
    random.shuffle(distribution)
    return distribution

def generate_base_data():
    """Gera dados base estáticos compartilhados (SIAFI, IBGE) - MANTÉM ARQUIVOS ORIGINAIS"""
    os.makedirs(os.path.join(DATA_DIR, "siafi"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "ibge"), exist_ok=True)
    
    # IBGE Base - Arquivos Obrigatórios
    meta = {
        'fonte_nome': 'IBGE', 'fonte_descricao': 'Dados Abertos IBGE',
        'fonte_licenca': 'CC-BY 4.0', 'fonte_url': 'https://www.ibge.gov.br',
        'fonte_coletado_em': datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    pd.DataFrame([{'id': '5', 'sigla': 'CO', 'nome': 'Centro-Oeste', **meta}]).to_csv(os.path.join(DATA_DIR, "ibge", "regioes.csv"), index=False)
    pd.DataFrame([{'id': '53', 'sigla': 'DF', 'nome': 'Distrito Federal', 'regiao_id': '5', **meta}]).to_csv(os.path.join(DATA_DIR, "ibge", "estados.csv"), index=False)
    pd.DataFrame([{'id': '5301', 'nome': 'Distrito Federal', 'UF_id': '53', **meta}]).to_csv(os.path.join(DATA_DIR, "ibge", "mesorregioes.csv"), index=False)
    pd.DataFrame([{'id': '53001', 'nome': 'Brasília', 'mesorregiao_id': '5301', **meta}]).to_csv(os.path.join(DATA_DIR, "ibge", "microrregioes.csv"), index=False)
    pd.DataFrame([{'id': '5300108', 'nome': 'Brasília', 'uf': 'DF', 'microrregiao_id': '53001', **meta}]).to_csv(os.path.join(DATA_DIR, "ibge", "municipios.csv"), index=False)
        
    # SIAFI Base (Órgãos e UASGs)
    orgaos = [
        {"id_orgao": "14000", "nome": "MINISTERIO DA SAUDE", "id_esfera": "1", "sigla": "MS"},
        {"id_orgao": "26000", "nome": "MINISTERIO DA EDUCACAO", "id_esfera": "1", "sigla": "MEC"}
    ]
    for org in orgaos:
        cd_uasg = str(random.randint(100000, 999999))
        db["uasgs"].append({
            "CD_UASG": cd_uasg, "SG_UASG": org["sigla"], "NO_UASG": f"UNIDADE DE {org['sigla']}",
            "ID_ORGAO": org["id_orgao"], "NO_ORGAO": org["nome"], "ID_ESFERA_ADMINISTRATIVA": org["id_esfera"],
            "NO_ESFERA_ADMINISTRATIVA": "FEDERAL"
        })

def apply_patterns_to_company(idx, patterns):
    """Cria uma empresa, sua linha do tempo e aplica os gatilhos dos padrões."""
    
    # 1. Timeline Base
    ano_abertura = random.randint(2010, 2019)
    dt_abertura = fake.date_between(start_date=datetime(ano_abertura, 1, 1).date(), end_date=datetime(ano_abertura, 12, 31).date())
    
    cnpj_raw = fake.cnpj()
    cnpj = clean_doc(cnpj_raw)
    razao_social = fake.company()
    
    # Sócio Base
    cpf_socio = clean_doc(fake.cpf())
    nome_socio = fake.name()
    
    db["empresas"].append({
        "cnpj_basico": cnpj[:8], "razao_social": razao_social, "natureza_juridica": "2062", 
        "capital_social": str(random.randint(50000, 500000)), "data_inicio_atividade": dt_abertura.strftime("%Y-%m-%d")
    })
    
    db["estabelecimentos"].append({
        "cnpj_basico": cnpj[:8], "cnpj": cnpj, "nome_fantasia": razao_social,
        "uf": "DF", "municipio_cod": "5300108", "municipio_nome": "Brasília"
    })
    
    db["simples"].append({
        "cnpj_basico": cnpj[:8], "opcao_simples": "S"
    })
    
    db["socios"].append({
        "cnpj_basico": cnpj[:8], "nome_socio": nome_socio, "cpf_cnpj_socio": cpf_socio,
        "qualificacao": "Sócio-Administrador", "data_entrada": dt_abertura.strftime("%Y-%m-%d"), "identificador_socio": "2"
    })

    has_sancao = False
    has_contrato = False
    dt_sancao_inicio = None
    dt_divida = None
    
    # ─────────────────────────────────────────
    # APLICAÇÃO DOS PADRÕES (Gatilhos)
    # ─────────────────────────────────────────
    
    if "sanctioned_contract" in patterns or "bndes_sanction_overlap" in patterns:
        has_sancao = True
        dt_sancao_inicio = dt_abertura + timedelta(days=random.randint(1000, 2000))
        db["sancoes"].append({
            "sancao_id": f"SANC_{idx}", "cpf_cnpj": cnpj, "nome": razao_social,
            "tipo_sancao": "Inidoneidade", "tipo_registro": "CEIS",
            "data_inicio": dt_sancao_inicio.strftime("%Y-%m-%d"), "data_fim": ""
        })

    if "debtor_contracts" in patterns:
        dt_divida = dt_abertura + timedelta(days=random.randint(500, 1000))
        db["pgfn"].append({
            "numero_inscricao": f"PGFN_{idx}", "cpf_cnpj": cnpj, "nome_devedor": razao_social,
            "tipo_credito": "Tributario", "valor_consolidado": "150000,00",
            "situacao": "Ativa", "data_inscricao": dt_divida.strftime("%Y-%m-%d")
        })

    if "donation_contract" in patterns:
        cpf_candidato = clean_doc(fake.cpf())
        nome_candidato = fake.name()
        db["tse_candidatos"].append({
            "ANO_ELEICAO": "2022", "SG_UF": "DF", "SQ_CANDIDATO": f"SQ_{idx}",
            "NR_TITULO_ELEITORAL_CANDIDATO": cpf_candidato, "NM_CANDIDATO": nome_candidato,
            "SG_PARTIDO": "PTST", "DS_CARGO": "DEPUTADO FEDERAL"
        })
        db["tse_doacoes"].append({
            "ano": "2022", "sq_candidato": f"SQ_{idx}", "cpf_candidato": cpf_candidato,
            "cpf_cnpj_doador": cnpj, "nome_doador": razao_social, "valor": "80000,00"
        })

    # Contratos Públicos (Gera Itens, Contratos e Empenhos)
    if set(patterns).intersection({"sanctioned_contract", "contract_concentration", "split_contracts", "inexigibility_recurrence", "servant_company", "debtor_contracts", "donation_contract"}):
        has_contrato = True
        uasg = random.choice(db["uasgs"])
        
        base_contrato_dt = max(dt_sancao_inicio or dt_abertura, dt_divida or dt_abertura, datetime(2023, 1, 1).date()) + timedelta(days=random.randint(30, 200))
        
        num_contratos = 1
        valor_base = random.randint(150000, 500000)
        modalidade = "Pregão"
        
        if "contract_concentration" in patterns:
            num_contratos = random.randint(3, 5)
            valor_base = random.randint(500000, 2000000)
        elif "split_contracts" in patterns:
            num_contratos = random.randint(5, 8)
            valor_base = random.randint(40000, 75000) 
        elif "inexigibility_recurrence" in patterns:
            num_contratos = random.randint(3, 5)
            modalidade = "Inexigibilidade"
            
        for c_idx in range(num_contratos):
            c_id = f"CONT_{idx}_{c_idx}"
            valor_str = str(valor_base)
            
            db["contratos"].append({
                "id": c_id, "numero": f"{c_idx+1}00/2024", "orgao_codigo": uasg["ID_ORGAO"], 
                "orgao_nome": uasg["NO_ORGAO"], "fonecedor_cnpj_cpf_idgener": cnpj, 
                "fornecedor_nome": razao_social, "modalidade": modalidade,
                "data_assinatura": (base_contrato_dt + timedelta(days=c_idx*10)).strftime("%Y-%m-%d"),
                "valor_global": valor_str
            })
            db["itens"].append({
                'id_contratacao_pncp': c_id, 'numero_item': '1', 'ni_fornecedor': cnpj,
                'nome_razao_social_fornecedor': razao_social, 'quantidade_homologada': '1',
                'valor_unitario_homologado': valor_str, 'orgao_entidade_cnpj': '00000000000191'
            })
            db["empenhos"].append({
                "id": f"EMP_{idx}_{c_idx}", "numero_empenho": f"2024NE{idx:04d}{c_idx}",
                "data_emissao": (base_contrato_dt + timedelta(days=c_idx*10 + 5)).strftime("%Y-%m-%d"),
                "contrato_id": c_id, "valor_empenhado": valor_str
            })

    # Servidores e Vínculos
    if "servant_company" in patterns or "enrichment_signal" in patterns:
        db["servidores"].append({
            "id_servidor": f"SRV_{idx}", "cpf": cpf_socio, "nome": nome_socio,
            "cargo": "ANALISTA", "org_exercicio": "MINISTERIO DA SAUDE", "situacao_vinculo": "ATIVO"
        })
        
        if "enrichment_signal" in patterns:
            db["tse_candidatos"].append({ 
                "ANO_ELEICAO": "2022", "SQ_CANDIDATO": f"SQ_SRV_{idx}", "NR_TITULO_ELEITORAL_CANDIDATO": cpf_socio, "NM_CANDIDATO": nome_socio
            })
            db["tse_bens"].append({
                "ANO_ELEICAO": "2022", "SQ_CANDIDATO": f"SQ_SRV_{idx}", "DS_TIPO_BEM": "Imóvel",
                "DS_BEM_CANDIDATO": "APARTAMENTO LUXO", "VR_BEM_CANDIDATO": "2500000,00"
            })

    # Parlamentares e Emendas (Gera Emendas, Convenios e Despesas)
    if "amendment_owner" in patterns or "expense_supplier_overlap" in patterns:
        cod_parlamentar = f"PARL_{idx}"
        
        if "amendment_owner" in patterns:
            nome_parlamentar = nome_socio 
            cpf_parlamentar = cpf_socio
            db["tse_candidatos"].append({ 
                "ANO_ELEICAO": "2022", "SQ_CANDIDATO": f"SQ_PARL_{idx}", "NR_TITULO_ELEITORAL_CANDIDATO": cpf_parlamentar, "NM_CANDIDATO": nome_parlamentar
            })
        else:
            nome_parlamentar = fake.name()
            
        cod_emenda = f"EMENDA_{idx}"
        db["emendas"].append({
            "Código da Emenda": cod_emenda, "Ano da Emenda": "2024", "Tipo de Emenda": "Emenda Individual",
            "Código do Autor da Emenda": cod_parlamentar, "Nome do Autor da Emenda": nome_parlamentar,
            "Valor Pago": "500000,00"
        })
        db["convenios"].append({
            "Código da Emenda": cod_emenda, "Código Função": "10", "Nome Função": "Saúde",
            "Código Subfunção": "302", "Nome Subfunção": "Assistência hospitalar", "Localidade do gasto": "SERRA (ES)",
            "Tipo de Emenda": "Emenda Individual", "Data Publicação Convênio": "01/01/2024", "Convenente": "PREFEITURA DE SERRA",
            "Objeto Convênio": "AQUISIÇÃO DE EQUIPAMENTOS", "Número Convênio": str(random.randint(800000, 999999)), "Valor Convênio": "500000,00"
        })
        db["despesas_emendas"].append({
            "Código da Emenda": cod_emenda, "Código do Favorecido": cnpj, "Favorecido": razao_social,
            "Tipo Favorecido": "Pessoa Jurídica", "Valor Recebido": "500000,00"
        })
        
        if "expense_supplier_overlap" in patterns:
            db["camara"].append({
                "despesa_id": f"CEAP_{idx}", "tipo_despesa": "DIVULGACAO", "valor_liquido": "15000.00",
                "ano": "2024", "mes": "05", "cnpj_fornecedor": cnpj, "nome_fornecedor": razao_social,
                "nome_parlamentar": nome_parlamentar
            })

    # Senado (Ruído base / vínculos fracos)
    db["senado"].append({
        "COD_SENADOR": "9999", "NOME_SENADOR": "SENADOR SINTETICO",
        "TIPO_DESPESA": "Consultoria", "CPF_CNPJ_FORNECEDOR": cnpj,
        "NOME_FORNECEDOR": razao_social, "VALOR_REEMBOLSADO": "5000,00",
        "ID": f"SEN_{idx}", "ANO": "2024", "MÊS": "05", "DATA": "2024-05-01",
        "fonte_nome": "Senado Federal"
    })

    if "bndes_sanction_overlap" in patterns:
        db["bndes"].append({
            "_id": f"BNDES_{idx}", "cliente": razao_social, "cnpj": cnpj,
            "valor_contratado_reais": "1500000,00", "data_da_contratacao": "2024-01-10", "produto": "FINEM"
        })

    # Adiciona algum ruído para empresas normais
    if not patterns:
        db["cpgf"].append({
            "CNPJ OU CPF FAVORECIDO": cnpj, "NOME FAVORECIDO": razao_social,
            "DATA TRANSAÇÃO": "15/05/2024", "VALOR TRANSAÇÃO": str(round(random.uniform(100, 900), 2)).replace('.', ',')
        })

# ─────────────────────────────────────────
# SALVAMENTO EM ARQUIVOS
# ─────────────────────────────────────────

def save_csvs():
    """Salva os dicionários em disco mapeando para TODAS as pastas exigidas pelos pipelines."""
    print("Salvando arquivos CSV de acordo com a estrutura do pipeline...")
    
    dirs = [
        "siafi", "cnpj/2024-01/csv", "pncp_csv", "sancoes_cgu", "bndes", 
        "servidores/2024/01", "emendas_cgu", "camara", "senado", 
        "tse/candidatos", "tse/doacoes", "tse/bens", "pgfn", "cpgf"
    ]
    for d in dirs:
        os.makedirs(os.path.join(DATA_DIR, d), exist_ok=True)
        
    # SIAFI
    pd.DataFrame(db["uasgs"]).to_excel(os.path.join(DATA_DIR, "siafi", "unidades.xlsx"), index=False)
    
    # CNPJ
    if db["empresas"]: pd.DataFrame(db["empresas"]).to_csv(os.path.join(DATA_DIR, "cnpj/2024-01/csv", "empresas.csv"), index=False)
    if db["socios"]: pd.DataFrame(db["socios"]).to_csv(os.path.join(DATA_DIR, "cnpj/2024-01/csv", "socios.csv"), index=False)
    if db["estabelecimentos"]: pd.DataFrame(db["estabelecimentos"]).to_csv(os.path.join(DATA_DIR, "cnpj/2024-01/csv", "estabelecimentos.csv"), index=False)
    if db["simples"]: pd.DataFrame(db["simples"]).to_csv(os.path.join(DATA_DIR, "cnpj/2024-01/csv", "simples.csv"), index=False)
    
    # PNCP
    if db["itens"]: pd.DataFrame(db["itens"]).to_csv(os.path.join(DATA_DIR, "pncp_csv", "itens.csv"), index=False)
    if db["contratos"]: pd.DataFrame(db["contratos"]).to_csv(os.path.join(DATA_DIR, "pncp_csv", "contratos.csv"), index=False)
    if db["empenhos"]: pd.DataFrame(db["empenhos"]).to_csv(os.path.join(DATA_DIR, "pncp_csv", "empenhos.csv"), index=False)
    
    # SANÇÕES
    if db["sancoes"]: pd.DataFrame(db["sancoes"]).to_csv(os.path.join(DATA_DIR, "sancoes_cgu", "ceis.csv"), index=False)
    
    # PGFN
    if db["pgfn"]: pd.DataFrame(db["pgfn"]).to_csv(os.path.join(DATA_DIR, "pgfn", "divida_ativa.csv"), index=False)
    
    # SERVIDORES
    if db["servidores"]: pd.DataFrame(db["servidores"]).to_csv(os.path.join(DATA_DIR, "servidores/2024/01", "cadastro.csv"), index=False)
    
    # EMENDAS
    if db["emendas"]: pd.DataFrame(db["emendas"]).to_csv(os.path.join(DATA_DIR, "emendas_cgu", "emendas.csv"), index=False)
    if db["convenios"]: pd.DataFrame(db["convenios"]).to_csv(os.path.join(DATA_DIR, "emendas_cgu", "convenios.csv"), index=False)
    if db["despesas_emendas"]: pd.DataFrame(db["despesas_emendas"]).to_csv(os.path.join(DATA_DIR, "emendas_cgu", "por_favorecido.csv"), index=False)
    
    # CAMARA E SENADO
    if db["camara"]: pd.DataFrame(db["camara"]).to_csv(os.path.join(DATA_DIR, "camara", "despesas_2024.csv"), index=False)
    if db["senado"]: pd.DataFrame(db["senado"]).to_csv(os.path.join(DATA_DIR, "senado", "despesas_2024.csv"), index=False)
    
    # BNDES
    if db["bndes"]: pd.DataFrame(db["bndes"]).to_csv(os.path.join(DATA_DIR, "bndes", "operacoes_2024.csv"), index=False, sep=';')
    
    # TSE
    if db["tse_candidatos"]: pd.DataFrame(db["tse_candidatos"]).to_csv(os.path.join(DATA_DIR, "tse/candidatos", "candidatos_2022.csv"), index=False)
    if db["tse_doacoes"]: pd.DataFrame(db["tse_doacoes"]).to_csv(os.path.join(DATA_DIR, "tse/doacoes", "doacoes_2022.csv"), index=False)
    if db["tse_bens"]: pd.DataFrame(db["tse_bens"]).to_csv(os.path.join(DATA_DIR, "tse/bens", "bens_2022.csv"), index=False)
    
    # CPGF
    if db["cpgf"]: pd.DataFrame(db["cpgf"]).to_csv(os.path.join(DATA_DIR, "cpgf", "cpgf.csv"), index=False, sep=';')

# ─────────────────────────────────────────
# EXECUÇÃO PRINCIPAL
# ─────────────────────────────────────────
if __name__ == "__main__":
    print(f"Gerando dados sintéticos para {TOTAL_EMPRESAS} empresas...")
    
    generate_base_data()
    
    distribution = distribute_patterns()
    
    for idx, patterns in enumerate(distribution):
        apply_patterns_to_company(idx, patterns)
        
    save_csvs()
    print("✓ Geração concluída com sucesso! Todos os arquivos base para o ETL foram recriados.")