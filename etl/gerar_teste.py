import pandas as pd
import os
import json
import random
import shutil
from datetime import datetime

# Configuração de pastas
DATA_DIR = "data"

# IDs padronizados para interconexões (base)
CPF_JOAO = "11111111111"
CPF_MARIA = "22222222222"
CPF_JOSE = "33333333333"
CPF_POLITICO = "99988877766"

CNPJ_FACHADA = "11111111000100"
CNPJ_AMIGA = "22222222000100"
CNPJ_SUSPEITOS = "33333333000199"
CNPJ_XYZ = "12345678000199"
CNPJ_HOTEL = "22345678000100"
CNPJ_GRAFICA = "32345678000100"

MUNICIPIO_BRASILIA = "9701"
UF_DF = "DF"

# Multiplicador
MULT = 10

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
    # Variações para Splink: alterar dígitos
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
    
    # Regiões
    pd.DataFrame({
        'id': ['1', '5'], 
        'sigla': ['N', 'CO'], 
        'nome': ['Norte', 'Centro-Oeste'],
        'fonte_nome': ['IBGE', 'IBGE'], 
        'fonte_url': ['', ''], 
        'fonte_descricao': ['', ''], 
        'fonte_licenca': ['', ''], 
        'fonte_coletado_em': ['', '']
    }).to_csv(os.path.join(ibge_dir, "regioes.csv"), index=False, sep=',', encoding='utf-8')

    # Estados
    pd.DataFrame({
        'id': ['53'], 
        'sigla': ['DF'], 
        'nome': ['Distrito Federal'], 
        'regiao_id': ['5'],
        'fonte_nome': ['IBGE'], 
        'fonte_url': [''], 
        'fonte_descricao': [''], 
        'fonte_licenca': [''], 
        'fonte_coletado_em': ['']
    }).to_csv(os.path.join(ibge_dir, "estados.csv"), index=False, sep=',', encoding='utf-8')

    # Mesorregiões
    pd.DataFrame({
        'id': ['5301'], 
        'nome': ['Distrito Federal'], 
        'UF_id': ['53'],
        'fonte_nome': ['IBGE'], 
        'fonte_url': [''], 
        'fonte_descricao': [''], 
        'fonte_licenca': [''], 
        'fonte_coletado_em': ['']
    }).to_csv(os.path.join(ibge_dir, "mesorregioes.csv"), index=False, sep=',', encoding='utf-8')

    # Microrregiões
    pd.DataFrame({
        'id': ['53001'], 
        'nome': ['Brasília'], 
        'mesorregiao_id': ['5301'],
        'fonte_nome': ['IBGE'], 
        'fonte_url': [''], 
        'fonte_descricao': [''], 
        'fonte_licenca': [''], 
        'fonte_coletado_em': ['']
    }).to_csv(os.path.join(ibge_dir, "microrregioes.csv"), index=False, sep=',', encoding='utf-8')

    # Municípios
    pd.DataFrame({
        'id': ['5300108'], 
        'nome': ['Brasília'], 
        'microrregiao_id': ['53001'],
        'fonte_nome': ['IBGE'], 
        'fonte_url': [''], 
        'fonte_descricao': [''], 
        'fonte_licenca': [''], 
        'fonte_coletado_em': ['']
    }).to_csv(os.path.join(ibge_dir, "municipios.csv"), index=False, sep=',', encoding='utf-8')

def generate_cnpj_data():
    print("Gerando dados de CNPJ...")
    snapshot_dir = os.path.join(DATA_DIR, "cnpj", "2024-01", "csv")
    os.makedirs(snapshot_dir, exist_ok=True)

    # Domínios (fixos)
    pd.DataFrame({'codigo_cnae': ['0000000'], 'descricao_cnae': ['ATIVIDADE TESTE']}).to_csv(os.path.join(snapshot_dir, "cnaes.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame({'codigo_natureza': ['2062'], 'descricao_natureza': ['SOCIEDADE LIMITADA']}).to_csv(os.path.join(snapshot_dir, "naturezas.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame({'codigo_qualificacao': ['05'], 'descricao_qualificacao': ['ADMINISTRADOR']}).to_csv(os.path.join(snapshot_dir, "qualificacoes.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame({'codigo_motivo': ['00'], 'descricao_motivo': ['SEM MOTIVO']}).to_csv(os.path.join(snapshot_dir, "motivos.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame({'codigo_municipio_rf': ['9701'], 'nome_municipio': ['BRASILIA']}).to_csv(os.path.join(snapshot_dir, "municipios_rf.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame({'codigo_pais': ['105'], 'nome_pais': ['BRASIL']}).to_csv(os.path.join(snapshot_dir, "paises.csv"), index=False, sep=';', encoding='utf-8')

    # Empresas (multiplicadas)
    empresas_data = []
    estab_data = []
    socios_data = []
    base_empresas = [
        {'cnpj': CNPJ_FACHADA, 'razao': 'EMPRESA FACHADA LTDA', 'natureza': '2062', 'porte': '01'},
        {'cnpj': CNPJ_AMIGA, 'razao': 'CONSTRUTORA AMIGA SA', 'natureza': '2054', 'porte': '05'},
        {'cnpj': CNPJ_SUSPEITOS, 'razao': 'SUPRIMENTOS SUSPEITOS ME', 'natureza': '2135', 'porte': '01'},
    ]
    for i in range(MULT):
        for emp in base_empresas:
            cnpj_varied = vary_cnpj(emp['cnpj']) if i > 0 else emp['cnpj']
            empresas_data.append({
                'cnpj_basico': cnpj_varied[:8],
                'razao_social': vary_name(emp['razao']) if i > 0 else emp['razao'],
                'natureza_juridica': emp['natureza'],
                'qualificacao_responsavel': '05',
                'capital_social': f'{random.randint(1000, 5000000)},00',
                'porte_empresa': emp['porte'],
                'ente_federativo': ''
            })
            estab_data.append({
                'cnpj_basico': cnpj_varied[:8],
                'cnpj_ordem': '0001',
                'cnpj_dv': cnpj_varied[-2:],
                'identificador_matriz_filial': '1',
                'nome_fantasia': vary_name(emp['razao'].split()[0]),
                'situacao_cadastral': '02',
                'data_situacao_cadastral': '20200101',
                'motivo_situacao_cadastral': '00',
                'nome_cidade_exterior': '',
                'pais': '105',
                'data_inicio_atividade': '20200101',
                'cnae_fiscal_principal': '0000000',
                'cnae_fiscal_secundaria': '',
                'tipo_logradouro': 'RUA',
                'logradouro': 'TESTE',
                'numero': str(i+1),
                'complemento': '',
                'bairro': 'CENTRO',
                'cep': '00000000',
                'uf': UF_DF,
                'municipio': MUNICIPIO_BRASILIA,
                'ddd_1': '', 'telefone_1': '',
                'ddd_2': '', 'telefone_2': '',
                'ddd_fax': '', 'fax': '',
                'correio_eletronico': '',
                'situacao_especial': '', 'data_situacao_especial': ''
            })

    # Sócios (multiplicados e com duplicatas para Splink)
    base_socios = [
        {'nome': 'JOAO SERVIDOR PUBLICO', 'cpf': CPF_JOAO, 'cnpj': CNPJ_FACHADA},
        {'nome': 'MARIA DOADORA CAMPANHA', 'cpf': CPF_MARIA, 'cnpj': CNPJ_AMIGA},
        {'nome': 'JOSE BENEFICIARIO AUXILIO', 'cpf': CPF_JOSE, 'cnpj': CNPJ_SUSPEITOS},
        {'nome': 'POLITICO INFLUENTE', 'cpf': CPF_POLITICO, 'cnpj': CNPJ_AMIGA},
    ]
    for i in range(MULT):
        for soc in base_socios:
            cpf_varied = vary_cpf(soc['cpf']) if i > 0 else soc['cpf']
            nome_varied = vary_name(soc['nome']) if i > 0 else soc['nome']
            cnpj_varied = vary_cnpj(soc['cnpj']) if i > 0 else soc['cnpj']
            socios_data.append({
                'cnpj_basico': cnpj_varied[:8],
                'identificador_socio': '2',
                'nome_socio': nome_varied,
                'cpf_cnpj_socio': cpf_varied,
                'qualificacao_socio': '49',
                'data_entrada': '20200101',
                'cpf_representante_legal': '00000000000',
                'nome_representante': '',
                'qualificacao_representante_legal': '00',
                'faixa_etaria': '0'
            })

    pd.DataFrame(empresas_data).to_csv(os.path.join(snapshot_dir, "empresas.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame(estab_data).to_csv(os.path.join(snapshot_dir, "estabelecimentos.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame(socios_data).to_csv(os.path.join(snapshot_dir, "socios.csv"), index=False, sep=';', encoding='utf-8')

def generate_tse_data():
    print("Gerando dados do TSE...")
    tse_dir = os.path.join(DATA_DIR, "tse")
    candidatos_dir = os.path.join(tse_dir, "candidatos")
    doacoes_dir = os.path.join(tse_dir, "doacoes")
    ano_dir_cand = os.path.join(candidatos_dir, "2022")
    ano_dir_doac = os.path.join(doacoes_dir, "2022")
    os.makedirs(ano_dir_cand, exist_ok=True)
    os.makedirs(ano_dir_doac, exist_ok=True)

    candidatos_data = []
    doacoes_data = []
    base_candidatos = [
        {'sq': '10001', 'nome': 'POLITICO INFLUENTE', 'cpf': CPF_POLITICO, 'partido': 'PTST'},
    ]
    for i in range(MULT):
        for cand in base_candidatos:
            cpf_varied = vary_cpf(cand['cpf']) if i > 0 else cand['cpf']
            nome_varied = vary_name(cand['nome']) if i > 0 else cand['nome']
            candidatos_data.append({
                'ANO_ELEICAO': '2022',
                'CD_TIPO_ELEICAO': '2', 'NM_TIPO_ELEICAO': 'ELEICAO ORDINARIA', 'NR_TURNO': '1',
                'CD_ELEICAO': '546', 'DS_ELEICAO': 'Eleições Gerais Estaduais 2022', 'DT_ELEICAO': '02/10/2022',
                'SG_UF': UF_DF, 'SG_UE': UF_DF, 'NM_UE': 'DISTRITO FEDERAL',
                'CD_CARGO': '6', 'DS_CARGO': 'DEPUTADO FEDERAL', 'SQ_CANDIDATO': cand['sq'],
                'NR_CANDIDATO': f'10{i+1:02d}', 'NM_CANDIDATO': nome_varied, 'NM_URNA_CANDIDATO': nome_varied.split()[0],
                'NR_TITULO_ELEITORAL_CANDIDATO': '123456789012',
                'NR_CPF_CANDIDATO': cpf_varied,
                'SG_PARTIDO': cand['partido'], 'NM_PARTIDO': 'PARTIDO DE TESTE', 'NR_PARTIDO': '10',
                'DT_NASCIMENTO': '01/01/1970', 'DS_GENERO': 'MASCULINO', 'DS_GRAU_INSTRUCAO': 'SUPERIOR COMPLETO',
                'DS_ESTADO_CIVIL': 'CASADO(A)', 'DS_COR_RACA': 'BRANCA', 'CD_OCUPACAO': '100', 'DS_OCUPACAO': 'OUTROS',
                'DS_SITUACAO_CANDIDATURA': 'APTO', 'CD_SITUACAO_CANDIDATURA': '12', 'DS_NACIONALIDADE': 'BRASILEIRA',
                'CD_MUNICIPIO_NASCIMENTO': '5300108'
            })

    base_doadores = [
        {'cpf': CPF_MARIA, 'nome': 'MARIA DOADORA CAMPANHA'},
    ]
    for i in range(MULT):
        for doa in base_doadores:
            cpf_varied = vary_cpf(doa['cpf']) if i > 0 else doa['cpf']
            nome_varied = vary_name(doa['nome']) if i > 0 else doa['nome']
            doacoes_data.append({
                'ANO_ELEICAO': '2022',
                'SQ_CANDIDATO': '10001', 'NR_CPF_CANDIDATO': CPF_POLITICO, 'NM_CANDIDATO': 'POLITICO INFLUENTE',
                'NR_CPF_DOADOR': cpf_varied, 'NM_DOADOR': nome_varied,
                'VR_RECEITA': f'{random.randint(1000, 100000)},00', 'DT_RECEITA': '01/10/2022', 'DS_RECEITA': 'DOACAO'
            })

    pd.DataFrame(candidatos_data).to_csv(os.path.join(ano_dir_cand, "candidatos_2022.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame(doacoes_data).to_csv(os.path.join(ano_dir_doac, "doacoes_2022.csv"), index=False, sep=';', encoding='utf-8')

def generate_cgu_data():
    print("Gerando dados da CGU...")
    cgu_dir = os.path.join(DATA_DIR, "emendas_cgu")
    os.makedirs(cgu_dir, exist_ok=True)
    
    # Emendas
    emendas = pd.DataFrame({
        'Código da Emenda': ['20240001'],
        'Ano da Emenda': ['2024'],
        'Código do Autor da Emenda': ['10001'],
        'Nome do Autor da Emenda': ['POLITICO INFLUENTE'],
        'Código Município IBGE': ['5300108'],
        'Nome do Município Beneficiado': ['BRASILIA'],
        'Valor Empenhado': ['1.000.000,00'],
        'Valor Liquidado': ['800.000,00'],
        'Valor Pago': ['800.000,00'],
        'Código Função': ['10'],
        'Nome Função': ['SAUDE'],
        'Código Subfunção': ['301'],
        'Nome Subfunção': ['ATENCAO BASICA'],
        'Código Programa': ['1234'],
        'Nome Programa': ['PROGRAMA TESTE'],
        'Tipo de Emenda': ['INDIVIDUAL'],
        'Número da Emenda': ['0001'],
        'Localidade do Gasto': ['BRASILIA'],
        'Região': ['CENTRO-OESTE'],
        'Valor Restos a Pagar Inscrito': ['0,00'],
        'Valor Restos a Pagar Cancelado': ['0,00'],
        'Valor Restos a Pagar Pago': ['0,00']
    })
    emendas.to_csv(os.path.join(cgu_dir, "emendas.csv"), index=False, sep=';', encoding='utf-8')

    # Sanções (CEIS)
    sancoes_dir = os.path.join(DATA_DIR, "sancoes_cgu")
    os.makedirs(sancoes_dir, exist_ok=True)
    sancoes = pd.DataFrame({
        'cpf_cnpj': [CNPJ_SUSPEITOS],
        'nome': ['SUPRIMENTOS SUSPEITOS ME'],
        'tipo_sancao': ['Inidoneidade'],
        'data_inicio': ['2023-01-01'],
        'data_fim': ['2025-01-01'],
        'orgao_sancionador': ['PREFEITURA DE TESTE'],
        'uf_orgao': [UF_DF],
        'esfera_governo': ['MUNICIPAL'],
        'fundamentacao': ['TESTE'],
        'numero_processo': ['123'],
        'valor_multa': ['0,00']
    })
    sancoes.to_csv(os.path.join(sancoes_dir, "ceis.csv"), index=False, sep=';', encoding='utf-8')

def generate_pncp_data():
    print("Gerando dados do PNCP...")
    pncp_dir = os.path.join(DATA_DIR, "pncp_csv")
    os.makedirs(pncp_dir, exist_ok=True)

    itens_data = []
    contratos_data = []
    empenhos_data = []
    base_fornecedores = [
        {'cnpj': CNPJ_FACHADA, 'nome': 'EMPRESA FACHADA LTDA'},
        {'cnpj': CNPJ_AMIGA, 'nome': 'CONSTRUTORA AMIGA SA'},
        {'cnpj': CNPJ_SUSPEITOS, 'nome': 'SUPRIMENTOS SUSPEITOS ME'},
    ]
    for i in range(MULT):
        for forn in base_fornecedores:
            cnpj_varied = vary_cnpj(forn['cnpj']) if i > 0 else forn['cnpj']
            nome_varied = vary_name(forn['nome']) if i > 0 else forn['nome']
            itens_data.append({
                'id_contratacao_pncp': f'2024-{i+1:03d}',
                'numero_item_pncp': '1',
                'ni_fornecedor': cnpj_varied,
                'tipo_pessoa': 'PJ',
                'nome_razao_social_fornecedor': nome_varied,
                'quantidade_homologada': '1',
                'valor_unitario_homologado': f'{random.randint(10000, 2000000)},00',
                'orgao_entidade_cnpj': '00000000000100',
                'unidade_orgao_uf_sigla': UF_DF,
                'municipio_nome': 'BRASILIA'
            })
            contratos_data.append({
                'id': f'C{i+1:03d}',
                'numero': f'{i+1:03d}/2024',
                'fonecedor_cnpj_cpf_idgener': cnpj_varied,
                'fornecedor_nome': nome_varied,
                'valor_global': f'{random.randint(10000, 2000000)},00',
                'data_assinatura': f'2024-01-{i+10:02d}',
                'objeto': random.choice(['LIMPEZA', 'OBRA PUBLICA', 'CONSULTORIA', 'FORNECIMENTO'])
            })
            empenhos_data.append({
                'id': f'E{i+1:03d}',
                'numero_empenho': f'EMP{i+1:03d}',
                'data_emissao': f'2024-01-{i+15:02d}',
                'valor_total': f'{random.randint(10000, 2000000)},00'
            })

    pd.DataFrame(itens_data).to_csv(os.path.join(pncp_dir, "itens.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame(contratos_data).to_csv(os.path.join(pncp_dir, "contratos.csv"), index=False, sep=';', encoding='utf-8')
    pd.DataFrame(empenhos_data).to_csv(os.path.join(pncp_dir, "empenhos.csv"), index=False, sep=';', encoding='utf-8')

def generate_tesouro_transparente_data():
    print("Gerando dados do Tesouro Transparente...")
    tt_dir = os.path.join(DATA_DIR, "tesouro_transparente")
    os.makedirs(tt_dir, exist_ok=True)

    emendas = pd.DataFrame({
        'ano': ['2024', '2024', '2024'],
        'codigo_emenda': ['E001', 'E002', 'E003'],
        'tipo_emenda': ['INDIVIDUAL', 'BANCADA', 'INDIVIDUAL'],
        'autor': ['POLITICO INFLUENTE', 'POLITICO INFLUENTE', 'OUTRO PARLAMENTAR'],
        'funcao': ['SAUDE', 'EDUCACAO', 'INFRAESTRUTURA'],
        'subfuncao': ['ATENCAO BASICA', 'ENSINO FUNDAMENTAL', 'TRANSPORTE'],
        'programa': ['PROGRAMA SAUDE', 'PROGRAMA EDUCACAO', 'PROGRAMA OBRAS'],
        'acao': ['ACAO 1', 'ACAO 2', 'ACAO 3'],
        'localidade': ['BRASILIA', 'BRASILIA', 'GOIANIA'],
        'uf': [UF_DF, UF_DF, 'GO'],
        'valor_empenhado': ['1000000.00', '500000.00', '2000000.00'],
        'valor_liquidado': ['800000.00', '400000.00', '1500000.00'],
        'valor_pago': ['800000.00', '400000.00', '1200000.00'],
        'fonte_nome': ['Tesouro Transparente'] * 3,
        'fonte_url': ['https://www.tesourotransparente.gov.br'] * 3,
        'fonte_descricao': ['Emendas parlamentares'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    emendas.to_csv(
        os.path.join(tt_dir, "emendas.csv"),
        index=False,
        sep=';',
        encoding='utf-8'
    )

def generate_servidores_cgu_data():
    print("Gerando dados de servidores (CGU)...")
    cgu_dir = os.path.join(DATA_DIR, "servidores", "2026", "01")
    os.makedirs(cgu_dir, exist_ok=True)

    cadastro_data = []
    remuneracao_data = []
    base_servidores = [
        {'cpf': CPF_JOAO, 'nome': 'JOAO SILVA', 'cargo': 'ANALISTA', 'org': 'MINISTERIO DA SAUDE', 'uf': UF_DF},
        {'cpf': CPF_MARIA, 'nome': 'MARIA SOUZA', 'cargo': 'ASSESSOR', 'org': 'MINISTERIO DA EDUCACAO', 'uf': UF_DF},
        {'cpf': CPF_JOSE, 'nome': 'CARLOS OLIVEIRA', 'cargo': 'TECNICO', 'org': 'PREFEITURA GOIANIA', 'uf': 'GO'},
    ]
    for i in range(MULT):
        for srv in base_servidores:
            cpf_varied = vary_cpf(srv['cpf']) if i > 0 else srv['cpf']
            nome_varied = vary_name(srv['nome']) if i > 0 else srv['nome']
            cadastro_data.append({
                "id_servidor": str(i*len(base_servidores) + base_servidores.index(srv) + 1),
                "cpf": cpf_varied,
                "nome": nome_varied,
                "cargo": srv['cargo'],
                "classe": random.choice(['A', 'B', 'C']),
                "org_lotacao": srv['org'],
                "org_exercicio": srv['org'],
                "uorg_lotacao": f"U{base_servidores.index(srv)+1}",
                "uorg_exercicio": f"U{base_servidores.index(srv)+1}",
                "situacao_vinculo": "ATIVO",
                "regime_juridico": "ESTATUTARIO",
                "tipo_vinculo": random.choice(["EFETIVO", "COMISSIONADO", "TEMPORARIO"]),
                "jornada_trabalho": "40h",
                "data_ingresso_orgao": "2015-03-10",
                "data_ingresso_servico": "2015-03-10",
                "uf_exercicio": srv['uf'],
                "municipio_exercicio": "BRASILIA" if srv['uf'] == UF_DF else "GOIANIA",
                "cd_uasg": f"10{base_servidores.index(srv)+1:02d}",
                "fonte_categoria": "CGU",
                "fonte_nome": "CGU",
                "fonte_url": "https://portaldatransparencia.gov.br"
            })
            remuneracao_data.append({
                "id_servidor": str(i*len(base_servidores) + base_servidores.index(srv) + 1),
                "ano": "2024",
                "mes": "01",
                "fonte_categoria": "CGU",
                "remuneracao_bruta": str(random.randint(5000, 20000)),
                "remuneracao_liquida": str(random.randint(4000, 15000)),
                "total_bruto": str(random.randint(5000, 20000)),
                "irrf": str(random.randint(200, 2000)),
                "pss_rpps": str(random.randint(100, 1000)),
                "abate_teto": "0",
                "gratificacao_natalina": "0",
                "ferias": "0",
                "verbas_indenizatorias": "0",
                "outras_verbas": "0"
            })

    pd.DataFrame(cadastro_data).to_csv(os.path.join(cgu_dir, "cadastro.csv"), index=False, sep=',', encoding='utf-8')
    pd.DataFrame(remuneracao_data).to_csv(os.path.join(cgu_dir, "remuneracao.csv"), index=False, sep=',', encoding='utf-8')

def generate_sancoes_cgu_data():
    print("Gerando dados de sanções CGU (compatível com pipeline)...")

    sancoes_dir = os.path.join(DATA_DIR, "sancoes_cgu")
    os.makedirs(sancoes_dir, exist_ok=True)

    # CEIS
    ceis = pd.DataFrame({
        'cpf_cnpj': [CPF_JOAO, CNPJ_SUSPEITOS],
        'nome': ['JOAO SILVA', 'SUPRIMENTOS SUSPEITOS ME'],
        'tipo_sancao': ['DEMITIDO', 'INIDONEIDADE'],
        'data_inicio': ['2022-05-10', '2023-01-01'],
        'data_fim': ['2027-05-10', '2025-01-01'],
        'orgao_sancionador': ['CGU', 'MINISTERIO DA SAUDE'],
        'uf_orgao': ['DF', 'DF'],
        'esfera_governo': ['FEDERAL', 'FEDERAL'],
        'fundamentacao': ['IMPROBIDADE', 'FRAUDE'],
        'numero_processo': ['123', '456'],
        'valor_multa': ['0,00', '10000,00'],
        'fonte_nome': ['CGU'] * 2,
        'fonte_url': ['https://portaldatransparencia.gov.br'] * 2
    })

    ceis.to_csv(
        os.path.join(sancoes_dir, "ceis.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

    # CNEP
    cnep = pd.DataFrame({
        'cpf_cnpj': ['22345678000100'],
        'nome': ['EMPRESA FRAUDULENTA SA'],
        'tipo_sancao': ['MULTA'],
        'data_inicio': ['2021-03-01'],
        'data_fim': ['2024-03-01'],
        'orgao_sancionador': ['CGU'],
        'uf_orgao': ['DF'],
        'esfera_governo': ['FEDERAL'],
        'fundamentacao': ['ATO LESIVO'],
        'numero_processo': ['789'],
        'valor_multa': ['50000,00'],
        'fonte_nome': ['CGU'],
        'fonte_url': ['https://portaldatransparencia.gov.br']
    })

    cnep.to_csv(
        os.path.join(sancoes_dir, "cnep.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

# Removida função duplicada generate_pgfn_data() - mantida a versão abaixo

def generate_pgfn_data():
    print("Gerando dados da PGFN (compatível com pipeline)...")
    pgfn_dir = os.path.join(DATA_DIR, "pgfn")
    os.makedirs(pgfn_dir, exist_ok=True)

    df = pd.DataFrame({
        'cpf_cnpj': [CNPJ_XYZ, CNPJ_AMIGA, CPF_JOAO],
        'nome_devedor': ['EMPRESA XYZ LTDA', 'CONSTRUTORA AMIGA SA', 'JOAO SILVA'],
        'numero_inscricao': ['DAU001', 'DAU002', 'DAU003'],
        'tipo_credito': ['TRIBUTARIO', 'NAO_TRIBUTARIO', 'TRIBUTARIO'],
        'receita_principal': ['IRPJ', 'MULTA', 'INSS'],
        'situacao': ['ATIVA', 'PARCELADA', 'ATIVA'],
        'situacao_juridica': ['REGULAR', 'REGULAR', 'IRREGULAR'],
        'valor_consolidado': ['1500000.00', '300000.00', '50000.00'],
        'data_inscricao': ['2020-06-15', '2021-09-10', '2019-03-22'],
        'indicador_ajuizado': ['SIM', 'NAO', 'SIM'],
        'uf_devedor': ['SP', 'RJ', UF_DF],
        'municipio_devedor': ['SAO PAULO', 'RIO DE JANEIRO', 'BRASILIA'],
        'fonte_nome': ['PGFN'] * 3
    })

    df.to_csv(
        os.path.join(pgfn_dir, "divida_ativa_2024.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

def generate_camara_data():
    print("Gerando dados da Câmara (compatível com pipeline)...")
    camara_dir = os.path.join(DATA_DIR, "camara")
    os.makedirs(camara_dir, exist_ok=True)

    despesas_data = []
    base_despesas = [
        {'parlamentar': 'DEP. POLITICO INFLUENTE', 'fornecedor': 'EMPRESA FACHADA LTDA', 'cnpj': CNPJ_FACHADA, 'tipo': 'COMBUSTIVEL'},
        {'parlamentar': 'DEP. MARIA SOUZA', 'fornecedor': 'HOTEL BRASILIA LTDA', 'cnpj': CNPJ_HOTEL, 'tipo': 'HOSPEDAGEM'},
        {'parlamentar': 'DEP. POLITICO INFLUENTE', 'fornecedor': 'GRAFICA CENTRAL', 'cnpj': CNPJ_GRAFICA, 'tipo': 'DIVULGACAO'},
    ]
    for i in range(MULT):
        for desp in base_despesas:
            cnpj_varied = vary_cnpj(desp['cnpj']) if i > 0 else desp['cnpj']
            nome_varied = vary_name(desp['fornecedor']) if i > 0 else desp['fornecedor']
            despesas_data.append({
                'despesa_id': f'D{i*len(base_despesas)+base_despesas.index(desp)+1:03d}',
                'ano': '2024',
                'mes': str((i % 12) + 1),
                'nome_parlamentar': desp['parlamentar'],
                'partido': 'PTST' if 'POLITICO' in desp['parlamentar'] else 'XYZ',
                'uf': UF_DF,
                'nome_fornecedor': nome_varied,
                'cnpj_fornecedor': cnpj_varied,
                'tipo_despesa': desp['tipo'],
                'valor_liquido': f'{random.randint(500, 5000)}.00',
                'data_emissao': f'2024-{(i % 12)+1:02d}-{random.randint(1,28):02d}',
                'fonte_nome': 'Câmara dos Deputados'
            })
    df = pd.DataFrame(despesas_data)

    df.to_csv(
        os.path.join(camara_dir, "despesas_2024.csv"),
        index=False,
        sep=',',
        encoding='utf-8'
    )

def generate_bndes_data():
    print("Gerando dados do BNDES (pipeline compatível)...")
    bndes_dir = os.path.join(DATA_DIR, "bndes")
    os.makedirs(bndes_dir, exist_ok=True)

    bndes_data = []
    base_bndes = [
        {'cnpj': CNPJ_FACHADA, 'cliente': 'EMPRESA FACHADA LTDA'},
        {'cnpj': CNPJ_AMIGA, 'cliente': 'CONSTRUTORA AMIGA SA'},
        {'cnpj': CNPJ_SUSPEITOS, 'cliente': 'SUPRIMENTOS SUSPEITOS ME'},
    ]
    for i in range(MULT):
        for bnd in base_bndes:
            cnpj_varied = vary_cnpj(bnd['cnpj']) if i > 0 else bnd['cnpj']
            cliente_varied = vary_name(bnd['cliente']) if i > 0 else bnd['cliente']
            bndes_data.append({
                '_id': f'EMP{i*len(base_bndes)+base_bndes.index(bnd)+1:03d}',
                'cnpj': cnpj_varied,
                'cliente': cliente_varied,
                'descricao_do_projeto': random.choice(['EXPANSAO', 'MODERNIZACAO', 'ENERGIA SOLAR', 'PESQUISA']),
                'uf': random.choice(['SP', 'RJ', 'MG', 'DF']),
                'municipio': 'SAO PAULO',
                'numero_do_contrato': f'CTR{i+1:03d}',
                'data_da_contratacao': f'202{random.randint(1,4)}-04-10',
                'valor_contratado_reais': f'{random.randint(1000000, 10000000)}.00',
                'valor_desembolsado_reais': f'{random.randint(500000, 5000000)}.00',
                'fonte_de_recurso': random.choice(['TESOURO', 'FAT']),
                'custo_financeiro': str(random.uniform(4.0, 7.0))[:3],
                'juros': str(random.uniform(1.0, 2.0))[:3],
                'prazo_carencia_meses': str(random.randint(6, 24)),
                'prazo_amortizacao_meses': str(random.randint(36, 120)),
                'modalidade_de_apoio': random.choice(['DIRETO', 'INDIRETO']),
                'forma_de_apoio': 'FINANCIAMENTO',
                'produto': random.choice(['FINEM', 'BNDES AUTOMATICO', 'FINAME']),
                'instrumento_financeiro': 'EMPRESTIMO',
                'inovacao': random.choice(['NAO', 'SIM']),
                'area_operacional': random.choice(['INDUSTRIA', 'SERVICOS', 'ENERGIA']),
                'setor_cnae': str(random.randint(1000, 9999)),
                'subsetor_cnae_nome': random.choice(['INDUSTRIA', 'SERVICOS', 'ENERGIA']),
                'setor_bndes': random.choice(['INDUSTRIA', 'SERVICOS', 'ENERGIA']),
                'porte_do_cliente': random.choice(['GRANDE', 'MEDIO', 'PEQUENO']),
                'natureza_do_cliente': 'PRIVADA',
                'situacao_do_contrato': random.choice(['ATIVA', 'ENCERRADA']),
                'fonte_nome': 'BNDES'
            })
    df = pd.DataFrame(bndes_data)

    df.to_csv(
        os.path.join(bndes_dir, "operacoes_2024.csv"),
        index=False,
        sep=';',
        encoding='utf-8'
    )

def generate_siafi_data():
    print("Gerando dados do Siafi...")
    siafi_dir = os.path.join(DATA_DIR, "siafi")
    os.makedirs(siafi_dir, exist_ok=True)

    # Unidades Gestoras
    unidades = pd.DataFrame({
        'codigo_unidade_gestora': ['1001', '1002'],
        'nome_unidade_gestora': ['UNIDADE GESTORA 1', 'UNIDADE GESTORA 2'],
        'codigo_orgao': ['10', '20'],
        'nome_orgao': ['ORGAO TESTE 1', 'ORGAO TESTE 2'],
        'codigo_esfera': ['1', '2'],
        'nome_esfera': ['FEDERAL', 'ESTADUAL'],
        'fonte_nome': ['Siafi'] * 2,
        'fonte_url': ['https://siafi.tesouro.gov.br'] * 2
    })
    unidades.to_excel(os.path.join(siafi_dir, "unidades.xlsx"), index=False)

def generate_senado_data():
    print("Gerando dados do Senado Federal (JSON - CEAP)...")
    senado_dir = os.path.join(DATA_DIR, "senado")
    os.makedirs(senado_dir, exist_ok=True)

    ano = 2024

    data = []
    base_senado = [
        {"cpf": CPF_POLITICO, "nome": "SEN. POLITICO INFLUENTE", "partido": "PTST", "fornecedor": "AUTO POSTO BRASIL LTDA", "cnpj": CNPJ_XYZ, "tipo": "COMBUSTIVEL"},
        {"cpf": CPF_JOAO, "nome": "SEN. JOAO SILVA", "partido": "ABC", "fornecedor": "HOTEL CENTRAL LTDA", "cnpj": CNPJ_HOTEL, "tipo": "HOSPEDAGEM"},
        {"cpf": CPF_POLITICO, "nome": "SEN. POLITICO INFLUENTE", "partido": "PTST", "fornecedor": "AGENCIA PUBLICIDADE", "cnpj": CNPJ_GRAFICA, "tipo": "DIVULGACAO"},
    ]
    for i in range(MULT):
        for sen in base_senado:
            cpf_varied = vary_cpf(sen["cpf"]) if i > 0 else sen["cpf"]
            nome_varied = vary_name(sen["nome"]) if i > 0 else sen["nome"]
            forn_varied = vary_name(sen["fornecedor"]) if i > 0 else sen["fornecedor"]
            cnpj_varied = vary_cnpj(sen["cnpj"]) if i > 0 else sen["cnpj"]
            data.append({
                "ano": 2024,
                "mes": (i % 12) + 1,
                "cpf_senador": cpf_varied,
                "nome_senador": nome_varied,
                "partido": sen["partido"],
                "uf": UF_DF,
                "fornecedor": forn_varied,
                "cnpj_fornecedor": cnpj_varied,
                "tipo_despesa": sen["tipo"],
                "descricao": "Despesa oficial",
                "valor": float(random.randint(500, 5000)),
                "data_emissao": f"2024-{(i % 12)+1:02d}-{random.randint(1,28):02d}"
            })

    wrapped = {
        "metadata": {
            "fonte_nome": "Senado Federal",
            "fonte_descricao": "Despesas CEAP - Cota Parlamentar",
            "fonte_url": "https://adm.senado.gov.br",
            "fonte_licenca": "CC-BY 4.0",
            "ano": ano,
            "coletado_em": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        },
        "data": data
    }

    with open(os.path.join(senado_dir, f"despesas_{ano}.json"), "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    # Limpar dados existentes para evitar conflitos (apenas subdirs)
    for subdir in ["ibge", "cnpj", "tse", "emendas_cgu", "sancoes_cgu", "tesouro_transparente", "servidores", "pncp_csv", "pgfn", "camara", "bndes", "senado", "siafi"]:
        path = os.path.join(DATA_DIR, subdir)
        if os.path.exists(path):
            shutil.rmtree(path)

    generate_ibge_data() #ibge
    generate_cnpj_data() #cnpj
    generate_siafi_data() #siafi
    generate_servidores_cgu_data() #servidores_cgu
    generate_cgu_data() #emendas_cgu
    generate_tse_data() #tse
    generate_sancoes_cgu_data() #sancoes_cgu
    generate_tesouro_transparente_data() #tesouro_transparente
    generate_pncp_data() #pncp
    generate_pgfn_data() #pgfn
    generate_camara_data() #camara
    generate_bndes_data() #bndes
    generate_senado_data() #senado
    print("\nDados sintéticos gerados com sucesso na pasta 'etl/data'!")
