import pandas as pd
import os
import json
from datetime import datetime

# Configuração de pastas
DATA_DIR = "data"

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
    }).to_csv(os.path.join(ibge_dir, "regioes.csv"), index=False, sep=',', encoding='utf-8-sig')

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
    }).to_csv(os.path.join(ibge_dir, "estados.csv"), index=False, sep=',', encoding='utf-8-sig')

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
    }).to_csv(os.path.join(ibge_dir, "mesorregioes.csv"), index=False, sep=',', encoding='utf-8-sig')

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
    }).to_csv(os.path.join(ibge_dir, "microrregioes.csv"), index=False, sep=',', encoding='utf-8-sig')

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
    }).to_csv(os.path.join(ibge_dir, "municipios.csv"), index=False, sep=',', encoding='utf-8-sig')

def generate_cnpj_data():
    print("Gerando dados de CNPJ...")
    snapshot_dir = os.path.join(DATA_DIR, "cnpj", "2024-01", "csv")
    os.makedirs(snapshot_dir, exist_ok=True)
    
    # Domínios
    pd.DataFrame({'codigo_cnae': ['0000000'], 'descricao_cnae': ['ATIVIDADE TESTE']}).to_csv(os.path.join(snapshot_dir, "cnaes.csv"), index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame({'codigo_natureza': ['2062'], 'descricao_natureza': ['SOCIEDADE LIMITADA']}).to_csv(os.path.join(snapshot_dir, "naturezas.csv"), index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame({'codigo_qualificacao': ['05'], 'descricao_qualificacao': ['ADMINISTRADOR']}).to_csv(os.path.join(snapshot_dir, "qualificacoes.csv"), index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame({'codigo_motivo': ['00'], 'descricao_motivo': ['SEM MOTIVO']}).to_csv(os.path.join(snapshot_dir, "motivos.csv"), index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame({'codigo_municipio_rf': ['9701'], 'nome_municipio': ['BRASILIA']}).to_csv(os.path.join(snapshot_dir, "municipios_rf.csv"), index=False, sep=';', encoding='utf-8-sig')
    pd.DataFrame({'codigo_pais': ['105'], 'nome_pais': ['BRASIL']}).to_csv(os.path.join(snapshot_dir, "paises.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Empresas
    empresas = pd.DataFrame({
        'cnpj_basico': ['11111111', '22222222', '33333333'],
        'razao_social': ['EMPRESA FACHADA LTDA', 'CONSTRUTORA AMIGA SA', 'SUPRIMENTOS SUSPEITOS ME'],
        'natureza_juridica': ['2062', '2054', '2135'],
        'qualificacao_responsavel': ['05', '05', '05'],
        'capital_social': ['1000,00', '5000000,00', '100,00'],
        'porte_empresa': ['01', '05', '01'],
        'ente_federativo': ['', '', '']
    })
    empresas.to_csv(os.path.join(snapshot_dir, "empresas.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Estabelecimentos
    estab = pd.DataFrame({
        'cnpj_basico': ['11111111', '22222222', '33333333'],
        'cnpj_ordem': ['0001', '0001', '0001'],
        'cnpj_dv': ['00', '00', '99'],
        'identificador_matriz_filial': ['1', '1', '1'],
        'nome_fantasia': ['FACHADA', 'AMIGA', 'SUSPEITOS'],
        'situacao_cadastral': ['02', '02', '02'],
        'data_situacao_cadastral': ['20200101', '20180515', '20211010'],
        'motivo_situacao_cadastral': ['00', '00', '00'],
        'nome_cidade_exterior': ['', '', ''],
        'pais': ['105', '105', '105'],
        'data_inicio_atividade': ['20200101', '20180515', '20211010'],
        'cnae_fiscal_principal': ['0000000', '0000000', '0000000'],
        'cnae_fiscal_secundaria': ['', '', ''],
        'tipo_logradouro': ['RUA', 'AV', 'RUA'],
        'logradouro': ['TESTE', 'TESTE', 'TESTE'],
        'numero': ['1', '2', '3'],
        'complemento': ['', '', ''],
        'bairro': ['CENTRO', 'CENTRO', 'CENTRO'],
        'cep': ['00000000', '00000000', '00000000'],
        'uf': ['DF', 'DF', 'DF'],
        'municipio': ['9701', '9701', '9701'],
        'ddd_1': ['', '', ''], 'telefone_1': ['', '', ''],
        'ddd_2': ['', '', ''], 'telefone_2': ['', '', ''],
        'ddd_fax': ['', '', ''], 'fax': ['', '', ''],
        'correio_eletronico': ['', '', ''],
        'situacao_especial': ['', '', ''], 'data_situacao_especial': ['', '', '']
    })
    estab.to_csv(os.path.join(snapshot_dir, "estabelecimentos.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Sócios
    socios = pd.DataFrame({
        'cnpj_basico': ['11111111', '22222222', '33333333'],
        'identificador_socio': ['2', '2', '2'],
        'nome_socio': ['JOAO SERVIDOR PUBLICO', 'MARIA DOADORA CAMPANHA', 'JOSE BENEFICIARIO AUXILIO'],
        'cpf_cnpj_socio': ['11111111111', '22222222222', '33333333333'],
        'qualificacao_socio': ['49', '49', '49'],
        'data_entrada': ['20200101', '20180515', '20211010'],
        'cpf_representante_legal': ['00000000000', '00000000000', '00000000000'],
        'nome_representante': ['', '', ''],
        'qualificacao_representante_legal': ['00', '00', '00'],
        'faixa_etaria': ['0', '0', '0']
    })
    socios.to_csv(os.path.join(snapshot_dir, "socios.csv"), index=False, sep=';', encoding='utf-8-sig')

def generate_tse_data():
    print("Gerando dados do TSE...")
    tse_dir = os.path.join(DATA_DIR, "tse")
    os.makedirs(tse_dir, exist_ok=True)
    
    # Candidatos (2022)
    candidatos = pd.DataFrame({
        'ANO_ELEICAO': ['2022'],
        'CD_TIPO_ELEICAO': ['2'], 'NM_TIPO_ELEICAO': ['ELEICAO ORDINARIA'], 'NR_TURNO': ['1'],
        'CD_ELEICAO': ['546'], 'DS_ELEICAO': ['Eleições Gerais Estaduais 2022'], 'DT_ELEICAO': ['02/10/2022'],
        'SG_UF': ['DF'], 'SG_UE': ['DF'], 'NM_UE': ['DISTRITO FEDERAL'],
        'CD_CARGO': ['6'], 'DS_CARGO': ['DEPUTADO FEDERAL'], 'SQ_CANDIDATO': ['10001'],
        'NR_CANDIDATO': ['1010'], 'NM_CANDIDATO': ['POLITICO INFLUENTE'], 'NM_URNA_CANDIDATO': ['POLITICO'],
        'NR_TITULO_ELEITORAL_CANDIDATO': ['123456789012'],
        'NR_CPF_CANDIDATO': ['99988877766'],
        'SG_PARTIDO': ['PTST'], 'NM_PARTIDO': ['PARTIDO DE TESTE'], 'NR_PARTIDO': ['10'],
        'DT_NASCIMENTO': ['01/01/1970'], 'DS_GENERO': ['MASCULINO'], 'DS_GRAU_INSTRUCAO': ['SUPERIOR COMPLETO'],
        'DS_ESTADO_CIVIL': ['CASADO(A)'], 'DS_COR_RACA': ['BRANCA'], 'CD_OCUPACAO': ['100'], 'DS_OCUPACAO': ['OUTROS'],
        'DS_SITUACAO_CANDIDATURA': ['APTO'], 'CD_SITUACAO_CANDIDATURA': ['12']
    })
    candidatos.to_csv(os.path.join(tse_dir, "candidatos_2022.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Doações (2022)
    doacoes = pd.DataFrame({
        'ANO_ELEICAO': ['2022'],
        'SQ_CANDIDATO': ['10001'], 'NR_CPF_CANDIDATO': ['99988877766'], 'NM_CANDIDATO': ['POLITICO INFLUENTE'],
        'NR_CPF_DOADOR': ['22222222222'], 'NM_DOADOR': ['MARIA DOADORA CAMPANHA'],
        'VR_RECEITA': ['50000,00'], 'DT_RECEITA': ['01/10/2022'], 'DS_RECEITA': ['DOACAO']
    })
    doacoes.to_csv(os.path.join(tse_dir, "doacoes_2022.csv"), index=False, sep=';', encoding='utf-8-sig')

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
    emendas.to_csv(os.path.join(cgu_dir, "emendas.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Sanções (CEIS)
    sancoes_dir = os.path.join(DATA_DIR, "sancoes_cgu")
    os.makedirs(sancoes_dir, exist_ok=True)
    sancoes = pd.DataFrame({
        'cpf_cnpj': ['33333333000199'],
        'nome': ['SUPRIMENTOS SUSPEITOS ME'],
        'tipo_sancao': ['Inidoneidade'],
        'data_inicio': ['01/01/2023'],
        'data_fim': ['01/01/2025'],
        'orgao_sancionador': ['PREFEITURA DE TESTE'],
        'uf_orgao': ['DF'],
        'esfera_governo': ['MUNICIPAL'],
        'fundamentacao': ['TESTE'],
        'numero_processo': ['123'],
        'valor_multa': ['0,00']
    })
    sancoes.to_csv(os.path.join(sancoes_dir, "ceis.csv"), index=False, sep=';', encoding='utf-8-sig')

def generate_pncp_data():
    print("Gerando dados do PNCP...")
    pncp_dir = os.path.join(DATA_DIR, "pncp_csv")
    os.makedirs(pncp_dir, exist_ok=True)
    
    # Itens
    itens = pd.DataFrame({
        'id_contratacao_pncp': ['2024-001', '2024-002', '2024-003'],
        'numero_item_pncp': ['1', '1', '1'],
        'ni_fornecedor': ['11111111000100', '22222222000100', '11111111000100'],
        'tipo_pessoa': ['PJ', 'PJ', 'PJ'],
        'nome_razao_social_fornecedor': ['EMPRESA FACHADA LTDA', 'CONSTRUTORA AMIGA SA', 'EMPRESA FACHADA LTDA'],
        'quantidade_homologada': ['1', '1', '1'],
        'valor_unitario_homologado': ['150000,00', '2000000,00', '300000,00'],
        'orgao_entidade_cnpj': ['00000000000100', '00000000000100', '00000000000100'],
        'unidade_orgao_uf_sigla': ['DF', 'DF', 'DF'],
        'municipio_nome': ['BRASILIA', 'BRASILIA', 'BRASILIA']
    })
    itens.to_csv(os.path.join(pncp_dir, "itens.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Contratos
    contratos = pd.DataFrame({
        'id': ['C001', 'C002', 'C003'],
        'numero': ['001/2024', '002/2024', '003/2024'],
        'fonecedor_cnpj_cpf_idgener': ['11111111000100', '22222222000100', '11111111000100'],
        'fornecedor_nome': ['EMPRESA FACHADA LTDA', 'CONSTRUTORA AMIGA SA', 'EMPRESA FACHADA LTDA'],
        'valor_global': ['150000,00', '2000000,00', '300000,00'],
        'data_assinatura': ['2024-01-10', '2024-02-15', '2024-03-20'],
        'objeto': ['LIMPEZA', 'OBRA PUBLICA', 'CONSULTORIA']
    })
    contratos.to_csv(os.path.join(pncp_dir, "contratos.csv"), index=False, sep=';', encoding='utf-8-sig')

    # Empenhos
    empenhos = pd.DataFrame({
        'id': ['E001', 'E002', 'E003'],
        'numero_empenho': ['EMP001', 'EMP002', 'EMP003'],
        'data_emissao': ['2024-01-15', '2024-02-20', '2024-03-25'],
        'valor_total': ['150000,00', '2000000,00', '300000,00']
    })
    empenhos.to_csv(os.path.join(pncp_dir, "empenhos.csv"), index=False, sep=';', encoding='utf-8-sig')

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
        'uf': ['DF', 'DF', 'GO'],
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
        encoding='utf-8-sig'
    )

def generate_servidores_cgu_data():
    print("Gerando dados de servidores (CGU)...")
    cgu_dir = os.path.join(DATA_DIR, "cgu")
    os.makedirs(cgu_dir, exist_ok=True)

    servidores = pd.DataFrame({
        'cpf': ['11111111111', '22222222222', '33333333333'],
        'nome': ['JOAO SILVA', 'MARIA SOUZA', 'CARLOS OLIVEIRA'],
        'orgao': ['MINISTERIO DA SAUDE', 'MINISTERIO DA EDUCACAO', 'PREFEITURA GOIANIA'],
        'tipo_vinculo': ['EFETIVO', 'COMISSIONADO', 'TEMPORARIO'],
        'cargo': ['ANALISTA', 'ASSESSOR', 'TECNICO'],
        'funcao': ['GESTAO', 'ADMINISTRATIVO', 'OPERACIONAL'],
        'uf_exercicio': ['DF', 'DF', 'GO'],
        'municipio_exercicio': ['BRASILIA', 'BRASILIA', 'GOIANIA'],
        'remuneracao_bruta': ['12000.00', '8000.00', '5000.00'],
        'remuneracao_liquida': ['9500.00', '6500.00', '4200.00'],
        'data_ingresso': ['2015-03-10', '2019-07-22', '2021-01-05'],
        'situacao': ['ATIVO', 'ATIVO', 'ATIVO'],

        # Metadados (padrão do teu projeto)
        'fonte_nome': ['CGU - Portal da Transparência'] * 3,
        'fonte_url': ['https://portaldatransparencia.gov.br'] * 3,
        'fonte_descricao': ['Servidores públicos federais'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    servidores.to_csv(
        os.path.join(cgu_dir, "servidores.csv"),
        index=False,
        sep=';',
        encoding='utf-8-sig'
    )

def generate_sancoes_cgu_data():
    print("Gerando dados de sanções (CGU)...")
    cgu_dir = os.path.join(DATA_DIR, "cgu")
    os.makedirs(cgu_dir, exist_ok=True)

    sancoes = pd.DataFrame({
        'cpf_cnpj': ['11111111111', '12345678000199', '22222222222'],
        'tipo_pessoa': ['FISICA', 'JURIDICA', 'FISICA'],
        'nome_sancionado': ['JOAO SILVA', 'EMPRESA XYZ LTDA', 'MARIA SOUZA'],
        'orgao_sancionador': ['CGU', 'MINISTERIO DA SAUDE', 'CGU'],
        'tipo_sancao': ['DEMITIDO', 'SUSPENSAO', 'DESTITUICAO'],
        'descricao_sancao': [
            'Demissão por improbidade administrativa',
            'Suspensão por irregularidade em contrato',
            'Destituição de cargo comissionado'
        ],
        'data_inicio_sancao': ['2022-05-10', '2023-08-15', '2021-11-20'],
        'data_fim_sancao': ['2027-05-10', '2025-08-15', '2024-11-20'],
        'situacao': ['ATIVA', 'ATIVA', 'ENCERRADA'],
        'uf': ['DF', 'SP', 'DF'],
        'municipio': ['BRASILIA', 'SAO PAULO', 'BRASILIA'],

        # Metadados padrão
        'fonte_nome': ['CGU - Portal da Transparência'] * 3,
        'fonte_url': ['https://portaldatransparencia.gov.br'] * 3,
        'fonte_descricao': ['Cadastro de sanções administrativas'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    sancoes.to_csv(
        os.path.join(cgu_dir, "sancoes.csv"),
        index=False,
        sep=';',
        encoding='utf-8-sig'
    )

def generate_pgfn_data():
    print("Gerando dados da PGFN (Dívida Ativa)...")
    pgfn_dir = os.path.join(DATA_DIR, "pgfn")
    os.makedirs(pgfn_dir, exist_ok=True)

    dividas = pd.DataFrame({
        'cpf_cnpj': ['12345678000199', '98765432000188', '11111111111'],
        'tipo_pessoa': ['JURIDICA', 'JURIDICA', 'FISICA'],
        'nome_devedor': ['EMPRESA XYZ LTDA', 'EMPRESA ABC SA', 'JOAO SILVA'],
        'numero_inscricao': ['DAU001', 'DAU002', 'DAU003'],
        'tipo_debito': ['TRIBUTARIO', 'NAO_TRIBUTARIO', 'TRIBUTARIO'],
        'situacao': ['ATIVA', 'PARCELADA', 'ATIVA'],
        'valor_consolidado': ['1500000.00', '300000.00', '50000.00'],
        'data_inscricao': ['2020-06-15', '2021-09-10', '2019-03-22'],
        'uf': ['SP', 'RJ', 'DF'],
        'municipio': ['SAO PAULO', 'RIO DE JANEIRO', 'BRASILIA'],

        # Metadados padrão
        'fonte_nome': ['PGFN - Dívida Ativa da União'] * 3,
        'fonte_url': ['https://www.gov.br/pgfn'] * 3,
        'fonte_descricao': ['Cadastro de débitos inscritos em dívida ativa'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    dividas.to_csv(
        os.path.join(pgfn_dir, "divida_ativa.csv"),
        index=False,
        sep=';',
        encoding='utf-8-sig'
    )

def generate_cpgf_data():
    print("Gerando dados de CPGF (Cartão de Pagamento do Governo Federal)...")
    cpgf_dir = os.path.join(DATA_DIR, "cpgf")
    os.makedirs(cpgf_dir, exist_ok=True)

    gastos = pd.DataFrame({
        'cpf_portador': ['11111111111', '22222222222', '11111111111'],
        'nome_portador': ['JOAO SILVA', 'MARIA SOUZA', 'JOAO SILVA'],
        'orgao': ['MINISTERIO DA SAUDE', 'MINISTERIO DA EDUCACAO', 'MINISTERIO DA SAUDE'],
        'uf': ['DF', 'DF', 'DF'],
        'municipio': ['BRASILIA', 'BRASILIA', 'BRASILIA'],
        'data_transacao': ['2024-01-10', '2024-02-15', '2024-03-05'],
        'estabelecimento': ['POSTO DE COMBUSTIVEL XPTO', 'HOTEL BRASILIA', 'RESTAURANTE CENTRAL'],
        'cnpj_estabelecimento': ['12345678000100', '22345678000100', '32345678000100'],
        'descricao_despesa': ['COMBUSTIVEL', 'HOSPEDAGEM', 'ALIMENTACAO'],
        'valor': ['500.00', '1200.00', '150.00'],

        # Metadados padrão
        'fonte_nome': ['Portal da Transparência - CPGF'] * 3,
        'fonte_url': ['https://portaldatransparencia.gov.br'] * 3,
        'fonte_descricao': ['Gastos com Cartão de Pagamento do Governo Federal'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    gastos.to_csv(
        os.path.join(cpgf_dir, "gastos_cpgf.csv"),
        index=False,
        sep=';',
        encoding='utf-8-sig'
    )

def generate_camara_data():
    print("Gerando dados da Câmara dos Deputados (CEAP)...")
    camara_dir = os.path.join(DATA_DIR, "camara")
    os.makedirs(camara_dir, exist_ok=True)

    despesas = pd.DataFrame({
        'ano': ['2024', '2024', '2024'],
        'mes': ['01', '02', '03'],
        'cpf_deputado': ['11111111111', '22222222222', '11111111111'],
        'nome_deputado': ['DEP. JOAO SILVA', 'DEP. MARIA SOUZA', 'DEP. JOAO SILVA'],
        'partido': ['ABC', 'XYZ', 'ABC'],
        'uf': ['SP', 'RJ', 'SP'],
        'fornecedor': ['EMPRESA COMBUSTIVEL LTDA', 'HOTEL BRASILIA LTDA', 'GRAFICA CENTRAL'],
        'cnpj_fornecedor': ['12345678000100', '22345678000100', '32345678000100'],
        'tipo_despesa': ['COMBUSTIVEL', 'HOSPEDAGEM', 'DIVULGACAO'],
        'descricao': ['Abastecimento veículo', 'Hospedagem em viagem oficial', 'Material gráfico'],
        'valor_liquido': ['800.00', '1500.00', '600.00'],
        'data_emissao': ['2024-01-12', '2024-02-18', '2024-03-22'],

        # Metadados padrão
        'fonte_nome': ['Câmara dos Deputados'] * 3,
        'fonte_url': ['https://dadosabertos.camara.leg.br'] * 3,
        'fonte_descricao': ['Despesas CEAP - Cota Parlamentar'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    despesas.to_csv(
        os.path.join(camara_dir, "despesas_ceap.csv"),
        index=False,
        sep=';',
        encoding='utf-8-sig'
    )

def generate_bndes_data():
    print("Gerando dados do BNDES (operações de crédito)...")
    bndes_dir = os.path.join(DATA_DIR, "bndes")
    os.makedirs(bndes_dir, exist_ok=True)

    operacoes = pd.DataFrame({
        'cnpj_beneficiario': ['12345678000199', '98765432000188', '22334455000166'],
        'nome_beneficiario': ['EMPRESA XYZ LTDA', 'EMPRESA ABC SA', 'INDUSTRIA BRASIL LTDA'],
        'uf': ['SP', 'RJ', 'MG'],
        'municipio': ['SAO PAULO', 'RIO DE JANEIRO', 'BELO HORIZONTE'],
        'setor': ['INFRAESTRUTURA', 'INDUSTRIA', 'ENERGIA'],
        'programa': ['FINEM', 'BNDES AUTOMATICO', 'FINAME'],
        'data_contratacao': ['2021-04-10', '2022-07-15', '2023-01-20'],
        'valor_contratado': ['5000000.00', '2000000.00', '7500000.00'],
        'valor_desembolsado': ['3000000.00', '1500000.00', '5000000.00'],
        'situacao': ['ATIVA', 'ATIVA', 'ENCERRADA'],

        # Metadados padrão
        'fonte_nome': ['BNDES - Transparência'] * 3,
        'fonte_url': ['https://www.bndes.gov.br'] * 3,
        'fonte_descricao': ['Operações de crédito e financiamento'] * 3,
        'fonte_licenca': ['CC-BY 4.0'] * 3,
        'fonte_coletado_em': ['2024-01-01'] * 3
    })

    operacoes.to_csv(
        os.path.join(bndes_dir, "operacoes_credito.csv"),
        index=False,
        sep=';',
        encoding='utf-8-sig'
    )

def generate_senado_data():
    print("Gerando dados do Senado Federal (JSON - CEAP)...")
    senado_dir = os.path.join(DATA_DIR, "senado")
    os.makedirs(senado_dir, exist_ok=True)

    ano = 2024

    data = [
        {
            "ano": 2024,
            "mes": 1,
            "cpf_senador": "11111111111",
            "nome_senador": "SEN. JOAO SILVA",
            "partido": "ABC",
            "uf": "SP",
            "fornecedor": "AUTO POSTO BRASIL LTDA",
            "cnpj_fornecedor": "12345678000100",
            "tipo_despesa": "COMBUSTIVEL",
            "descricao": "Abastecimento de veículo oficial",
            "valor": 900.00,
            "data_emissao": "2024-01-15"
        },
        {
            "ano": 2024,
            "mes": 2,
            "cpf_senador": "22222222222",
            "nome_senador": "SEN. MARIA SOUZA",
            "partido": "XYZ",
            "uf": "RJ",
            "fornecedor": "HOTEL CENTRAL LTDA",
            "cnpj_fornecedor": "22345678000100",
            "tipo_despesa": "HOSPEDAGEM",
            "descricao": "Hospedagem em viagem oficial",
            "valor": 1800.00,
            "data_emissao": "2024-02-20"
        },
        {
            "ano": 2024,
            "mes": 3,
            "cpf_senador": "11111111111",
            "nome_senador": "SEN. JOAO SILVA",
            "partido": "ABC",
            "uf": "SP",
            "fornecedor": "AGENCIA PUBLICIDADE",
            "cnpj_fornecedor": "32345678000100",
            "tipo_despesa": "DIVULGACAO",
            "descricao": "Serviços de publicidade",
            "valor": 700.00,
            "data_emissao": "2024-03-25"
        }
    ]

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
    generate_ibge_data() #ibge
    generate_cnpj_data() #cnpj
    generate_tse_data() #tse
    generate_cgu_data() #emendas_cgu
    generate_tesouro_transparente_data() #tesouro_transparente
    generate_servidores_cgu_data() #servidores_cgu
    generate_sancoes_cgu_data() #sancoes_cgu
    generate_pncp_data() #pncp
    generate_pgfn_data() #pgfn
    generate_cpgf_data() #cpgf
    generate_camara_data() #camara
    generate_bndes_data() #bndes
    generate_senado_data() #senado
    print("\nDados sintéticos gerados com sucesso na pasta 'etl/data'!")
