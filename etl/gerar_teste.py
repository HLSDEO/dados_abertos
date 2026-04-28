import pandas as pd
import os

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

if __name__ == "__main__":
    generate_ibge_data()
    generate_cnpj_data()
    generate_tse_data()
    generate_cgu_data()
    generate_pncp_data()
    print("\nDados sintéticos gerados com sucesso na pasta 'etl/data'!")
