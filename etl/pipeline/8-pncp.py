"""
Pipeline 8 - PNCP (CSV) -> Neo4j

Carrega CSVs baixados do Portal Nacional de Contratações Públicas (PNCP)
e ComprasNet Contratos no grafo Neo4j.

Arquivos esperados em data/pncp_csv/:
  - itens.csv          (PNCP_ITEM_RESULTADO)
  - contratos.csv      (comprasnet-contratos-anual-contratos)
  - empenhos.csv       (comprasnet-contratos-anual-empenhos)

Nos criados/atualizados:
  (:ItemResultado)        — id_contratacao_pncp + numero_item como chave
  (:Fornecedor)           — ni_fornecedor como chave
  (:ContratoComprasNet)   — id unico baseado em cnpj+ano+seq+numero
  (:Empenho)              — id unico baseado em cnpj+ano+numero_empenho
  (:Orgao)                — cnpj como chave

Relacionamentos:
  (:Fornecedor)-[:DISPUTA_ITEM]->(:ItemResultado)
  (:ItemResultado)-[:PERTENCE_A]->(:GrupoContratacao)
  (:ContratoComprasNet)-[:PAGO_POR]->(:Empenho)
  (:ContratoComprasNet)-[:CELEBRADO_COM]->(:Fornecedor)
  (:Orgao)-[:REALIZA_ITEM]->(:ItemResultado)
  (:Orgao)-[:CELEBRA]->(:ContratoComprasNet)
  (:Orgao)-[:REALIZA]->(:Empenho)
"""

import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, IngestionRun, apply_schema, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
CSV_DIR    = DATA_DIR / "pncp_csv"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "20000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "Dados Abertos — PNCP / ComprasNet",
    "fonte_url":  "https://dados.gov.br",
}


# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT item_resultado_id IF NOT EXISTS FOR (ir:ItemResultado) REQUIRE ir.item_id IS UNIQUE",
    "CREATE CONSTRAINT fornecedor_ni     IF NOT EXISTS FOR (f:Fornecedor)  REQUIRE f.ni_fornecedor IS UNIQUE",
    "CREATE CONSTRAINT contrato_id       IF NOT EXISTS FOR (c:ContratoComprasNet) REQUIRE c.contrato_id IS UNIQUE",
    "CREATE CONSTRAINT empenho_id        IF NOT EXISTS FOR (e:Empenho)     REQUIRE e.empenho_id IS UNIQUE",
    "CREATE CONSTRAINT orgao_cnpj        IF NOT EXISTS FOR (o:Orgao)       REQUIRE o.cnpj IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX item_id_contratacao    IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.id_contratacao_pncp)",
    "CREATE INDEX item_numero            IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.numero_item)",
    "CREATE INDEX fornecedor_tipo        IF NOT EXISTS FOR (f:Fornecedor)    ON (f.tipo_pessoa)",
    "CREATE INDEX contrato_ano           IF NOT EXISTS FOR (c:ContratoComprasNet) ON (c.ano_contrato)",
    "CREATE INDEX empenho_ano            IF NOT EXISTS FOR (e:Empenho)       ON (e.ano_empenho)",
]


# ── helpers ────────────────────────────────────────────────────────────────────

def _safe_str(v) -> str:
    return str(v or "").strip()


def _safe_float(v) -> str:
    try:
        s = str(v or "").strip().replace(",", ".")
        return str(float(s)) if s else "0"
    except (ValueError, TypeError):
        return "0"


def _safe_int(v) -> str:
    try:
        s = str(v or "").strip()
        return str(int(float(s))) if s else "0"
    except (ValueError, TypeError):
        return "0"


# ── iter_csv ───────────────────────────────────────────────────────────────────

def iter_csv(path: Path, chunk_size: int = CHUNK_SIZE, encoding: str = "utf-8-sig", delimiter: str = "auto"):
    """
    Lê CSV em chunks sem carregar tudo em memória.
    Auto-detecta delimitador se delimiter='auto'.
    """
    if not path.exists():
        log.warning(f"  CSV ausente: {path.name} — pulando")
        return
    total = 0
    with open(path, encoding=encoding, newline="") as f:
        if delimiter == "auto":
            sample = f.read(4096)
            f.seek(0)
            comma_count = sample.count(",")
            semicolon_count = sample.count(";")
            delimiter = ";" if semicolon_count > comma_count else ","
            log.debug(f"  Delimitador detectado para {path.name}: '{delimiter}' (,={comma_count} ;={semicolon_count})")
        reader = csv.DictReader(f, delimiter=delimiter)
        chunk: list[dict] = []
        for row in reader:
            # Normaliza chaves: strip, lower, remove BOM se presente
            # Normaliza valores: strip
            row = {
                k.strip().lower().replace('\ufeff', ''): (v or "").strip()
                for k, v in row.items()
                if k is not None
            }
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                total += len(chunk)
                chunk = []
        if chunk:
            total += len(chunk)
            yield chunk
    log.info(f"    {path.name}: {total:,} linhas lidas")


# ── Transforms ────────────────────────────────────────────────────────────────

def _t_itens(chunk: list[dict]) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Parse do CSV PNCP_ITEM_RESULTADO.

    Campos mapeados (dos solicitados):
      - ni_fornecedor, tipo_pessoa, data_inclusao_pncp, numero_item_pncp,
      - nome_razao_social_fornecedor, codigo_pais, quantidade_homologada,
      - valor_unitario_homologado, orgao_entidade_cnpj,
      - unidade_orgao_codigo_unidade, unidade_orgao_uf_sigla

    Retorna: (itens, fornecedores, orgaos, municipios)
    """
    itens, fornecedores, orgaos, municipios = [], [], [], []
    seen_forn = set()
    seen_org = set()

    for r in chunk:
        id_contrat = _safe_str(
            r.get("id_contratacao_pncp") or
            r.get("numero_contratacao") or
            r.get("numero_compra") or
            ""
        )
        if not id_contrat:
            continue

        ni_forn = _safe_str(r.get("ni_fornecedor") or r.get("documento_fornecedor") or "")
        tipo_pessoa = _safe_str(r.get("tipo_pessoa") or "")
        if tipo_pessoa.upper() in ("", "N/A"):
            tipo_pessoa = "PJ" if len(ni_forn) >= 14 else "PF" if len(ni_forn) >= 11 else "PE"

        data_inc    = _safe_str(r.get("data_inclusao_pncp") or r.get("data_inclusao") or "")
        numero_item = _safe_str(r.get("numero_item_pncp") or r.get("numero_item") or "")
        nome_forn   = _safe_str(r.get("nome_razao_social_fornecedor") or r.get("razao_social") or "")
        cod_pais    = _safe_str(r.get("codigo_pais") or "")
        qtd         = _safe_float(r.get("quantidade_homologada") or r.get("quantidade") or 0)
        val_unit    = _safe_float(r.get("valor_unitario_homologado") or r.get("valor_unitario") or 0)

        orgao_cnpj  = _safe_str(r.get("orgao_entidade_cnpj") or r.get("cnpj_orgao") or "")
        cod_uni     = _safe_str(r.get("unidade_orgao_codigo_unidade") or r.get("codigo_unidade") or "")
        uf_sigla    = _safe_str(r.get("unidade_orgao_uf_sigla") or r.get("uf_sigla") or r.get("uf") or "")
        nome_uni    = _safe_str(r.get("unidade_orgao_nome") or r.get("nome_unidade") or "")
        nome_mun    = _safe_str(r.get("municipio_nome") or "")

        item_id = f"{id_contrat}_{numero_item}" if numero_item else id_contrat

        itens.append({
            "item_id":                          item_id,
            "id_contratacao_pncp":              id_contrat,
            "numero_item":                      numero_item,
            "ni_fornecedor":                    ni_forn,
            "tipo_pessoa":                      tipo_pessoa.upper() if tipo_pessoa else "PJ",
            "data_inclusao_pncp":               data_inc[:19] if data_inc else "",
            "nome_razao_social_fornecedor":     nome_forn[:500] if nome_forn else "",
            "codigo_pais":                      cod_pais,
            "quantidade_homologada":            qtd,
            "valor_unitario_homologado":        val_unit,
            "orgao_entidade_cnpj":              orgao_cnpj,
            "unidade_orgao_codigo_unidade":     cod_uni,
            "unidade_orgao_uf_sigla":           uf_sigla,
            "nome_unidade":                     nome_uni[:300],
            "nome_municipio":                   nome_mun[:300],
            **FONTE,
        })

        if ni_forn and ni_forn not in seen_forn:
            seen_forn.add(ni_forn)
            fornecedores.append({
                "ni_fornecedor": ni_forn,
                "doc_fornecedor": ni_forn,
                "tipo_pessoa": tipo_pessoa.upper() if tipo_pessoa else "PJ",
                "nome": nome_forn[:500] if nome_forn else "",
                **FONTE,
            })

        if orgao_cnpj and orgao_cnpj not in seen_org:
            seen_org.add(orgao_cnpj)
            orgaos.append({
                "cnpj": orgao_cnpj,
                "nome": nome_uni or nome_forn or "Órgão",
                "uf": uf_sigla,
                **FONTE,
            })

        if nome_mun or uf_sigla:
            municipios.append({
                "item_id": item_id,
                "nome_municipio": nome_mun or "Não especificado",
                "uf_sigla": uf_sigla,
            })

    return itens, fornecedores, orgaos, municipios


def _t_contratos(chunk: list[dict]) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Parse do CSV comprasnet-contratos-anual-contratos.
    Colunas esperadas (exemplo):
      id, numero, orgao_codigo, orgao_nome, fornecedor_tipo,
      fonecedor_cnpj_cpf_idgener, fornecedor_nome, objeto,
      data_assinatura, data_publicacao, valor_global, ...
    """
    contratos, fornecedores, orgaos, vinculos = [], [], [], []
    seen_forn = set()
    # Orgaos não são criados a partir de contratos pois não há CNPJdo órgão neste CSV

    for r in chunk:
        # ID único do contrato (campo 'id')
        contrato_id = _safe_str(r.get("id") or r.get("contrato_id") or "")
        if not contrato_id:
            continue

        # Número do contrato (campo 'numero')
        num_contrato = _safe_str(r.get("numero") or r.get("numero_contrato") or "")
        # Data de assinatura (para ano)
        data_ass    = _safe_str(r.get("data_assinatura") or "")
        # Extrai ano da data ou do número (ex: 00065/2025 -> 2025)
        ano = ""
        if data_ass and len(data_ass) >= 4:
            ano = data_ass[:4]
        else:
            partes = num_contrato.split("/")
            if len(partes) > 1 and partes[1].isdigit():
                ano = partes[1]
            else:
                ano = "0"
        seq         = "1"  # sequencial não presente, usa padrão
        valor       = _safe_float(r.get("valor_global") or r.get("valor_inicial") or 0)
        objeto      = _safe_str(r.get("objeto") or "")
        data_pub    = _safe_str(r.get("data_publicacao") or "")

        # Fornecedor
        cnpj_forn_raw = _safe_str(
            r.get("fonecedor_cnpj_cpf_idgener") or
            r.get("cnpj_fornecedor") or
            r.get("cnpj_contratado") or
            ""
        )
        # Limpa pontuação, deixa apenas dígitos
        cnpj_forn = ''.join(filter(str.isdigit, cnpj_forn_raw))
        nome_forn = _safe_str(
            r.get("fornecedor_nome") or
            r.get("nome_fornecedor") or
            r.get("razao_social_fornecedor") or
            ""
        )
        # Tipo pessoa: a partir de 'fornecedor_tipo' (JURIDICA/FISICA) ou por tamanho do doc
        tipo_pessoa = "PJ" if len(cnpj_forn) >= 14 else "PF" if len(cnpj_forn) >= 11 else "PE"
        forn_tipo_raw = _safe_str(r.get("fornecedor_tipo") or "")
        if forn_tipo_raw.upper().startswith("JUR"):
            tipo_pessoa = "PJ"
        elif forn_tipo_raw.upper().startswith("FIS"):
            tipo_pessoa = "PF"

        contratos.append({
            "contrato_id":      contrato_id,
            "cnpj_orgao":       "",   # não disponível no CSV
            "ano_contrato":     ano,
            "numero_contrato":  num_contrato,
            "sequencial":       seq,
            "valor_global":     valor,
            "objeto":           objeto[:2000],
            "data_assinatura":  data_ass[:19] if data_ass else "",
            "data_publicacao":  data_pub[:19] if data_pub else "",
            **FONTE,
        })

        if cnpj_forn:
            if cnpj_forn not in seen_forn:
                seen_forn.add(cnpj_forn)
                fornecedores.append({
                    "ni_fornecedor": cnpj_forn,
                    "doc_fornecedor": cnpj_forn,
                    "tipo_pessoa": tipo_pessoa,
                    "nome": nome_forn[:500],
                    **FONTE,
                })
            vinculos.append({
                "contrato_id": contrato_id,
                "ni_fornecedor": cnpj_forn,
            })

    return contratos, fornecedores, orgaos, vinculos


def _t_empenhos(chunk: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Parse do CSV comprasnet-contratos-anual-empenhos.
    Colunas esperadas (exemplo):
      id, numero_empenho, data_emissao, cpf_cnpj_credor, credor,
      valor_empenhado, contrato_id, ...
    """
    empenhos, orgaos, vinculos = [], [], []
    # Orgaos não são criados a partir de empenhos pois lack CNPJ

    for r in chunk:
        # ID único do empenho: campo 'id'
        empenho_id = _safe_str(r.get("id") or r.get("empenho_id") or "")
        if not empenho_id:
            continue

        # Ano: extrair de data_emissao (YYYY) ou campo 'ano' se existir
        data_emissao = _safe_str(r.get("data_emissao") or "")
        ano = data_emissao[:4] if len(data_emissao) >= 4 else _safe_int(r.get("ano") or "")
        numero_empenho = _safe_str(r.get("numero_empenho") or "")
        valor = _safe_float(r.get("valor_empenhado") or r.get("valor_empenho") or 0)
        # Referência ao contrato (pode ser o ID do contrato)
        num_contrato = _safe_str(r.get("contrato_id") or r.get("numero_contrato") or "")

        empenhos.append({
            "empenho_id":      empenho_id,
            "cnpj_orgao":      "",   # não disponível
            "ano_empenho":     ano,
            "numero_empenho":  numero_empenho,
            "valor_empenho":   valor,
            "numero_contrato": num_contrato,
            **FONTE,
        })

        if num_contrato:
            vinculos.append({
                "empenho_id": empenho_id,
                "contrato_id": num_contrato,
            })

    return empenhos, orgaos, vinculos


# ── Queries Cypher ─────────────────────────────────────────────────────────────

Q_ITEM_UPSERT = """
UNWIND $rows AS r
MERGE (ir:ItemResultado {item_id: r.item_id})
  ON CREATE SET ir.criado_em = datetime(), ir.fonte_nome = r.fonte_nome, ir.fonte_url = r.fonte_url
SET
  ir.id_contratacao_pncp    = r.id_contratacao_pncp,
  ir.numero_item           = r.numero_item,
  ir.ni_fornecedor         = r.ni_fornecedor,
  ir.tipo_pessoa           = r.tipo_pessoa,
  ir.data_inclusao_pncp    = r.data_inclusao_pncp,
  ir.nome_razao_social_fornecedor = r.nome_razao_social_fornecedor,
  ir.codigo_pais           = r.codigo_pais,
  ir.quantidade_homologada= r.quantidade_homologada,
  ir.valor_unitario_homologado = r.valor_unitario_homologado,
  ir.orgao_entidade_cnpj   = r.orgao_entidade_cnpj,
  ir.unidade_orgao_codigo_unidade = r.unidade_orgao_codigo_unidade,
  ir.unidade_orgao_uf_sigla = r.unidade_orgao_uf_sigla,
  ir.nome_unidade          = r.nome_unidade,
  ir.nome_municipio        = r.nome_municipio,
  ir.atualizado_em         = datetime()
"""

Q_FORNECEDOR_UPSERT = """
UNWIND $rows AS r
MERGE (f:Fornecedor {ni_fornecedor: r.ni_fornecedor})
  ON CREATE SET f.criado_em = datetime(), f.fonte_nome = r.fonte_nome
SET
  f.tipo_pessoa  = r.tipo_pessoa,
  f.nome         = coalesce(f.nome, r.nome),
  f.doc_fornecedor = r.doc_fornecedor,
  f.atualizado_em = datetime()
"""

Q_ORGAO_UPSERT = """
UNWIND $rows AS r
MERGE (o:Orgao {cnpj: r.cnpj})
  ON CREATE SET o.criado_em = datetime(), o.fonte_nome = r.fonte_nome
SET
  o.nome = coalesce(o.nome, r.nome),
  o.uf   = coalesce(o.uf, r.uf),
  o.atualizado_em = datetime()
"""

Q_MUNICIPIO_LINK = """
UNWIND $rows AS r
MATCH (ir:ItemResultado {item_id: r.item_id})
MERGE (m:Municipio {nome: toLower(trim(r.nome_municipio)), uf: r.uf_sigla})
  ON CREATE SET m.criado_em = datetime()
MERGE (ir)-[:LOCALIZADO_EM]->(m)
"""

Q_GRUPO_CONTRATACAO = """
UNWIND $rows AS r
MATCH (ir:ItemResultado {item_id: r.item_id})
MERGE (g:GrupoContratacao {id_contratacao: r.id_contratacao_pncp})
  ON CREATE SET g.criado_em = datetime()
MERGE (ir)-[:PERTENCE_A]->(g)
"""

Q_CONTRATO_UPSERT = """
UNWIND $rows AS r
MERGE (c:ContratoComprasNet {contrato_id: r.contrato_id})
  ON CREATE SET c.criado_em = datetime(), c.fonte_nome = r.fonte_nome, c.fonte_url = r.fonte_url
SET
  c.numero_contrato = r.numero_contrato,
  c.ano_contrato    = toInteger(r.ano_contrato),
  c.sequicial       = r.sequencial,
  c.valor_global     = toFloat(r.valor_global),
  c.objeto           = r.objeto,
  c.data_assinatura  = r.data_assinatura,
  c.data_publicacao  = r.data_publicacao,
  c.cnpj_orgao       = r.cnpj_orgao,
  c.atualizado_em    = datetime()
"""

Q_CONTRATO_FORN = """
UNWIND $rows AS r
MATCH (c:ContratoComprasNet {contrato_id: r.contrato_id})
MATCH (f:Fornecedor {ni_fornecedor: r.ni_fornecedor})
MERGE (c)-[:CELEBRADO_COM]->(f)
MERGE (f)-[:DISPUTOU]->(c)
"""

Q_CONTRATO_ORGAO = """
UNWIND $rows AS r
MATCH (c:ContratoComprasNet {contrato_id: r.contrato_id})
MATCH (o:Orgao {cnpj: r.cnpj_orgao})
MERGE (o)-[:CELEBRA]->(c)
"""

Q_EMPENHO_UPSERT = """
UNWIND $rows AS r
MERGE (e:Empenho {empenho_id: r.empenho_id})
  ON CREATE SET e.criado_em = datetime(), e.fonte_nome = r.fonte_nome, e.fonte_url = r.fonte_url
SET
  e.numero_empenho  = r.numero_empenho,
  e.ano_empenho     = toInteger(r.ano_empenho),
  e.valor_empenho   = toFloat(r.valor_empenho),
  e.numero_contrato = r.numero_contrato,
  e.cnpj_orgao      = r.cnpj_orgao,
  e.atualizado_em   = datetime()
"""

Q_EMPENHO_CONTRATO = """
UNWIND $rows AS r
MATCH (e:Empenho {empenho_id: r.empenho_id})
MATCH (c:ContratoComprasNet {contrato_id: r.contrato_id})
MERGE (c)-[:PAGO_POR]->(e)
MERGE (e)-[:REFERE_SE]->(c)
"""

Q_ORGAO_ITEM = """
UNWIND $rows AS r
MATCH (ir:ItemResultado {orgao_entidade_cnpj: r.orgao_entidade_cnpj})
WHERE r.orgao_entidade_cnpj <>
MATCH (o:Orgao {cnpj: r.orgao_entidade_cnpj})
MERGE (o)-[:REALIZA_ITEM]->(ir)
"""


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_itens(driver, csv_path: Path, limite: int | None = None, stats: dict | None = None) -> None:
    log.info("  Lendo itens PNCP...")
    total_items = total_forn = total_org = total_mun = 0
    if stats is None:
        stats = {'total': 0}

    with driver.session() as session:
        for chunk in iter_csv(csv_path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [itens] Limite de {limite:,} atingido. Parando.")
                break
            itens, forn, org, mun = _t_itens(chunk)
            if limite is not None and itens:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if restante < len(itens):
                    itens = itens[:restante]
            if itens:
                run_batches(session, Q_ITEM_UPSERT, itens)
                total_items += len(itens)
                stats['total'] += len(itens)
            if forn:
                run_batches(session, Q_FORNECEDOR_UPSERT, forn)
                total_forn += len(forn)
            if org:
                run_batches(session, Q_ORGAO_UPSERT, org)
                total_org += len(org)
            if mun:
                run_batches(session, Q_MUNICIPIO_LINK, mun)
                total_mun += len(mun)

    log.info(f"    [OK] itens={total_items:,}  fornecedores={total_forn:,}  orgaos={total_org:,}  municipios={total_mun:,}")


def _load_contratos(driver, csv_path: Path, limite: int | None = None, stats: dict | None = None) -> None:
    log.info("  Lendo contratos ComprasNet...")
    total_cont = total_forn = total_org = 0
    if stats is None:
        stats = {'total': 0}

    with driver.session() as session:
        for chunk in iter_csv(csv_path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [contratos] Limite de {limite:,} atingido. Parando.")
                break
            cont, forn, org, vincs = _t_contratos(chunk)
            if limite is not None and cont:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if restante < len(cont):
                    cont = cont[:restante]
            if cont:
                run_batches(session, Q_CONTRATO_UPSERT, cont)
                total_cont += len(cont)
                stats['total'] += len(cont)
            if forn:
                run_batches(session, Q_FORNECEDOR_UPSERT, forn)
                total_forn += len(forn)
            if org:
                run_batches(session, Q_ORGAO_UPSERT, org)
                total_org += len(org)
            if vincs:
                run_batches(session, Q_CONTRATO_FORN, vincs)

    log.info(f"    [OK] contratos={total_cont:,}  fornecedores={total_forn:,}  orgaos={total_org:,}")


def _load_empenhos(driver, csv_path: Path, limite: int | None = None, stats: dict | None = None) -> None:
    log.info("  Lendo empenhos ComprasNet...")
    total_emp = total_org = 0
    if stats is None:
        stats = {'total': 0}

    with driver.session() as session:
        for chunk in iter_csv(csv_path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [empenhos] Limite de {limite:,} atingido. Parando.")
                break
            emp, org, vincs = _t_empenhos(chunk)
            if limite is not None and emp:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if restante < len(emp):
                    emp = emp[:restante]
            if emp:
                run_batches(session, Q_EMPENHO_UPSERT, emp)
                total_emp += len(emp)
                stats['total'] += len(emp)
            if org:
                run_batches(session, Q_ORGAO_UPSERT, org)
                total_org += len(org)
            if vincs:
                run_batches(session, Q_EMPENHO_CONTRATO, vincs)

    log.info(f"    [OK] empenhos={total_emp:,}  orgaos={total_org:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    csv_dir: Path | None = None,
    limite: int | None = None,
) -> None:
    """
    Executa pipeline de carga no Neo4j.

    Args:
        neo4j_uri: URI do Neo4j
        neo4j_user: usuário
        neo4j_password: senha
        csv_dir: diretório com CSVs (padrão: DATA_DIR/pncp_csv)
        limite: número máximo de linhas a inserir no total (carga parcial)
    """
    log.info("[PNCP-CSV] Pipeline iniciado")

    if csv_dir is None:
        csv_dir = CSV_DIR

    files = {
        "itens":     csv_dir / "itens.csv",
        "contratos": csv_dir / "contratos.csv",
        "empenhos":  csv_dir / "empenhos.csv",
    }

    for k, p in files.items():
        if not p.exists():
            raise FileNotFoundError(f"CSV não encontrado: {p}\nExecute primeiro: python etl/download/8-pncp.py")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    try:
        with IngestionRun(driver, "pncp-csv") as run_ctx:
            with driver.session() as session:
                log.info("  Aplicando schema (constraints + indices)...")
                try:
                    apply_schema(session, Q_CONSTRAINTS, Q_INDEXES)
                except Exception:
                    pass  # constraint ja existe

            log.info("  Carregando dados...")
            stats = {'total': 0}
            for etapa, path in files.items():
                if limite is not None and stats['total'] >= limite:
                    log.info(f"  Limite de {limite:,} linhas atingido antes de {etapa}. Parando.")
                    break
                if etapa == "itens":
                    _load_itens(driver, path, limite=limite, stats=stats)
                elif etapa == "contratos":
                    _load_contratos(driver, path, limite=limite, stats=stats)
                elif etapa == "empenhos":
                    _load_empenhos(driver, path, limite=limite, stats=stats)
                if limite is not None and stats['total'] >= limite:
                    log.info(f"  Limite de {limite:,} linhas atingido após {etapa}. Parando.")
                    break
            run_ctx.add(rows_in=stats['total'], rows_out=stats['total'])

        log.info("[PNCP-CSV] Pipeline concluído com sucesso")

    finally:
        driver.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline PNCP — carrega CSVs no Neo4j")
    parser.add_argument("--csv-dir", type=Path, default=None, help="Diretório com CSVs (padrão: data/pncp_csv)")
    parser.add_argument("--limite", type=int, default=None, help="Número máximo de linhas a inserir (carga parcial)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    run(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "senha"),
        csv_dir=args.csv_dir,
        limite=args.limite,
    )


