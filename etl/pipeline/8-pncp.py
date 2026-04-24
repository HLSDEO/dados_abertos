"""
Pipeline 8 - PNCP (CSV) -> Neo4j

Baixa e processa CSVs do Portal Nacional de Contratações Públicas (PNCP)
e do ComprasNet Contratos, carregando no grafo Neo4j.

Arquivos processados:
  - PNCP_ITEM_RESULTADO (itens/resultados de contratação)
  - comprasnet-contratos-anual-contratos
  - comprasnet-contratos-anual-empenhos

Nos criados/atualizados:
  (:ItemResultado)        — id_contratacao_pncp + numero_item como chave
  (:Fornecedor)           — ni_fornecedor como chave
  (:ContratoComprasNet)   — id unico baseado em cnpj+ano+seq+numero
  (:Empenho)              — id unico baseado em cnpj+ano+numero_empenho
  (:Orgao)                — cnpj como chave

Relacionamentos:
  (:Fornecedor)-[:DISPUTA_ITEM]->(:ItemResultado)
  (:ItemResultado)-[:PERTENCE_A]->(:ContratoComprasNet)
  (:ContratoComprasNet)-[:PAGO_POR]->(:Empenho)
  (:ContratoComprasNet)-[:CELEBRADO_COM]->(:Fornecedor)
  (:Orgao)-[:REALIZA_ITEM|:CELEBRA_CONTRATO|:PAGA_EMpenho]->(...)
"""

import csv
import hashlib
import logging
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data"))
CSV_DIR    = DATA_DIR / "pncp_csv"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "20000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

# URLs dos CSVs (2026)
URLS = {
    "itens":       "https://repositorio.dados.gov.br/seges/comprasgov/anual/2026/comprasGOV-anual-VW_DM_PNCP_ITEM_RESULTADO-2026.csv",
    "contratos":   "https://repositorio.dados.gov.br/seges/comprasnet_contratos/anual/2026/comprasnet-contratos-anual-contratos-2026.csv",
    "empenhos":    "https://repositorio.dados.gov.br/seges/comprasnet_contratos/anual/2026/comprasnet-contratos-anual-empenhos-2026.csv",
}

FONTE_PNCP = {
    "fonte_nome": "PNCP — Portal Nacional de Contratações Publicas",
    "fonte_url":  "https://pncp.gov.br",
}

FONTE_COMPRASNET = {
    "fonte_nome": "ComprasNet Contratos",
    "fonte_url":  "https://comprasnet.gov.br",
}


# === Constraints e indices ===

Q_CONSTRAINTS = [
    # ItemResultado
    "CREATE CONSTRAINT item_resultado_id IF NOT EXISTS FOR (ir:ItemResultado) REQUIRE ir.item_id IS UNIQUE",
    "CREATE INDEX item_id_contratacao    IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.id_contratacao_pncp)",
    "CREATE INDEX item_numero            IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.numero_item)",
    # Fornecedor
    "CREATE CONSTRAINT fornecedor_ni     IF NOT EXISTS FOR (f:Fornecedor) REQUIRE f.ni_fornecedor IS UNIQUE",
    "CREATE INDEX fornecedor_doc         IF NOT EXISTS FOR (f:Fornecedor) ON (f.doc_fornecedor)",
    "CREATE INDEX fornecedor_tipo        IF NOT EXISTS FOR (f:Fornecedor) ON (f.tipo_pessoa)",
    # ContratoComprasNet
    "CREATE CONSTRAINT contrato_id       IF NOT EXISTS FOR (c:ContratoComprasNet) REQUIRE c.contrato_id IS UNIQUE",
    "CREATE INDEX contrato_ano           IF NOT EXISTS FOR (c:ContratoComprasNet) ON (c.ano_contrato)",
    "CREATE INDEX contrato_numero        IF NOT EXISTS FOR (c:ContratoComprasNet) ON (c.numero_contrato)",
    # Empenho
    "CREATE CONSTRAINT empenho_id        IF NOT EXISTS FOR (e:Empenho) REQUIRE e.empenho_id IS UNIQUE",
    "CREATE INDEX empenho_ano            IF NOT EXISTS FOR (e:Empenho) ON (e.ano_empenho)",
    # Orgao
    "CREATE CONSTRAINT orgao_cnpj        IF NOT EXISTS FOR (o:Orgao) REQUIRE o.cnpj IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX item_valor_unitario    IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.valor_unitario_homologado)",
    "CREATE INDEX item_quantidade        IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.quantidade_homologada)",
    "CREATE INDEX item_data_inclusao     IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.data_inclusao_pncp)",
    "CREATE INDEX item_nome_fornec       IF NOT EXISTS FOR (ir:ItemResultado) ON (ir.nome_razao_social_fornecedor)",
    "CREATE INDEX contrato_valor         IF NOT EXISTS FOR (c:ContratoComprasNet) ON (c.valor_global)",
    "CREATE INDEX empenho_valor          IF NOT EXISTS FOR (e:Empenho) ON (e.valor_empenho)",
]


# === Download ===

def _download_csv(url: str, dest_path: Path, max_retries: int = 3) -> bool:
    """Baixa CSV com retry e validacao basica."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"  [{attempt}/{max_retries}] Baixando {dest_path.name}...")
            req = urllib.request.Request(url, headers={"User-Agent": "PNCP-ETL/1.0"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                content = resp.read()
                dest_path.write_bytes(content)
            if len(content) < 100:
                log.warning(f"  Arquivo muito pequeno ({len(content)} bytes), ignorando")
                return False
            log.info(f"  OK: {len(content):,} bytes -> {dest_path.name}")
            return True
        except urllib.error.HTTPError as e:
            log.warning(f"  Erro HTTP {e.code} em {dest_path.name}: {e.reason}")
            if e.code == 404:
                return False
        except Exception as e:
            log.warning(f"  Erro ao baixar {dest_path.name}: {e}")
        if attempt < max_retries:
            time.sleep(2 ** attempt)
    return False


def baixar_csvs() -> dict[str, Path]:
    """Baixa todos os CSVs necessarios. Retorna mapeamento nome -> path."""
    baixados = {}
    for nome, url in URLS.items():
        ext = Path(url).suffix
        dest = CSV_DIR / f"{nome}{ext}"
        ok = _download_csv(url, dest)
        if ok:
            baixados[nome] = dest
    return baixados


# === Parsers / Transforms ===

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


def _parse_itens(chunk: list[dict]) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict]]:
    """
    Parse do CSV PNCP_ITEM_RESULTADO.

    Campos mapeados (dos solicitados):
      - ni_fornecedor, tipo_pessoa, data_inclusao_pncp, numero_item_pncp,
      - nome_razao_social_fornecedor, codigo_pais, quantidade_homologada,
      - valor_unitario_homologado, orgao_entidade_cnpj,
      - unidade_orgao_codigo_unidade, unidade_orgao_uf_sigla

    Retorna 5 listas de dicts:
      (itens, fornecedores, orgaos, municipios, ids_contratacao_para_link)
    """
    itens, fornecedores, orgaos, municipios, ids_link = [], [], [], [], []
    seen_forn = set()
    seen_org = set()

    for r in chunk:
        id_contrat = _safe_str(r.get("id_contratacao_pncp") or r.get("numero_contratacao") or r.get("numero_compra") or "")
        if not id_contrat:
            continue

        ni_forn = _safe_str(r.get("ni_fornecedor") or r.get("documento_fornecedor") or "")
        tipo_pessoa = _safe_str(r.get("tipo_pessoa") or "")
        if tipo_pessoa.upper() in ("", "N/A"):
            tipo_pessoa = "PJ" if len(ni_forn) >= 14 else "PF" if len(ni_forn) >= 11 else "PE"

        data_inc = _safe_str(r.get("data_inclusao_pncp") or r.get("data_inclusao") or "")
        numero_item = _safe_str(r.get("numero_item_pncp") or r.get("numero_item") or "")
        nome_forn = _safe_str(r.get("nome_razao_social_fornecedor") or r.get("razao_social") or "")
        cod_pais = _safe_str(r.get("codigo_pais") or "")
        qtd = _safe_float(r.get("quantidade_homologada") or r.get("quantidade") or 0)
        val_unit = _safe_float(r.get("valor_unitario_homologado") or r.get("valor_unitario") or 0)

        orgao_cnpj = _safe_str(r.get("orgao_entidade_cnpj") or r.get("cnpj_orgao") or "")
        cod_uni = _safe_str(r.get("unidade_orgao_codigo_unidade") or r.get("codigo_unidade") or "")
        uf_sigla = _safe_str(r.get("unidade_orgao_uf_sigla") or r.get("uf_sigla") or r.get("uf") or "")
        nome_uni = _safe_str(r.get("unidade_orgao_nome") or r.get("nome_unidade") or "")
        nome_mun = _safe_str(r.get("municipio_nome") or "")

        item_id = f"{id_contrat}_{numero_item}" if numero_item else id_contrat

        itens.append({
            "item_id":              item_id,
            "id_contratacao_pncp":  id_contrat,
            "numero_item":          numero_item,
            "ni_fornecedor":        ni_forn,
            "tipo_pessoa":          tipo_pessoa.upper() if tipo_pessoa else "PJ",
            "data_inclusao_pncp":   data_inc[:19] if data_inc else "",
            "nome_razao_social_fornecedor": nome_forn[:500] if nome_forn else "",
            "codigo_pais":          cod_pais,
            "quantidade_homologada":qtd,
            "valor_unitario_homologado": val_unit,
            "orgao_entidade_cnpj":  orgao_cnpj,
            "unidade_orgao_codigo_unidade": cod_uni,
            "unidade_orgao_uf_sigla": uf_sigla,
            "nome_unidade":         nome_uni[:300],
            "nome_municipio":       nome_mun[:300],
            **FONTE_PNCP,
        })

        if ni_forn and ni_forn not in seen_forn:
            seen_forn.add(ni_forn)
            fornecedores.append({
                "ni_fornecedor":     ni_forn,
                "doc_fornecedor":    ni_forn,
                "tipo_pessoa":       tipo_pessoa.upper() if tipo_pessoa else "PJ",
                "nome":              nome_forn[:500] if nome_forn else "",
                **FONTE_PNCP,
            })

        if orgao_cnpj and orgao_cnpj not in seen_org:
            seen_org.add(orgao_cnpj)
            orgaos.append({
                "cnpj":              orgao_cnpj,
                "nome":              nome_uni or nome_forn or "Orgao",
                "uf":                uf_sigla,
                **FONTE_PNCP,
            })

        if nome_mun or uf_sigla:
            municipios.append({
                "item_id":           item_id,
                "nome_municipio":    nome_mun or "Nao especificado",
                "uf_sigla":          uf_sigla,
            })

        ids_link.append({"item_id": item_id, "id_contratacao_pncp": id_contrat})

    return itens, fornecedores, orgaos, municipios, ids_link


def _parse_contratos(chunk: list[dict]) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict]]:
    """
    Parse do CSV comprasnet-contratos-anual-contratos.
    Cria nos ContratoComprasNet e relacoes com Fornecedores.
    """
    contratos, fornecedores, orgaos, vinculos, municipios = [], [], [], [], []
    seen_forn = set()
    seen_org = set()

    for r in chunk:
        cnpj_org = _safe_str(r.get("cnpj_ente") or r.get("cnpj_orgao") or r.get("cnpj") or "")
        if not cnpj_org:
            continue

        ano = _safe_int(r.get("ano_contrato") or r.get("ano") or "")
        num_contrato = _safe_str(r.get("numero_contrato") or "")
        seq = _safe_str(r.get("sequencial") or "1")
        valor = _safe_float(r.get("valor_global") or r.get("valor") or 0)
        objeto = _safe_str(r.get("objeto") or "")
        data_ass = _safe_str(r.get("data_assinatura") or "")
        data_pub = _safe_str(r.get("data_publicacao") or "")

        cnpj_forn = _safe_str(r.get("cnpj_fornecedor") or r.get("cnpj_contratado") or "")
        nome_forn = _safe_str(r.get("nome_fornecedor") or r.get("razao_social_fornecedor") or r.get("nome_razao_social") or "")

        contrato_id = f"{cnpj_org}_{ano}_{seq}_{num_contrato}"

        contratos.append({
            "contrato_id":         contrato_id,
            "cnpj_orgao":          cnpj_org,
            "ano_contrato":        ano,
            "numero_contrato":     num_contrato,
            "sequencial":          seq,
            "valor_global":        _safe_float(valor),
            "objeto":              objeto[:2000],
            "data_assinatura":     data_ass[:19] if data_ass else "",
            "data_publicacao":     data_pub[:19] if data_pub else "",
            **FONTE_COMPRASNET,
        })

        nome_org = _safe_str(r.get("nome_ente") or r.get("nome_orgao") or cnpj_org)
        uf_org = _safe_str(r.get("uf_ente") or r.get("uf_orgao") or "")
        if cnpj_org not in seen_org:
            seen_org.add(cnpj_org)
            orgaos.append({
                "cnpj":              cnpj_org,
                "nome":              nome_org[:300],
                "uf":                uf_org,
                **FONTE_COMPRASNET,
            })

        if cnpj_forn:
            if cnpj_forn not in seen_forn:
                seen_forn.add(cnpj_forn)
                fornecedores.append({
                    "ni_fornecedor":     cnpj_forn,
                    "doc_fornecedor":    cnpj_forn,
                    "tipo_pessoa":       "PJ",
                    "nome":              nome_forn[:500] if nome_forn else "",
                    **FONTE_COMPRASNET,
                })
            vinculos.append({
                "contrato_id":       contrato_id,
                "ni_fornecedor":     cnpj_forn,
            })

    return contratos, fornecedores, orgaos, vinculos, municipios


def _parse_empenhos(chunk: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Parse do CSV comprasnet-contratos-anual-empenhos.
    Cria nos Empenho e relacionamentos com Contratos e Orgaos.
    """
    empenhos, orgaos, vinculos = [], [], []
    seen_org = set()

    for r in chunk:
        cnpj = _safe_str(r.get("cnpj_ente") or r.get("cnpj_orgao") or r.get("cnpj") or "")
        if not cnpj:
            continue

        ano = _safe_int(r.get("ano_empenho") or r.get("ano") or "")
        num_emp = _safe_str(r.get("numero_empenho") or "")
        valor = _safe_float(r.get("valor_empenho") or r.get("valor") or 0)
        num_contrato = _safe_str(r.get("numero_contrato") or "")

        if not num_emp:
            continue

        empenho_id = f"{cnpj}_{ano}_{num_emp}"

        empenhos.append({
            "empenho_id":          empenho_id,
            "cnpj_orgao":          cnpj,
            "ano_empenho":         ano,
            "numero_empenho":      num_emp,
            "valor_empenho":       valor,
            "numero_contrato":     num_contrato,
            **FONTE_COMPRASNET,
        })

        if num_contrato and cnpj and ano:
            contrato_id = f"{cnpj}_{ano}_1_{num_contrato}"
            vinculos.append({
                "empenho_id":       empenho_id,
                "contrato_id":      contrato_id,
            })

        if cnpj not in seen_org:
            seen_org.add(cnpj)
            nome_org = _safe_str(r.get("nome_ente") or r.get("nome_orgao") or cnpj)
            uf_org = _safe_str(r.get("uf_ente") or r.get("uf_orgao") or "")
            orgaos.append({
                "cnpj":              cnpj,
                "nome":              nome_org[:300],
                "uf":                uf_org,
                **FONTE_COMPRASNET,
            })

    return empenhos, orgaos, vinculos


# === Loaders ===

def _load_itens(driver, csv_path: Path) -> None:
    """Carrega itens de resultado de contratação do PNCP."""
    log.info("  Lendo itens PNCP...")
    total_items = total_forn = total_org = total_mun = total_ids = 0

    with driver.session() as session:
        for chunk in iter_csv(csv_path, chunk_size=CHUNK_SIZE, delimiter=";"):
            its, forn, org, mun, ids = _parse_itens(chunk)
            del chunk

            if its:
                run_batches(session, Q_ITEM_UPSERT, its)
                total_items += len(its)
            if forn:
                run_batches(session, Q_FORNECEDOR_UPSERT, forn)
                total_forn += len(forn)
            if org:
                run_batches(session, Q_ORGAO_UPSERT, org)
                total_org += len(org)
            if mun:
                run_batches(session, Q_MUNICIPIO_UPSERT, mun)
                total_mun += len(mun)
            if ids:
                run_batches(session, Q_ITEM_VINCULO_CONTRATACAO, ids)
                total_ids += len(ids)

    log.info(f"    [OK] itens={total_items:,}  fornecedores={total_forn:,}  orgaos={total_org:,}  municipios={total_mun:,}  vinculos={total_ids:,}")


def _load_contratos(driver, csv_path: Path) -> None:
    """Carrega contratos do ComprasNet."""
    log.info("  Lendo contratos ComprasNet...")
    total_cont = total_forn = total_org = 0

    with driver.session() as session:
        for chunk in iter_csv(csv_path, chunk_size=CHUNK_SIZE, delimiter=";"):
            cont, forn, org, vincs, _ = _parse_contratos(chunk)
            del chunk

            if cont:
                run_batches(session, Q_CONTRATO_UPSERT, cont)
                total_cont += len(cont)
            if forn:
                run_batches(session, Q_FORNECEDOR_UPSERT, forn)
                total_forn += len(forn)
            if org:
                run_batches(session, Q_ORGAO_UPSERT, org)
                total_org += len(org)
            if vincs:
                run_batches(session, Q_CONTRATO_VINCULO_FORN, vincs)

    log.info(f"    [OK] contratos={total_cont:,}  fornecedores={total_forn:,}  orgaos={total_org:,}")


def _load_empenhos(driver, csv_path: Path) -> None:
    """Carrega empenhos do ComprasNet."""
    log.info("  Lendo empenhos ComprasNet...")
    total_emp = total_org = total_vinc = 0

    with driver.session() as session:
        for chunk in iter_csv(csv_path, chunk_size=CHUNK_SIZE, delimiter=";"):
            emp, org, vincs = _parse_empenhos(chunk)
            del chunk

            if emp:
                run_batches(session, Q_EMPENHO_UPSERT, emp)
                total_emp += len(emp)
            if org:
                run_batches(session, Q_ORGAO_UPSERT, org)
                total_org += len(org)
            if vincs:
                run_batches(session, Q_EMPENHO_VINCULO_CONTRATO, vincs)
                total_vinc += len(vincs)

    log.info(f"    [OK] empenhos={total_emp:,}  orgaos={total_org:,}  vinculos_contrato={total_vinc:,}")


# === Queries de upsert e relacionamento ===

Q_ITEM_UPSERT = """
UNWIND $rows AS r
MERGE (ir:ItemResultado {item_id: r.item_id})
  ON CREATE SET
    ir.id_contratacao_pncp   = r.id_contratacao_pncp,
    ir.numero_item          = r.numero_item,
    ir.criado_em            = datetime(),
    ir.fonte_nome           = r.fonte_nome,
    ir.fonte_url            = r.fonte_url
SET
    ir.ni_fornecedor        = r.ni_fornecedor,
    ir.tipo_pessoa          = r.tipo_pessoa,
    ir.data_inclusao_pncp   = r.data_inclusao_pncp,
    ir.nome_razao_social_fornecedor = r.nome_razao_social_fornecedor,
    ir.codigo_pais          = r.codigo_pais,
    ir.quantidade_homologada= r.quantidade_homologada,
    ir.valor_unitario_homologado = r.valor_unitario_homologado,
    ir.orgao_entidade_cnpj  = r.orgao_entidade_cnpj,
    ir.unidade_orgao_codigo_unidade = r.unidade_orgao_codigo_unidade,
    ir.unidade_orgao_uf_sigla = r.unidade_orgao_uf_sigla,
    ir.nome_unidade         = r.nome_unidade,
    ir.nome_municipio       = r.nome_municipio,
    ir.atualizado_em        = datetime()
"""

Q_FORNECEDOR_UPSERT = """
UNWIND $rows AS r
MERGE (f:Fornecedor {ni_fornecedor: r.ni_fornecedor})
  ON CREATE SET
    f.criado_em   = datetime(),
    f.fonte_nome  = r.fonte_nome
SET
  f.tipo_pessoa   = r.tipo_pessoa,
  f.nome          = coalesce(f.nome, r.nome),
  f.doc_fornecedor = r.doc_fornecedor,
  f.atualizado_em = datetime()
"""

Q_ORGAO_UPSERT = """
UNWIND $rows AS r
MERGE (o:Orgao {cnpj: r.cnpj})
  ON CREATE SET
    o.criado_em   = datetime(),
    o.fonte_nome  = r.fonte_nome
SET
  o.nome = coalesce(o.nome, r.nome),
  o.uf   = coalesce(o.uf, r.uf),
  o.atualizado_em = datetime()
"""

Q_MUNICIPIO_UPSERT = """
UNWIND $rows AS r
MERGE (m:Municipio {nome: toLower(trim(r.nome_municipio)), uf: r.uf_sigla})
  ON CREATE SET m.criado_em = datetime()
WITH r, m
MATCH (ir:ItemResultado {item_id: r.item_id})
MERGE (ir)-[:LOCALIZADO_EM]->(m)
"""

Q_ITEM_VINCULO_CONTRATACAO = """
UNWIND $rows AS r
MATCH (ir:ItemResultado {item_id: r.item_id})
MERGE (g:GrupoContratacao {id_contratacao: r.id_contratacao_pncp})
  ON CREATE SET g.criado_em = datetime()
MERGE (ir)-[:PERTENCE_A]->(g)
"""

Q_CONTRATO_UPSERT = """
UNWIND $rows AS r
MERGE (c:ContratoComprasNet {contrato_id: r.contrato_id})
  ON CREATE SET
    c.criado_em         = datetime(),
    c.fonte_nome        = r.fonte_nome,
    c.fonte_url         = r.fonte_url
SET
  c.numero_contrato     = r.numero_contrato,
  c.ano_contrato        = toInteger(r.ano_contrato),
  c.sequencial          = r.sequencial,
  c.valor_global        = toFloat(r.valor_global),
  c.objeto              = r.objeto,
  c.data_assinatura     = r.data_assinatura,
  c.data_publicacao     = r.data_publicacao,
  c.cnpj_orgao          = r.cnpj_orgao,
  c.atualizado_em       = datetime()
"""

Q_CONTRATO_VINCULO_FORN = """
UNWIND $rows AS r
MATCH (c:ContratoComprasNet {contrato_id: r.contrato_id})
MATCH (f:Fornecedor {ni_fornecedor: r.ni_fornecedor})
MERGE (c)-[:CELEBRADO_COM]->(f)
MERGE (f)-[:DISPUTOU]->(c)
"""

Q_EMPENHO_UPSERT = """
UNWIND $rows AS r
MERGE (e:Empenho {empenho_id: r.empenho_id})
  ON CREATE SET
    e.criado_em      = datetime(),
    e.fonte_nome     = r.fonte_nome,
    e.fonte_url      = r.fonte_url
SET
  e.numero_empenho    = r.numero_empenho,
  e.ano_empenho       = toInteger(r.ano_empenho),
  e.valor_empenho     = toFloat(r.valor_empenho),
  e.numero_contrato   = r.numero_contrato,
  e.cnpj_orgao        = r.cnpj_orgao,
  e.atualizado_em     = datetime()
"""

Q_EMPENHO_VINCULO_CONTRATO = """
UNWIND $rows AS r
MATCH (e:Empenho {empenho_id: r.empenho_id})
MATCH (c:ContratoComprasNet {contrato_id: r.contrato_id})
MERGE (c)-[:PAGO_POR]->(e)
MERGE (e)-[:REFERE_SE]->(c)
"""

Q_ORGAO_CONTRATO = """
UNWIND $rows AS r
MATCH (c:ContratoComprasNet {contrato_id: r.contrato_id})
MATCH (o:Orgao {cnpj: r.cnpj_orgao})
MERGE (o)-[:CELEBRA]->(c)
"""

Q_ORGAO_EMPENHO = """
UNWIND $rows AS r
MATCH (e:Empenho {empenho_id: r.empenho_id})
MATCH (o:Orgao {cnpj: r.cnpj_orgao})
MERGE (o)-[:REALIZA]->(e)
"""

Q_ORGAO_ITEM = """
UNWIND $rows AS r
MATCH (ir:ItemResultado {orgao_entidade_cnpj: r.orgao_entidade_cnpj})
WHERE r.orgao_entidade_cnpj <> ""
MATCH (o:Orgao {cnpj: r.orgao_entidade_cnpj})
MERGE (o)-[:REALIZA_ITEM]->(ir)
"""

Q_ITEM_CONTRATO_VINCULO = """
UNWIND $rows AS r
MATCH (ir:ItemResultado {id_contratacao_pncp: r.id_contratacao_pncp})
MATCH (c:ContratoComprasNet)
WHERE c.numero_contrato = r.numero_contrato_comprasnet
  AND toString(c.ano_contrato) = r.ano_compra
MERGE (ir)-[:PERTENCE_A]->(c)
MERGE (c)-[:CONTEM_ITEM]->(ir)
"""


# === Entry-point ===

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str,
        baixar: bool = True) -> None:
    """
    Executa pipeline completo.

    Args:
        neo4j_uri: URI do Neo4j (ex: bolt://localhost:7687)
        neo4j_user: usuario
        neo4j_password: senha
        baixar: se True, baixa CSVs antes de processar
    """
    log.info("[PNCP-CSV] Pipeline iniciado")

    if baixar:
        log.info("[1/4] Baixando CSVs...")
        baixados = baixar_csvs()
        if not baixados:
            log.error("Nenhum CSV baixado com sucesso!")
            return
        for nome, path in baixados.items():
            log.info(f"  {nome}: {path}")
    else:
        baixados = {k: CSV_DIR / f"{k}.csv" for k in URLS.keys()}
        for nome, path in list(baixados.items()):
            if not path.exists():
                log.warning(f"  {nome}: nao encontrado em {path}")
                del baixados[nome]

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    try:
        with driver.session() as session:
            log.info("[2/4] Aplicando schema (constraints + indices)...")
            for q in Q_CONSTRAINTS + Q_INDEXES:
                try:
                    session.run(q)
                except Exception as e:
                    log.debug(f"  Info: {e}")
            setup_schema(driver)

        with IngestionRun(driver, "pncp-csv") as runctx:
            log.info("[3/4] Carregando dados...")

            if itens_path := baixados.get("itens"):
                _load_itens(driver, itens_path)
                runctx.add(rows_in=1)

            if contratos_path := baixados.get("contratos"):
                _load_contratos(driver, contratos_path)
                runctx.add(rows_in=1)

            if empenhos_path := baixados.get("empenhos"):
                _load_empenhos(driver, empenhos_path)
                runctx.add(rows_in=1)

        log.info("[4/4] Pipeline concluido com sucesso")

    finally:
        driver.close()
        log.info("[PNCP-CSV] Conexao encerrada")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    run(
        neo4j_uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.environ.get("NEO4J_USER", "neo4j"),
        neo4j_password=os.environ.get("NEO4J_PASSWORD", "senha"),
        baixar=True,
    )
