"""
Pipeline 8 - PNCP → Neo4j

Lê os JSONs de data/pncp/ e carrega no grafo.

  editais/pncp_editais_{YYYYMM}.json   → :Licitacao
  contratos/pncp_contratos_{YYYYMM}.json → :Contrato

Nós criados/atualizados:
  (:Licitacao)  — numeroControlePNCP como chave
  (:Contrato)   — chave composta: cnpj + ano + seq + numeroContrato
  (:Empresa)    — merge pelo cnpj (orgaoEntidade.cnpj, cnpj 8 dígitos)
  (:Municipio)  — merge pelo nome/UF da unidadeOrgao

Relacionamentos:
  (:Empresa)-[:PUBLICOU_LICITACAO]->(:Licitacao)
  (:Empresa)-[:FIRMOU_CONTRATO]->(:Contrato)
  (:Contrato)-[:VINCULADO_A]->(:Licitacao)       ← pelo numeroControlePNCP do edital
  (:Licitacao)-[:LOCALIZADA_EM]->(:Municipio)    ← pela UF/nome da unidadeOrgao
  (:Contrato)-[:LOCALIZADA_EM]->(:Municipio)
"""

import json
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "pncp"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "5000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "PNCP — Portal Nacional de Contratações Públicas",
    "fonte_url":  "https://pncp.gov.br",
}


# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Licitacao) REQUIRE n.numero_controle IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Contrato)  REQUIRE n.contrato_id IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX licitacao_ano      IF NOT EXISTS FOR (l:Licitacao) ON (l.ano_compra)",
    "CREATE INDEX licitacao_modalide IF NOT EXISTS FOR (l:Licitacao) ON (l.modalidade_id)",
    "CREATE INDEX licitacao_situacao IF NOT EXISTS FOR (l:Licitacao) ON (l.situacao_id)",
    "CREATE INDEX licitacao_valor    IF NOT EXISTS FOR (l:Licitacao) ON (l.valor_total_estimado)",
    "CREATE INDEX contrato_ano       IF NOT EXISTS FOR (c:Contrato)  ON (c.ano)",
    "CREATE INDEX contrato_valor     IF NOT EXISTS FOR (c:Contrato)  ON (c.valor_global)",
]


# ── Queries ───────────────────────────────────────────────────────────────────

Q_LICITACAO = """
UNWIND $rows AS r
MERGE (l:Licitacao {numero_controle: r.numero_controle})
SET l.ano_compra            = toInteger(r.ano_compra),
    l.sequencial_compra     = r.sequencial_compra,
    l.numero_compra         = r.numero_compra,
    l.processo              = r.processo,
    l.objeto                = r.objeto,
    l.modalidade_id         = toInteger(r.modalidade_id),
    l.modalidade_nome       = r.modalidade_nome,
    l.situacao_id           = toInteger(r.situacao_id),
    l.situacao_nome         = r.situacao_nome,
    l.modo_disputa_id       = toInteger(r.modo_disputa_id),
    l.modo_disputa_nome     = r.modo_disputa_nome,
    l.valor_total_estimado  = toFloat(r.valor_total_estimado),
    l.valor_total_homologado= toFloat(r.valor_total_homologado),
    l.data_publicacao       = r.data_publicacao,
    l.data_abertura_proposta= r.data_abertura_proposta,
    l.data_encerramento     = r.data_encerramento,
    l.amparo_legal_codigo   = r.amparo_legal_codigo,
    l.amparo_legal_nome     = r.amparo_legal_nome,
    l.link_sistema_origem   = r.link_sistema_origem,
    l.srp                   = r.srp,
    l.cnpj_orgao            = r.cnpj_orgao,
    l.nome_orgao            = r.nome_orgao,
    l.uf_sigla              = r.uf_sigla,
    l.uf_nome               = r.uf_nome,
    l.codigo_unidade        = r.codigo_unidade,
    l.nome_unidade          = r.nome_unidade,
    l.fonte_nome            = r.fonte_nome,
    l.fonte_url             = r.fonte_url
"""

Q_LICITACAO_EMPRESA = """
UNWIND $rows AS r
MATCH (l:Licitacao {numero_controle: r.numero_controle})
MERGE (e:Empresa {cnpj_basico: r.cnpj_basico})
  ON CREATE SET e.razao_social = r.razao_social,
                e.fonte_nome   = r.fonte_nome
  ON MATCH  SET e.razao_social = coalesce(e.razao_social, r.razao_social)
MERGE (e)-[:PUBLICOU_LICITACAO]->(l)
"""

Q_LICITACAO_MUNICIPIO = """
UNWIND $rows AS r
MATCH (l:Licitacao {numero_controle: r.numero_controle})
MATCH (m:Municipio)
WHERE toUpper(trim(m.nome)) = toUpper(trim(r.nome_municipio))
  AND (r.uf_sigla = "" OR m.uf = r.uf_sigla
       OR EXISTS { MATCH (m)-[:PERTENCE_A*]->(:Estado {sigla: r.uf_sigla}) })
WITH l, m ORDER BY m.id IS NOT NULL DESC LIMIT 1
MERGE (l)-[:LOCALIZADA_EM]->(m)
"""

Q_CONTRATO = """
UNWIND $rows AS r
MERGE (c:Contrato {contrato_id: r.contrato_id})
SET c.numero_contrato       = r.numero_contrato,
    c.tipo_contrato_id      = toInteger(r.tipo_contrato_id),
    c.tipo_contrato_nome    = r.tipo_contrato_nome,
    c.categoria_id          = toInteger(r.categoria_id),
    c.categoria_nome        = r.categoria_nome,
    c.objeto                = r.objeto,
    c.valor_global          = toFloat(r.valor_global),
    c.valor_acumulado       = toFloat(r.valor_acumulado),
    c.data_assinatura       = r.data_assinatura,
    c.data_publicacao       = r.data_publicacao,
    c.data_vigencia_inicio  = r.data_vigencia_inicio,
    c.data_vigencia_fim     = r.data_vigencia_fim,
    c.ano                   = toInteger(r.ano),
    c.sequencial            = r.sequencial,
    c.cnpj_orgao            = r.cnpj_orgao,
    c.nome_orgao            = r.nome_orgao,
    c.uf_sigla              = r.uf_sigla,
    c.nome_contratado       = r.nome_contratado,
    c.cpf_cnpj_contratado   = r.cpf_cnpj_contratado,
    c.numero_controle_pncp_edital = r.numero_controle_pncp_edital,
    c.fonte_nome            = r.fonte_nome,
    c.fonte_url             = r.fonte_url
"""

Q_CONTRATO_ORGAO = """
UNWIND $rows AS r
MATCH (c:Contrato {contrato_id: r.contrato_id})
MERGE (e:Empresa {cnpj_basico: r.cnpj_basico_orgao})
  ON CREATE SET e.razao_social = r.nome_orgao,
                e.fonte_nome   = r.fonte_nome
MERGE (e)-[:FIRMOU_CONTRATO]->(c)
"""

Q_CONTRATO_CONTRATADO = """
UNWIND $rows AS r
MATCH (c:Contrato {contrato_id: r.contrato_id})
WHERE r.cnpj_basico_contratado <> ""
MERGE (e:Empresa {cnpj_basico: r.cnpj_basico_contratado})
  ON CREATE SET e.razao_social = r.nome_contratado,
                e.fonte_nome   = r.fonte_nome
MERGE (c)-[:CONTRATOU]->(e)
"""

Q_CONTRATO_LICITACAO = """
UNWIND $rows AS r
MATCH (c:Contrato {contrato_id: r.contrato_id})
WHERE r.numero_controle_pncp_edital <> ""
MATCH (l:Licitacao {numero_controle: r.numero_controle_pncp_edital})
MERGE (c)-[:VINCULADO_A]->(l)
"""

Q_CONTRATO_MUNICIPIO = """
UNWIND $rows AS r
MATCH (c:Contrato {contrato_id: r.contrato_id})
MATCH (m:Municipio)
WHERE toUpper(trim(m.nome)) = toUpper(trim(r.nome_municipio))
  AND (r.uf_sigla = "" OR m.uf = r.uf_sigla
       OR EXISTS { MATCH (m)-[:PERTENCE_A*]->(:Estado {sigla: r.uf_sigla}) })
WITH c, m ORDER BY m.id IS NOT NULL DESC LIMIT 1
MERGE (c)-[:LOCALIZADA_EM]->(m)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v) -> str:
    try:
        return str(float(v or 0))
    except (TypeError, ValueError):
        return "0"


def _safe_int(v) -> str:
    try:
        return str(int(v or 0))
    except (TypeError, ValueError):
        return "0"


def _cnpj_basico(cnpj: str) -> str:
    digits = "".join(c for c in (cnpj or "") if c.isdigit())
    return digits[:8].zfill(8) if len(digits) >= 8 else ""


def _iter_json(path: Path):
    """Lê arquivo JSON mensal em chunks."""
    if not path.exists():
        return
    try:
        raw = json.loads(path.read_text("utf-8"))
        records = raw if isinstance(raw, list) else raw.get("data", [])
    except (json.JSONDecodeError, OSError) as exc:
        log.warning(f"  Erro lendo {path.name}: {exc}")
        return

    for i in range(0, len(records), CHUNK_SIZE):
        yield records[i : i + CHUNK_SIZE]

    log.info(f"    {path.name}: {len(records):,} registros")


# ── Transforms ────────────────────────────────────────────────────────────────

def _t_editais(chunk: list[dict]) -> dict:
    licitacoes, empresas, municipios = [], [], []

    for r in chunk:
        numero = str(r.get("numeroControlePNCP") or "").strip()
        if not numero:
            continue

        orgao  = r.get("orgaoEntidade") or {}
        unidade = r.get("unidadeOrgao") or {}
        amparo = r.get("amparoLegal") or {}

        cnpj_org = str(orgao.get("cnpj") or "").strip()
        nome_org = str(orgao.get("razaoSocial") or "").strip()
        uf_sigla = str(unidade.get("ufSigla") or "").strip()
        uf_nome  = str(unidade.get("ufNome") or "").strip()
        nome_mun = str(unidade.get("municipioNome") or "").strip()
        cod_uni  = str(unidade.get("codigoUnidade") or "").strip()
        nome_uni = str(unidade.get("nomeUnidade") or "").strip()

        licitacoes.append({
            "numero_controle":       numero,
            "ano_compra":            _safe_int(r.get("anoCompra")),
            "sequencial_compra":     str(r.get("sequencialCompra") or ""),
            "numero_compra":         str(r.get("numeroCompra") or ""),
            "processo":              str(r.get("processo") or ""),
            "objeto":                str(r.get("objetoCompra") or "")[:2000],
            "modalidade_id":         _safe_int(r.get("modalidadeId")),
            "modalidade_nome":       str(r.get("modalidadeNome") or r.get("_modalidade_nome") or ""),
            "situacao_id":           _safe_int(r.get("situacaoCompraId")),
            "situacao_nome":         str(r.get("situacaoCompraNome") or ""),
            "modo_disputa_id":       _safe_int(r.get("modoDisputaId")),
            "modo_disputa_nome":     str(r.get("modoDisputaNome") or ""),
            "valor_total_estimado":  _safe_float(r.get("valorTotalEstimado")),
            "valor_total_homologado":_safe_float(r.get("valorTotalHomologado")),
            "data_publicacao":       str(r.get("dataPublicacaoPncp") or "")[:19],
            "data_abertura_proposta":str(r.get("dataAberturaProposta") or "")[:19],
            "data_encerramento":     str(r.get("dataEncerramentoProposta") or "")[:19],
            "amparo_legal_codigo":   _safe_int((amparo or {}).get("codigo")),
            "amparo_legal_nome":     str((amparo or {}).get("nome") or ""),
            "link_sistema_origem":   str(r.get("linkSistemaOrigem") or "")[:500],
            "srp":                   str(r.get("srp") or ""),
            "cnpj_orgao":            cnpj_org,
            "nome_orgao":            nome_org,
            "uf_sigla":              uf_sigla,
            "uf_nome":               uf_nome,
            "codigo_unidade":        cod_uni,
            "nome_unidade":          nome_uni,
            **FONTE,
        })

        if cnpj_org:
            empresas.append({
                "numero_controle": numero,
                "cnpj_basico":     _cnpj_basico(cnpj_org),
                "razao_social":    nome_org,
                **FONTE,
            })

        if nome_mun:
            municipios.append({
                "numero_controle": numero,
                "nome_municipio":  nome_mun,
                "uf_sigla":        uf_sigla,
            })

    return {"licitacoes": licitacoes, "empresas": empresas, "municipios": municipios}


def _t_contratos(chunk: list[dict]) -> dict:
    contratos, orgaos, contratados, licitacoes, municipios = [], [], [], [], []

    for r in chunk:
        # chave: cnpj_orgao + ano + sequencial + numero_contrato
        cnpj_org  = str(r.get("cnpjOrgao") or "").strip()
        ano       = _safe_int(r.get("anoContrato") or r.get("ano"))
        seq       = str(r.get("sequencialContrato") or r.get("sequencial") or "")
        num_cont  = str(r.get("numeroContratoEmpenho") or "").strip()
        contrato_id = f"{cnpj_org}_{ano}_{seq}_{num_cont}"
        if not cnpj_org:
            continue

        nome_org  = str(r.get("razaoSocialOrgao") or r.get("orgaoEntidade", {}).get("razaoSocial") or "")
        unidade   = r.get("unidadeOrgao") or {}
        uf_sigla  = str(unidade.get("ufSigla") or r.get("ufSigla") or "")
        nome_mun  = str(unidade.get("municipioNome") or "")
        nome_cont = str(r.get("nomeRazaoSocialContratado") or "")
        cpf_cnpj_cont = str(r.get("cpfCnpjContratado") or "").strip()
        nr_controle_edital = str(r.get("numeroControlePNCPCompra") or "").strip()

        contratos.append({
            "contrato_id":                contrato_id,
            "numero_contrato":            num_cont,
            "tipo_contrato_id":           _safe_int(r.get("tipoContrato", {}).get("id") if isinstance(r.get("tipoContrato"), dict) else r.get("tipoContrato")),
            "tipo_contrato_nome":         str((r.get("tipoContrato") or {}).get("nome") or ""),
            "categoria_id":               _safe_int(r.get("categoriaProcesso", {}).get("id") if isinstance(r.get("categoriaProcesso"), dict) else None),
            "categoria_nome":             str((r.get("categoriaProcesso") or {}).get("nome") or ""),
            "objeto":                     str(r.get("objetoContrato") or "")[:2000],
            "valor_global":               _safe_float(r.get("valorGlobal")),
            "valor_acumulado":            _safe_float(r.get("valorAcumulado")),
            "data_assinatura":            str(r.get("dataAssinatura") or "")[:19],
            "data_publicacao":            str(r.get("dataPublicacaoPncp") or "")[:19],
            "data_vigencia_inicio":       str(r.get("dataVigenciaInicio") or "")[:10],
            "data_vigencia_fim":          str(r.get("dataVigenciaFim") or "")[:10],
            "ano":                        ano,
            "sequencial":                 seq,
            "cnpj_orgao":                 cnpj_org,
            "nome_orgao":                 nome_org,
            "uf_sigla":                   uf_sigla,
            "nome_contratado":            nome_cont,
            "cpf_cnpj_contratado":        cpf_cnpj_cont,
            "numero_controle_pncp_edital":nr_controle_edital,
            **FONTE,
        })

        orgaos.append({
            "contrato_id":    contrato_id,
            "cnpj_basico_orgao": _cnpj_basico(cnpj_org),
            "nome_orgao":     nome_org,
            **FONTE,
        })

        digits_cont = "".join(c for c in cpf_cnpj_cont if c.isdigit())
        if len(digits_cont) >= 8:
            contratados.append({
                "contrato_id":              contrato_id,
                "cnpj_basico_contratado":   digits_cont[:8].zfill(8),
                "nome_contratado":          nome_cont,
                **FONTE,
            })

        if nr_controle_edital:
            licitacoes.append({
                "contrato_id": contrato_id,
                "numero_controle_pncp_edital": nr_controle_edital,
            })

        if nome_mun:
            municipios.append({
                "contrato_id":  contrato_id,
                "nome_municipio": nome_mun,
                "uf_sigla":     uf_sigla,
            })

    return {
        "contratos":   contratos,
        "orgaos":      orgaos,
        "contratados": contratados,
        "licitacoes":  licitacoes,
        "municipios":  municipios,
    }


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_editais(driver) -> None:
    editais_dir = DATA_DIR / "editais"
    if not editais_dir.exists():
        log.warning("  data/pncp/editais/ não encontrado — rode download pncp primeiro")
        return

    json_files = sorted(editais_dir.glob("pncp_editais_*.json"))
    total_l = total_e = total_m = 0

    with driver.session() as session:
        for jf in json_files:
            for chunk in _iter_json(jf):
                t = _t_editais(chunk)
                if t["licitacoes"]:
                    run_batches(session, Q_LICITACAO, t["licitacoes"])
                    total_l += len(t["licitacoes"])
                if t["empresas"]:
                    run_batches(session, Q_LICITACAO_EMPRESA, t["empresas"])
                    total_e += len(t["empresas"])
                if t["municipios"]:
                    run_batches(session, Q_LICITACAO_MUNICIPIO, t["municipios"])
                    total_m += len(t["municipios"])

    log.info(f"    ✓ licitações={total_l:,}  empresas={total_e:,}  municípios={total_m:,}")


def _load_contratos(driver) -> None:
    contratos_dir = DATA_DIR / "contratos"
    if not contratos_dir.exists():
        log.warning("  data/pncp/contratos/ não encontrado — rode download pncp primeiro")
        return

    json_files = sorted(contratos_dir.glob("pncp_contratos_*.json"))
    total_c = total_o = total_cont = total_l = total_m = 0

    with driver.session() as session:
        for jf in json_files:
            for chunk in _iter_json(jf):
                t = _t_contratos(chunk)
                if t["contratos"]:
                    run_batches(session, Q_CONTRATO, t["contratos"])
                    total_c += len(t["contratos"])
                if t["orgaos"]:
                    run_batches(session, Q_CONTRATO_ORGAO, t["orgaos"])
                    total_o += len(t["orgaos"])
                if t["contratados"]:
                    run_batches(session, Q_CONTRATO_CONTRATADO, t["contratados"])
                    total_cont += len(t["contratados"])
                if t["licitacoes"]:
                    run_batches(session, Q_CONTRATO_LICITACAO, t["licitacoes"])
                    total_l += len(t["licitacoes"])
                if t["municipios"]:
                    run_batches(session, Q_CONTRATO_MUNICIPIO, t["municipios"])
                    total_m += len(t["municipios"])

    log.info(f"    ✓ contratos={total_c:,}  orgaos={total_o:,}  contratados={total_cont:,}  "
             f"vínculos_licitacao={total_l:,}  municípios={total_m:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info(f"[pncp] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "pncp"):
        log.info("  [1/2] Licitações/Editais...")
        _load_editais(driver)

        log.info("  [2/2] Contratos...")
        _load_contratos(driver)

    driver.close()
    log.info("[pncp] Pipeline concluído")