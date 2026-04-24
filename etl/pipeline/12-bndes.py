"""
Pipeline 12 - BNDES: Operações de Financiamento → Neo4j

Nós criados/atualizados:
  (:Emprestimo {
      emprestimo_id, cliente, descricao_do_projeto, uf, municipio,
      numero_do_contrato, data_da_contratacao, valor_contratado_reais,
      valor_desembolsado_reais, fonte_de_recurso, custo_financeiro,
      juros, prazo_carencia_meses, prazo_amortizacao_meses,
      modalidade_de_apoio, forma_de_apoio, produto, instrumento_financeiro,
      inovacao, area_operacional, setor_cnae, subsetor_cnae_nome,
      setor_bndes, porte_do_cliente, natureza_do_cliente,
      situacao_do_contrato, fonte_nome
  })

Relacionamentos:
  (:Empresa {cnpj_basico})-[:RECEBU_EMPRESTIMO]->(:Emprestimo)
  (:Empresa {cnpj}) cases tenha cnpj_completo
"""

import logging
import os
from pathlib import Path

from pipeline.lib import (wait_for_neo4j, run_batches, iter_csv,
                        IngestionRun, setup_schema, strip_doc)

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "bndes"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "50000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "2000"))

FONTE = {
    "fonte_nome": "BNDES — Banco Nacional de Desenvolvimento Econômico e Social",
    "fonte_url":  "https://dadosabertos.bndes.gov.br",
}


# ── Constraints / índices ─────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Emprestimo) REQUIRE e.emprestimo_id IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX emprestimo_data     IF NOT EXISTS FOR (e:Emprestimo) ON (e.data_da_contratacao)",
    "CREATE INDEX emprestimo_setor   IF NOT EXISTS FOR (e:Emprestimo) ON (e.setor_bndes)",
    "CREATE INDEX emprestimo_produto IF NOT EXISTS FOR (e:Emprestimo) ON (e.produto)",
    "CREATE INDEX emprestimo_situacao IF NOT EXISTS FOR (e:Emprestimo) ON (e.situacao_do_contrato)",
]


# ── Queries Cypher ────────────────────────────────────────────────────

Q_EMPRESTIMO_EMPRESA = """
UNWIND $rows AS r
MERGE (e:Emprestimo {emprestimo_id: r.emprestimo_id})
SET e.cliente                 = r.cliente,
    e.descricao_do_projeto    = r.descricao_do_projeto,
    e.uf                      = r.uf,
    e.municipio              = r.municipio,
    e.numero_do_contrato      = r.numero_do_contrato,
    e.data_da_contratacao     = r.data_da_contratacao,
    e.valor_contratado_reais  = toFloat(r.valor_contratado_reais),
    e.valor_desembolsado_reais = toFloat(r.valor_desembolsado_reais),
    e.fonte_de_recurso       = r.fonte_de_recurso,
    e.custo_financeiro       = toFloat(r.custo_financeiro),
    e.juros                  = toFloat(r.juros),
    e.prazo_carencia_meses   = toInteger(r.prazo_carencia_meses),
    e.prazo_amortizacao_meses = toInteger(r.prazo_amortizacao_meses),
    e.modalidade_de_apoio    = r.modalidade_de_apoio,
    e.forma_de_apoio        = r.forma_de_apoio,
    e.produto               = r.produto,
    e.instrumento_financeiro = r.instrumento_financeiro,
    e.inovacao              = r.inovacao,
    e.area_operacional      = r.area_operacional,
    e.setor_cnae            = r.setor_cnae,
    e.subsetor_cnae_nome    = r.subsetor_cnae_nome,
    e.setor_bndes           = r.setor_bndes,
    e.porte_do_cliente      = r.porte_do_cliente,
    e.natureza_do_cliente   = r.natureza_do_cliente,
    e.situacao_do_contrato  = r.situacao_do_contrato,
    e.fonte_nome            = r.fonte_nome
WITH e, r
MERGE (emp:Empresa {cnpj_basico: r.cnpj_basico})
  ON CREATE SET emp.cnpj = r.cnpj_completo, emp.nome = r.cliente, emp.fonte_nome = r.fonte_nome
MERGE (emp)-[:RECEBEU_EMPRESTIMO]->(e)
"""


# ── Helpers ───────────────────────────────────────────────────────────

def _clean_cnpj(raw: str) -> tuple[str, str]:
    """
    Limpa CNPJ e retorna (cnpj_basico, cnpj_completo).
    cnpj_basico: 8 dígitos para matching com Empresa
    cnpj_completo: 14 dígitos se disponível
    """
    digits = strip_doc(raw or "")
    if len(digits) == 14:
        return digits[:8], digits
    elif len(digits) == 8:
        return digits, ""
    return "", ""


def _transform_chunk(chunk: list[dict]) -> list[dict]:
    """Transforma chunk para o formato do Neo4j."""
    result = []
    for r in chunk:
        cnpj_raw = r.get("cnpj", "")
        cnpj_basico, cnpj_completo = _clean_cnpj(cnpj_raw)

        # Usa _id como emprestimo_id (único)
        emprestimo_id = r.get("_id", "").strip()
        if not emprestimo_id:
            continue

        mapped = {
            "emprestimo_id":           emprestimo_id,
            "cliente":                r.get("cliente", "").strip(),
            "cnpj_basico":            cnpj_basico,
            "cnpj_completo":          cnpj_completo,
            "descricao_do_projeto":    r.get("descricao_do_projeto", "").strip(),
            "uf":                     r.get("uf", "").strip(),
            "municipio":              r.get("municipio", "").strip(),
            "municipio_codigo":       r.get("municipio_codigo", "").strip(),
            "numero_do_contrato":     r.get("numero_do_contrato", "").strip(),
            "data_da_contratacao":    r.get("data_da_contratacao", "").strip(),
            "valor_contratado_reais":  (r.get("valor_contratado_reais", "0") or "0").replace(",", "."),
            "valor_desembolsado_reais": (r.get("valor_desembolsado_reais", "0") or "0").replace(",", "."),
            "fonte_de_recurso":       r.get("fonte_de_recurso_desembolsos", r.get("fonte_de_recurso", "")).strip(),
            "custo_financeiro":       (r.get("custo_financeiro", "0") or "0").replace(",", "."),
            "juros":                  (r.get("juros", "0") or "0").replace(",", "."),
            "prazo_carencia_meses":  (r.get("prazo_carencia_meses", "0") or "0").replace(",", "."),
            "prazo_amortizacao_meses": (r.get("prazo_amortizacao_meses", "0") or "0").replace(",", "."),
            "modalidade_de_apoio":    r.get("modalidade_de_apoio", "").strip(),
            "forma_de_apoio":        r.get("forma_de_apoio", "").strip(),
            "produto":                r.get("produto", "").strip(),
            "instrumento_financeiro": r.get("instrumento_financeiro", "").strip(),
            "inovacao":              r.get("inovacao", "").strip(),
            "area_operacional":       r.get("area_operacional", "").strip(),
            "setor_cnae":            r.get("setor_cnae", "").strip(),
            "subsetor_cnae_nome":    r.get("subsetor_cnae_nome", "").strip(),
            "subsetor_cnae_agrupado": r.get("subsetor_cnae_agrupado", "").strip(),
            "setor_bndes":           r.get("setor_bndes", "").strip(),
            "subsetor_bndes":         r.get("subsetor_bndes", "").strip(),
            "porte_do_cliente":      r.get("porte_do_cliente", "").strip(),
            "natureza_do_cliente":   r.get("natureza_do_cliente", "").strip(),
            "situacao_do_contrato":  r.get("situacao_do_contrato", "").strip(),
            "fonte_nome":            r.get("fonte_nome", FONTE["fonte_nome"]),
        }
        result.append(mapped)
    return result


def _load_bndes(driver, limite: int | None = None, stats: dict = None) -> None:
    """Carrega todos os CSVs do BNDES."""
    todos = sorted(DATA_DIR.glob("operacoes_*.csv"))
    if not todos:
        log.warning("  Nenhum arquivo operacoes_*.csv encontrado — execute download bndes primeiro")
        return
    if stats is None:
        stats = {'total': 0}

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        total_rows = 0

        with driver.session() as session:
            for chunk in iter_csv(path, delimiter="auto"):
                if limite is not None and stats['total'] >= limite:
                    log.info(f"    [bndes] Limite de {limite:,} atingido. Parando.")
                    return
                if limite is not None:
                    restante = limite - stats['total']
                    if restante <= 0:
                        return
                    if len(chunk) > restante:
                        chunk = chunk[:restante]

                transformed = _transform_chunk(chunk)
                if transformed:
                    run_batches(session, Q_EMPRESTIMO_EMPRESA, transformed)
                    total_rows += len(transformed)
                    stats['total'] += len(transformed)

        log.info(f"    ✓ {path.name}  {total_rows:,} empréstimos carregados")


# ── Entry-point ─────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, limite: int | None = None):
    log.info(f"[bndes] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "bndes"):
        log.info("  [1/1] Empréstimos → Emprestimo, RECEBEU_EMPRESTIMO...")
        stats = {'total': 0}
        _load_bndes(driver, limite, stats)

    driver.close()
    log.info("[bndes] Pipeline concluído")
