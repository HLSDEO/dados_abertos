"""
Pipeline 13 - Senado Federal: Despesas CEAP → Neo4j

Nós criados/atualizados:
  (:Parlamentar {id_senado}) — criado/atualizado via ID do Senado
  (:Despesa {despesa_id, tipo_despesa, valor_liquido, data_emissao, ano, mes,
              nome_fornecedor, partido, uf})

Relacionamentos:
  (:Parlamentar)-[:GASTOU]->(:Despesa)
  (:Empresa {cnpj})-[:FORNECEU]->(:Despesa) via CNPJ do fornecedor
"""

import logging
import os
from pathlib import Path
from pipeline.lib import (wait_for_neo4j, run_batches, IngestionRun,
                        apply_schema, setup_schema, strip_doc, iter_csv)

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "senado"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "50000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "2000"))

FONTE = {
    "fonte_nome": "Senado Federal",
    "fonte_url":  "https://adm.senado.gov.br",
}

# ── Constraints / índices ──────────────────────────────────────
Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Despesa) REQUIRE d.despesa_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Parlamentar) REQUIRE p.id_senado IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX despesa_ano         IF NOT EXISTS FOR (d:Despesa) ON (d.ano)",
    "CREATE INDEX despesa_mes         IF NOT EXISTS FOR (d:Despesa) ON (d.mes)",
    "CREATE INDEX despesa_tipo        IF NOT EXISTS FOR (d:Despesa) ON (d.tipo_despesa)",
]

# ── Queries Cypher ──────────────────────────────────────────────
Q_PARLAMENTAR = """
UNWIND $rows AS r
MERGE (p:Parlamentar {id_senado: r.id_senado})
SET p.nome_parlamentar = r.nome_parlamentar,
    p.partido         = r.partido,
    p.uf              = r.uf,
    p.fonte_nome      = r.fonte_nome
"""

Q_DESPESA = """
UNWIND $rows AS r
MATCH (p:Parlamentar {id_senado: r.id_senado})
MERGE (d:Despesa {despesa_id: r.despesa_id})
SET d.tipo_despesa    = r.tipo_despesa,
    d.valor_liquido    = toFloat(r.valor_liquido),
    d.data_emissao    = r.data_emissao,
    d.ano             = toInteger(r.ano),
    d.mes             = toInteger(r.mes),
    d.nome_fornecedor = r.nome_fornecedor,
    d.partido         = r.partido,
    d.uf              = r.uf,
    d.fonte_nome      = r.fonte_nome
MERGE (p)-[:GASTOU]->(d)
"""

Q_DESPESA_EMPRESA = """
UNWIND $rows AS r
MATCH (e:Empresa {cnpj: r.cnpj_fornecedor})
MATCH (d:Despesa {despesa_id: r.despesa_id})
MERGE (e)-[:FORNECEU]->(d)
"""

# ── Helpers ───────────────────────────────────────────────────

def _clean_cnpj(raw: str) -> str:
    """Remove pontuação e retorna CNPJ com 14 dígitos ou vazio."""
    digits = strip_doc(raw or "")
    if len(digits) == 14:
        return digits
    return ""

def _transform_item(item: dict) -> tuple[dict | None, dict | None]:
    """
    Transforma um item do CSV para os formatos do Neo4j.
    Retorna (parlamentar_dict, empresa_dict) ou (None, None) se inválido.
    """
    # Mapeamento para o novo formato CSV (cabeçalhos em maiúsculo)
    id_senado = str(item.get("COD_SENADOR", item.get("codigoParlamentar", ""))).strip()
    if not id_senado:
        return None, None

    nome_parlamentar = item.get("NOME_SENADOR", item.get("nomeParlamentar", "")).strip()
    tipo_despesa    = item.get("TIPO_DESPESA", item.get("tipoDespesa", "")).strip()
    
    # Valor no CSV vem como "261,03"
    valor_raw = str(item.get("VALOR_REEMBOLSADO", item.get("valorLiquido", "0")))
    valor_liquido = valor_raw.replace(".", "").replace(",", ".")
    
    data_emissao    = str(item.get("DATA", item.get("dataEmissao", ""))).strip()
    ano            = str(item.get("ANO", item.get("ano", ""))).strip()
    mes            = str(item.get("MÊS", item.get("mes", ""))).strip()
    
    cnpj_fornecedor = _clean_cnpj(item.get("CPF_CNPJ_FORNECEDOR", item.get("cnpjFornecedor", "")))
    nome_fornecedor = item.get("NOME_FORNECEDOR", item.get("nomeFornecedor", "")).strip()
    
    # Partido e UF
    partido = item.get("partido", "").strip()
    uf      = item.get("uf", "").strip()

    # Cria despesa_id único (usando ID do registro se disponível ou hash)
    row_id = item.get("ID", "")
    if row_id:
        despesa_id = f"senado_{row_id}"
    else:
        despesa_id = f"{id_senado}_{ano}_{mes}_{hash(valor_liquido)}"[:100]

    base = {
        "despesa_id":      despesa_id,
        "id_senado":       id_senado,
        "nome_parlamentar": nome_parlamentar,
        "tipo_despesa":    tipo_despesa,
        "valor_liquido":   valor_liquido,
        "data_emissao":   data_emissao,
        "ano":             ano,
        "mes":             mes,
        "cnpj_fornecedor": cnpj_fornecedor,
        "nome_fornecedor": nome_fornecedor,
        "partido":         partido,
        "uf":              uf,
        "fonte_nome":      FONTE["fonte_nome"],
    }

    empresa = None
    if cnpj_fornecedor:
        empresa = base.copy()

    return base, empresa

def _load_senado(driver, limite: int | None = None, stats: dict = None) -> None:
    """Carrega todos os CSVs de despesas do Senado."""
    todos = sorted(DATA_DIR.glob("despesas_*.csv"))
    if not todos:
        log.warning("  Nenhum arquivo despesas_*.csv encontrado — execute download senado primeiro")
        return

    if stats is None:
        stats = {'total': 0}

    for path in todos:
        log.info(f"  Carregando {path.name}...")
        parlamentar_t = empresa_t = 0
        
        # Usa iter_csv para ler em chunks de forma eficiente
        for chunk_data in iter_csv(path, chunk_size=CHUNK_SIZE):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [senado] Limite de {limite:,} atingido. Parando.")
                return

            parlamentar_rows = []
            empresa_rows = []

            for item in chunk_data:
                if limite is not None and stats['total'] >= limite:
                    break
                
                parlamentar_dict, empresa_dict = _transform_item(item)
                if parlamentar_dict:
                    parlamentar_rows.append(parlamentar_dict)
                    stats['total'] += 1
                    parlamentar_t += 1
                    if empresa_dict:
                        empresa_rows.append(empresa_dict)
                        empresa_t += 1

            if parlamentar_rows:
                with driver.session() as session:
                    run_batches(session, Q_PARLAMENTAR, parlamentar_rows)
                    run_batches(session, Q_DESPESA, parlamentar_rows)
                
                if empresa_rows:
                    with driver.session() as session:
                        run_batches(session, Q_DESPESA_EMPRESA, empresa_rows)

        log.info(f"    ✓ {path.name}  Parlamentar={parlamentar_t:,}  Empresa={empresa_t:,}")

# ── Link Parlamentar → Pessoa (Senado) ──────────────────────

def _link_parlamentar_pessoa(driver) -> None:
    """Linka Parlamentares do Senado com Pessoa via CPF (usando mapa TSE)."""
    log.info("  Linkando Parlamentar (Senado) → Pessoa...")
    try:
        from pipeline.emendas_cgu import _build_nome_cpf_map, _resolve_cpf, _norm_tokens
        exact_map, cand_tokens = _build_nome_cpf_map(driver)
    except ImportError:
        log.warning("    Não foi possível importar funções de emendas_cgu")
        return

    if not exact_map and not cand_tokens:
        log.info("    Dados TSE não disponíveis para link")
        return

    linked = 0
    with driver.session() as session:
        result = session.run(
            "MATCH (par:Parlamentar) WHERE par.id_senado IS NOT NULL "
            "RETURN par.id_senado AS id, par.nome_parlamentar AS nome"
        )
        
        ambiguous = missed = 0
        for rec in result:
            cpf = _resolve_cpf(rec["nome"] or "", exact_map, cand_tokens)
            if cpf:
                session.run(
                    "MATCH (par:Parlamentar {id_senado: $id}) "
                    "MATCH (p:Pessoa {cpf: $cpf}) "
                    "MERGE (par)-[:MESMO_QUE]->(p)",
                    id=rec["id"], cpf=cpf
                )
                linked += 1
            else:
                tokens = _norm_tokens(rec["nome"] or "")
                if any(tokens & t for t, _ in cand_tokens):
                    ambiguous += 1
                else:
                    missed += 1
                    
    log.info(f"    ✓ {linked} parlamentares linkados  ambíguos={ambiguous}  sem_match={missed}")

# ── Entry-point ──────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, limite: int | None = None):
    log.info(f"[senado] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")
    
    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)
    
    with driver.session() as session:
        log.info("  Constraints e índices...")
        apply_schema(session, Q_CONSTRAINTS, Q_INDEXES)

    with IngestionRun(driver, "senado") as run_ctx:
        log.info("  [1/2] Parlamentar + Despesa → GASTOU, FORNECEU...")
        stats = {'total': 0}
        _load_senado(driver, limite=limite, stats=stats)
        run_ctx.add(rows_in=stats['total'], rows_out=stats['total'])

        log.info("  [2/2] Linkando Parlamentar → Pessoa (Senado)...")
        _link_parlamentar_pessoa(driver)

    driver.close()
    log.info("[senado] Pipeline concluído")
