"""
Pipeline 7 - Sanções CGU (CEIS + CNEP) → Neo4j

Lê os CSVs de data/sancoes_cgu/ e carrega no grafo.

  ceis.csv → :Sancao {tipo_registro: "CEIS"}
  cnep.csv → :Sancao {tipo_registro: "CNEP"}

Nós criados/atualizados:
  (:Sancao)  — chave: cpf_cnpj + tipo_registro + data_inicio
  (:Empresa) — merge pelo cnpj_basico (left 8 dígitos) com Receita Federal
  (:Pessoa)  — merge pelo CPF com candidatos TSE / servidores

Relacionamentos:
  (:Empresa)-[:POSSUI_SANCAO]->(:Sancao)
  (:Pessoa)-[:POSSUI_SANCAO]->(:Sancao)
  (:Sancao)-[:APLICADA_POR]->(:OrgaoSancionador)
"""

import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, apply_schema, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "sancoes_cgu"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "10000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "CGU — Portal da Transparência",
    "fonte_url":  "https://portaldatransparencia.gov.br",
}


# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Sancao) REQUIRE n.sancao_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:OrgaoSancionador) REQUIRE n.nome_orgao IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX sancao_tipo   IF NOT EXISTS FOR (s:Sancao) ON (s.tipo_registro)",
    "CREATE INDEX sancao_inicio IF NOT EXISTS FOR (s:Sancao) ON (s.data_inicio)",
    "CREATE INDEX sancao_cpfcnpj IF NOT EXISTS FOR (s:Sancao) ON (s.cpf_cnpj)",
]


# ── Queries ───────────────────────────────────────────────────────────────────

Q_ORGAO_SANCIONADOR = """
UNWIND $rows AS r
MERGE (o:OrgaoSancionador {nome_orgao: r.orgao_sancionador})
SET o.uf_orgao       = r.uf_orgao,
    o.esfera_governo = r.esfera_governo,
    o.fonte_nome     = r.fonte_nome
"""

Q_SANCAO = """
UNWIND $rows AS r
MERGE (s:Sancao {sancao_id: r.sancao_id})
SET s.cpf_cnpj         = r.cpf_cnpj,
    s.nome             = r.nome,
    s.tipo_sancao      = r.tipo_sancao,
    s.tipo_registro    = r.tipo_registro,
    s.data_inicio      = r.data_inicio,
    s.data_fim         = r.data_fim,
    s.fundamentacao    = r.fundamentacao,
    s.numero_processo  = r.numero_processo,
    s.valor_multa      = CASE r.valor_multa WHEN '' THEN null ELSE toFloat(replace(replace(r.valor_multa,'.',''),',','.')) END,
    s.fonte_nome       = r.fonte_nome,
    s.fonte_url        = r.fonte_url
"""

Q_SANCAO_ORGAO = """
UNWIND $rows AS r
MATCH (s:Sancao {sancao_id: r.sancao_id})
WHERE r.orgao_sancionador <> ""
MATCH (o:OrgaoSancionador {nome_orgao: r.orgao_sancionador})
MERGE (s)-[:APLICADA_POR]->(o)
"""

Q_EMPRESA_SANCAO = """
UNWIND $rows AS r
MERGE (e:Empresa {cnpj_basico: r.cnpj_basico})
  ON CREATE SET e.fonte_nome = r.fonte_nome
WITH e, r
MATCH (s:Sancao {sancao_id: r.sancao_id})
MERGE (e)-[:POSSUI_SANCAO]->(s)
"""

Q_PESSOA_SANCAO = """
UNWIND $rows AS r
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome      = r.nome,
                p.fonte_nome = r.fonte_nome
  ON MATCH  SET p.nome      = coalesce(p.nome, r.nome)
WITH p, r
MATCH (s:Sancao {sancao_id: r.sancao_id})
MERGE (p)-[:POSSUI_SANCAO]->(s)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_doc(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def _is_cnpj(digits: str) -> bool:
    """CNPJ tem 14 dígitos e não pode ser todos iguais."""
    return len(digits) == 14 and not all(c == digits[0] for c in digits)


def _is_cpf(digits: str) -> bool:
    """CPF tem 11 dígitos e não pode ser todos iguais."""
    return len(digits) == 11 and not all(c == digits[0] for c in digits)


# ── Transform ─────────────────────────────────────────────────────────────────

def _t_sancoes(chunk: list[dict], tipo_registro: str) -> dict:
    orgaos   = {}
    sancoes  = []
    empresas = []
    pessoas  = []
    com_orgao = []

    for r in chunk:
        doc_raw  = r.get("cpf_cnpj", "").strip()
        digits   = _strip_doc(doc_raw)
        nome     = r.get("nome", "").strip()
        inicio   = r.get("data_inicio", "").strip()

        if not digits and not nome:
            continue

        # chave única: doc + tipo_registro + data_inicio
        # chave robusta: usa documento quando disponível, senão hash do nome
        import hashlib as _hl
        doc_key = digits if digits else _hl.md5(nome.encode()).hexdigest()[:16]
        sancao_id = f"{doc_key}_{tipo_registro}_{inicio}"

        orgao = r.get("orgao_sancionador", "").strip()
        if orgao and orgao not in orgaos:
            orgaos[orgao] = {
                "orgao_sancionador": orgao,
                "uf_orgao":          r.get("uf_orgao", "").strip(),
                "esfera_governo":    r.get("esfera_governo", "").strip(),
                **FONTE,
            }

        sancoes.append({
            "sancao_id":       sancao_id,
            "cpf_cnpj":        doc_raw,
            "nome":            nome,
            "tipo_sancao":     r.get("tipo_sancao", "").strip(),
            "tipo_registro":   tipo_registro,
            "data_inicio":     inicio,
            "data_fim":        r.get("data_fim", "").strip(),
            "fundamentacao":   r.get("fundamentacao", "").strip(),
            "numero_processo": r.get("numero_processo", "").strip(),
            "valor_multa":     r.get("valor_multa", "").strip(),
            **FONTE,
        })

        if orgao:
            com_orgao.append({"sancao_id": sancao_id,
                               "orgao_sancionador": orgao})

        if _is_cnpj(digits):
            empresas.append({
                "sancao_id":   sancao_id,
                "cnpj_basico": digits[:8],
                **FONTE,
            })
        elif _is_cpf(digits):
            pessoas.append({
                "sancao_id": sancao_id,
                "cpf":       digits,
                "nome":      nome,
                **FONTE,
            })

    return {
        "orgaos":    list(orgaos.values()),
        "sancoes":   sancoes,
        "com_orgao": com_orgao,
        "empresas":  empresas,
        "pessoas":   pessoas,
    }


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_dataset(driver, path: Path, tipo_registro: str, limite: int | None = None, stats: dict = None) -> None:
    total_s = total_e = total_p = 0
    if stats is None:
        stats = {'total': 0}

    with driver.session() as session:
        for chunk in iter_csv(path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [{tipo_registro}] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]

            t = _t_sancoes(chunk, tipo_registro)

            if t["orgaos"]:
                run_batches(session, Q_ORGAO_SANCIONADOR, t["orgaos"])
            if t["sancoes"]:
                run_batches(session, Q_SANCAO, t["sancoes"])
                total_s += len(t["sancoes"])
                stats['total'] += len(t["sancoes"])
            if t["com_orgao"]:
                run_batches(session, Q_SANCAO_ORGAO, t["com_orgao"])
            if t["empresas"]:
                run_batches(session, Q_EMPRESA_SANCAO, t["empresas"])
                total_e += len(t["empresas"])
            if t["pessoas"]:
                run_batches(session, Q_PESSOA_SANCAO, t["pessoas"])
                total_p += len(t["pessoas"])

    log.info(f"    ✓ {tipo_registro}: sanções={total_s:,}  empresas={total_e:,}  pessoas={total_p:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, limite: int | None = None):
    log.info(f"[sancoes_cgu] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        apply_schema(session, Q_CONSTRAINTS, Q_INDEXES)

    with IngestionRun(driver, "sancoes_cgu"):
        stats = {'total': 0}
        for dataset, tipo in [("ceis", "CEIS"), ("cnep", "CNEP")]:
            path = DATA_DIR / f"{dataset}.csv"
            log.info(f"  [{tipo}]...")
            _load_dataset(driver, path, tipo, limite, stats)
            if limite is not None and stats['total'] >= limite:
                log.info(f"  Limite de {limite:,} linhas atingido após {tipo}. Parando.")
                break

    driver.close()
    log.info("[sancoes_cgu] Pipeline concluído")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline Sanções CGU — carrega CEIS e CNEP no Neo4j")
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
        limite=args.limite,
    )
