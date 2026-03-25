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

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "sancoes_cgu"
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
    s.valor_multa      = r.valor_multa,
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

def _iter_csv(path: Path):
    if not path.exists():
        log.warning(f"  {path.name} não encontrado — pulando")
        return
    total = 0
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        chunk  = []
        for row in reader:
            row = {k: (v or "").strip() for k, v in row.items() if k is not None}
            chunk.append(row)
            if len(chunk) >= CHUNK_SIZE:
                yield chunk
                total += len(chunk)
                chunk  = []
        if chunk:
            total += len(chunk)
            yield chunk
    log.info(f"    {path.name}: {total:,} linhas")


def _run_batches(session, query: str, rows: list[dict],
                 retries: int = 5) -> None:
    import time
    from neo4j.exceptions import TransientError
    for i in range(0, len(rows), BATCH):
        batch = rows[i : i + BATCH]
        for attempt in range(1, retries + 1):
            try:
                with session.begin_transaction() as tx:
                    tx.run(query, rows=batch)
                    tx.commit()
                break
            except TransientError as exc:
                if "DeadlockDetected" in str(exc) and attempt < retries:
                    import time as t; t.sleep(attempt * 0.5)
                else:
                    raise


def _wait_for_neo4j(uri, user, password, retries=20, delay=5.0):
    import time
    from neo4j.exceptions import ServiceUnavailable
    driver = GraphDatabase.driver(uri, auth=(user, password))
    for attempt in range(1, retries + 1):
        try:
            with driver.session() as s:
                s.run("RETURN 1")
            return driver
        except ServiceUnavailable:
            log.warning(f"  Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Neo4j não disponível")


def _strip_doc(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def _is_cnpj(digits: str) -> bool:
    return len(digits) == 14

def _is_cpf(digits: str) -> bool:
    return len(digits) == 11


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
        sancao_id = f"{digits or nome[:20]}_{tipo_registro}_{inicio}"

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

def _load_dataset(driver, path: Path, tipo_registro: str) -> None:
    total_s = total_e = total_p = 0

    with driver.session() as session:
        for chunk in _iter_csv(path):
            t = _t_sancoes(chunk, tipo_registro)

            if t["orgaos"]:
                _run_batches(session, Q_ORGAO_SANCIONADOR, t["orgaos"])
            if t["sancoes"]:
                _run_batches(session, Q_SANCAO, t["sancoes"])
                total_s += len(t["sancoes"])
            if t["com_orgao"]:
                _run_batches(session, Q_SANCAO_ORGAO, t["com_orgao"])
            if t["empresas"]:
                _run_batches(session, Q_EMPRESA_SANCAO, t["empresas"])
                total_e += len(t["empresas"])
            if t["pessoas"]:
                _run_batches(session, Q_PESSOA_SANCAO, t["pessoas"])
                total_p += len(t["pessoas"])

    log.info(f"    ✓ {tipo_registro}: sanções={total_s:,}  empresas={total_e:,}  pessoas={total_p:,}")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str):
    log.info(f"[sancoes_cgu] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    driver = _wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    for dataset, tipo in [("ceis", "CEIS"), ("cnep", "CNEP")]:
        path = DATA_DIR / f"{dataset}.csv"
        log.info(f"  [{tipo}]...")
        _load_dataset(driver, path, tipo)

    driver.close()
    log.info("[sancoes_cgu] Pipeline concluído")