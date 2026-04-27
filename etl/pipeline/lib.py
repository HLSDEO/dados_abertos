"""
etl/pipeline/lib.py — Utilitários compartilhados entre pipelines

  - wait_for_neo4j()   cria driver e aguarda Bolt disponível
  - run_batches()      executa Cypher UNWIND em lotes com retry em deadlock
  - iter_csv()         lê CSV em chunks sem carregar tudo em memória
  - setup_schema()     cria constraints, índices e fulltext index
  - IngestionRun       context manager de auditoria por execução de pipeline
"""

import csv
import hashlib
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "20000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))


# ── Conexão ───────────────────────────────────────────────────────────────────

def wait_for_neo4j(uri: str, user: str, password: str,
                   retries: int = 20, delay: float = 5.0,
                   max_pool: int = 8):
    """Cria driver Neo4j e aguarda Bolt ficar disponível."""
    from neo4j.exceptions import ServiceUnavailable
    driver = GraphDatabase.driver(uri, auth=(user, password),
                                  max_connection_pool_size=max_pool)
    for attempt in range(1, retries + 1):
        try:
            with driver.session() as s:
                s.run("RETURN 1")
            log.info(f"  Neo4j pronto (tentativa {attempt})")
            return driver
        except ServiceUnavailable:
            log.warning(f"  Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)
    raise RuntimeError(f"Neo4j não ficou disponível após {retries} tentativas")


# ── Escrita em lotes ──────────────────────────────────────────────────────────

def run_batches(session, query: str, rows: list[dict],
                extra_params: dict | None = None,
                batch: int = BATCH,
                retries: int = 5) -> None:
    """Executa query em batches com retry automático em DeadlockDetected."""
    from neo4j.exceptions import TransientError
    params = extra_params or {}
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        for attempt in range(1, retries + 1):
            try:
                with session.begin_transaction() as tx:
                    tx.run(query, rows=chunk, **params)
                    tx.commit()
                break
            except TransientError as exc:
                if "DeadlockDetected" in str(exc) and attempt < retries:
                    wait = attempt * 0.5 + random.uniform(0, 0.1 * attempt)
                    log.warning(f"    Deadlock — retry {attempt}/{retries} em {wait:.2f}s")
                    time.sleep(wait)
                else:
                    raise


# ── Leitura CSV ───────────────────────────────────────────────────────────────

def iter_csv(path: Path, chunk_size: int = CHUNK_SIZE,
             encoding: str = "utf-8-sig", delimiter: str = ","):
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
            delimiter = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        chunk: list[dict] = []
        for row in reader:
            row = {k: (v or "").strip() for k, v in row.items()
                   if k is not None}
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                total += len(chunk)
                chunk = []
        if chunk:
            total += len(chunk)
            yield chunk
    log.info(f"    {path.name}: {total:,} linhas lidas")


# ── Schema (constraints + índices + fulltext) ─────────────────────────────────

_SCHEMA_QUERIES = [
    # IngestionRun
    "CREATE CONSTRAINT ingestion_run_id IF NOT EXISTS FOR (r:IngestionRun) REQUIRE r.run_id IS UNIQUE",
    "CREATE INDEX ingestion_run_source  IF NOT EXISTS FOR (r:IngestionRun) ON (r.source_id)",
    "CREATE INDEX ingestion_run_status  IF NOT EXISTS FOR (r:IngestionRun) ON (r.status)",
    "CREATE INDEX ingestion_run_started IF NOT EXISTS FOR (r:IngestionRun) ON (r.started_at)",
    # Partner (identidade parcial)
    "CREATE CONSTRAINT partner_id IF NOT EXISTS FOR (p:Partner) REQUIRE p.partner_id IS UNIQUE",
    "CREATE INDEX partner_name           IF NOT EXISTS FOR (p:Partner) ON (p.nome)",
    "CREATE INDEX partner_doc_partial    IF NOT EXISTS FOR (p:Partner) ON (p.doc_partial)",
    "CREATE INDEX partner_nome_doc       IF NOT EXISTS FOR (p:Partner) ON (p.nome, p.doc_partial)",
    # Índices base para endpoints da API (lookup + filtros frequentes)
    "CREATE INDEX api_pessoa_cpf         IF NOT EXISTS FOR (p:Pessoa) ON (p.cpf)",
    "CREATE INDEX api_pessoa_nome        IF NOT EXISTS FOR (p:Pessoa) ON (p.nome)",
    "CREATE INDEX api_empresa_cnpj_basico IF NOT EXISTS FOR (e:Empresa) ON (e.cnpj_basico)",
    "CREATE INDEX api_empresa_cnpj       IF NOT EXISTS FOR (e:Empresa) ON (e.cnpj)",
    "CREATE INDEX api_empresa_uf         IF NOT EXISTS FOR (e:Empresa) ON (e.uf)",
    "CREATE INDEX api_parlamentar_id     IF NOT EXISTS FOR (p:Parlamentar) ON (p.id)",
    "CREATE INDEX api_parlamentar_id_camara IF NOT EXISTS FOR (p:Parlamentar) ON (p.id_camara)",
    "CREATE INDEX api_parlamentar_cpf    IF NOT EXISTS FOR (p:Parlamentar) ON (p.cpf)",
    "CREATE INDEX api_parlamentar_codigo_autor IF NOT EXISTS FOR (p:Parlamentar) ON (p.codigo_autor)",
    "CREATE INDEX api_parlamentar_nome_autor IF NOT EXISTS FOR (p:Parlamentar) ON (p.nome_autor)",
    "CREATE INDEX api_municipio_uf       IF NOT EXISTS FOR (m:Municipio) ON (m.uf)",
    "CREATE INDEX api_municipio_sigla_uf IF NOT EXISTS FOR (m:Municipio) ON (m.sigla_uf)",
    "CREATE INDEX api_municipio_codigo_ibge IF NOT EXISTS FOR (m:Municipio) ON (m.codigo_ibge)",
    "CREATE INDEX api_emenda_ano         IF NOT EXISTS FOR (e:Emenda) ON (e.ano)",
    "CREATE INDEX api_emenda_ano_emenda  IF NOT EXISTS FOR (e:Emenda) ON (e.ano_emenda)",
    "CREATE INDEX api_sancao_data_inicio IF NOT EXISTS FOR (s:Sancao) ON (s.data_inicio)",
    "CREATE INDEX api_sancao_data_inicio_sancao IF NOT EXISTS FOR (s:Sancao) ON (s.data_inicio_sancao)",
]

# Fulltext criado separado — sintaxe diferente das demais DDLs
_FULLTEXT_QUERY = """
CREATE FULLTEXT INDEX entidade_busca IF NOT EXISTS
FOR (n:Pessoa|Empresa|Parceiro|Servidor|Parlamentar|Emenda|Contrato|Sancao|Municipio|Estado|Partido|Eleicao)
ON EACH [n.nome, n.razao_social, n.cpf, n.cnpj, n.doc_partial, n.nome_urna,
         n.nome_autor, n.objeto, n.codigo_emenda, n.numero_contrato,
         n.motivo_sancao, n.nome_favorecido]
"""


def setup_schema(driver) -> None:
    """Cria constraints, índices e fulltext index de forma idempotente."""
    with driver.session() as session:
        for q in _SCHEMA_QUERIES:
            session.run(q)
        try:
            session.run(_FULLTEXT_QUERY)
        except Exception as exc:
            # Fulltext já existe ou versão sem suporte — não é bloqueante
            log.debug(f"  fulltext index: {exc}")
    log.info("  Schema aplicado (constraints + índices + fulltext)")


# ── IngestionRun ──────────────────────────────────────────────────────────────

class IngestionRun:
    """
    Context manager de auditoria por execução de pipeline.

    Uso:
        with IngestionRun(driver, "cnpj") as run:
            for chunk in iter_csv(path):
                ...
                run.add(len(chunk))   # conta linhas carregadas
    """

    def __init__(self, driver, source_id: str) -> None:
        self._driver    = driver
        self.source_id  = source_id
        self.run_id     = f"{source_id}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        self.rows_in    = 0
        self.rows_out   = 0
        self._started   = ""

    # chamado externamente para acumular contagens
    def add(self, rows_in: int = 0, rows_out: int | None = None) -> None:
        self.rows_in  += rows_in
        self.rows_out += rows_out if rows_out is not None else rows_in

    def __enter__(self):
        self._started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._upsert("running")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        finished = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if exc_type is None:
            self._upsert("loaded", finished_at=finished)
            log.info(
                f"  [IngestionRun] {self.source_id}  status=loaded  "
                f"rows_in={self.rows_in:,}  rows_out={self.rows_out:,}"
            )
        else:
            self._upsert("quality_fail", finished_at=finished,
                         error=str(exc_val)[:1000])
            log.error(
                f"  [IngestionRun] {self.source_id}  status=quality_fail  "
                f"error={str(exc_val)[:120]}"
            )
        return False  # não suprime exceção

    def _upsert(self, status: str, finished_at: str | None = None,
                error: str | None = None) -> None:
        q = (
            "MERGE (r:IngestionRun {run_id: $run_id}) "
            "SET r.source_id   = $source_id, "
            "    r.status      = $status, "
            "    r.started_at  = coalesce($started_at,  r.started_at), "
            "    r.finished_at = coalesce($finished_at, r.finished_at), "
            "    r.error       = coalesce($error,       r.error), "
            "    r.rows_in     = $rows_in, "
            "    r.rows_out    = $rows_out"
        )
        try:
            with self._driver.session() as s:
                s.run(q, run_id=self._run_id_safe(), source_id=self.source_id,
                      status=status, started_at=self._started,
                      finished_at=finished_at, error=error,
                      rows_in=self.rows_in, rows_out=self.rows_out)
        except Exception as exc:
            log.warning(f"  IngestionRun upsert falhou: {exc}")

    def _run_id_safe(self) -> str:
        return self.run_id


# ── Utilitários de documento ──────────────────────────────────────────────────

def strip_doc(s: str) -> str:
    """Extrai apenas dígitos de um documento."""
    return "".join(c for c in (s or "") if c.isdigit())


def classify_doc(doc: str | None) -> str:
    """
    Classifica um documento brasileiro.
    Retorna: 'cpf_valid' | 'cpf_partial' | 'cnpj_valid' | 'invalid'
    """
    raw    = (doc or "").strip()
    digits = strip_doc(raw)
    masked = "*" in raw

    if masked and len(digits) >= 6:
        return "cpf_partial"
    if not masked and len(digits) == 11:
        return "cpf_valid"
    if not masked and len(digits) in (14, 8):
        return "cnpj_valid"
    return "invalid"


def make_partner_id(nome: str, doc_raw: str, tipo: str, fonte: str = "rfb") -> str:
    """ID estável para identidades parciais/inválidas (SHA-256 truncado a 16 hex)."""
    raw = f"{nome.strip().upper()}|{strip_doc(doc_raw)}|{doc_raw.strip()}|{tipo}|{fonte}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
