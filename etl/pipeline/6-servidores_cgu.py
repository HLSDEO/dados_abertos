"""
Pipeline 6 - Servidores Públicos Federais → Neo4j

Lê os CSVs de data/servidores/{ano}/{mes:02d}/
  cadastro.csv    → :Servidor, :UnidadeGestora (merge com siafi)
  remuneracao.csv → :Remuneracao vinculada ao :Servidor

Nós criados/atualizados:
  (:Servidor)    — id_servidor como chave
  (:Pessoa)      — merge pelo CPF com candidatos TSE e CNPJ
  (:Remuneracao) — id_servidor + ano + mes como chave

Relacionamentos:
  (:Pessoa)-[:EH_SERVIDOR]->(:Servidor)
  (:Servidor)-[:LOTADO_EM]->(:UnidadeGestora)     ← pelo cd_uasg (siafi)
  (:Servidor)-[:EXERCE_EM]->(:UnidadeGestora)     ← org_exercicio
  (:Servidor)-[:TEM_REMUNERACAO]->(:Remuneracao)
  (:Servidor)-[:LOCALIZADO_EM]->(:Municipio)      ← municipio_exercicio + IBGE
"""

import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "servidores"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "10000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "CGU — Portal da Transparência",
    "fonte_url":  "https://portaldatransparencia.gov.br/servidores",
}


# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Servidor)    REQUIRE n.id_servidor IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Remuneracao) REQUIRE n.remuneracao_id IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX servidor_nome    IF NOT EXISTS FOR (s:Servidor) ON (s.nome)",
    "CREATE INDEX servidor_cpf     IF NOT EXISTS FOR (s:Servidor) ON (s.cpf)",
    "CREATE INDEX servidor_cargo   IF NOT EXISTS FOR (s:Servidor) ON (s.cargo)",
    "CREATE INDEX servidor_orgao   IF NOT EXISTS FOR (s:Servidor) ON (s.org_exercicio)",
    "CREATE INDEX remuneracao_ano  IF NOT EXISTS FOR (r:Remuneracao) ON (r.ano)",
]


# ── Queries ───────────────────────────────────────────────────────────────────

Q_SERVIDOR = """
UNWIND $rows AS r
MERGE (s:Servidor {id_servidor: r.id_servidor})
SET s.nome                  = r.nome,
    s.cpf                   = r.cpf,
    s.cargo                 = r.cargo,
    s.classe                = r.classe,
    s.org_lotacao           = r.org_lotacao,
    s.org_exercicio         = r.org_exercicio,
    s.uorg_lotacao          = r.uorg_lotacao,
    s.uorg_exercicio        = r.uorg_exercicio,
    s.situacao_vinculo      = r.situacao_vinculo,
    s.regime_juridico       = r.regime_juridico,
    s.tipo_vinculo          = r.tipo_vinculo,
    s.jornada_trabalho      = r.jornada_trabalho,
    s.data_ingresso_orgao   = r.data_ingresso_orgao,
    s.data_ingresso_servico = r.data_ingresso_servico,
    s.uf_exercicio          = r.uf_exercicio,
    s.municipio_exercicio   = r.municipio_exercicio,
    s.cd_uasg               = r.cd_uasg,
    s.categoria             = r.fonte_categoria,
    s.fonte_nome            = r.fonte_nome,
    s.fonte_url             = r.fonte_url
"""

# Merge Servidor → Pessoa pelo CPF
Q_SERVIDOR_PESSOA = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
WHERE r.cpf <> "" AND r.cpf <> "***.***.***-**"
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome      = r.nome,
                p.fonte_nome = r.fonte_nome
  ON MATCH  SET p.nome      = coalesce(p.nome, r.nome)
MERGE (p)-[:EH_SERVIDOR]->(s)
"""

# Vincula ao UASG (siafi) pelo cd_uasg
Q_SERVIDOR_UASG = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
WHERE r.cd_uasg <> ""
MATCH (u:UnidadeGestora {cd_uasg: r.cd_uasg})
MERGE (s)-[:LOTADO_EM]->(u)
"""

# Vincula ao município de exercício (pelo nome — IBGE já carregado)
Q_SERVIDOR_MUNICIPIO = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
WHERE r.municipio_exercicio <> ""
MATCH (m:Municipio)
WHERE toUpper(trim(m.nome)) = toUpper(trim(r.municipio_exercicio))
  AND (m.uf = r.uf_exercicio OR r.uf_exercicio = "")
MERGE (s)-[:LOCALIZADO_EM]->(m)
"""

Q_REMUNERACAO = """
UNWIND $rows AS r
MERGE (rem:Remuneracao {remuneracao_id: r.remuneracao_id})
SET rem.ano                    = toInteger(r.ano),
    rem.mes                    = toInteger(r.mes),
    rem.remuneracao_bruta      = toFloat(r.remuneracao_bruta),
    rem.remuneracao_liquida    = toFloat(r.remuneracao_liquida),
    rem.total_bruto            = toFloat(r.total_bruto),
    rem.irrf                   = toFloat(r.irrf),
    rem.pss_rpps               = toFloat(r.pss_rpps),
    rem.abate_teto             = toFloat(r.abate_teto),
    rem.gratificacao_natalina  = toFloat(r.gratificacao_natalina),
    rem.ferias                 = toFloat(r.ferias),
    rem.verbas_indenizatorias  = toFloat(r.verbas_indenizatorias),
    rem.outras_verbas          = toFloat(r.outras_verbas),
    rem.fonte_nome             = r.fonte_nome
WITH rem, r
MATCH (s:Servidor {id_servidor: r.id_servidor})
MERGE (s)-[:TEM_REMUNERACAO]->(rem)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _iter_csv(path: Path):
    if not path.exists():
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


def _safe_float(s: str) -> str:
    try:
        float(s or "0")
        return s or "0"
    except ValueError:
        return "0"


def _discover_periodos() -> list[tuple[int, int]]:
    """Descobre todos os pares (ano, mes) disponíveis em data/servidores/."""
    result = []
    if not DATA_DIR.exists():
        return result
    for ano_dir in sorted(DATA_DIR.iterdir()):
        if not ano_dir.is_dir() or not ano_dir.name.isdigit():
            continue
        for mes_dir in sorted(ano_dir.iterdir()):
            if not mes_dir.is_dir() or not mes_dir.name.isdigit():
                continue
            cad = mes_dir / "cadastro.csv"
            rem = mes_dir / "remuneracao.csv"
            if cad.exists() or rem.exists():
                result.append((int(ano_dir.name), int(mes_dir.name)))
    return result


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_cadastro(driver, path: Path) -> int:
    total = 0
    rows_uasg = []
    rows_mun  = []

    with driver.session() as session:
        for chunk in _iter_csv(path):
            _run_batches(session, Q_SERVIDOR, chunk)

            # filtra para vínculos
            for r in chunk:
                if r.get("cd_uasg"):
                    rows_uasg.append({"id_servidor": r["id_servidor"],
                                      "cd_uasg": r["cd_uasg"]})
                if r.get("municipio_exercicio"):
                    rows_mun.append({
                        "id_servidor":        r["id_servidor"],
                        "municipio_exercicio":r["municipio_exercicio"],
                        "uf_exercicio":       r.get("uf_exercicio", ""),
                    })

            # pessoas (CPF)
            com_cpf = [r for r in chunk
                       if r.get("cpf") and "***" not in r.get("cpf","")]
            if com_cpf:
                _run_batches(session, Q_SERVIDOR_PESSOA, com_cpf)

            total += len(chunk)

        if rows_uasg:
            _run_batches(session, Q_SERVIDOR_UASG, rows_uasg)
        if rows_mun:
            _run_batches(session, Q_SERVIDOR_MUNICIPIO, rows_mun)

    return total


def _load_remuneracao(driver, path: Path) -> int:
    total = 0
    with driver.session() as session:
        for chunk in _iter_csv(path):
            # cria chave composta única
            for r in chunk:
                r["remuneracao_id"] = f"{r.get('id_servidor','')}_{r.get('ano','')}_{r.get('mes','')}"
                for col in ("remuneracao_bruta", "remuneracao_liquida", "total_bruto",
                            "irrf", "pss_rpps", "abate_teto", "gratificacao_natalina",
                            "ferias", "verbas_indenizatorias", "outras_verbas"):
                    r[col] = _safe_float(r.get(col, "0"))
            _run_batches(session, Q_REMUNERACAO, chunk)
            total += len(chunk)
    return total


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str,
        anos: list[int] | None = None, meses: list[int] | None = None):
    """
    anos:  filtra anos específicos. None = todos disponíveis.
    meses: filtra meses específicos. None = todos disponíveis.
    """
    log.info(f"[servidores] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    periodos = _discover_periodos()
    if not periodos:
        log.warning(f"  Nenhum dado em {DATA_DIR} — rode 'download servidores' primeiro")
        return

    # aplica filtros
    if anos:
        periodos = [(a, m) for a, m in periodos if a in anos]
    if meses:
        periodos = [(a, m) for a, m in periodos if m in meses]

    if not periodos:
        log.warning("  Nenhum período encontrado com os filtros aplicados")
        return

    log.info(f"  Períodos a processar: {len(periodos)}  {periodos[:5]}{'...' if len(periodos)>5 else ''}")

    driver = _wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    for ano, mes in periodos:
        mes_dir = DATA_DIR / str(ano) / f"{mes:02d}"
        log.info(f"  === {ano}/{mes:02d} ===")

        cad_path = mes_dir / "cadastro.csv"
        rem_path = mes_dir / "remuneracao.csv"

        if cad_path.exists():
            log.info(f"  [cadastro]...")
            n = _load_cadastro(driver, cad_path)
            log.info(f"    ✓ {n:,} servidores")

        if rem_path.exists():
            log.info(f"  [remuneracao]...")
            n = _load_remuneracao(driver, rem_path)
            log.info(f"    ✓ {n:,} registros de remuneração")

    driver.close()
    log.info("[servidores] Pipeline concluído")