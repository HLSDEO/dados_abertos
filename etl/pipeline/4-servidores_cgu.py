"""
Pipeline 6 - Servidores Públicos Federais → Neo4j
"""
import csv
import logging
import os
from pathlib import Path

from neo4j import GraphDatabase
from pipeline.lib import wait_for_neo4j, run_batches, iter_csv, IngestionRun, setup_schema

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "servidores"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "10000"))
BATCH      = int(os.environ.get("NEO4J_BATCH", "500"))

FONTE = {
    "fonte_nome": "CGU — Portal da Transparência",
    "fonte_url":  "https://portaldatransparencia.gov.br/servidores",
}


# ── Constraints e índices ─────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Servidor)    REQUIRE n.id_servidor    IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Remuneracao) REQUIRE n.remuneracao_id IS UNIQUE",
]

Q_INDEXES = [
    "CREATE INDEX servidor_nome   IF NOT EXISTS FOR (s:Servidor) ON (s.nome)",
    "CREATE INDEX servidor_cpf    IF NOT EXISTS FOR (s:Servidor) ON (s.cpf)",
    "CREATE INDEX servidor_cargo  IF NOT EXISTS FOR (s:Servidor) ON (s.cargo)",
    "CREATE INDEX servidor_orgao  IF NOT EXISTS FOR (s:Servidor) ON (s.org_exercicio)",
    "CREATE INDEX remuneracao_ano IF NOT EXISTS FOR (r:Remuneracao) ON (r.ano)",
    # índices para Q_EXERCE_EM — evita full scan em 53k UnidadeGestora
    "CREATE INDEX uasg_no_uasg IF NOT EXISTS FOR (u:UnidadeGestora) ON (u.no_uasg)",
    "CREATE INDEX uasg_sg_uasg IF NOT EXISTS FOR (u:UnidadeGestora) ON (u.sg_uasg)",
    # índice para Q_SERVIDOR_MUNICIPIO — evita full scan com EXISTS traversal
    "CREATE INDEX municipio_uf IF NOT EXISTS FOR (m:Municipio) ON (m.uf)",
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

Q_SERVIDOR_PESSOA = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome      = r.nome,
                p.fonte_nome = r.fonte_nome
  ON MATCH  SET p.nome      = coalesce(p.nome, r.nome)
MERGE (p)-[:EH_SERVIDOR]->(s)
"""

Q_SERVIDOR_UASG = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
MATCH (u:UnidadeGestora {cd_uasg: r.cd_uasg})
MERGE (s)-[:LOTADO_EM]->(u)
"""

# Sem EXISTS traversal — usa só m.uf (indexado)
Q_SERVIDOR_MUNICIPIO = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
MATCH (m:Municipio)
WHERE toUpper(trim(m.nome)) = toUpper(trim(r.municipio_exercicio))
  AND (r.uf_exercicio = "" OR m.uf = r.uf_exercicio)
WITH s, m ORDER BY m.id IS NOT NULL DESC LIMIT 1
MERGE (s)-[:LOCALIZADO_EM]->(m)
"""

# EXERCE_EM usa no_uasg indexado (sem OR com sg_uasg para evitar full scan)
Q_EXERCE_EM = """
UNWIND $rows AS r
MATCH (s:Servidor {id_servidor: r.id_servidor})
MATCH (u:UnidadeGestora {no_uasg: r.org_exercicio})
MERGE (s)-[:EXERCE_EM]->(u)
"""

# Remuneracao: nó separado do relacionamento para reduzir contenção
Q_REMUNERACAO_NOS = """
UNWIND $rows AS r
MERGE (rem:Remuneracao {remuneracao_id: r.remuneracao_id})
SET rem.ano                   = toInteger(r.ano),
    rem.mes                   = toInteger(r.mes),
    rem.remuneracao_bruta     = toFloat(r.remuneracao_bruta),
    rem.remuneracao_liquida   = toFloat(r.remuneracao_liquida),
    rem.total_bruto           = toFloat(r.total_bruto),
    rem.irrf                  = toFloat(r.irrf),
    rem.pss_rpps              = toFloat(r.pss_rpps),
    rem.abate_teto            = toFloat(r.abate_teto),
    rem.gratificacao_natalina = toFloat(r.gratificacao_natalina),
    rem.ferias                = toFloat(r.ferias),
    rem.verbas_indenizatorias = toFloat(r.verbas_indenizatorias),
    rem.outras_verbas         = toFloat(r.outras_verbas),
    rem.fonte_nome            = r.fonte_nome
"""

Q_REMUNERACAO_REL = """
UNWIND $rows AS r
MATCH (s:Servidor   {id_servidor:   r.id_servidor})
MATCH (rem:Remuneracao {remuneracao_id: r.remuneracao_id})
MERGE (s)-[:TEM_REMUNERACAO]->(rem)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(s: str) -> str:
    try:
        float(s or "0")
        return s or "0"
    except ValueError:
        return "0"


def _discover_periodos() -> list[tuple[int, int]]:
    result = []
    if not DATA_DIR.exists():
        return result
    for ano_dir in sorted(DATA_DIR.iterdir()):
        if not ano_dir.is_dir() or not ano_dir.name.isdigit():
            continue
        for mes_dir in sorted(ano_dir.iterdir()):
            if not mes_dir.is_dir() or not mes_dir.name.isdigit():
                continue
            if (mes_dir / "cadastro.csv").exists() or (mes_dir / "remuneracao.csv").exists():
                result.append((int(ano_dir.name), int(mes_dir.name)))
    return result


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_cadastro(driver, path: Path) -> int:
    total = 0
    with driver.session() as session:
        for chunk in iter_csv(path):
            # 1. cria/atualiza nós :Servidor
            run_batches(session, Q_SERVIDOR, chunk)

            # 2. vincula :Pessoa pelo CPF (filtra mascarados em Python)
            com_cpf = [r for r in chunk
                       if r.get("cpf") and "***" not in r["cpf"] and len(r["cpf"]) >= 11]
            if com_cpf:
                run_batches(session, Q_SERVIDOR_PESSOA, com_cpf)

            # 3. vincula :UnidadeGestora (LOTADO_EM) — flush por chunk
            com_uasg = [{"id_servidor": r["id_servidor"], "cd_uasg": r["cd_uasg"]}
                        for r in chunk if r.get("cd_uasg")]
            if com_uasg:
                run_batches(session, Q_SERVIDOR_UASG, com_uasg)

            # 4. EXERCE_EM — só quando org_exercicio != org_lotacao
            exerce = [{"id_servidor": r["id_servidor"],
                       "org_exercicio": r["org_exercicio"]}
                      for r in chunk
                      if r.get("org_exercicio")
                      and r.get("org_exercicio") != r.get("org_lotacao")]
            if exerce:
                run_batches(session, Q_EXERCE_EM, exerce)

            # 5. LOCALIZADO_EM — flush por chunk
            com_mun = [{"id_servidor":         r["id_servidor"],
                        "municipio_exercicio":  r["municipio_exercicio"],
                        "uf_exercicio":         r.get("uf_exercicio", "")}
                       for r in chunk if r.get("municipio_exercicio")]
            if com_mun:
                run_batches(session, Q_SERVIDOR_MUNICIPIO, com_mun)

            total += len(chunk)

    return total


def _load_remuneracao(driver, path: Path) -> int:
    total = 0
    with driver.session() as session:
        for chunk in iter_csv(path):
            for r in chunk:
                r["remuneracao_id"] = (
                    f"{r.get('id_servidor','')}_{r.get('fonte_categoria','')} "
                    f"_{r.get('ano','')}_{r.get('mes','')}"
                )
                for col in ("remuneracao_bruta", "remuneracao_liquida", "total_bruto",
                            "irrf", "pss_rpps", "abate_teto", "gratificacao_natalina",
                            "ferias", "verbas_indenizatorias", "outras_verbas"):
                    r[col] = _safe_float(r.get(col, "0"))

            # nós primeiro, rel depois — menos contenção
            run_batches(session, Q_REMUNERACAO_NOS, chunk)
            run_batches(session, Q_REMUNERACAO_REL, chunk)
            total += len(chunk)
    return total


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str,
        anos: list[int] | None = None, meses: list[int] | None = None):
    log.info(f"[servidores] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}")

    periodos = _discover_periodos()
    if not periodos:
        log.warning(f"  Nenhum dado em {DATA_DIR} — rode 'download servidores' primeiro")
        return

    if anos:
        periodos = [(a, m) for a, m in periodos if a in anos]
    if meses:
        periodos = [(a, m) for a, m in periodos if m in meses]

    if not periodos:
        log.warning("  Nenhum período encontrado com os filtros aplicados")
        return

    log.info(f"  Períodos: {len(periodos)}  {periodos[:5]}{'...' if len(periodos)>5 else ''}")

    driver = wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    setup_schema(driver)

    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    with IngestionRun(driver, "servidores_cgu") as run_ctx:
        for ano, mes in periodos:
            mes_dir  = DATA_DIR / str(ano) / f"{mes:02d}"
            cad_path = mes_dir / "cadastro.csv"
            rem_path = mes_dir / "remuneracao.csv"
            log.info(f"  === {ano}/{mes:02d} ===")

            if cad_path.exists():
                log.info("  [cadastro]...")
                n = _load_cadastro(driver, cad_path)
                run_ctx.add(n)
                log.info(f"    ✓ {n:,} servidores")

            if rem_path.exists():
                log.info("  [remuneracao]...")
                n = _load_remuneracao(driver, rem_path)
                run_ctx.add(n)
                log.info(f"    ✓ {n:,} registros")

    driver.close()
    log.info("[servidores] Pipeline concluído")