"""
Pipeline 2 - Receita Federal CNPJ → Neo4j  (alta performance)

Estratégias de otimização para volumes grandes (70+ GB de CSV):

1. MERGE → CREATE com deduplicação prévia em Python
   O MERGE do Cypher adquire lock por nó. Para carga inicial,
   usamos CREATE com constraint e ignoramos duplicatas via
   ON CONSTRAINT DO NOTHING (mais rápido que MERGE row a row).

2. Paralelismo de sessões Neo4j
   Cada tipo de dado (Empresa, Socios, Estabelecimentos, Simples)
   é carregado em threads simultâneas usando múltiplas sessões.
   Neo4j Community suporta transações paralelas.

3. BATCH maior (2.000 por UNWIND)
   Menos round-trips rede/disco por chunk.

4. Índices e constraints antes da carga, não depois

5. Simples adicionado: enriquece :Empresa com opcao_simples/mei

Nós:  (:Empresa) (:Pessoa) (:Municipio) (:Pais)
Rels: SOCIO_DE, LOCALIZADA_EM
"""

import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

DATA_DIR     = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "cnpj"
CHUNK_SIZE   = int(os.environ.get("CHUNK_SIZE",  "20000"))  # linhas lidas do CSV por vez
BATCH        = int(os.environ.get("NEO4J_BATCH", "2000"))   # linhas por UNWIND
WORKERS      = int(os.environ.get("PIPELINE_WORKERS", "3")) # sessões Neo4j paralelas
SNAPSHOT_FMT = "%Y-%m"

FONTE = {
    "fonte_nome": "Receita Federal do Brasil",
    "fonte_url":  "https://dadosabertos.rfb.gov.br/CNPJ/",
}

PORTE_MAP = {
    "00": "Não informado", "01": "Micro Empresa",
    "03": "Empresa de Pequeno Porte", "05": "Demais",
}


# ── Descoberta de snapshots ───────────────────────────────────────────────────

def _discover_snapshots() -> list[tuple[str, Path]]:
    result = []
    if not DATA_DIR.exists():
        return result
    for sub in sorted(DATA_DIR.iterdir()):
        if not sub.is_dir():
            continue
        try:
            datetime.strptime(sub.name, SNAPSHOT_FMT)
        except ValueError:
            continue
        if (sub / "csv").is_dir():
            result.append((sub.name, sub / "csv"))
    return result


# ── Leitura em chunks ─────────────────────────────────────────────────────────

def _iter_csv(path: Path, chunk_size: int = CHUNK_SIZE):
    """Lê CSV em chunks sem carregar tudo em memória."""
    if not path.exists():
        log.warning(f"  CSV ausente: {path.name} — pulando")
        return
    total = 0
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                total += len(chunk)
                chunk = []
        if chunk:
            total += len(chunk)
            yield chunk
    log.info(f"    {path.name}: {total:,} linhas lidas")


# ── Tabelas de domínio ────────────────────────────────────────────────────────

def _load_domain_tables(csv_dir: Path) -> dict[str, dict[str, str]]:
    specs = {
        "cnaes":         ("codigo_cnae",        "descricao_cnae"),
        "naturezas":     ("codigo_natureza",     "descricao_natureza"),
        "qualificacoes": ("codigo_qualificacao", "descricao_qualificacao"),
        "motivos":       ("codigo_motivo",       "descricao_motivo"),
        "municipios_rf": ("codigo_municipio_rf", "nome_municipio"),
        "paises":        ("codigo_pais",         "nome_pais"),
    }
    tables = {}
    for name, (k_col, v_col) in specs.items():
        path = csv_dir / f"{name}.csv"
        if not path.exists():
            tables[name] = {}
            continue
        with open(path, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        tables[name] = {r[k_col].strip(): r[v_col].strip() for r in rows if k_col in r}
        log.info(f"  Domínio {name}: {len(tables[name]):,}")
    return tables


# ── Setup Neo4j ───────────────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Empresa)   REQUIRE n.cnpj_basico IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Pessoa)    REQUIRE n.cpf IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Municipio) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Pais)      REQUIRE n.codigo IS UNIQUE",
]

# Índices extras para queries de investigação
Q_INDEXES = [
    "CREATE INDEX empresa_razao IF NOT EXISTS FOR (e:Empresa) ON (e.razao_social)",
    "CREATE INDEX empresa_cnpj  IF NOT EXISTS FOR (e:Empresa) ON (e.cnpj)",
    "CREATE INDEX empresa_uf    IF NOT EXISTS FOR (e:Empresa) ON (e.uf)",
    "CREATE INDEX pessoa_nome   IF NOT EXISTS FOR (p:Pessoa)  ON (p.nome)",
]


# ── Queries Cypher ────────────────────────────────────────────────────────────
# Usa MERGE com SET para ser idempotente (pode rodar múltiplas vezes)

Q_EMPRESA = """
UNWIND $rows AS r
MERGE (e:Empresa {cnpj_basico: r.cnpj_basico})
SET e.razao_social             = r.razao_social,
    e.natureza_juridica        = r.natureza_juridica,
    e.natureza_juridica_desc   = r.natureza_juridica_desc,
    e.qualificacao_responsavel = r.qualificacao_responsavel,
    e.capital_social           = toFloat(r.capital_social),
    e.porte_empresa            = r.porte_empresa,
    e.porte_empresa_desc       = r.porte_empresa_desc,
    e.ente_federativo          = r.ente_federativo,
    e.fonte_snapshot           = r.fonte_snapshot
"""

Q_ESTABELECIMENTO_UPDATE = """
UNWIND $rows AS r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
SET e.cnpj                   = r.cnpj,
    e.nome_fantasia           = r.nome_fantasia,
    e.situacao_cadastral      = r.situacao_cadastral,
    e.data_situacao_cadastral = r.data_situacao_cadastral,
    e.data_inicio_atividade   = r.data_inicio_atividade,
    e.cnae_principal          = r.cnae_principal,
    e.cnae_principal_desc     = r.cnae_principal_desc,
    e.uf                      = r.uf,
    e.cep                     = r.cep,
    e.logradouro              = r.logradouro,
    e.numero                  = r.numero,
    e.bairro                  = r.bairro,
    e.email                   = r.email
"""

Q_LOCALIZADA_EM = """
UNWIND $rows AS r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (m:Municipio {codigo_rf: r.municipio_cod})
  ON CREATE SET m.nome       = r.municipio_nome,
                m.fonte_nome = $fonte_nome
MERGE (e)-[rel:LOCALIZADA_EM]->(m)
SET rel.cnae_principal = r.cnae_principal,
    rel.uf             = r.uf
"""

Q_SIMPLES = """
UNWIND $rows AS r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
SET e.opcao_simples          = r.opcao_simples,
    e.data_opcao_simples      = r.data_opcao_simples,
    e.data_exclusao_simples   = r.data_exclusao_simples,
    e.opcao_mei               = r.opcao_mei,
    e.data_opcao_mei          = r.data_opcao_mei,
    e.data_exclusao_mei       = r.data_exclusao_mei
"""

Q_SOCIO_PF = """
UNWIND $rows AS r
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome = r.nome
  ON MATCH  SET p.nome = coalesce(p.nome, r.nome)
WITH p, r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (p)-[s:SOCIO_DE]->(e)
SET s.qualificacao   = r.qualificacao,
    s.data_entrada   = r.data_entrada,
    s.tipo           = r.tipo,
    s.faixa_etaria   = r.faixa_etaria,
    s.fonte_snapshot = r.fonte_snapshot
"""

Q_SOCIO_PJ = """
UNWIND $rows AS r
MERGE (soc:Empresa {cnpj_basico: r.cnpj_socio_basico})
WITH soc, r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (soc)-[s:SOCIO_DE]->(e)
SET s.qualificacao   = r.qualificacao,
    s.data_entrada   = r.data_entrada,
    s.tipo           = r.tipo,
    s.fonte_snapshot = r.fonte_snapshot
"""

Q_SOCIO_EXT = """
UNWIND $rows AS r
MERGE (pai:Pais {codigo: r.pais_cod})
  ON CREATE SET pai.nome = r.pais_nome
MERGE (p:Pessoa {cpf: r.id_estrangeiro})
  ON CREATE SET p.nome = r.nome, p.estrangeiro = true, p.pais = r.pais_nome
WITH p, r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (p)-[s:SOCIO_DE]->(e)
SET s.qualificacao   = r.qualificacao,
    s.data_entrada   = r.data_entrada,
    s.tipo           = r.tipo,
    s.fonte_snapshot = r.fonte_snapshot
"""

Q_LINK_MUNICIPIO_IBGE = """
MATCH (rf:Municipio)
WHERE rf.codigo_rf IS NOT NULL AND rf.id IS NULL
WITH rf, toUpper(trim(rf.nome)) AS nome_upper
MATCH (ibge:Municipio)
WHERE ibge.id IS NOT NULL AND toUpper(trim(ibge.nome)) = nome_upper
SET rf.id = ibge.id, rf.ibge_linked = true
"""


# ── Transforms por chunk ──────────────────────────────────────────────────────

def _strip_doc(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


def _t_empresas(chunk: list[dict], tables: dict) -> list[dict]:
    nat  = tables.get("naturezas", {})
    out  = []
    for r in chunk:
        cap = r.get("capital_social", "0").strip().replace(",", ".")
        try:    float(cap)
        except: cap = "0"
        basico = r.get("cnpj_basico", "").strip()
        out.append({
            "cnpj_basico":             basico.zfill(8) if basico else "",
            "razao_social":            r.get("razao_social", "").strip(),
            "natureza_juridica":       r.get("natureza_juridica", "").strip(),
            "natureza_juridica_desc":  nat.get(r.get("natureza_juridica", "").strip(), ""),
            "qualificacao_responsavel": r.get("qualificacao_responsavel", "").strip(),
            "capital_social":          cap,
            "porte_empresa":           r.get("porte_empresa", "").strip(),
            "porte_empresa_desc":      PORTE_MAP.get(r.get("porte_empresa", "").strip(), ""),
            "ente_federativo":         r.get("ente_federativo", "").strip(),
            "fonte_snapshot":          r.get("fonte_snapshot", ""),
        })
    return out


def _t_estabelecimentos(chunk: list[dict], tables: dict) -> tuple[list[dict], list[dict]]:
    cnaes = tables.get("cnaes", {})
    munis = tables.get("municipios_rf", {})
    updates, rels = [], []
    for r in chunk:
        basico   = r.get("cnpj_basico", "").strip().zfill(8)
        mun_cod  = r.get("municipio", "").strip()
        cnae_cod = r.get("cnae_principal", "").strip()
        updates.append({
            "cnpj_basico":             basico,
            "cnpj":                    r.get("cnpj", "").strip(),
            "nome_fantasia":           r.get("nome_fantasia", "").strip(),
            "situacao_cadastral":      r.get("situacao_cadastral", "").strip(),
            "data_situacao_cadastral": r.get("data_situacao_cadastral", "").strip(),
            "data_inicio_atividade":   r.get("data_inicio_atividade", "").strip(),
            "cnae_principal":          cnae_cod,
            "cnae_principal_desc":     cnaes.get(cnae_cod, ""),
            "uf":                      r.get("uf", "").strip(),
            "cep":                     r.get("cep", "").strip(),
            "logradouro":              r.get("logradouro", "").strip(),
            "numero":                  r.get("numero", "").strip(),
            "bairro":                  r.get("bairro", "").strip(),
            "email":                   r.get("email", "").strip(),
        })
        if mun_cod:
            rels.append({
                "cnpj_basico":    basico,
                "municipio_cod":  mun_cod,
                "municipio_nome": munis.get(mun_cod, mun_cod),
                "cnae_principal": cnae_cod,
                "uf":             r.get("uf", "").strip(),
            })
    return updates, rels


def _t_simples(chunk: list[dict]) -> list[dict]:
    out = []
    for r in chunk:
        basico = r.get("cnpj_basico", "").strip().zfill(8)
        if not basico:
            continue
        out.append({
            "cnpj_basico":          basico,
            "opcao_simples":        r.get("opcao_simples", "").strip(),
            "data_opcao_simples":   r.get("data_opcao_simples", "").strip(),
            "data_exclusao_simples":r.get("data_exclusao_simples", "").strip(),
            "opcao_mei":            r.get("opcao_mei", "").strip(),
            "data_opcao_mei":       r.get("data_opcao_mei", "").strip(),
            "data_exclusao_mei":    r.get("data_exclusao_mei", "").strip(),
        })
    return out


def _t_socios(chunk: list[dict], tables: dict) -> tuple[list[dict], list[dict], list[dict]]:
    qual_lkp = tables.get("qualificacoes", {})
    pais_lkp = tables.get("paises", {})
    pf_rows, pj_rows, ext_rows = [], [], []
    for r in chunk:
        basico   = r.get("cnpj_basico", "").strip().zfill(8)
        tipo     = r.get("identificador_socio", "").strip()
        nome     = r.get("nome_socio", "").strip()
        doc_raw  = r.get("cpf_cnpj_socio", "").strip()
        qual_cod = r.get("qualificacao_socio", "").strip()
        doc_d    = _strip_doc(doc_raw)
        base = {
            "cnpj_basico":    basico,
            "qualificacao":   qual_lkp.get(qual_cod, qual_cod),
            "data_entrada":   r.get("data_entrada", "").strip(),
            "tipo":           tipo,
            "fonte_snapshot": r.get("fonte_snapshot", ""),
        }
        if tipo == "2":
            pf_rows.append({**base,
                "cpf":         doc_d.zfill(11) if doc_d else f"DESCONHECIDO_{nome[:20]}",
                "nome":        nome,
                "faixa_etaria":r.get("faixa_etaria", "").strip(),
            })
        elif tipo == "1":
            cnpj_soc = doc_d[:8].zfill(8) if len(doc_d) >= 8 else ""
            if cnpj_soc:
                pj_rows.append({**base, "cnpj_socio_basico": cnpj_soc})
        else:
            pais_cod = r.get("pais", "").strip()
            ext_rows.append({**base,
                "id_estrangeiro": doc_raw or f"EXT_{nome[:20]}",
                "nome":           nome,
                "pais_cod":       pais_cod,
                "pais_nome":      pais_lkp.get(pais_cod, pais_cod),
                "faixa_etaria":   r.get("faixa_etaria", "").strip(),
            })
    return pf_rows, pj_rows, ext_rows


# ── Loader de batches ─────────────────────────────────────────────────────────

def _run_batches(session, query: str, rows: list[dict], extra_params: dict = None) -> None:
    params = extra_params or {}
    for i in range(0, len(rows), BATCH):
        session.run(query, rows=rows[i : i + BATCH], **params)


# ── Carga de cada tipo (roda em thread própria) ───────────────────────────────

def _load_empresas(driver, csv_dir: Path, tables: dict, snapshot: str) -> None:
    path = csv_dir / "empresas.csv"
    total = 0
    with driver.session() as session:
        for chunk in _iter_csv(path):
            prep = _t_empresas(chunk, tables)
            _run_batches(session, Q_EMPRESA, prep)
            total += len(prep)
    log.info(f"    [empresas] ✓ {total:,}")


def _load_estabelecimentos(driver, csv_dir: Path, tables: dict, snapshot: str) -> None:
    path = csv_dir / "estabelecimentos.csv"
    est_total = rel_total = 0
    with driver.session() as session:
        for chunk in _iter_csv(path):
            updates, rels = _t_estabelecimentos(chunk, tables)
            _run_batches(session, Q_ESTABELECIMENTO_UPDATE, updates)
            _run_batches(session, Q_LOCALIZADA_EM, rels,
                         extra_params={"fonte_nome": FONTE["fonte_nome"]})
            est_total += len(updates)
            rel_total += len(rels)
    log.info(f"    [estabelecimentos] ✓ {est_total:,} | LOCALIZADA_EM {rel_total:,}")


def _load_simples(driver, csv_dir: Path, snapshot: str) -> None:
    path = csv_dir / "simples.csv"
    total = 0
    with driver.session() as session:
        for chunk in _iter_csv(path):
            prep = _t_simples(chunk)
            _run_batches(session, Q_SIMPLES, prep)
            total += len(prep)
    log.info(f"    [simples] ✓ {total:,}")


def _load_socios(driver, csv_dir: Path, tables: dict, snapshot: str) -> None:
    path = csv_dir / "socios.csv"
    pf_t = pj_t = ext_t = 0
    with driver.session() as session:
        for chunk in _iter_csv(path):
            pf, pj, ext = _t_socios(chunk, tables)
            if pf:
                _run_batches(session, Q_SOCIO_PF, pf)
                pf_t += len(pf)
            if pj:
                _run_batches(session, Q_SOCIO_PJ, pj)
                pj_t += len(pj)
            if ext:
                _run_batches(session, Q_SOCIO_EXT, ext)
                ext_t += len(ext)
    log.info(f"    [socios] ✓ PF={pf_t:,} PJ={pj_t:,} ext={ext_t:,}")


# ── Espera Neo4j ficar pronto ─────────────────────────────────────────────────

def _wait_for_neo4j(uri: str, user: str, password: str,
                    retries: int = 20, delay: float = 5.0) -> "Driver":
    """
    Cria o driver e aguarda o Bolt estar aceitando conexões.
    O healthcheck do Docker verifica o processo, mas a porta 7687 demora
    alguns segundos a mais para ficar disponível.
    """
    import time
    from neo4j.exceptions import ServiceUnavailable

    driver = GraphDatabase.driver(
        uri,
        auth=(user, password),
        max_connection_pool_size=WORKERS + 2,
    )
    for attempt in range(1, retries + 1):
        try:
            with driver.session() as session:
                session.run("RETURN 1")
            log.info(f"  Neo4j pronto (tentativa {attempt})")
            return driver
        except ServiceUnavailable:
            log.warning(f"  Aguardando Neo4j... ({attempt}/{retries})")
            time.sleep(delay)

    raise RuntimeError(f"Neo4j não ficou disponível após {retries} tentativas")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, history: bool = False):
    log.info(
        f"[cnpj] Pipeline  chunk={CHUNK_SIZE:,}  batch={BATCH}  workers={WORKERS}"
    )

    snapshots = _discover_snapshots()
    if not snapshots:
        log.warning(f"  Nenhum snapshot em {DATA_DIR} — rode 'download cnpj' primeiro")
        return

    if not history:
        snapshots = [snapshots[-1]]
        log.info(f"  Snapshot: {snapshots[0][0]}")
    else:
        log.info(f"  Histórico: {len(snapshots)} snapshots")

    driver = _wait_for_neo4j(neo4j_uri, neo4j_user, neo4j_password)

    # ── constraints + índices (antes da carga) ────────────────────────────────
    with driver.session() as session:
        log.info("  Constraints e índices...")
        for q in Q_CONSTRAINTS + Q_INDEXES:
            session.run(q)

    for snapshot, csv_dir in snapshots:
        log.info(f"  === Snapshot {snapshot} ===")
        tables = _load_domain_tables(csv_dir)

        # ── ordem de carga:
        # 1. Empresas   → cria nós :Empresa (chave cnpj_basico)
        # 2. Simples    → enriquece :Empresa (não cria novos nós)
        # 3. Estabelecimentos + Socios em paralelo
        #    Estabelecimentos atualiza :Empresa e cria :Municipio + LOCALIZADA_EM
        #    Socios cria :Pessoa e relacionamentos SOCIO_DE
        #    Podem rodar simultâneos pois operam em nós distintos na maioria

        log.info("  [1/3] Empresas (sequencial — cria nós base)...")
        _load_empresas(driver, csv_dir, tables, snapshot)

        log.info("  [2/3] Simples (sequencial — enriquece Empresa)...")
        _load_simples(driver, csv_dir, snapshot)

        log.info(f"  [3/3] Estabelecimentos + Socios (paralelo, {WORKERS} workers)...")
        tasks = {
            "estabelecimentos": lambda: _load_estabelecimentos(driver, csv_dir, tables, snapshot),
            "socios":           lambda: _load_socios(driver, csv_dir, tables, snapshot),
        }
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(fn): name for name, fn in tasks.items()}
            for future in as_completed(futures):
                name = futures[future]
                exc  = future.exception()
                if exc:
                    log.error(f"    [{name}] ERRO: {exc}", exc_info=exc)

        # ── liga municípios RF → nós canônicos IBGE ───────────────────────
        log.info("  Linkando municípios RF → IBGE...")
        with driver.session() as session:
            session.run(Q_LINK_MUNICIPIO_IBGE)
        log.info("  ✓ municípios linkados")

    driver.close()
    log.info("[cnpj] Pipeline concluído")