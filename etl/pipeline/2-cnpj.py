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
6. Identidade parcial: sócios com CPF mascarado → nó :Partner
   em vez de `:Pessoa {cpf: "DESCONHECIDO_..."}` ou descarte silencioso.
   partner_id = SHA-256(nome|doc_digits|doc_raw|tipo|rfb)[:16]
Nós:  (:Empresa) (:Pessoa) (:Partner) (:Municipio) (:Pais)
Rels: SOCIO_DE, LOCALIZADA_EM
"""
import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import random
from neo4j import GraphDatabase
from pipeline.lib import classify_doc, make_partner_id, IngestionRun, apply_schema, setup_schema
log = logging.getLogger(__name__)
DATA_DIR     = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "cnpj"
CHUNK_SIZE   = int(os.environ.get("CHUNK_SIZE",  "50000"))  # linhas lidas do CSV por vez
BATCH        = int(os.environ.get("NEO4J_BATCH", "5000"))   # linhas por UNWIND
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
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Partner)   REQUIRE n.partner_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Municipio) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Pais)      REQUIRE n.codigo IS UNIQUE",
    # constraint em codigo_rf torna o MERGE em Q_ESTABELECIMENTO_UPDATE O(1) em vez de full scan
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Municipio) REQUIRE n.codigo_rf IS UNIQUE",
]
Q_INDEXES = [
    "CREATE INDEX empresa_razao     IF NOT EXISTS FOR (e:Empresa) ON (e.razao_social)",
    "CREATE INDEX empresa_cnpj      IF NOT EXISTS FOR (e:Empresa) ON (e.cnpj)",
    "CREATE INDEX empresa_uf        IF NOT EXISTS FOR (e:Empresa) ON (e.uf)",
    "CREATE INDEX empresa_sit       IF NOT EXISTS FOR (e:Empresa) ON (e.situacao_cadastral)",
    "CREATE INDEX pessoa_nome       IF NOT EXISTS FOR (p:Pessoa)  ON (p.nome)",
    "CREATE INDEX partner_nome      IF NOT EXISTS FOR (p:Partner) ON (p.nome)",
    "CREATE INDEX partner_doc       IF NOT EXISTS FOR (p:Partner) ON (p.doc_partial)",
    "CREATE INDEX partner_nome_doc  IF NOT EXISTS FOR (p:Partner) ON (p.nome, p.doc_partial)",
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
# Query unificada: SET + MERGE Municipio em uma só transação
# Elimina metade dos round-trips (era 2 queries por chunk, agora é 1)
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
WITH e, r
WHERE r.municipio_cod IS NOT NULL AND r.municipio_cod <> ""
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

# Sócio PF com CPF mascarado/inválido → identidade parcial
Q_PARTNER = """
UNWIND $rows AS r
MERGE (p:Partner {partner_id: r.partner_id})
SET p.nome          = r.nome,
    p.doc_raw       = r.doc_raw,
    p.doc_partial   = r.doc_partial,
    p.doc_tipo      = r.doc_tipo,
    p.tipo_socio    = r.tipo_socio,
    p.qualidade_id  = r.qualidade_id,
    p.fonte         = r.fonte
"""

Q_PARTNER_SOCIO_DE = """
UNWIND $rows AS r
MATCH  (p:Partner  {partner_id:  r.partner_id})
MATCH  (e:Empresa  {cnpj_basico: r.cnpj_basico})
MERGE  (p)-[s:SOCIO_DE]->(e)
SET s.qualificacao   = r.qualificacao,
    s.data_entrada   = r.data_entrada,
    s.tipo           = r.tipo,
    s.fonte_snapshot = r.fonte_snapshot
"""
# ── CORREÇÃO: usa MESMO_QUE em vez de SET rf.id = ibge.id ────────────────────
# SET rf.id violava a constraint REQUIRE n.id IS UNIQUE quando dois nós RF
# tentavam receber o mesmo id IBGE (municípios homônimos em estados diferentes).
# Agora cria relacionamento entre os dois nós sem alterar propriedades.
Q_LINK_MUNICIPIO_IBGE = """
MATCH (rf:Municipio)
WHERE rf.codigo_rf IS NOT NULL AND NOT (rf)-[:MESMO_QUE]->()
WITH rf, toUpper(trim(rf.nome)) AS nome_upper
MATCH (ibge:Municipio)
WHERE ibge.id IS NOT NULL AND ibge.codigo_rf IS NULL
  AND toUpper(trim(ibge.nome)) = nome_upper
MERGE (rf)-[:MESMO_QUE]->(ibge)
SET rf.ibge_linked = true
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
            "municipio_cod":           mun_cod,
            "municipio_nome":          munis.get(mun_cod, mun_cod),
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
def _t_socios(chunk: list[dict], tables: dict
              ) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict]]:
    """
    Classifica sócios em 5 listas:
      pf_rows     — PF com CPF válido → :Pessoa
      pj_rows     — PJ → :Empresa sócia
      ext_rows    — Estrangeiro → :Pessoa estrangeira
      part_rows   — PF com CPF mascarado/inválido → :Partner (identidade parcial)
      part_rels   — relacionamentos :Partner -[SOCIO_DE]-> :Empresa
    """
    qual_lkp = tables.get("qualificacoes", {})
    pais_lkp = tables.get("paises", {})
    pf_rows, pj_rows, ext_rows, part_rows, part_rels = [], [], [], [], []
    for r in chunk:
        basico   = r.get("cnpj_basico", "").strip().zfill(8)
        tipo     = r.get("identificador_socio", "").strip()
        nome     = r.get("nome_socio", "").strip()
        doc_raw  = r.get("cpf_cnpj_socio", "").strip()
        qual_cod = r.get("qualificacao_socio", "").strip()
        doc_d    = _strip_doc(doc_raw)
        qual_desc = qual_lkp.get(qual_cod, qual_cod)
        base = {
            "cnpj_basico":    basico,
            "qualificacao":   qual_desc,
            "data_entrada":   r.get("data_entrada", "").strip(),
            "tipo":           tipo,
            "fonte_snapshot": r.get("fonte_snapshot", ""),
        }
        if tipo == "2":
            doc_class = classify_doc(doc_raw)
            if doc_class == "cpf_valid":
                pf_rows.append({**base,
                    "cpf":          doc_d.zfill(11),
                    "nome":         nome,
                    "faixa_etaria": r.get("faixa_etaria", "").strip(),
                })
            else:
                # CPF mascarado (ex: "***839.8**-**") ou inválido → identidade parcial
                pid = make_partner_id(nome, doc_raw, tipo, "rfb")
                part_rows.append({
                    "partner_id":  pid,
                    "nome":        nome,
                    "doc_raw":     doc_raw,
                    "doc_partial": doc_d[:6] if doc_class == "cpf_partial" else "",
                    "doc_tipo":    doc_class,
                    "tipo_socio":  tipo,
                    "qualidade_id": "partial" if doc_class == "cpf_partial" else "unknown",
                    "fonte":       "rfb",
                })
                part_rels.append({**base, "partner_id": pid})
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
    return pf_rows, pj_rows, ext_rows, part_rows, part_rels
# ── Loader de batches com retry em deadlock ───────────────────────────────────
def _run_batches(session, query: str, rows: list[dict], extra_params: dict = None,
                 retries: int = 5) -> None:
    """
    Executa query em batches com transação explícita e retry em DeadlockDetected.
    begin_transaction() garante que o erro é capturado ANTES do commit,
    permitindo retry correto (session.run() usa auto-commit — retry nunca funciona).
    """
    import time
    from neo4j.exceptions import TransientError
    params = extra_params or {}
    for i in range(0, len(rows), BATCH):
        batch = rows[i : i + BATCH]
        for attempt in range(1, retries + 1):
            try:
                with session.begin_transaction() as tx:
                    tx.run(query, rows=batch, **params)
                    tx.commit()
                break
            except TransientError as exc:
                if "DeadlockDetected" in str(exc) and attempt < retries:
                    wait = attempt * 0.5 + random.uniform(0, 0.1 * attempt)
                    log.warning(f"    Deadlock — retry {attempt}/{retries} em {wait:.2f}s")
                    time.sleep(wait)
                else:
                    raise
_LOG_EVERY = 500_000   # loga progresso a cada N linhas
# ── Carga de cada tipo (roda em thread própria) ───────────────────────────────
def _load_empresas(driver, csv_dir: Path, tables: dict, snapshot: str, limite: int | None = None, stats: dict = None) -> int:
    path = csv_dir / "empresas.csv"
    total = 0
    if stats is None:
        stats = {'total': 0}
    with driver.session() as session:
        for chunk in _iter_csv(path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [empresas] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]
            prep = _t_empresas(chunk, tables)
            _run_batches(session, Q_EMPRESA, prep)
            total += len(prep)
            stats['total'] += len(prep)
            if total % _LOG_EVERY < CHUNK_SIZE:
                log.info(f"    [empresas] {total:,} linhas inseridas...")
    log.info(f"    [empresas] ✓ {total:,}")
    return total
def _load_estabelecimentos(driver, csv_dir: Path, tables: dict, snapshot: str, limite: int | None = None, stats: dict = None) -> int:
    path = csv_dir / "estabelecimentos.csv"
    est_total = 0
    if stats is None:
        stats = {'total': 0}
    fonte_params = {"fonte_nome": FONTE["fonte_nome"]}
    with driver.session() as session:
        for chunk in _iter_csv(path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [estabelecimentos] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]
            updates, _ = _t_estabelecimentos(chunk, tables)
            _run_batches(session, Q_ESTABELECIMENTO_UPDATE, updates, extra_params=fonte_params)
            est_total += len(updates)
            stats['total'] += len(updates)
            if est_total % _LOG_EVERY < CHUNK_SIZE:
                log.info(f"    [estabelecimentos] {est_total:,} linhas...")
    log.info(f"    [estabelecimentos] ✓ {est_total:,}")
    return est_total
def _load_simples(driver, csv_dir: Path, snapshot: str, limite: int | None = None, stats: dict = None) -> int:
    path = csv_dir / "simples.csv"
    total = 0
    if stats is None:
        stats = {'total': 0}
    with driver.session() as session:
        for chunk in _iter_csv(path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [simples] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]
            prep = _t_simples(chunk)
            _run_batches(session, Q_SIMPLES, prep)
            total += len(prep)
            stats['total'] += len(prep)
            if total % _LOG_EVERY < CHUNK_SIZE:
                log.info(f"    [simples] {total:,} linhas inseridas...")
    log.info(f"    [simples] ✓ {total:,}")
    return total
def _load_socios(driver, csv_dir: Path, tables: dict, snapshot: str, limite: int | None = None, stats: dict = None) -> int:
    path = csv_dir / "socios.csv"
    pf_t = pj_t = ext_t = part_t = 0
    if stats is None:
        stats = {'total': 0}
    with driver.session() as session:
        for chunk in _iter_csv(path):
            if limite is not None and stats['total'] >= limite:
                log.info(f"    [socios] Limite de {limite:,} atingido. Parando.")
                break
            if limite is not None:
                restante = limite - stats['total']
                if restante <= 0:
                    break
                if len(chunk) > restante:
                    chunk = chunk[:restante]
            pf, pj, ext, part_nodes, part_rels = _t_socios(chunk, tables)
            if pf:
                _run_batches(session, Q_SOCIO_PF, pf)
                pf_t += len(pf)
                stats['total'] += len(pf)
            if pj:
                _run_batches(session, Q_SOCIO_PJ, pj)
                pj_t += len(pj)
                stats['total'] += len(pj)
            if ext:
                _run_batches(session, Q_SOCIO_EXT, ext)
                ext_t += len(ext)
                stats['total'] += len(ext)
            if part_nodes:
                _run_batches(session, Q_PARTNER, part_nodes)
                _run_batches(session, Q_PARTNER_SOCIO_DE, part_rels)
                part_t += len(part_nodes)
                stats['total'] += len(part_nodes)
            total = pf_t + pj_t + ext_t + part_t
            if total % _LOG_EVERY < CHUNK_SIZE:
                log.info(
                    f"    [socios] {total:,} "
                    f"(PF={pf_t:,} PJ={pj_t:,} ext={ext_t:,} partner={part_t:,})..."
                )
    log.info(f"    [socios] ✓ PF={pf_t:,} PJ={pj_t:,} ext={ext_t:,} partner={part_t:,}")
    return pf_t + pj_t + ext_t + part_t
# ── Espera Neo4j ficar pronto ─────────────────────────────────────────────────
def _wait_for_neo4j(uri: str, user: str, password: str,
                    retries: int = 20, delay: float = 5.0) -> "Driver":
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
def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, history: bool = False, limite: int | None = None):
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
    # ── constraints + índices + fulltext (antes da carga) ─────────────────────
    with driver.session() as session:
        log.info("  Constraints e índices...")
        apply_schema(session, Q_CONSTRAINTS, Q_INDEXES)
    setup_schema(driver)

    with IngestionRun(driver, "cnpj") as run_ctx:
        for snapshot, csv_dir in snapshots:
            log.info(f"  === Snapshot {snapshot} ===")
            tables = _load_domain_tables(csv_dir)
            log.info("  [1/4] Empresas (sequencial — cria nós base)...")
            stats = {'total': 0}
            total_empresas = _load_empresas(driver, csv_dir, tables, snapshot, limite, stats)
            if limite is not None and stats['total'] >= limite:
                log.info(f"  Limite de {limite:,} linhas atingido após empresas. Parando.")
                break
            log.info(f"  [2-4] Simples + Estabelecimentos + Sócios (paralelo, workers={WORKERS})...")
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                futures = {
                    pool.submit(_load_simples,          driver, csv_dir,        snapshot, limite, stats): "simples",
                    pool.submit(_load_estabelecimentos, driver, csv_dir, tables, snapshot, limite, stats): "estabelecimentos",
                    pool.submit(_load_socios,           driver, csv_dir, tables, snapshot, limite, stats): "socios",
                }
                for future in as_completed(futures):
                    name = futures[future]
                    future.result()   # propaga exceção imediatamente
                    if limite is not None and stats['total'] >= limite:
                        log.info(f"  Limite de {limite:,} atingido durante {name}. Cancelando tarefas restantes...")
                        break
            run_ctx.add(rows_in=stats['total'], rows_out=stats['total'])
            if limite is not None and stats['total'] >= limite:
                log.info(f"  Limite de {limite:,} linhas atingido após snapshot {snapshot}. Parando.")
                break
            # ── liga municípios RF → nós canônicos IBGE via MESMO_QUE ─────
            log.info("  Linkando municípios RF → IBGE via MESMO_QUE...")
            with driver.session() as session:
                with session.begin_transaction() as tx:
                    tx.run(Q_LINK_MUNICIPIO_IBGE)
                    tx.commit()
            log.info("  ✓ municípios linkados")
    driver.close()
    log.info("[cnpj] Pipeline concluído")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline CNPJ — carrega dados da Receita Federal no Neo4j")
    parser.add_argument("--history", action="store_true", help="Carrega histórico completo (padrão: apenas último snapshot)")
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
        history=args.history,
        limite=args.limite,
    )
