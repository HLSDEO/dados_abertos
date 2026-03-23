"""
Pipeline 2 - Receita Federal CNPJ → Neo4j

Lê os CSVs normalizados de data/cnpj/{snapshot}/csv/ e carrega no Neo4j.

Nós criados:
  (:Empresa)       — cnpj_basico como chave, razao_social, capital_social, etc.
  (:Pessoa)        — CPF (sócios PF)
  (:Empresa)       — sócios PJ referenciam outro nó Empresa
  (:Municipio)     — reutiliza os nós criados pelo pipeline IBGE; cria se ausente
  (:Pais)          — país de origem de sócios estrangeiros

Relacionamentos:
  (:Pessoa|Empresa)-[:SOCIO_DE {qualificacao, data_entrada, tipo}]->(:Empresa)
  (:Empresa)-[:LOCALIZADA_EM {cnae_principal, uf}]->(:Municipio)

O pipeline processa apenas o snapshot mais recente por padrão.
Para processar todos: chame run(..., history=True)
"""

import csv
import logging
import os
from pathlib import Path
from datetime import datetime

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "cnpj"

SNAPSHOT_FMT = "%Y-%m"

FONTE = {
    "fonte_nome":      "Receita Federal do Brasil",
    "fonte_descricao": "Dados Abertos CNPJ",
    "fonte_url":       "https://dadosabertos.rfb.gov.br/CNPJ/",
}

# Tamanho do lote para UNWIND no Cypher
BATCH = 500


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        log.warning(f"  CSV não encontrado: {path}")
        return []
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    log.info(f"  Lido {path.parent.parent.name}/{path.name}  ({len(rows):,} linhas)")
    return rows


def _batched(rows: list, size: int):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def _run_batches(session, query: str, rows: list[dict]) -> None:
    for batch in _batched(rows, BATCH):
        session.run(query, rows=batch)


# ── Tabelas de domínio ────────────────────────────────────────────────────────

def _load_domain_tables(csv_dir: Path) -> dict[str, dict[str, str]]:
    """Carrega todas as tabelas de domínio em dicts codigo→descricao."""
    tables = {}
    specs = {
        "cnaes":         ("codigo_cnae",        "descricao_cnae"),
        "naturezas":     ("codigo_natureza",     "descricao_natureza"),
        "qualificacoes": ("codigo_qualificacao", "descricao_qualificacao"),
        "motivos":       ("codigo_motivo",       "descricao_motivo"),
        "municipios_rf": ("codigo_municipio_rf", "nome_municipio"),
        "paises":        ("codigo_pais",         "nome_pais"),
    }
    for name, (k_col, v_col) in specs.items():
        rows = _read_csv(csv_dir / f"{name}.csv")
        tables[name] = {r[k_col].strip(): r[v_col].strip() for r in rows if k_col in r}
    return tables


# ── Constraints ───────────────────────────────────────────────────────────────

Q_CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Empresa)   REQUIRE n.cnpj_basico IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Pessoa)    REQUIRE n.cpf IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Municipio) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Pais)      REQUIRE n.codigo IS UNIQUE",
]


# ── Carga de nós Empresa ──────────────────────────────────────────────────────

PORTE_MAP = {
    "00": "Não informado", "01": "Micro Empresa",
    "03": "Empresa de Pequeno Porte", "05": "Demais",
}

Q_EMPRESA = """
UNWIND $rows AS r
MERGE (e:Empresa {cnpj_basico: r.cnpj_basico})
SET e.razao_social              = r.razao_social,
    e.natureza_juridica         = r.natureza_juridica,
    e.natureza_juridica_desc    = r.natureza_juridica_desc,
    e.qualificacao_responsavel  = r.qualificacao_responsavel,
    e.capital_social            = toFloat(r.capital_social),
    e.porte_empresa             = r.porte_empresa,
    e.porte_empresa_desc        = r.porte_empresa_desc,
    e.ente_federativo           = r.ente_federativo,
    e.fonte_nome                = r.fonte_nome,
    e.fonte_url                 = r.fonte_url,
    e.fonte_snapshot            = r.fonte_snapshot
"""

def _prepare_empresas(rows: list[dict], tables: dict) -> list[dict]:
    nat  = tables.get("naturezas", {})
    qual = tables.get("qualificacoes", {})
    out  = []
    for r in rows:
        cap = r.get("capital_social", "0").strip().replace(",", ".")
        try:
            float(cap)
        except ValueError:
            cap = "0"
        out.append({
            "cnpj_basico":             r["cnpj_basico"].strip().zfill(8),
            "razao_social":            r.get("razao_social", "").strip(),
            "natureza_juridica":       r.get("natureza_juridica", "").strip(),
            "natureza_juridica_desc":  nat.get(r.get("natureza_juridica", "").strip(), ""),
            "qualificacao_responsavel":r.get("qualificacao_responsavel", "").strip(),
            "capital_social":          cap,
            "porte_empresa":           r.get("porte_empresa", "").strip(),
            "porte_empresa_desc":      PORTE_MAP.get(r.get("porte_empresa", "").strip(), ""),
            "ente_federativo":         r.get("ente_federativo", "").strip(),
            "fonte_nome":              r.get("fonte_nome", FONTE["fonte_nome"]),
            "fonte_url":               r.get("fonte_url",  FONTE["fonte_url"]),
            "fonte_snapshot":          r.get("fonte_snapshot", ""),
        })
    return out


# ── Carga de estabelecimentos → LOCALIZADA_EM ─────────────────────────────────

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
                m.fonte_nome = r.fonte_nome,
                m.fonte_url  = r.fonte_url
MERGE (e)-[rel:LOCALIZADA_EM]->(m)
SET rel.cnae_principal = r.cnae_principal,
    rel.uf             = r.uf
"""

def _prepare_estabelecimentos(rows: list[dict], tables: dict) -> tuple[list[dict], list[dict]]:
    cnaes = tables.get("cnaes", {})
    munis = tables.get("municipios_rf", {})
    updates, rels = [], []

    for r in rows:
        basico = r.get("cnpj_basico", "").strip().zfill(8)
        cnpj   = r.get("cnpj", "").strip()
        mun_cod  = r.get("municipio", "").strip()
        mun_nome = munis.get(mun_cod, mun_cod)
        cnae_cod  = r.get("cnae_principal", "").strip()
        cnae_desc = cnaes.get(cnae_cod, "")

        updates.append({
            "cnpj_basico":             basico,
            "cnpj":                    cnpj,
            "nome_fantasia":           r.get("nome_fantasia", "").strip(),
            "situacao_cadastral":      r.get("situacao_cadastral", "").strip(),
            "data_situacao_cadastral": r.get("data_situacao_cadastral", "").strip(),
            "data_inicio_atividade":   r.get("data_inicio_atividade", "").strip(),
            "cnae_principal":          cnae_cod,
            "cnae_principal_desc":     cnae_desc,
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
                "municipio_nome": mun_nome,
                "cnae_principal": cnae_cod,
                "uf":             r.get("uf", "").strip(),
                "fonte_nome":     r.get("fonte_nome", FONTE["fonte_nome"]),
                "fonte_url":      r.get("fonte_url",  FONTE["fonte_url"]),
            })
    return updates, rels


# ── Carga de sócios → SOCIO_DE ────────────────────────────────────────────────

# identificador_socio: 1=PJ, 2=PF, 3=Estrangeiro
Q_SOCIO_PF = """
UNWIND $rows AS r
MERGE (p:Pessoa {cpf: r.cpf})
  ON CREATE SET p.nome       = r.nome,
                p.fonte_nome = r.fonte_nome,
                p.fonte_url  = r.fonte_url
  ON MATCH  SET p.nome       = coalesce(p.nome, r.nome)
WITH p, r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (p)-[s:SOCIO_DE]->(e)
SET s.qualificacao  = r.qualificacao,
    s.data_entrada  = r.data_entrada,
    s.tipo          = r.tipo,
    s.faixa_etaria  = r.faixa_etaria,
    s.fonte_snapshot = r.fonte_snapshot
"""

Q_SOCIO_PJ = """
UNWIND $rows AS r
MERGE (soc:Empresa {cnpj_basico: r.cnpj_socio_basico})
  ON CREATE SET soc.fonte_nome = r.fonte_nome,
                soc.fonte_url  = r.fonte_url
WITH soc, r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (soc)-[s:SOCIO_DE]->(e)
SET s.qualificacao   = r.qualificacao,
    s.data_entrada   = r.data_entrada,
    s.tipo           = r.tipo,
    s.fonte_snapshot = r.fonte_snapshot
"""

Q_SOCIO_ESTRANGEIRO = """
UNWIND $rows AS r
MERGE (pai:Pais {codigo: r.pais_cod})
  ON CREATE SET pai.nome = r.pais_nome
MERGE (p:Pessoa {cpf: r.id_estrangeiro})
  ON CREATE SET p.nome       = r.nome,
                p.estrangeiro = true,
                p.pais        = r.pais_nome,
                p.fonte_nome  = r.fonte_nome,
                p.fonte_url   = r.fonte_url
WITH p, r
MATCH (e:Empresa {cnpj_basico: r.cnpj_basico})
MERGE (p)-[s:SOCIO_DE]->(e)
SET s.qualificacao   = r.qualificacao,
    s.data_entrada   = r.data_entrada,
    s.tipo           = r.tipo,
    s.fonte_snapshot = r.fonte_snapshot
"""

def _strip_doc(s: str) -> str:
    return "".join(c for c in s if c.isdigit())

def _prepare_socios(
    rows: list[dict], tables: dict
) -> tuple[list[dict], list[dict], list[dict]]:
    qual_lkp  = tables.get("qualificacoes", {})
    pais_lkp  = tables.get("paises", {})
    pf_rows, pj_rows, ext_rows = [], [], []

    for r in rows:
        basico      = r.get("cnpj_basico", "").strip().zfill(8)
        tipo        = r.get("identificador_socio", "").strip()
        nome        = r.get("nome_socio", "").strip()
        doc_raw     = r.get("cpf_cnpj_socio", "").strip()
        qual_cod    = r.get("qualificacao_socio", "").strip()
        qual_desc   = qual_lkp.get(qual_cod, qual_cod)
        data_ent    = r.get("data_entrada", "").strip()
        faixa       = r.get("faixa_etaria", "").strip()
        pais_cod    = r.get("pais", "").strip()
        pais_nome   = pais_lkp.get(pais_cod, pais_cod)
        fonte_nome  = r.get("fonte_nome", FONTE["fonte_nome"])
        fonte_url   = r.get("fonte_url", FONTE["fonte_url"])
        snapshot    = r.get("fonte_snapshot", "")
        doc_digits  = _strip_doc(doc_raw)

        base = {
            "cnpj_basico":    basico,
            "qualificacao":   qual_desc,
            "data_entrada":   data_ent,
            "tipo":           tipo,
            "fonte_nome":     fonte_nome,
            "fonte_url":      fonte_url,
            "fonte_snapshot": snapshot,
        }

        if tipo == "2":  # PF
            cpf = doc_digits.zfill(11) if doc_digits else f"DESCONHECIDO_{nome[:20]}"
            pf_rows.append({**base, "cpf": cpf, "nome": nome, "faixa_etaria": faixa})

        elif tipo == "1":  # PJ sócia
            cnpj_basico_socio = doc_digits[:8].zfill(8) if len(doc_digits) >= 8 else ""
            if cnpj_basico_socio:
                pj_rows.append({**base, "cnpj_socio_basico": cnpj_basico_socio})

        else:  # tipo "3" = estrangeiro ou outros
            id_ext = doc_raw if doc_raw else f"EXT_{nome[:20]}"
            ext_rows.append({
                **base,
                "id_estrangeiro": id_ext,
                "nome":           nome,
                "pais_cod":       pais_cod,
                "pais_nome":      pais_nome,
                "faixa_etaria":   faixa,
            })

    return pf_rows, pj_rows, ext_rows


# ── Integração Municipio IBGE ─────────────────────────────────────────────────

Q_LINK_MUNICIPIO_IBGE = """
// Liga nós Municipio criados pelo RF ao nó canônico do IBGE pelo nome
MATCH (rf:Municipio)
WHERE rf.codigo_rf IS NOT NULL AND rf.id IS NULL
WITH rf, toUpper(trim(rf.nome)) AS nome_upper
MATCH (ibge:Municipio)
WHERE ibge.id IS NOT NULL AND toUpper(trim(ibge.nome)) = nome_upper
WITH rf, ibge
SET rf.id          = ibge.id,
    rf.ibge_linked = true
"""


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(neo4j_uri: str, neo4j_user: str, neo4j_password: str, history: bool = False):
    log.info("[cnpj] Iniciando pipeline → Neo4j")

    snapshots = _discover_snapshots()
    if not snapshots:
        log.warning(f"  Nenhum snapshot processado encontrado em {DATA_DIR}")
        log.warning("  Execute primeiro: etl/main.py download cnpj")
        return

    # history=False → só o mais recente
    if not history:
        snapshots = [snapshots[-1]]
        log.info(f"  Modo padrão: apenas snapshot mais recente → {snapshots[0][0]}")
    else:
        log.info(f"  Modo histórico: {len(snapshots)} snapshot(s)")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with driver.session() as session:
        log.info("  Criando constraints...")
        for q in Q_CONSTRAINTS:
            session.run(q)

    for snapshot, csv_dir in snapshots:
        log.info(f"  === Snapshot {snapshot} ===")

        # 1. Tabelas de domínio
        tables = _load_domain_tables(csv_dir)

        with driver.session() as session:

            # 2. Empresas
            emp_rows = _read_csv(csv_dir / "empresas.csv")
            if emp_rows:
                log.info(f"    Carregando empresas...")
                prep = _prepare_empresas(emp_rows, tables)
                _run_batches(session, Q_EMPRESA, prep)
                log.info(f"    {len(prep):,} empresas carregadas")

            # 3. Estabelecimentos → enriquece Empresa + LOCALIZADA_EM
            est_rows = _read_csv(csv_dir / "estabelecimentos.csv")
            if est_rows:
                log.info(f"    Carregando estabelecimentos...")
                updates, rels = _prepare_estabelecimentos(est_rows, tables)
                _run_batches(session, Q_ESTABELECIMENTO_UPDATE, updates)
                _run_batches(session, Q_LOCALIZADA_EM, rels)
                log.info(f"    {len(updates):,} estabelecimentos, {len(rels):,} LOCALIZADA_EM")

            # 4. Sócios → Pessoa/Empresa + SOCIO_DE
            soc_rows = _read_csv(csv_dir / "socios.csv")
            if soc_rows:
                log.info(f"    Carregando sócios...")
                pf_rows, pj_rows, ext_rows = _prepare_socios(soc_rows, tables)
                if pf_rows:
                    _run_batches(session, Q_SOCIO_PF, pf_rows)
                    log.info(f"    {len(pf_rows):,} sócios PF")
                if pj_rows:
                    _run_batches(session, Q_SOCIO_PJ, pj_rows)
                    log.info(f"    {len(pj_rows):,} sócios PJ")
                if ext_rows:
                    _run_batches(session, Q_SOCIO_ESTRANGEIRO, ext_rows)
                    log.info(f"    {len(ext_rows):,} sócios estrangeiros")

            # 5. Tenta ligar Municipios criados pelo RF ao grafo IBGE
            log.info("    Linkando municípios RF → IBGE...")
            result = session.run(Q_LINK_MUNICIPIO_IBGE)
            log.info("    Municipios linkados ao IBGE (por nome)")

    driver.close()
    log.info("[cnpj] Pipeline concluído")