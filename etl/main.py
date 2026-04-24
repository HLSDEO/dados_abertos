#!/usr/bin/env python3
"""
etl/main.py  –  Orquestrador ETL via CLI

Uso:
  python main.py download                                  # baixa / extrai tudo
  python main.py download cnpj                             # só cnpj
  python main.py download cnpj --chunk 100000              # chunk customizado
  python main.py download cnpj --workers 4                 # 4 ZIPs em paralelo
  python main.py download cnpj --chunk 50000 --workers 4   # ambos
  python main.py download tse                               # todas as eleições TSE
  python main.py download tse --eleicao 2024                # só 2024
  python main.py download tse --eleicao 2024 --eleicao 2022 # 2024 e 2022
  python main.py pipeline cnpj --history                   # todos os snapshots
  python main.py run cnpj --chunk 75000 --workers 4        # download + pipeline
  python main.py run cnpj --full                           # download + pipeline + analytics
  python main.py analytics                                 # só analytics (tudo)
  python main.py analytics gds                             # só o GDS
  python main.py analytics splink                          # deduplicação probabilística
  python main.py schema                                    # aplica constraints + índices + fulltext
  python main.py ingestion-status                          # mostra status dos últimos runs

Flags:
  --chunk N     Linhas por chunk na leitura dos ZIPs (default: 50000)
  --workers N   ZIPs processados em paralelo (default: 2)
  --limite N   Número máximo de linhas a processar (para testes rápidos)
  --history     Processa todos os snapshots (só pipelines que suportam)
  --full        No comando 'run': executa analytics após pipeline
  --eleicao ANO Ano de eleição — repetível (ex: --eleicao 2024 --eleicao 2022)

Variáveis de ambiente (ou .env):
  NEO4J_URI       bolt://localhost:7687
  NEO4J_USER      neo4j
  NEO4J_PASSWORD  changeme
  CHUNK_SIZE      50000
  WORKERS         2
"""

import importlib.util
import inspect
import logging
import os
import sys
from pathlib import Path

# ── setup de paths ────────────────────────────────────────────────────────────
ETL_DIR = Path(__file__).resolve().parent
ROOT    = ETL_DIR.parent
sys.path.insert(0, str(ETL_DIR))

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── carrega .env se existir ───────────────────────────────────────────────────
def _load_dotenv():
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

_load_dotenv()

NEO4J_URI      = os.environ.get("NEO4J_URI",      "bolt://neo4j:7687")
NEO4J_USER     = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "changeme")


# ── registry ──────────────────────────────────────────────────────────────────
DOWNLOADS = {
    "ibge":                 "download/1-ibge.py",
    "cnpj":                 "download/2-cnpj.py",
    "tse":                  "download/3-tse.py",
    "emendas_cgu":          "download/4-emendas_cgu.py",
    "tesouro_transparente": "download/5-tesouro_transparente.py",
    "servidores_cgu":       "download/6-servidores_cgu.py",
    "sancoes_cgu":          "download/7-sancoes_cgu.py",
    "pncp":                 "download/8-pncp.py",
    "pgfn":                 "download/9-pgfn.py",
    "camara":               "download/11-camara.py",
    "bndes":                "download/12-bndes.py",
    "senado":               "download/13-senado.py",
}

PIPELINES = {
    "ibge":             "pipeline/1-ibge.py",
    "cnpj":             "pipeline/2-cnpj.py",
    "siafi":            "pipeline/3-siafi.py",
    "servidores_cgu":   "pipeline/4-servidores_cgu.py",
    "emendas_cgu":      "pipeline/5-emendas_cgu.py",
    "tse":              "pipeline/6-tse.py",
    "sancoes_cgu":      "pipeline/7-sancoes_cgu.py",
    "pncp":             "pipeline/8-pncp.py",
    "pgfn":             "pipeline/9-pgfn.py",
    "camara":           "pipeline/11-camara.py",
    "bndes":            "pipeline/12-bndes.py",
    "senado":            "pipeline/13-senado.py",
}

ANALYTICS = {
    "gds":    "analytics/1-gds.py",
    "splink": "analytics/2-splink.py",
}


# ── loader dinâmico ───────────────────────────────────────────────────────────
def _load(rel_path: str):
    path = ETL_DIR / rel_path
    # usa o rel_path completo como nome do módulo para evitar colisão de cache
    # entre download/7-sancoes_cgu.py e pipeline/7-sancoes_cgu.py (mesmo stem)
    mod_name = rel_path.replace("/", ".").replace("\\", ".").rstrip(".py")
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


DEFAULT_CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "50000"))
DEFAULT_WORKERS    = int(os.environ.get("WORKERS",     "2"))


# ── parse de flags ────────────────────────────────────────────────────────────

def _parse_flags(flags: list[str]) -> dict:
    opts = {
        "history":    False,
        "full":       False,
        "chunk_size": DEFAULT_CHUNK_SIZE,
        "workers":    DEFAULT_WORKERS,
        "limite":     None,
        "eleicoes":   [],   # --eleicao ANO (repetível)
        "anos":       [],   # --ano ANO    (repetível)
        "meses":      [],   # --mes MES    (repetível)
    }
    i = 0
    while i < len(flags):
        f = flags[i]
        if f == "--history":
            opts["history"] = True
        elif f == "--full":
            opts["full"] = True
        elif f in ("--chunk", "--workers", "--limite", "--eleicao", "--ano", "--mes"):
            if i + 1 < len(flags):
                try:
                    val = int(flags[i + 1])
                    i  += 1
                except ValueError:
                    log.error(f"{f} requer um número inteiro, recebido: '{flags[i+1]}'")
                    sys.exit(1)
                if f == "--chunk":
                    opts["chunk_size"] = val
                elif f == "--workers":
                    opts["workers"] = val
                elif f == "--limite":
                    opts["limite"] = val
                elif f == "--eleicao":
                    opts["eleicoes"].append(val)
                elif f == "--ano":
                    opts["anos"].append(val)
                elif f == "--mes":
                    opts["meses"].append(val)
            else:
                log.error(f"{f} requer um valor, ex: {f} 2025")
                sys.exit(1)
        else:
            log.warning(f"Flag desconhecida ignorada: '{f}'")
        i += 1
    return opts


# ── execução ──────────────────────────────────────────────────────────────────

def do_download(names: list[str], opts: dict):
    for name in names:
        if name not in DOWNLOADS:
            log.error(f"Download desconhecido: '{name}'. Disponíveis: {list(DOWNLOADS)}")
            sys.exit(1)
        log.info(f"=== DOWNLOAD: {name} ===")
        mod = _load(DOWNLOADS[name])
        sig = inspect.signature(mod.run)
        kwargs = {}
        if "chunk_size" in sig.parameters:
            kwargs["chunk_size"] = opts["chunk_size"]
        if "workers" in sig.parameters:
            kwargs["workers"] = opts["workers"]
        if "eleicoes" in sig.parameters:
            kwargs["eleicoes"] = opts["eleicoes"] or None
            if kwargs["eleicoes"]:
                log.info(f"  eleicoes={kwargs['eleicoes']}")
        if "anos" in sig.parameters:
            kwargs["anos"] = opts["anos"] or None
            if kwargs["anos"]:
                log.info(f"  anos={kwargs['anos']}")
        if "meses" in sig.parameters:
            kwargs["meses"] = opts["meses"] or None
            if kwargs["meses"]:
                log.info(f"  meses={kwargs['meses']}")
        if any(k in kwargs for k in ("chunk_size", "workers")):
            log.info(f"  chunk_size={opts['chunk_size']:,}  workers={opts['workers']}")
        mod.run(**kwargs)


def do_pipeline(names: list[str], opts: dict):
    for name in names:
        if name not in PIPELINES:
            log.error(f"Pipeline desconhecido: '{name}'. Disponíveis: {list(PIPELINES)}")
            sys.exit(1)
        log.info(f"=== PIPELINE: {name} ===")
        mod = _load(PIPELINES[name])
        sig = inspect.signature(mod.run)
        kwargs = dict(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
        )
        if "history" in sig.parameters:
            kwargs["history"] = opts["history"]
        if "chunk_size" in sig.parameters:
            kwargs["chunk_size"] = opts["chunk_size"]
        if "limite" in sig.parameters:
            kwargs["limite"] = opts["limite"]
            if opts["limite"] is not None:
                log.info(f"  limite={opts['limite']:,}")
        if "eleicoes" in sig.parameters:
            kwargs["eleicoes"] = opts["eleicoes"] or None
            if kwargs["eleicoes"]:
                log.info(f"  eleicoes={kwargs['eleicoes']}")
        if "anos" in sig.parameters:
            kwargs["anos"] = opts["anos"] or None
            if kwargs["anos"]:
                log.info(f"  anos={kwargs['anos']}")
        if "meses" in sig.parameters:
            kwargs["meses"] = opts["meses"] or None
            if kwargs["meses"]:
                log.info(f"  meses={kwargs['meses']}")
        mod.run(**kwargs)


def do_analytics(names: list[str], opts: dict):
    for name in names:
        if name not in ANALYTICS:
            log.error(f"Analytics desconhecido: '{name}'. Disponíveis: {list(ANALYTICS)}")
            sys.exit(1)
        log.info(f"=== ANALYTICS: {name} ===")
        mod = _load(ANALYTICS[name])
        sig = inspect.signature(mod.run)
        kwargs = dict(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
        )
        mod.run(**{k: v for k, v in kwargs.items() if k in sig.parameters})


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    command = args[0]
    rest    = args[1:]

    targets = [a for a in rest if not a.startswith("--")] or None

    raw_flags = []
    i = 0
    while i < len(rest):
        if rest[i].startswith("--"):
            raw_flags.append(rest[i])
            if rest[i] in ("--chunk", "--workers", "--limite", "--eleicao", "--ano", "--mes") \
                    and i + 1 < len(rest) and not rest[i+1].startswith("--"):
                i += 1
                raw_flags.append(rest[i])
        i += 1

    opts = _parse_flags(raw_flags)

    if command == "download":
        names = targets or list(DOWNLOADS)
        do_download(names, opts)

    elif command == "pipeline":
        names = targets or list(PIPELINES)
        do_pipeline(names, opts)

    elif command == "analytics":
        names = targets or list(ANALYTICS)
        do_analytics(names, opts)

    elif command == "run":
        names = targets or list(DOWNLOADS)
        do_download(names, opts)
        names = targets or list(PIPELINES)
        do_pipeline(names, opts)
        if opts["full"]:
            log.info("=== [--full] executando analytics após pipeline ===")
            do_analytics(list(ANALYTICS), opts)

    elif command == "schema":
        # Aplica constraints, índices e fulltext index sem carregar dados
        from pipeline.lib import wait_for_neo4j, setup_schema
        driver = wait_for_neo4j(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        setup_schema(driver)
        driver.close()
        log.info("=== Schema aplicado ===")

    elif command == "ingestion-status":
        # Mostra status das últimas execuções de cada pipeline
        from pipeline.lib import wait_for_neo4j
        driver = wait_for_neo4j(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        with driver.session() as s:
            result = s.run("""
                MATCH (r:IngestionRun)
                WITH r.source_id AS source, max(r.started_at) AS last_run
                MATCH (r:IngestionRun {source_id: source, started_at: last_run})
                RETURN source, r.status AS status, r.started_at AS started,
                       r.finished_at AS finished, r.rows_in AS rows_in,
                       r.rows_out AS rows_out, r.error AS error
                ORDER BY source
            """)
            rows = list(result)
        if not rows:
            log.info("Nenhum IngestionRun encontrado — rode pipelines primeiro")
        else:
            log.info("=== Status dos pipelines ===")
            for r in rows:
                err = f"  ERROR={r['error'][:80]}" if r.get("error") else ""
                log.info(
                    f"  {r['source']:<25} {r['status']:<15} "
                    f"in={r['rows_in'] or 0:>10,}  out={r['rows_out'] or 0:>10,}  "
                    f"started={r['started']}{err}"
                )
        driver.close()

    else:
        log.error(
            f"Comando inválido: '{command}'. "
            "Use: download | pipeline | analytics | run | schema | ingestion-status"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()