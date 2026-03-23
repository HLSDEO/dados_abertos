#!/usr/bin/env python3
"""
etl/main.py  –  Orquestrador ETL via CLI

Uso:
  python main.py download              # baixa tudo
  python main.py download ibge         # baixa só ibge
  python main.py pipeline              # executa todos os pipelines
  python main.py pipeline ibge         # executa só o pipeline ibge
  python main.py run                   # download + pipeline (tudo)
  python main.py run ibge              # download + pipeline (só ibge)

Variáveis de ambiente (ou .env):
  NEO4J_URI       bolt://localhost:7687
  NEO4J_USER      neo4j
  NEO4J_PASSWORD  changeme
"""

import importlib.util
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


# ── registry: adicione novos módulos aqui ─────────────────────────────────────
#  chave = nome usado na CLI, valor = caminho relativo ao ETL_DIR
DOWNLOADS = {
    "ibge": "download/1-ibge.py",
    # "outro": "download/2-outro.py",
}

PIPELINES = {
    "ibge": "pipeline/1-ibge.py",
    # "outro": "pipeline/2-outro.py",
}


# ── loader dinâmico ───────────────────────────────────────────────────────────
def _load(rel_path: str):
    path = ETL_DIR / rel_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── execução ──────────────────────────────────────────────────────────────────
def do_download(names: list[str]):
    for name in names:
        if name not in DOWNLOADS:
            log.error(f"Download desconhecido: '{name}'. Disponíveis: {list(DOWNLOADS)}")
            sys.exit(1)
        log.info(f"=== DOWNLOAD: {name} ===")
        _load(DOWNLOADS[name]).run()


def do_pipeline(names: list[str]):
    for name in names:
        if name not in PIPELINES:
            log.error(f"Pipeline desconhecido: '{name}'. Disponíveis: {list(PIPELINES)}")
            sys.exit(1)
        log.info(f"=== PIPELINE: {name} ===")
        _load(PIPELINES[name]).run(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
        )


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    command = args[0]
    targets = args[1:] if len(args) > 1 else None   # None = todos

    if command == "download":
        names = targets or list(DOWNLOADS)
        do_download(names)

    elif command == "pipeline":
        names = targets or list(PIPELINES)
        do_pipeline(names)

    elif command == "run":
        names = targets or list(DOWNLOADS)
        do_download(names)
        names = targets or list(PIPELINES)
        do_pipeline(names)

    else:
        log.error(f"Comando inválido: '{command}'. Use: download | pipeline | run")
        sys.exit(1)


if __name__ == "__main__":
    main()