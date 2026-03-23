#!/usr/bin/env python3
"""
etl/main.py  –  Orquestrador ETL via CLI

Uso:
  python main.py download              # baixa / extrai tudo
  python main.py download ibge         # só ibge
  python main.py download cnpj         # só cnpj (extrai ZIPs locais)
  python main.py pipeline              # executa todos os pipelines
  python main.py pipeline ibge         # só ibge
  python main.py pipeline cnpj         # só cnpj (snapshot mais recente)
  python main.py pipeline cnpj --history  # todos os snapshots
  python main.py run                   # download + pipeline (tudo)
  python main.py run ibge              # download + pipeline (só ibge)
  python main.py run cnpj              # download + pipeline (só cnpj)

Variáveis de ambiente (ou .env):
  NEO4J_URI       bolt://localhost:7687
  NEO4J_USER      neo4j
  NEO4J_PASSWORD  senha123
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
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "senha123")


# ── registry ──────────────────────────────────────────────────────────────────
DOWNLOADS = {
    "ibge": "download/1-ibge.py",
    "cnpj": "download/2-cnpj.py",
}

PIPELINES = {
    "ibge": "pipeline/1-ibge.py",
    "cnpj": "pipeline/2-cnpj.py",
}


# ── loader dinâmico ───────────────────────────────────────────────────────────
def _load(rel_path: str):
    path = ETL_DIR / rel_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── execução ──────────────────────────────────────────────────────────────────
def do_download(names: list[str], flags: list[str]):
    for name in names:
        if name not in DOWNLOADS:
            log.error(f"Download desconhecido: '{name}'. Disponíveis: {list(DOWNLOADS)}")
            sys.exit(1)
        log.info(f"=== DOWNLOAD: {name} ===")
        _load(DOWNLOADS[name]).run()


def do_pipeline(names: list[str], flags: list[str]):
    history = "--history" in flags
    for name in names:
        if name not in PIPELINES:
            log.error(f"Pipeline desconhecido: '{name}'. Disponíveis: {list(PIPELINES)}")
            sys.exit(1)
        log.info(f"=== PIPELINE: {name} ===")
        mod = _load(PIPELINES[name])
        import inspect
        sig = inspect.signature(mod.run)
        kwargs = dict(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
        )
        # passa --history só para pipelines que aceitam o parâmetro
        if "history" in sig.parameters:
            kwargs["history"] = history
        mod.run(**kwargs)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    command = args[0]
    rest    = args[1:]

    # separa nomes de targets dos flags (começam com --)
    targets = [a for a in rest if not a.startswith("--")] or None
    flags   = [a for a in rest if a.startswith("--")]

    if command == "download":
        names = targets or list(DOWNLOADS)
        do_download(names, flags)

    elif command == "pipeline":
        names = targets or list(PIPELINES)
        do_pipeline(names, flags)

    elif command == "run":
        names = targets or list(DOWNLOADS)
        do_download(names, flags)
        names = targets or list(PIPELINES)
        do_pipeline(names, flags)

    else:
        log.error(f"Comando inválido: '{command}'. Use: download | pipeline | run")
        sys.exit(1)


if __name__ == "__main__":
    main()