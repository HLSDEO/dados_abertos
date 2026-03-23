#!/usr/bin/env python3
"""
etl/main.py  –  Orquestrador ETL via CLI

Uso:
  python main.py download                        # baixa / extrai tudo
  python main.py download ibge                   # só ibge
  python main.py download cnpj                   # só cnpj (extrai ZIPs locais)
  python main.py download cnpj --chunk 100000    # com chunk size customizado
  python main.py pipeline                        # executa todos os pipelines
  python main.py pipeline ibge                   # só ibge
  python main.py pipeline cnpj                   # só cnpj (snapshot mais recente)
  python main.py pipeline cnpj --history         # todos os snapshots
  python main.py run                             # download + pipeline (tudo)
  python main.py run cnpj --chunk 75000          # cnpj com chunk customizado
  python main.py run cnpj --chunk 75000 --history

Flags:
  --chunk N     Linhas por chunk na leitura dos ZIPs (default: 50000)
  --history     Processa todos os snapshots (só pipelines que suportam)

Variáveis de ambiente (ou .env):
  NEO4J_URI       bolt://localhost:7687
  NEO4J_USER      neo4j
  NEO4J_PASSWORD  senha123
  CHUNK_SIZE      50000   (sobrescrito por --chunk)
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


DEFAULT_CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "50000"))


# ── parse de flags ────────────────────────────────────────────────────────────

def _parse_flags(flags: list[str]) -> dict:
    """
    Converte lista de flags em dict de opções.
    Suporta:  --chunk 100000   --history
    """
    opts = {"history": False, "chunk_size": DEFAULT_CHUNK_SIZE}
    i = 0
    while i < len(flags):
        f = flags[i]
        if f == "--history":
            opts["history"] = True
        elif f == "--chunk":
            if i + 1 < len(flags):
                try:
                    opts["chunk_size"] = int(flags[i + 1])
                    i += 1
                except ValueError:
                    log.error(f"--chunk requer um número inteiro, recebido: '{flags[i+1]}'")
                    sys.exit(1)
            else:
                log.error("--chunk requer um valor, ex: --chunk 100000")
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
        import inspect
        sig = inspect.signature(mod.run)
        kwargs = {}
        if "chunk_size" in sig.parameters:
            kwargs["chunk_size"] = opts["chunk_size"]
            log.info(f"  chunk_size={opts['chunk_size']:,}")
        mod.run(**kwargs)


def do_pipeline(names: list[str], opts: dict):
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
        if "history" in sig.parameters:
            kwargs["history"] = opts["history"]
        if "chunk_size" in sig.parameters:
            kwargs["chunk_size"] = opts["chunk_size"]
        mod.run(**kwargs)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    command = args[0]
    rest    = args[1:]

    # separa targets (sem --) dos flags (com --)
    targets = [a for a in rest if not a.startswith("--")] or None
    flags   = [a for a in rest if a.startswith("--")]
    # inclui valores após flags com valor (ex: --chunk 100000)
    raw_flags = []
    i = 0
    while i < len(rest):
        if rest[i].startswith("--"):
            raw_flags.append(rest[i])
            if rest[i] == "--chunk" and i + 1 < len(rest) and not rest[i+1].startswith("--"):
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

    elif command == "run":
        names = targets or list(DOWNLOADS)
        do_download(names, opts)
        names = targets or list(PIPELINES)
        do_pipeline(names, opts)

    else:
        log.error(f"Comando inválido: '{command}'. Use: download | pipeline | run")
        sys.exit(1)


if __name__ == "__main__":
    main()