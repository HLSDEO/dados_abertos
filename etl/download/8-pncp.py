"""
Download 8 - PNCP & ComprasNet Contratos

Baixa CSVs brutos do Portal Nacional de Contratações Públicas (PNCP)
e ComprasNet Contratos (contratos e empenhos).

Ano padrão: 2026 (configurável via argumento ou variável PNCP_YEAR).

Arquivos baixados para data/pncp_csv/:
  - itens.csv          (PNCP_ITEM_RESULTADO)
  - contratos.csv      (comprasnet-contratos-anual-contratos)
  - empenhos.csv       (comprasnet-contratos-anual-empenhos)

Uso:
    python etl/download/8-pncp.py          # baixa 2026
    python etl/download/8-pncp.py --anos 2025     # baixa 2025
    python etl/download/8-pncp.py --anos 2024 2023 # múltiplos anos
"""

import argparse
import logging
import os
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "pncp_csv"
DEFAULT_YEAR = os.environ.get("PNCP_YEAR", "2026")

HEADERS = {"User-Agent": "dados-abertos-etl/1.0 (+https://github.com/dadosabertos)"}


def _build_urls(ano: str) -> dict:
    """Constrói URLs para o ano informado."""
    base = str(ano)
    return {
        "itens":
            f"https://repositorio.dados.gov.br/seges/comprasgov/anual/{base}/comprasGOV-anual-VW_DM_PNCP_ITEM_RESULTADO-{base}.csv",
        "contratos":
            f"https://repositorio.dados.gov.br/seges/comprasnet_contratos/anual/{base}/comprasnet-contratos-anual-contratos-{base}.csv",
        "empenhos":
            f"https://repositorio.dados.gov.br/seges/comprasnet_contratos/anual/{base}/comprasnet-contratos-anual-empenhos-{base}.csv",
    }


def _download(url: str, dest: Path, retries: int = 3, delay: float = 2.0) -> bool:
    """Baixa arquivo e salva em dest. Retorna True se OK."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"  GET {dest.name}  ({attempt}/{retries})")
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 404:
                log.warning(f"  404 — não disponível")
                return False
            r.raise_for_status()
            content = r.content
            if len(content) < 100:
                log.warning(f"  Arquivo muito pequeno ({len(content)} bytes)")
                return False
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            log.info(f"  OK: {len(content):,} bytes")
            return True
        except requests.RequestException as exc:
            log.warning(f"  Erro: {exc}")
            if attempt < retries:
                time.sleep(delay * attempt)
    log.error(f"  Falha após {retries} tentativas")
    return False


def run(anos: list[int] | None = None) -> None:
    """
    Baixa os CSVs brutos para data/pncp_csv/.

    Args:
        anos: lista de anos. Se None/vazio, usa DEFAULT_YEAR.
    """
    if not anos:
        anos = [int(DEFAULT_YEAR)]

    log.info(f"[pncp-download] Anos: {anos}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"[pncp-download] Destino: {DATA_DIR}")

    ok = 0
    total = 0

    for ano in anos:
        urls = _build_urls(str(ano))
        for nome, url in urls.items():
            total += 1
            dest = DATA_DIR / f"{nome}.csv"
            if dest.exists():
                size = dest.stat().st_size
                log.info(f"  [skip] {dest.name} já existe ({size:,} bytes)")
                ok += 1
                continue
            if _download(url, dest):
                ok += 1

    log.info(f"[pncp-download] Concluído: {ok}/{total} arquivos")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Baixa CSVs brutos do PNCP e ComprasNet Contratos")
    parser.add_argument(
        "anos",
        nargs="*",
        type=int,
        default=None,
        help="Ano(s) dos dados (ex: 2026 ou 2025 2024). Padrão: PNCP_YEAR ou 2026"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    run(anos=args.anos)
