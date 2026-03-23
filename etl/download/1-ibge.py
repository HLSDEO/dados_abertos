"""
Download 1 - IBGE Localidades
Baixa da API pública do IBGE e salva CSVs brutos em data/ibge/
"""

import csv
import time
import logging
from pathlib import Path

import requests

log = logging.getLogger(__name__)

BASE_URL  = "https://servicodados.ibge.gov.br/api/v1/localidades"
DATA_DIR  = Path(__file__).resolve().parents[2] / "data" / "ibge"

ENDPOINTS = {
    "regioes":       f"{BASE_URL}/regioes",
    "estados":       f"{BASE_URL}/estados",
    "mesorregioes":  f"{BASE_URL}/mesorregioes",
    "microrregioes": f"{BASE_URL}/microrregioes",
    "municipios":    f"{BASE_URL}/municipios",
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _fetch(url: str, retries: int = 3, delay: float = 2.0) -> list:
    for attempt in range(1, retries + 1):
        try:
            log.info(f"  GET {url}  (tentativa {attempt}/{retries})")
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            log.warning(f"  Erro: {exc}")
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Falha após {retries} tentativas: {url}")


def _flatten(record: dict) -> dict:
    """Achata um dict aninhado com underscore como separador."""
    out = {}
    def _walk(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _walk(v, f"{prefix}{k}_" if prefix else f"{k}_")
        else:
            out[prefix.rstrip("_")] = obj
    _walk(record)
    return out


def _save_csv(name: str, rows: list[dict]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.csv"
    flat = [_flatten(r) for r in rows]
    fieldnames = list(flat[0].keys()) if flat else []
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat)
    log.info(f"  Salvo → {path}  ({len(flat):,} registros, {len(fieldnames)} colunas)")
    return path


# ── entry-point ───────────────────────────────────────────────────────────────

def run():
    log.info("[ibge] Iniciando download")
    for name, url in ENDPOINTS.items():
        data = _fetch(url)
        _save_csv(name, data)
    log.info("[ibge] Download concluído")