"""
Download 1 - IBGE Localidades
Baixa da API pública do IBGE e salva CSVs brutos em data/ibge/
Cada linha do CSV carrega colunas de rastreabilidade da fonte.
"""

import csv
import os
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "ibge"

# Metadados da fonte — altere aqui se a fonte mudar
FONTE = {
    "fonte_nome":      "IBGE",
    "fonte_descricao": "Instituto Brasileiro de Geografia e Estatística",
    "fonte_licenca":   "Dados Abertos — https://www.ibge.gov.br/acesso-informacao/dados-abertos.html",
}

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


def _flatten(record: dict, prefix: str = "") -> dict:
    """
    Achata um dict aninhado usando underscore como separador.
    Campos None são mantidos como string vazia para garantir
    colunas consistentes entre todos os registros.
    """
    out = {}
    for k, v in record.items():
        key = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten(v, prefix=f"{key}_"))
        elif v is None:
            out[key] = ""          # None → string vazia, coluna sempre presente
        else:
            out[key] = v
    return out


def _save_csv(name: str, url: str, rows: list[dict]) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.csv"
    coletado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    flat = [_flatten(r) for r in rows]

    # injeta colunas de rastreabilidade em cada linha
    meta = {**FONTE, "fonte_url": url, "fonte_coletado_em": coletado_em}
    for row in flat:
        row.update(meta)

    # união de todas as chaves para cobrir registros com campos opcionais
    fieldnames = list(dict.fromkeys(k for row in flat for k in row))

    # garante que linhas com campos ausentes recebam string vazia
    for row in flat:
        for f in fieldnames:
            row.setdefault(f, "")

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
        _save_csv(name, url, data)
    log.info("[ibge] Download concluído")