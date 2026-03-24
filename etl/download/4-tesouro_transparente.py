"""
Download 4 - Tesouro Transparente
Fonte: https://www.tesourotransparente.gov.br/ckan/

Baixa:
  Emendas parlamentares (individuais e de bancada)
  → data/tesouro_transparente/emendas.csv

Arquivo já existente é pulado (idempotente).

Uso:
  python main.py download tesouro_transparente
"""

import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "tesouro_transparente"

FONTE = {
    "fonte_nome":      "Tesouro Nacional — Tesouro Transparente",
    "fonte_descricao": "Tesouro Transparente - STN",
    "fonte_url":       "https://www.tesourotransparente.gov.br",
    "fonte_licenca":   "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
}

# ── Fontes de dados ───────────────────────────────────────────────────────────
ARQUIVOS = {
    "emendas": {
        "url": (
            "https://www.tesourotransparente.gov.br/ckan/dataset/"
            "83e419da-1552-46bf-bfc3-05160b2c46c9/resource/"
            "66d69917-a5d8-4500-b4b2-ef1f5d062430/download/"
            "emendas-parlamentares.csv"
        ),
        "descricao": "Emendas parlamentares (individuais e de bancada)",
        "encoding":  "utf-8-sig",
        "sep":       ";",
    },
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _download_text(url: str, retries: int = 3, delay: float = 5.0) -> bytes | None:
    """Download simples — arquivo CSV pequeno, sem necessidade de streaming."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(
                url, timeout=120,
                headers={"User-Agent": "dados-abertos-etl/1.0"},
            )
            if r.status_code == 404:
                log.warning(f"    404 — não disponível: {url}")
                return None
            r.raise_for_status()
            return r.content
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(delay)
    log.error(f"    Falha após {retries} tentativas: {url}")
    return None


# ── Processamento ─────────────────────────────────────────────────────────────

def _add_fonte(row: dict, url: str) -> dict:
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row.update({
        **FONTE,
        "fonte_url_origem":  url,
        "fonte_coletado_em": coletado,
    })
    return row


def _normalize_valor(s: str) -> str:
    """
    Normaliza valores monetários do Tesouro Transparente.
    Exemplos:
      "243750"     → "243750.00"
      "7,71167E+13"→ CNPJ em notação científica — mantém como string
      "1.234,56"   → "1234.56"
    """
    s = s.strip()
    if not s:
        return ""
    # notação científica (ex: CNPJs) — não converter
    if "E+" in s.upper() or "E-" in s.upper():
        return s
    # formato BR: ponto como milhar, vírgula como decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return str(float(s))
    except ValueError:
        return s


def _process(key: str, cfg: dict) -> None:
    out_path = DATA_DIR / f"{key}.csv"
    if out_path.exists():
        log.info(f"    ✓ {key}.csv já existe ({out_path.stat().st_size/1e6:.1f} MB) — pulando")
        return

    url     = cfg["url"]
    content = _download_text(url)
    if not content:
        return

    # detecta encoding verificando BOM nos primeiros bytes
    # UTF-16 LE: FF FE  |  UTF-16 BE: FE FF  |  UTF-8 BOM: EF BB BF
    if content[:2] in (b"\xff\xfe", b"\xfe\xff"):
        encs = ("utf-16", "utf-8-sig", "latin-1")
    elif content[:3] == b"\xef\xbb\xbf":
        encs = ("utf-8-sig", "latin-1", "utf-8")
    else:
        encs = ("utf-8-sig", cfg.get("encoding", "utf-8-sig"), "latin-1", "utf-8")

    text = None
    enc  = None
    for try_enc in encs:
        try:
            text = content.decode(try_enc).lstrip("\ufeff")
            enc  = try_enc
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    if text is None:
        log.error(f"    Não foi possível decodificar {key}.csv")
        return

    sep       = cfg.get("sep", ",")
    skip_rows = cfg.get("skip_rows", 0)

    lines = [l for l in text.splitlines() if l.strip()]  # remove linhas vazias

    # loga primeiras linhas para debug
    log.info(f"    Primeiras linhas do arquivo:")
    for i, l in enumerate(lines[:4]):
        log.info(f"      [{i}]: {repr(l[:120])}")

    # se skip_rows configurado, tenta pular linhas de título
    # mas verifica se a linha resultante realmente parece um cabeçalho (contém o separador)
    if skip_rows and len(lines) > skip_rows:
        candidate = lines[skip_rows]
        if sep in candidate:
            log.info(f"    Pulando {skip_rows} linha(s) de título")
            lines = lines[skip_rows:]
        else:
            # auto-detecta: procura a primeira linha que contenha o separador
            for idx, line in enumerate(lines):
                if sep in line:
                    if idx > 0:
                        log.info(f"    Auto-skip: pulando {idx} linha(s) de título")
                        lines = lines[idx:]
                    break

    # auto-detecta separador se o configurado não aparece na primeira linha
    if lines and sep not in lines[0]:
        for candidate_sep in (";", "\t", "|"):
            if candidate_sep in lines[0]:
                log.warning(f"    Separador '{sep}' não encontrado — usando '{candidate_sep}'")
                sep = candidate_sep
                break

    reader = csv.DictReader(lines, delimiter=sep)

    if not reader.fieldnames:
        log.warning(f"    {key}.csv sem cabeçalho detectado")
        return

    # UTF-16 pode gerar fieldnames com None ou colunas vazias — filtra
    fieldnames_clean = [f for f in reader.fieldnames if f is not None and f.strip()]
    if not fieldnames_clean:
        log.warning(f"    {key}.csv cabeçalho inválido: {reader.fieldnames}")
        return

    log.info(f"    Encoding: {enc}  sep: '{sep}'  colunas: {len(fieldnames_clean)}")
    log.info(f"    Colunas: {fieldnames_clean}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = None
        for row in reader:
            # remove chaves None geradas por colunas extras do CSV
            row = {k: v for k, v in row.items() if k is not None}
            # normaliza campo de valor se existir
            for col in list(row.keys()):
                if col and ("valor" in col.lower()):
                    row[col] = _normalize_valor(row[col])

            _add_fonte(row, url)

            if writer is None:
                writer = csv.DictWriter(
                    f,
                    fieldnames=list(row.keys()),
                    extrasaction="ignore",
                )
                writer.writeheader()

            writer.writerow(row)
            rows_written += 1

    log.info(f"    ✓ {key}.csv  ({rows_written:,} linhas)")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    log.info("[tesouro_transparente] Iniciando download")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for key, cfg in ARQUIVOS.items():
        log.info(f"  === {cfg['descricao']} ===")
        _process(key, cfg)
        time.sleep(1)

    log.info("[tesouro_transparente] Download concluído")