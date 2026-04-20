"""
Download 7 - Sanções CGU (CEIS + CNEP) / Portal da Transparência
Fonte: https://portaldatransparencia.gov.br/download-de-dados

Baixa os dois registros de sanções publicados pela CGU:
  CEIS — Cadastro de Empresas Inidôneas e Suspensas
  CNEP — Cadastro Nacional de Empresas Punidas

Cada registro é publicado como ZIP contendo CSV separado por ; em latin-1.
O portal publica com data no nome — tenta a data mais recente e recua até
30 dias caso não encontre.

Saída:
  data/sancoes_cgu/ceis.csv
  data/sancoes_cgu/cnep.csv

Uso:
  python main.py download sancoes_cgu
"""

import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "sancoes_cgu"
BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados"
LOOKBACK = 30   # dias para recuar buscando a publicação mais recente

FONTE = {
    "fonte_nome":      "CGU — Controladoria-Geral da União",
    "fonte_descricao": "Portal da Transparência — Sanções (CEIS/CNEP)",
    "fonte_url":       "https://portaldatransparencia.gov.br",
    "fonte_licenca":   "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
}

# ── Mapeamento de colunas governo → destino ───────────────────────────────────
# Pares (coluna_original, coluna_destino).
# Para o mesmo destino, o primeiro valor não-vazio prevalece (fallback).
COL_MAP = [
    ("CPF OU CNPJ DO SANCIONADO",        "cpf_cnpj"),
    ("NOME DO SANCIONADO",               "nome"),
    ("RAZÃO SOCIAL - CADASTRO RECEITA",  "nome"),        # fallback
    ("NOME INFORMADO PELO ÓRGÃO SANCIONADOR", "nome"),   # fallback 2
    ("CATEGORIA DA SANÇÃO",              "tipo_sancao"),
    ("TIPO DE SANÇÃO",                   "tipo_sancao"), # fallback
    ("DATA INÍCIO SANÇÃO",               "data_inicio"),
    ("DATA FINAL SANÇÃO",                "data_fim"),
    ("DATA FIM SANÇÃO",                  "data_fim"),    # fallback
    ("FUNDAMENTAÇÃO LEGAL",              "fundamentacao"),
    ("NÚMERO DO PROCESSO",               "numero_processo"),
    ("ÓRGÃO SANCIONADOR",                "orgao_sancionador"),
    ("UF ÓRGÃO SANCIONADOR",             "uf_orgao"),
    ("ESFERA DE GOVERNO",                "esfera_governo"),
    ("VALOR DA MULTA",                   "valor_multa"),
]


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _download_to_tmp(url: str, retries: int = 3,
                     delay: float = 5.0) -> Path | None:
    import tempfile
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(url, timeout=300, stream=True,
                             headers={"User-Agent": "dados-abertos-etl/1.0"},
                             allow_redirects=True)
            if r.status_code == 404:
                return None  # data não disponível — tenta próxima
            r.raise_for_status()
            tmp = Path(tempfile.mktemp(suffix=".zip"))
            downloaded = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
            log.info(f"    {downloaded/1e6:.1f} MB baixados")
            return tmp
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                backoff = delay * (2 ** (attempt - 1))
                time.sleep(backoff)
    return None


# ── Processamento ─────────────────────────────────────────────────────────────

def _add_fonte(row: dict, url: str, dataset: str, data_ref: str) -> dict:
    row.update({
        **FONTE,
        "fonte_dataset":     dataset.upper(),
        "fonte_data_ref":    data_ref,
        "fonte_url_origem":  url,
        "fonte_coletado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    return row


def _remap_row(row: dict) -> dict:
    """Aplica COL_MAP: primeiro valor não-vazio para cada destino prevalece."""
    out = {}
    for orig, dest in COL_MAP:
        if orig not in row:
            continue
        val = row[orig].strip()
        if dest not in out or not out[dest]:
            out[dest] = val
    return out


def _extract_and_process(tmp_zip: Path, url: str,
                          dataset: str, data_ref: str,
                          out_path: Path) -> int:
    """Extrai CSV do ZIP, remapeia colunas e salva normalizado."""
    with zipfile.ZipFile(tmp_zip) as zf:
        # ZIP traversal guard + pega primeiro CSV
        members = [m for m in zf.namelist()
                   if m.lower().endswith(".csv") and ".." not in m
                   and "__macosx" not in m.lower()]
        if not members:
            log.warning(f"    Nenhum CSV no ZIP de {dataset}")
            return 0
        target = members[0]
        log.info(f"    Extraindo {target}")
        raw = zf.read(target)

    text   = raw.decode("latin-1", errors="replace")
    lines  = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return 0

    reader = csv.DictReader(lines, delimiter=";")
    if not reader.fieldnames:
        return 0

    log.info(f"    Colunas ({len(reader.fieldnames)}): {reader.fieldnames[:6]}...")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = None
        for row in reader:
            row = {k: (v or "").strip() for k, v in row.items() if k is not None}
            mapped = _remap_row(row)
            if not mapped.get("cpf_cnpj") and not mapped.get("nome"):
                continue
            _add_fonte(mapped, url, dataset, data_ref)

            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(mapped.keys()),
                                        extrasaction="ignore")
                writer.writeheader()
            writer.writerow(mapped)
            rows_written += 1

    return rows_written


def _process_dataset(dataset: str) -> None:
    out_path = DATA_DIR / f"{dataset}.csv"
    if out_path.exists():
        log.info(f"  ✓ {dataset}.csv já existe ({out_path.stat().st_size/1e6:.1f} MB) — pulando")
        return

    today = datetime.now()
    for days_back in range(LOOKBACK):
        data_ref = (today - timedelta(days=days_back)).strftime("%Y%m%d")
        url      = f"{BASE_URL}/{dataset}/{data_ref}"
        tmp_zip  = _download_to_tmp(url)
        if not tmp_zip:
            continue  # data não disponível — tenta dia anterior

        try:
            total = _extract_and_process(tmp_zip, url, dataset, data_ref, out_path)
            if total > 0:
                log.info(f"  ✓ {dataset}.csv  ({total:,} linhas)  data_ref={data_ref}")
                return
        finally:
            tmp_zip.unlink(missing_ok=True)

    log.error(f"  {dataset}: não foi possível baixar nos últimos {LOOKBACK} dias")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    log.info("[sancoes_cgu] Iniciando download")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for dataset in ("ceis", "cnep"):
        log.info(f"  === {dataset.upper()} ===")
        _process_dataset(dataset)
        time.sleep(2)

    log.info("[sancoes_cgu] Download concluído")