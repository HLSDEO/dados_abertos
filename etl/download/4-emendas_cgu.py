"""
Download 4 - Emendas Parlamentares CGU / Portal da Transparência
Fonte: https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/emendas-parlamentares/

Baixa o ZIP e extrai 3 CSVs:
  EmendasParlamentares.csv         → data/emendas_cgu/emendas.csv
  EmendasParlamentaresConvenios.csv → data/emendas_cgu/convenios.csv
  EmendasParlamentaresDespesas.csv  → data/emendas_cgu/despesas.csv

O ZIP é atualizado periodicamente com a série histórica completa.
Arquivo já existente é pulado (idempotente).

Uso:
  python main.py download emendas_cgu
"""

import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "emendas_cgu"

ZIP_URL = (
    "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/"
    "saida/emendas-parlamentares/EmendasParlamentares.zip"
)

FONTE = {
    "fonte_nome":      "CGU — Controladoria-Geral da União",
    "fonte_descricao": "Portal da Transparência — Emendas Parlamentares",
    "fonte_url":       "https://portaldatransparencia.gov.br",
    "fonte_licenca":   "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
}

# Mapeamento arquivo no ZIP → nome de saída
# Os nomes exatos dentro do ZIP podem variar; usamos correspondência parcial
ARQUIVOS = {
    "emendas":   "EmendasParlamentares",        # arquivo principal
    "convenios": "EmendasParlamentaresConvenios",
    "despesas":  "EmendasParlamentaresDespesas",
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _download_to_tmp(url: str, retries: int = 3,
                     delay: float = 10.0) -> Path | None:
    """Download em streaming para arquivo temporário."""
    import tempfile
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(url, timeout=300, stream=True,
                             headers={"User-Agent": "dados-abertos-etl/1.0"})
            if r.status_code == 404:
                log.warning(f"    404 — não disponível")
                return None
            r.raise_for_status()
            tmp = Path(tempfile.mktemp(suffix=".zip"))
            downloaded = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
            log.info(f"    Baixado: {downloaded / 1e6:.1f} MB")
            return tmp
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(delay)
    log.error(f"    Falha após {retries} tentativas")
    return None


# ── Processamento ─────────────────────────────────────────────────────────────

def _add_fonte(row: dict, url: str) -> dict:
    row.update({
        **FONTE,
        "fonte_url_origem":  url,
        "fonte_coletado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    return row


def _normalize_valor(s: str) -> str:
    """Converte formato BR (1.234,56) para float string."""
    s = (s or "").strip()
    if not s:
        return ""
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return str(float(s))
    except ValueError:
        return s


COLUNAS_VALOR = {
    "Valor Empenhado", "Valor Liquidado", "Valor Pago",
    "Valor Restos A Pagar Inscritos", "Valor Restos A Pagar Cancelados",
    "Valor Restos A Pagar Pagos",
}


def _extract_csv(tmp_zip: Path, name_hint: str, out_path: Path) -> int:
    """Extrai um CSV do ZIP pelo nome parcial e salva normalizado."""
    with zipfile.ZipFile(tmp_zip) as zf:
        members = zf.namelist()
        target = next(
            (m for m in members
             if name_hint.lower() in m.lower() and m.lower().endswith(".csv")),
            None,
        )
        if not target:
            log.warning(f"    '{name_hint}' não encontrado no ZIP. Disponíveis: {members}")
            return 0

        log.info(f"    Extraindo {target}")
        raw  = zf.read(target)

    # detecta encoding pelo BOM
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw.decode("utf-16").lstrip("\ufeff")
    elif raw[:3] == b"\xef\xbb\xbf":
        text = raw.decode("utf-8-sig")
    else:
        text = raw.decode("latin-1", errors="replace")

    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        log.warning(f"    {name_hint}: arquivo vazio")
        return 0

    # detecta separador
    sep = ";"
    if lines[0] and ";" not in lines[0] and "\t" in lines[0]:
        sep = "\t"

    reader = csv.DictReader(lines, delimiter=sep)
    if not reader.fieldnames:
        log.warning(f"    {name_hint}: sem cabeçalho")
        return 0

    log.info(f"    Colunas ({len(reader.fieldnames)}): {reader.fieldnames[:6]}...")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = None
        for row in reader:
            row = {k: v for k, v in row.items() if k is not None}
            # normaliza valores monetários
            for col in COLUNAS_VALOR:
                if col in row:
                    row[col] = _normalize_valor(row[col])
            _add_fonte(row, ZIP_URL)
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()),
                                        extrasaction="ignore")
                writer.writeheader()
            writer.writerow(row)
            rows_written += 1

    return rows_written


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    log.info("[emendas_cgu] Iniciando download")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # verifica se todos já existem
    todos_existem = all(
        (DATA_DIR / f"{nome}.csv").exists()
        for nome in ARQUIVOS
    )
    if todos_existem:
        for nome in ARQUIVOS:
            p = DATA_DIR / f"{nome}.csv"
            log.info(f"  ✓ {nome}.csv já existe ({p.stat().st_size/1e6:.1f} MB) — pulando")
        return

    tmp_zip = _download_to_tmp(ZIP_URL)
    if not tmp_zip:
        return

    try:
        for nome, hint in ARQUIVOS.items():
            out_path = DATA_DIR / f"{nome}.csv"
            if out_path.exists():
                log.info(f"  ✓ {nome}.csv já existe ({out_path.stat().st_size/1e6:.1f} MB) — pulando")
                continue
            log.info(f"  === {nome} ===")
            total = _extract_csv(tmp_zip, hint, out_path)
            log.info(f"  ✓ {nome}.csv  ({total:,} linhas)")
            time.sleep(0.5)
    finally:
        tmp_zip.unlink(missing_ok=True)

    log.info("[emendas_cgu] Download concluído")