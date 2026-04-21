"""
Download 9 - PGFN: Dívida Ativa da União
Fonte: https://portaldatransparencia.gov.br/download-de-dados/pgfn

O Portal publica atualizações mensais. O script descobre os ZIPs disponíveis
scrapeando a página de índice; se isso falhar, tenta as convenções de nome
conhecidas nos últimos LOOKBACK dias.

Três tipos de crédito, cada um em ZIP separado:
  PGFN_PREVIDENCIARIO_{YYYYMMDD}.zip
  PGFN_NAO_PREVIDENCIARIO_{YYYYMMDD}.zip
  PGFN_FGTS_{YYYYMMDD}.zip

Saída (por tipo, consolidada):
  data/pgfn/previdenciario.csv
  data/pgfn/nao_previdenciario.csv
  data/pgfn/fgts.csv

Arquivos já existentes são pulados (idempotente).

Uso:
  python main.py download pgfn
"""

import csv
import io
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "pgfn"
BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados/pgfn"
LOOKBACK = 60   # dias para trás na descoberta por força-bruta

FONTE = {
    "fonte_nome":      "PGFN — Procuradoria-Geral da Fazenda Nacional",
    "fonte_descricao": "Dívida Ativa da União",
    "fonte_url":       "https://portaldatransparencia.gov.br/download-de-dados/pgfn",
    "fonte_licenca":   "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
}

# Mapeamento de colunas → destino (primeiro valor não-vazio prevalece)
COL_MAP = [
    ("CPF OU CNPJ DO DEVEDOR",             "cpf_cnpj"),
    ("CPF_CNPJ",                            "cpf_cnpj"),
    ("TIPO DE PESSOA",                      "tipo_pessoa"),
    ("TIPO_PESSOA",                         "tipo_pessoa"),
    ("NOME DO DEVEDOR",                     "nome_devedor"),
    ("NOME_DEVEDOR",                        "nome_devedor"),
    ("NÚMERO DA INSCRIÇÃO",                 "numero_inscricao"),
    ("NUMERO_INSCRICAO",                    "numero_inscricao"),
    ("DATA DE INSCRIÇÃO NA DÍVIDA ATIVA",  "data_inscricao"),
    ("DATA_INSCRICAO",                      "data_inscricao"),
    ("TIPO DE SITUAÇÃO DA INSCRIÇÃO",       "situacao"),
    ("TIPO_SITUACAO_INSCRICAO",             "situacao"),
    ("SITUAÇÃO JURÍDICA DA INSCRIÇÃO",      "situacao_juridica"),
    ("SITUACAO_JURIDICA_INSCRICAO",         "situacao_juridica"),
    ("TIPO DE CRÉDITO",                     "tipo_credito"),
    ("TIPO_CREDITO",                        "tipo_credito"),
    ("RECEITA PRINCIPAL",                   "receita_principal"),
    ("RECEITA_PRINCIPAL",                   "receita_principal"),
    ("VALOR CONSOLIDADO",                   "valor_consolidado"),
    ("VALOR_CONSOLIDADO",                   "valor_consolidado"),
    ("INDICADOR AJUIZADO",                  "indicador_ajuizado"),
    ("INDICADOR_AJUIZADO",                  "indicador_ajuizado"),
    ("UF DO DEVEDOR",                       "uf_devedor"),
    ("UF_DEVEDOR",                          "uf_devedor"),
    ("MUNICÍPIO DO DEVEDOR",                "municipio_devedor"),
    ("MUNICIPIO_DEVEDOR",                   "municipio_devedor"),
]


# ── Descoberta de URLs ────────────────────────────────────────────────────────

def _discover_urls_from_page() -> list[str]:
    """
    Busca a página de índice do PGFN e extrai links .zip.
    O Portal da Transparência serve HTML estático com os links.
    """
    try:
        log.info(f"  Descobrindo arquivos em {BASE_URL} ...")
        r = requests.get(BASE_URL, timeout=30,
                         headers={"User-Agent": "dados-abertos-etl/1.0",
                                  "Accept": "text/html,application/xhtml+xml"})
        r.raise_for_status()
        found = re.findall(r'href=["\']([^"\']*\.zip)["\']', r.text, re.IGNORECASE)
        urls = []
        for u in found:
            if not u.startswith("http"):
                u = "https://portaldatransparencia.gov.br" + u
            if "pgfn" in u.lower():
                urls.append(u)
        log.info(f"  {len(urls)} link(s) encontrado(s) na página")
        return urls
    except Exception as exc:
        log.warning(f"  Descoberta automática falhou: {exc}")
        return []


def _brute_force_urls() -> list[str]:
    """
    Tenta as convenções de nome conhecidas nos últimos LOOKBACK dias.
    Retorna apenas as URLs que respondem com 200.
    """
    today = datetime.now()
    candidates = []
    for days_back in range(LOOKBACK):
        d = (today - timedelta(days=days_back)).strftime("%Y%m%d")
        for tipo in ("PGFN_PREVIDENCIARIO", "PGFN_NAO_PREVIDENCIARIO", "PGFN_FGTS"):
            # dois padrões de URL conhecidos no Portal da Transparência
            candidates.append(f"{BASE_URL}/{d}/{tipo}_{d}.zip")
            candidates.append(f"{BASE_URL}/{d}_{tipo}.zip")

    found = []
    seen_dates: set[str] = set()
    for url in candidates:
        # extrai data do URL para parar cedo quando encontrar uma competência completa
        m = re.search(r"(\d{8})", url)
        date_str = m.group(1) if m else ""

        try:
            r = requests.head(url, timeout=10, allow_redirects=True,
                              headers={"User-Agent": "dados-abertos-etl/1.0"})
            if r.status_code == 200:
                log.info(f"  ✓ encontrado: {url}")
                found.append(url)
                if date_str:
                    seen_dates.add(date_str)
        except requests.RequestException:
            pass

        # se já encontrou todos os 3 tipos de uma mesma data, para
        if len(seen_dates) == 1 and sum(1 for u in found if date_str in u) >= 3:
            break

    return found


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _download_to_tmp(url: str, retries: int = 3) -> Path | None:
    import tempfile
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(url, timeout=300, stream=True,
                             headers={"User-Agent": "dados-abertos-etl/1.0"},
                             allow_redirects=True)
            if r.status_code == 404:
                log.debug(f"    404 — {url}")
                return None
            r.raise_for_status()
            _, tmp_str = tempfile.mkstemp(suffix=".zip")
            tmp = Path(tmp_str)
            downloaded = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
            log.info(f"    {downloaded / 1e6:.1f} MB baixados")
            return tmp
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(10)
    return None


# ── Processamento ─────────────────────────────────────────────────────────────

def _remap_row(row: dict) -> dict:
    out: dict[str, str] = {}
    for orig, dest in COL_MAP:
        if orig in row:
            val = row[orig].strip()
            if val and dest not in out:
                out[dest] = val
    return out


def _tipo_from_url(url: str) -> str:
    u = url.upper()
    if "PREVIDENCIARIO" in u and "NAO" not in u:
        return "previdenciario"
    if "NAO_PREVIDENCIARIO" in u or "NAO%20PREVIDENCIARIO" in u:
        return "nao_previdenciario"
    if "FGTS" in u:
        return "fgts"
    return "outros"


def _process_zip(tmp: Path, url: str, out_path: Path) -> int:
    total = 0
    try:
        with zipfile.ZipFile(tmp) as zf:
            csvs = [n for n in zf.namelist()
                    if n.lower().endswith(".csv") and ".." not in n
                    and "__macosx" not in n.lower()]
        if not csvs:
            log.warning(f"    Nenhum CSV no ZIP: {url}")
            return 0

        mode = "a" if out_path.exists() else "w"
        writer = None

        for name in csvs:
            with zipfile.ZipFile(tmp) as zf:
                raw  = zf.read(name)
                text = raw.decode("latin-1", errors="replace")
                reader = csv.DictReader(io.StringIO(text), delimiter=";")

                if writer is None and reader.fieldnames:
                    log.info(f"    Colunas: {reader.fieldnames[:5]}...")

                with open(out_path, mode, newline="", encoding="utf-8-sig") as f:
                    for row in reader:
                        row = {k: (v or "").strip() for k, v in row.items()
                               if k is not None}
                        mapped = _remap_row(row)
                        if not mapped.get("cpf_cnpj"):
                            continue
                        mapped.update({**FONTE, "fonte_url_origem": url})

                        if writer is None:
                            writer = csv.DictWriter(
                                f, fieldnames=list(mapped.keys()),
                                extrasaction="ignore")
                            writer.writeheader()
                        else:
                            writer = csv.DictWriter(
                                f, fieldnames=list(mapped.keys()),
                                extrasaction="ignore")
                        writer.writerow(mapped)
                        total += 1
                        mode = "a"   # segunda iteração sempre appenda

            log.info(f"    {name}  (+{total:,} registros)")

    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido: {exc}")
    finally:
        tmp.unlink(missing_ok=True)
    return total


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. tenta descobrir via página de índice
    urls = _discover_urls_from_page()

    # 2. fallback: força-bruta nas datas recentes
    if not urls:
        log.info("  Tentando descoberta por força-bruta nas datas recentes...")
        urls = _brute_force_urls()

    if not urls:
        log.error(
            "[pgfn] Nenhum arquivo encontrado.\n"
            "  Acesse https://portaldatransparencia.gov.br/download-de-dados/pgfn\n"
            "  e verifique o nome exato dos arquivos disponíveis."
        )
        return

    # agrupa por tipo para evitar baixar o mesmo tipo duas vezes
    por_tipo: dict[str, str] = {}
    for url in urls:
        tipo = _tipo_from_url(url)
        por_tipo.setdefault(tipo, url)   # mantém primeira ocorrência (mais recente)

    for tipo, url in por_tipo.items():
        out_path = DATA_DIR / f"{tipo}.csv"
        if out_path.exists():
            log.info(f"  ✓ {out_path.name} já existe — pulando")
            continue

        log.info(f"  === {tipo.upper()} ===")
        tmp_zip = _download_to_tmp(url)
        if not tmp_zip:
            log.warning(f"  Falha ao baixar {url}")
            continue

        total = _process_zip(tmp_zip, url, out_path)
        log.info(f"  ✓ {out_path.name}  ({total:,} registros)")
        time.sleep(3)

    log.info("[pgfn] Download concluído")
