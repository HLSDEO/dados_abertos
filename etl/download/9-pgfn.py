"""
Download 9 - PGFN: Dívida Ativa da União
Fonte: https://dadosabertos.pgfn.gov.br/

Arquivos publicados por trimestre, 2020–2025:
  {ANO}_trimestre_{TRI}/Dados_abertos_Previdenciario.zip
  {ANO}_trimestre_{TRI}/Dados_abertos_Nao_Previdenciario.zip
  {ANO}_trimestre_{TRI}/Dados_abertos_FGTS.zip

Saída:
  data/pgfn/previdenciario_{ANO}_t{TRI}.csv
  data/pgfn/nao_previdenciario_{ANO}_t{TRI}.csv
  data/pgfn/fgts_{ANO}_t{TRI}.csv

Arquivos já existentes são pulados (idempotente).

Uso:
  python main.py download pgfn
"""

import csv
import io
import logging
import os
import time
import zipfile
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "pgfn"
BASE_URL = "https://dadosabertos.pgfn.gov.br"

ANO_INICIO = 2020
ANO_FIM    = 2025
TRIMESTRES = ["01", "02", "03", "04"]

TIPOS = {
    "previdenciario":    "Dados_abertos_Previdenciario.zip",
    "nao_previdenciario":"Dados_abertos_Nao_Previdenciario.zip",
    "fgts":              "Dados_abertos_FGTS.zip",
}

FONTE = {
    "fonte_nome":      "PGFN — Procuradoria-Geral da Fazenda Nacional",
    "fonte_descricao": "Dívida Ativa da União",
    "fonte_url":       "https://dadosabertos.pgfn.gov.br",
    "fonte_licenca":   "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
}

# Mapeamento de colunas → destino (primeiro valor não-vazio prevalece)
COL_MAP = [
    ("CPF_CNPJ",                            "cpf_cnpj"),
    ("CPF OU CNPJ DO DEVEDOR",              "cpf_cnpj"),
    ("TIPO_PESSOA",                         "tipo_pessoa"),
    ("TIPO DE PESSOA",                      "tipo_pessoa"),
    ("NOME_DEVEDOR",                        "nome_devedor"),
    ("NOME DO DEVEDOR",                     "nome_devedor"),
    ("NUMERO_INSCRICAO",                    "numero_inscricao"),
    ("NÚMERO DA INSCRIÇÃO",                 "numero_inscricao"),
    ("DATA_INSCRICAO",                      "data_inscricao"),
    ("DATA DE INSCRIÇÃO NA DÍVIDA ATIVA",   "data_inscricao"),
    ("TIPO_SITUACAO_INSCRICAO",             "situacao"),
    ("TIPO DE SITUAÇÃO DA INSCRIÇÃO",       "situacao"),
    ("SITUACAO_JURIDICA_INSCRICAO",         "situacao_juridica"),
    ("SITUAÇÃO JURÍDICA DA INSCRIÇÃO",      "situacao_juridica"),
    ("TIPO_CREDITO",                        "tipo_credito"),
    ("TIPO DE CRÉDITO",                     "tipo_credito"),
    ("RECEITA_PRINCIPAL",                   "receita_principal"),
    ("RECEITA PRINCIPAL",                   "receita_principal"),
    ("VALOR_CONSOLIDADO",                   "valor_consolidado"),
    ("VALOR CONSOLIDADO",                   "valor_consolidado"),
    ("INDICADOR_AJUIZADO",                  "indicador_ajuizado"),
    ("INDICADOR AJUIZADO",                  "indicador_ajuizado"),
    ("UF_DEVEDOR",                          "uf_devedor"),
    ("UF DO DEVEDOR",                       "uf_devedor"),
    ("MUNICIPIO_DEVEDOR",                   "municipio_devedor"),
    ("MUNICÍPIO DO DEVEDOR",                "municipio_devedor"),
]


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
                log.debug(f"    404 — não disponível: {url}")
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


def _process_zip(tmp: Path, url: str, competencia: str, out_path: Path) -> int:
    total = 0
    try:
        with zipfile.ZipFile(tmp) as zf:
            csvs = [n for n in zf.namelist()
                    if n.lower().endswith(".csv") and ".." not in n
                    and "__macosx" not in n.lower()]

        if not csvs:
            log.warning(f"    Nenhum CSV no ZIP: {url}")
            return 0

        out_path.parent.mkdir(parents=True, exist_ok=True)
        writer = None

        for name in csvs:
            with zipfile.ZipFile(tmp) as zf:
                raw  = zf.read(name)
                text = raw.decode("latin-1", errors="replace")
                reader = csv.DictReader(io.StringIO(text), delimiter=";")

                if reader.fieldnames:
                    log.info(f"    Colunas: {reader.fieldnames[:5]}...")

                with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
                    for row in reader:
                        row = {k: (v or "").strip() for k, v in row.items()
                               if k is not None}
                        mapped = _remap_row(row)
                        if not mapped.get("cpf_cnpj"):
                            continue
                        mapped.update({**FONTE,
                                       "fonte_url_origem": url,
                                       "competencia":      competencia})

                        if writer is None:
                            writer = csv.DictWriter(
                                f, fieldnames=list(mapped.keys()),
                                extrasaction="ignore")
                            writer.writeheader()
                        writer.writerow(mapped)
                        total += 1

            log.info(f"    {name}: {total:,} registros")

    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido: {exc}")
        out_path.unlink(missing_ok=True)
    finally:
        tmp.unlink(missing_ok=True)
    return total


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_baixados = 0
    total_pulados  = 0

    for ano in range(ANO_FIM, ANO_INICIO - 1, -1):   # mais recente primeiro
        for tri in TRIMESTRES:
            competencia  = f"{ano}_t{tri}"
            path_prefix  = f"{ano}_trimestre_{tri}"

            for tipo, filename in TIPOS.items():
                out_path = DATA_DIR / f"{tipo}_{competencia}.csv"

                if out_path.exists():
                    log.info(f"  ✓ {out_path.name} já existe — pulando")
                    total_pulados += 1
                    continue

                url     = f"{BASE_URL}/{path_prefix}/{filename}"
                tmp_zip = _download_to_tmp(url)

                if not tmp_zip:
                    continue   # trimestre ainda não publicado — normal para o atual

                total = _process_zip(tmp_zip, url, competencia, out_path)
                if total > 0:
                    log.info(f"  ✓ {out_path.name}  ({total:,} registros)")
                    total_baixados += 1
                else:
                    out_path.unlink(missing_ok=True)

                time.sleep(2)

    log.info(f"[pgfn] Concluído — baixados={total_baixados}  pulados={total_pulados}")
