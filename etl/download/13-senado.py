"""
Download 13 - Senado Federal: Despesas CEAP
Fonte: https://adm.senado.gov.br/adm-dados-abertos/

API REST: /senadores/despesas_ceaps/{ano}/csv
  Parâmetro: ano (no path)
  Retorno: CSV com separador ';' e encoding Latin-1

Saída:
  data/senado/despesas_{ano}.csv

Uso:
  python main.py download senado
  python main.py download senado --ano 2024
  python main.py download senado --ano 2023 --ano 2024
"""
import csv
import io
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "senado"
BASE_URL = "https://adm.senado.gov.br/adm-dadosabertos/api/v1"
ANO_INICIO = 2008
ANO_FIM    = datetime.now().year

FONTE = {
    "fonte_nome":      "Senado Federal",
    "fonte_descricao": "Despesas CEAP — Cota para Exercício da Atividade Parlamentar",
    "fonte_url":       "https://adm.senado.gov.br",
    "fonte_licenca":   "Dados Abertos — https://www.senado.leg.br/transparencia",
}


def _download_ano(ano: int, retries: int = 3) -> bytes | None:
    """Baixa o CSV bruto de um ano via API REST."""
    url = f"{BASE_URL}/senadores/despesas_ceaps/{ano}/csv"

    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(
                url,
                timeout=120,
                headers={"User-Agent": "dados-abertos-etl/1.0"},
                allow_redirects=True,
            )
            if r.status_code == 404:
                log.debug(f"    404 — ano {ano} não disponível")
                return None
            r.raise_for_status()
            log.info(f"    {len(r.content) / 1e3:.1f} KB recebidos")
            return r.content
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(5)
    return None


def _process_csv(raw: bytes, ano: int, out_path: Path) -> int:
    """
    Decodifica o CSV em Latin-1, normaliza e salva em UTF-8-sig.
    Adiciona colunas de metadados ao final de cada linha.
    """
    if not raw:
        return 0

    # A API retorna Latin-1 (Windows-1252); acentos corrompidos se lido como UTF-8
    text = raw.decode("latin-1", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")

    if not reader.fieldnames:
        log.warning(f"    CSV sem cabeçalho para o ano {ano}")
        return 0

    url = f"{BASE_URL}/senadores/despesas_ceaps/{ano}/csv"
    coletado = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    meta_cols = {
        **FONTE,
        "fonte_url_origem": url,
        "coletado_em":      coletado,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = None

        for row in reader:
            # limpa espaços e None keys vindos de colunas extras
            row = {k.strip(): (v or "").strip() for k, v in row.items() if k}
            row.update(meta_cols)

            if writer is None:
                writer = csv.DictWriter(
                    f,
                    fieldnames=list(row.keys()),
                    extrasaction="ignore",
                )
                writer.writeheader()

            writer.writerow(row)
            total += 1

    return total


def run(anos: list[int] | None = None):
    """
    anos: lista de anos. None = todos de 2008 até o ano corrente.
    """
    log.info("[senado] Iniciando download de despesas CEAP")

    if not anos:
        anos = list(range(ANO_FIM, ANO_INICIO - 1, -1))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"  Anos: {anos}")

    total_baixados = 0
    total_pulados  = 0

    for ano in anos:
        out_path = DATA_DIR / f"despesas_{ano}.csv"

        if out_path.exists():
            log.info(f"  ✓ despesas_{ano}.csv já existe — pulando")
            total_pulados += 1
            continue

        raw = _download_ano(ano)
        if raw is None:
            log.warning(f"  Ano {ano}: não disponível (404 ou erro)")
            continue

        total = _process_csv(raw, ano, out_path)
        if total > 0:
            log.info(f"  ✓ despesas_{ano}.csv  ({total:,} registros)")
            total_baixados += 1
        else:
            out_path.unlink(missing_ok=True)

        time.sleep(2)

    log.info(f"[senado] Concluído — baixados={total_baixados}  pulados={total_pulados}")