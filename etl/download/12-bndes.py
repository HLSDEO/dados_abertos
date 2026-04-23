"""
Download 12 - BNDES: Operações de Financiamento
Fonte: https://dadosabertos.bndes.gov.br/

Duas fontes de dados (CKAN resources):
  1. Operações não automáticas (revisadas manualmente):
     ID: 6f56b78c-510f-44b6-8274-78a5b7e931f4
     URL: https://dadosabertos.bndes.gov.br/datastore/dump/{resource_id}?format=csv

  2. Operações indiretas automáticas:
     ID: 612faa0b-b6be-4b2c-9317-da5dc2c0b901
     URL: https://dadosabertos.bndes.gov.br/datastore/dump/{resource_id}?format=csv

Saída:
  data/bndes/operacoes_nao_automaticas.csv
  data/bndes/operacoes_indiretas_automaticas.csv

Uso:
  python main.py download bndes
"""
import csv
import io
import logging
import os
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "bndes"
BASE_URL = "https://dadosabertos.bndes.gov.br/datastore/dump"

# Resource IDs (fixos no CKAN)
RESOURCES = {
    "nao_automaticas": {
        "id":   "6f56b78c-510f-44b6-8274-78a5b7e931f4",
        "nome": "operacoes_nao_automaticas",
        "desc": "Operações não automáticas (revisadas manualmente)",
    },
    "indiretas_automaticas": {
        "id":   "612faa0b-b6be-4b2c-9317-da5dc2c0b901",
        "nome": "operacoes_indiretas_automaticas",
        "desc": "Operações indiretas automáticas",
    },
}

FONTE = {
    "fonte_nome":      "BNDES — Banco Nacional de Desenvolvimento Econômico e Social",
    "fonte_descricao": "Operações de Financiamento",
    "fonte_url":       "https://dadosabertos.bndes.gov.br",
    "fonte_licenca":   "Licença Aberta para Bases de Dados (ODbL)",
}


def _download_csv(resource_id: str, retries: int = 3) -> bytes | None:
    """Baixa CSV via CKAN datastore dump."""
    url = f"{BASE_URL}/{resource_id}?format=csv"
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(
                url,
                timeout=300,
                headers={"User-Agent": "dados-abertos-etl/1.0"},
                allow_redirects=True,
            )
            if r.status_code == 404:
                log.warning(f"    404 — recurso não disponível")
                return None
            r.raise_for_status()
            log.info(f"    {len(r.content) / 1e6:.1f} MB baixados")
            return r.content
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(5)
    return None


def _process_csv(raw_bytes: bytes, resource_name: str, out_path: Path) -> int:
    """Processa CSV e salva normalizado."""
    text = raw_bytes.decode("utf-8", errors="replace")
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return 0

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return 0

    log.info(f"    Colunas ({len(reader.fieldnames)}): {reader.fieldnames[:6]}...")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    coletado = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = None
        for row in reader:
            row = {k: (v or "").strip() for k, v in row.items() if k is not None}

            # Adiciona metadados da fonte
            row.update({
                **FONTE,
                "fonte_url_origem": f"{BASE_URL}/{RESOURCES[resource_name]['id']}?format=csv",
                "fonte_coletado_em": coletado,
                "fonte_recurso":    RESOURCES[resource_name]["desc"],
            })

            if writer is None:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()), extrasaction="ignore")
                writer.writeheader()
            writer.writerow(row)
            rows_written += 1

    return rows_written


def run():
    """Baixa todos os recursos do BNDES."""
    log.info("[bndes] Iniciando download de operações de financiamento")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_baixados = 0
    total_pulados  = 0

    for key, info in RESOURCES.items():
        out_path = DATA_DIR / f"{info['nome']}.csv"

        if out_path.exists():
            log.info(f"  ✓ {info['nome']}.csv já existe — pulando")
            total_pulados += 1
            continue

        log.info(f"  === {info['desc']} ===")
        raw = _download_csv(info["id"])

        if not raw:
            log.warning(f"  {info['nome']}: falha no download")
            continue

        total = _process_csv(raw, key, out_path)
        if total > 0:
            log.info(f"  ✓ {info['nome']}.csv  ({total:,} registros)")
            total_baixados += 1
        else:
            out_path.unlink(missing_ok=True)

        time.sleep(2)

    log.info(f"[bndes] Concluído — baixados={total_baixados}  pulados={total_pulados}")
