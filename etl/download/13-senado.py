"""
Download 13 - Senado Federal: Despesas CEAP
Fonte: https://adm.senado.gov.br/adm-dados-abertos/

API REST: /Senadores/buscarDespesasCeapsPorAno
  Parâmetro: ano (obrigatório)
  Retorno: JSON com despesas dos senadores

Saída:
  data/senado/despesas_{ano}.json

Uso:
  python main.py download senado
  python main.py download senado --ano 2024
  python main.py download senado --ano 2023 --ano 2024
"""
import json
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


def _download_ano(ano: int, retries: int = 3) -> dict | None:
    """Baixa despesas de um ano via API REST."""
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
            data = r.json()
            log.info(f"    {len(str(data)) / 1e3:.1f} KB recebidos")
            return data
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(5)
    return None


def _process_ano(data: dict, ano: int, out_path: Path) -> int:
    """Processa JSON e salva em JSON (preservando estrutura)."""
    if not data:
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    coletado = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{BASE_URL}/senadores/despesas_ceaps/{ano}/csv"

    # Adiciona metadados
    wrapped = {
        "metadata": {
            **FONTE,
            "ano":        ano,
            "coletado_em": coletado,
            "fonte_url_origem": url,
        },
        "data": data,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)

    total = len(data) if isinstance(data, list) else 1
    log.info(f"    {total} registros processados")
    return total


def run(anos: list[int] | None = None):
    """
    anos: lista de anos. None = todos de 2008 até hoje.
    """
    log.info("[senado] Iniciando download de despesas CEAP")

    if not anos:
        anos = list(range(ANO_FIM, ANO_INICIO - 1, -1))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"  Anos: {anos}")

    total_baixados = 0
    total_pulados  = 0

    for ano in anos:
        out_path = DATA_DIR / f"despesas_{ano}.json"

        if out_path.exists():
            log.info(f"  ✓ despesas_{ano}.json já existe — pulando")
            total_pulados += 1
            continue

        data = _download_ano(ano)
        if not data:
            log.warning(f"  Ano {ano}: não disponível (404 ou erro)")
            continue

        total = _process_ano(data, ano, out_path)
        if total > 0:
            log.info(f"  ✓ despesas_{ano}.json  ({total:,} registros)")
            total_baixados += 1
        else:
            out_path.unlink(missing_ok=True)

        time.sleep(2)

    log.info(f"[senado] Concluído — baixados={total_baixados}  pulados={total_pulados}")
