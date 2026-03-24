"""
Download 3 - Cartão de Pagamento do Governo Federal (CPGF)
Fonte 1 — CGU dados abertos:
  https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/cpgf/{YYYYMM}_CPGF.zip
  Dados: gastos com cartão corporativo por portador/favorecido

Fonte 2 — Portal da Transparência (Compras Centralizadas):
  https://portaldatransparencia.gov.br/download-de-dados/cpcc/{YYYYMM}
  Dados: compras centralizadas com cartão corporativo

Baixa os últimos 4 anos (mês a mês), extrai os ZIPs e consolida em:
  data/cpgf/cpgf.csv          ← Fonte 1 consolidada
  data/cpgf/cpcc.csv          ← Fonte 2 consolidada

Colunas adicionadas:
  fonte_nome, fonte_url, fonte_arquivo, fonte_ano, fonte_mes, fonte_coletado_em

Uso:
  python main.py download cpgf
"""

import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime, date, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "cpgf"

# ── Janela de tempo ───────────────────────────────────────────────────────────
ANOS = 4   # quantos anos para trás baixar

# ── Fontes ────────────────────────────────────────────────────────────────────
FONTES = {
    "cpgf": {
        "nome":    "CGU — Cartão de Pagamento do Governo Federal",
        "url_tpl": "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/cpgf/{yyyymm}_CPGF.zip",
        "sep":     ";",
        "enc":     "latin-1",
        "out":     "cpgf.csv",
    },
    "cpcc": {
        "nome":    "CGU — CPGF Compras Centralizadas",
        "url_tpl": "https://portaldatransparencia.gov.br/download-de-dados/cpcc/{yyyymm}",
        "sep":     ";",
        "enc":     "latin-1",
        "out":     "cpcc.csv",
    },
}

FONTE_META = {
    "fonte_nome":      "",   # preenchido por fonte
    "fonte_url":       "",
    "fonte_arquivo":   "",
    "fonte_ano":       "",
    "fonte_mes":       "",
    "fonte_coletado_em": "",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _periodos(anos: int) -> list[tuple[int, int]]:
    """Retorna lista de (ano, mes) dos últimos N anos até o mês atual."""
    hoje = date.today()
    result = []
    ano, mes = hoje.year, hoje.month
    for _ in range(anos * 12):
        result.append((ano, mes))
        mes -= 1
        if mes == 0:
            mes = 12
            ano -= 1
    return list(reversed(result))


def _download(url: str, retries: int = 3, delay: float = 5.0) -> bytes | None:
    """Faz GET com retry. Retorna None se 404 (mês ainda não publicado)."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  (tentativa {attempt}/{retries})")
            r = requests.get(url, timeout=60, headers={"User-Agent": "dados-abertos-etl/1.0"})
            if r.status_code == 404:
                log.warning(f"    404 — arquivo não disponível: {url}")
                return None
            r.raise_for_status()
            return r.content
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(delay)
    log.error(f"    Falha após {retries} tentativas: {url}")
    return None


def _extract_csv_from_zip(content: bytes, sep: str, enc: str) -> list[dict] | None:
    """Extrai o primeiro CSV de um ZIP e retorna lista de dicts."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = [n for n in zf.namelist() if not n.startswith("__MACOSX")]
            if not names:
                log.warning("    ZIP vazio")
                return None
            # pega o primeiro arquivo (pode ter mais de um em cpcc)
            csv_name = names[0]
            log.info(f"    Extraindo {csv_name} ({len(names)} arquivo(s) no ZIP)")
            raw = zf.read(csv_name)
            text = raw.decode(enc, errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=sep)
            return list(reader)
    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido: {exc}")
        return None


def _add_meta(rows: list[dict], fonte_nome: str, url: str,
              arquivo: str, ano: int, mes: int) -> list[dict]:
    """Injeta colunas de rastreabilidade em cada linha."""
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "fonte_nome":       fonte_nome,
        "fonte_url":        url,
        "fonte_arquivo":    arquivo,
        "fonte_ano":        str(ano),
        "fonte_mes":        f"{mes:02d}",
        "fonte_coletado_em": coletado,
    }
    for row in rows:
        row.update(meta)
    return rows


class _CsvAppender:
    """Abre (ou cria) um CSV e permite adicionar linhas incrementalmente."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path     = path
        self._file     = open(path, "w", newline="", encoding="utf-8-sig")
        self._writer   = None
        self._total    = 0

    def append(self, rows: list[dict]) -> None:
        if not rows:
            return
        if self._writer is None:
            self._writer = csv.DictWriter(
                self._file,
                fieldnames=list(rows[0].keys()),
                extrasaction="ignore",
            )
            self._writer.writeheader()
        # garante que todas as linhas têm os mesmos campos
        for row in rows:
            for f in self._writer.fieldnames:
                row.setdefault(f, "")
        self._writer.writerows(rows)
        self._total += len(rows)

    def close(self) -> int:
        self._file.close()
        return self._total


# ── Processamento por fonte ───────────────────────────────────────────────────

def _process_fonte(key: str, cfg: dict, periodos: list[tuple[int, int]]) -> None:
    out_path = DATA_DIR / cfg["out"]
    appender = _CsvAppender(out_path)
    ok, skip, fail = 0, 0, 0

    for ano, mes in periodos:
        yyyymm = f"{ano}{mes:02d}"
        url    = cfg["url_tpl"].format(yyyymm=yyyymm)

        content = _download(url)
        if content is None:
            skip += 1
            continue

        rows = _extract_csv_from_zip(content, cfg["sep"], cfg["enc"])
        if rows is None:
            fail += 1
            continue

        rows = _add_meta(rows, cfg["nome"], url, f"{yyyymm}_{key.upper()}", ano, mes)
        appender.append(rows)
        log.info(f"    ✓ {yyyymm}  {len(rows):,} linhas")
        ok += 1

        # pausa entre requisições para não sobrecarregar o servidor
        time.sleep(1)

    total = appender.close()
    log.info(
        f"  [{key}] concluído — ok={ok}  ignorados={skip}  erros={fail}  "
        f"total={total:,} linhas → {out_path}"
    )


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    periodos = _periodos(ANOS)
    log.info(
        f"[cpgf] Baixando {len(periodos)} meses "
        f"({periodos[0][0]}/{periodos[0][1]:02d} → "
        f"{periodos[-1][0]}/{periodos[-1][1]:02d})"
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for key, cfg in FONTES.items():
        log.info(f"  === {cfg['nome']} ===")
        _process_fonte(key, cfg, periodos)

    log.info("[cpgf] Download concluído")