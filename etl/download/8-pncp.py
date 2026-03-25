"""
Download 8 - PNCP: Portal Nacional de Contratações Públicas
API oficial: https://pncp.gov.br/api/consulta/v1/

Baixa em duas fases independentes por janela de datas:

  Fase 1 — Licitações/Editais
    GET /contratacoes/publicacao
      ?dataInicial=YYYYMMDD&dataFinal=YYYYMMDD
      &codigoModalidadeContratacao={1..13}
      &pagina=N&tamanhoPagina=50
    → data/pncp/editais/pncp_editais_{YYYYMM}.json

  Fase 2 — Contratos
    GET /contratos
      ?dataInicial=YYYYMMDD&dataFinal=YYYYMMDD
      &pagina=N&tamanhoPagina=500
    → data/pncp/contratos/pncp_contratos_{YYYYMM}.json

Ambas as fases são checkpointed por janela — retomáveis após interrupção.
Os arquivos mensais são merge+dedup com execuções anteriores (idempotente).

Uso:
  python main.py download pncp                          # mês atual
  python main.py download pncp --ano 2024               # ano inteiro
  python main.py download pncp --ano 2024 --mes 1       # jan/2024
  python main.py download pncp --ano 2023 --ano 2024    # dois anos
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "pncp"

API_BASE   = "https://pncp.gov.br/api/consulta/v1"
MAX_DAYS   = 10      # janela máxima permitida pela API de editais
DELAY      = 0.8     # segundos entre requests
MAX_RETRY  = 3

# Todas as modalidades de contratação (editais)
ALL_MODALIDADES = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

MODALIDADE_NOME = {
    1: "Leilão-Eletrônico", 3: "Concurso",
    4: "Concorrência-Eletrônica", 5: "Concorrência-Presencial",
    6: "Pregão-Eletrônico", 7: "Pregão-Presencial",
    8: "Dispensa", 9: "Inexigibilidade",
    10: "Manifestação de Interesse", 11: "Pré-qualificação",
    12: "Credenciamento", 13: "Leilão-Presencial",
}

FONTE = {
    "_fonte_nome":    "PNCP — Portal Nacional de Contratações Públicas",
    "_fonte_url":     "https://pncp.gov.br",
    "_fonte_licenca": "Dados Abertos — https://www.gov.br/pncp",
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

_sess: requests.Session | None = None

def _session() -> requests.Session:
    global _sess
    if _sess is None:
        _sess = requests.Session()
        _sess.headers.update({
            "User-Agent": "dados-abertos-etl/1.0 (pesquisa dados públicos BR)",
            "Accept": "application/json",
        })
    return _sess


def _get(url: str, params: dict | None = None) -> dict | list | None:
    """GET com retry e backoff. Retorna None em 204/404 ou após falha."""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            r = _session().get(url, params=params, timeout=90)
            if r.status_code in (204, 404):
                return None
            if r.status_code == 400:
                log.debug(f"    400 Bad Request: {url} {params}")
                return None
            if r.status_code == 429:
                wait = DELAY * (3 ** attempt)
                log.warning(f"    Rate limit (429) — aguardando {wait:.0f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            text = r.text.strip()
            return json.loads(text) if text else None
        except requests.RequestException as exc:
            log.warning(f"    Erro (tentativa {attempt}/{MAX_RETRY}): {exc}")
            if attempt < MAX_RETRY:
                time.sleep(DELAY * attempt)
    return None


# ── Checkpoint ────────────────────────────────────────────────────────────────

def _load_ckpt(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(path.read_text("utf-8").strip().splitlines())


def _save_ckpt(path: Path, key: str) -> None:
    with path.open("a", "utf-8") as f:
        f.write(key + "\n")


# ── Persistência mensal ───────────────────────────────────────────────────────

def _month_of(rec: dict, fallback: str) -> str:
    """Extrai YYYYMM da data de publicação ou assinatura."""
    for field in ("dataPublicacaoPncp", "dataAssinatura", "dataInclusao", "dataAtualizacao"):
        val = str(rec.get(field) or "")
        if len(val) >= 7:
            return val[:7].replace("-", "")
    return fallback[:6]


def _flush(out_file: Path, records: list[dict], id_field: str) -> int:
    """Merge + dedup em arquivo mensal JSON. Retorna total após merge."""
    out_file.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict] = []
    if out_file.exists():
        try:
            raw = json.loads(out_file.read_text("utf-8"))
            existing = raw if isinstance(raw, list) else raw.get("data", [])
        except (json.JSONDecodeError, OSError):
            pass

    seen = {str(r.get(id_field, "")) for r in existing}
    novos = [r for r in records if str(r.get(id_field, "")) not in seen]
    merged = existing + novos
    out_file.write_text(json.dumps(merged, ensure_ascii=False), "utf-8")
    return len(merged)


# ── Janelas de datas ──────────────────────────────────────────────────────────

def _janelas(inicio: datetime, fim: datetime,
             max_days: int = MAX_DAYS) -> list[tuple[str, str]]:
    result = []
    cur = inicio
    while cur <= fim:
        end = min(cur + timedelta(days=max_days - 1), fim)
        result.append((cur.strftime("%Y%m%d"), end.strftime("%Y%m%d")))
        cur = end + timedelta(days=1)
    return result


# ── Fase 1: Editais/Licitações ────────────────────────────────────────────────

def _fetch_editais_janela(d_ini: str, d_fim: str, mod: int) -> list[dict]:
    """Baixa todas as páginas de uma janela + modalidade."""
    url = f"{API_BASE}/contratacoes/publicacao"
    params = {
        "dataInicial": d_ini,
        "dataFinal":   d_fim,
        "codigoModalidadeContratacao": mod,
        "pagina":       1,
        "tamanhoPagina": 50,
    }
    first = _get(url, params)
    if not first or not isinstance(first, dict):
        return []

    items = first.get("data", [])
    if not items or first.get("empty", True):
        return []

    records = list(items)
    total_pg = int(first.get("totalPaginas", 1) or 1)

    for pg in range(2, total_pg + 1):
        params["pagina"] = pg
        resp = _get(url, params)
        if not resp:
            break
        pg_items = resp.get("data", [])
        if not pg_items or resp.get("empty", False):
            break
        records.extend(pg_items)
        time.sleep(DELAY)

    return records


def fase1_editais(out_dir: Path, periodos: list[tuple[datetime, datetime]]) -> None:
    editais_dir = out_dir / "editais"
    editais_dir.mkdir(parents=True, exist_ok=True)
    ckpt = out_dir / ".ckpt_editais"
    done = _load_ckpt(ckpt)
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for inicio, fim in periodos:
        janelas = _janelas(inicio, fim)
        n = 0
        total = len(janelas) * len(ALL_MODALIDADES)

        for d_ini, d_fim in janelas:
            for mod in ALL_MODALIDADES:
                n += 1
                key = f"editais_{d_ini}_{d_fim}_{mod}"
                if key in done:
                    continue

                log.info(f"  [{n}/{total}] editais {d_ini}→{d_fim} mod={mod} ({MODALIDADE_NOME.get(mod,'')})")
                records = _fetch_editais_janela(d_ini, d_fim, mod)

                if records:
                    for r in records:
                        r.update(FONTE)
                        r["_modalidade_nome"] = MODALIDADE_NOME.get(mod, "")
                        r["_coletado_em"] = coletado

                    by_month: dict[str, list[dict]] = defaultdict(list)
                    for r in records:
                        by_month[_month_of(r, d_ini)].append(r)

                    for mk, recs in by_month.items():
                        out_file = editais_dir / f"pncp_editais_{mk}.json"
                        total_f = _flush(out_file, recs, "numeroControlePNCP")
                        log.info(f"    {mk}: +{len(recs)} editais (arquivo: {total_f})")

                _save_ckpt(ckpt, key)
                done.add(key)
                time.sleep(DELAY)


# ── Fase 2: Contratos ─────────────────────────────────────────────────────────
# Endpoint independente — não precisa iterar por modalidade

def _fetch_contratos_janela(d_ini: str, d_fim: str) -> list[dict]:
    """
    GET /contratos?dataInicial=&dataFinal=
    Endpoint de lote: traz todos os contratos publicados no período,
    independente de modalidade. Página máxima: 500.
    """
    url = f"{API_BASE}/contratos"
    params = {
        "dataInicial":  d_ini,
        "dataFinal":    d_fim,
        "pagina":       1,
        "tamanhoPagina": 500,
    }
    first = _get(url, params)
    if not first or not isinstance(first, dict):
        return []

    items = first.get("data", [])
    if not items or first.get("empty", True):
        return []

    records = list(items)
    total_pg = int(first.get("totalPaginas", 1) or 1)

    for pg in range(2, total_pg + 1):
        params["pagina"] = pg
        resp = _get(url, params)
        if not resp:
            break
        pg_items = resp.get("data", [])
        if not pg_items or resp.get("empty", False):
            break
        records.extend(pg_items)
        time.sleep(DELAY)

    return records


def fase2_contratos(out_dir: Path, periodos: list[tuple[datetime, datetime]]) -> None:
    contratos_dir = out_dir / "contratos"
    contratos_dir.mkdir(parents=True, exist_ok=True)
    ckpt = out_dir / ".ckpt_contratos"
    done = _load_ckpt(ckpt)
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Contratos: janela maior permitida (30 dias — sem limite de modalidade)
    for inicio, fim in periodos:
        janelas = _janelas(inicio, fim, max_days=30)
        for i, (d_ini, d_fim) in enumerate(janelas, 1):
            key = f"contratos_{d_ini}_{d_fim}"
            if key in done:
                continue

            log.info(f"  [{i}/{len(janelas)}] contratos {d_ini}→{d_fim}")
            records = _fetch_contratos_janela(d_ini, d_fim)

            if records:
                for r in records:
                    r.update(FONTE)
                    r["_coletado_em"] = coletado

                by_month: dict[str, list[dict]] = defaultdict(list)
                for r in records:
                    by_month[_month_of(r, d_ini)].append(r)

                for mk, recs in by_month.items():
                    out_file = contratos_dir / f"pncp_contratos_{mk}.json"
                    # contratos usam numeroContratoEmpenho como id, com fallback
                    id_field = "numeroContratoEmpenho"
                    total_f = _flush(out_file, recs, id_field)
                    log.info(f"    {mk}: +{len(recs)} contratos (arquivo: {total_f})")

            _save_ckpt(ckpt, key)
            done.add(key)
            time.sleep(DELAY)


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(anos: list[int] | None = None,
        meses: list[int] | None = None):
    """
    anos:  lista de anos. None = ano atual.
    meses: lista de meses (1-12). None = todos do ano.

    Saída:
      data/pncp/editais/pncp_editais_{YYYYMM}.json
      data/pncp/contratos/pncp_contratos_{YYYYMM}.json
    """
    hoje = datetime.now()

    if not anos:
        anos = [hoje.year]
    if not meses:
        meses = list(range(1, 13))

    periodos: list[tuple[datetime, datetime]] = []
    for ano in sorted(anos):
        for mes in sorted(meses):
            if ano == hoje.year and mes > hoje.month:
                continue
            inicio = datetime(ano, mes, 1)
            fim    = datetime(ano + (mes == 12), (mes % 12) + 1, 1) - timedelta(days=1)
            fim    = min(fim, hoje)
            periodos.append((inicio, fim))

    if not periodos:
        log.warning("  Nenhum período a processar")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    meses_str = [f"{i.strftime('%Y-%m')}" for i, _ in periodos]
    log.info(f"[pncp] {len(periodos)} período(s): {meses_str}")

    log.info("[pncp] Fase 1 — Editais/Licitações...")
    fase1_editais(DATA_DIR, periodos)

    log.info("[pncp] Fase 2 — Contratos...")
    fase2_contratos(DATA_DIR, periodos)

    log.info("[pncp] Download concluído")
    log.info(f"  editais   → {DATA_DIR / 'editais'}")
    log.info(f"  contratos → {DATA_DIR / 'contratos'}")