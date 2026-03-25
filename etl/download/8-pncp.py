"""
Download 8 - PNCP: Portal Nacional de Contratações Públicas
API: https://pncp.gov.br/api/consulta/v1/

Baixa em duas fases independentes por janela de datas:

  Fase 1 — Licitações/Editais
    GET /contratacoes/publicacao
      ?dataInicial=YYYYMMDD&dataFinal=YYYYMMDD
      &codigoModalidadeContratacao={mod}&pagina=N&tamanhoPagina=50
    → data/pncp/editais/pncp_editais_{YYYYMM}.json
    Checkpointed por (d_ini, d_fim, modalidade).

  Fase 2 — Contratos
    GET /contratos
      ?dataInicial=YYYYMMDD&dataFinal=YYYYMMDD&pagina=N&tamanhoPagina=500
    → data/pncp/contratos/pncp_contratos_{YYYYMM}.json
    Checkpointed por (d_ini, d_fim).

Ambas as fases são retomáveis após interrupção (--skip-existing por padrão).
Os arquivos mensais são merge+dedup com execuções anteriores (idempotente).

Uso:
  python main.py download pncp                             # mês atual
  python main.py download pncp --ano 2024                  # ano inteiro
  python main.py download pncp --ano 2021 --ano 2022       # dois anos
  python main.py download pncp --ano 2024 --mes 1 --mes 2  # meses específicos
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "pncp"

API_EDITAIS   = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
API_CONTRATOS = "https://pncp.gov.br/api/consulta/v1/contratos"

MAX_PAGE_SIZE_EDITAIS   = 50
MAX_PAGE_SIZE_CONTRATOS = 500
MAX_DAYS_EDITAIS        = 10   # limite da API para editais
MAX_DAYS_CONTRATOS      = 30   # contratos aceitam janela maior
REQUEST_DELAY           = 1.0  # segundos entre requests
MAX_RETRIES             = 3
RETRY_BACKOFF           = 5.0
TIMEOUT                 = 90   # httpx lida melhor com timeout que requests

# Modalidades prioritárias para investigação (Pregão + Dispensa + Inexigibilidade)
# Representam ~85% do volume. Use ALL_MODALIDADES para tudo (muito mais lento).
DEFAULT_MODALIDADES = [6, 7, 8, 9]
ALL_MODALIDADES     = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]

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


# ── HTTP (httpx — melhor suporte a timeout e streaming) ───────────────────────

def _make_client(timeout: int = TIMEOUT) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers={
            "User-Agent": "dados-abertos-etl/1.0 (pesquisa dados publicos BR)",
            "Accept":     "application/json",
        },
    )


def _fetch_page(client: httpx.Client, url: str, params: dict) -> dict | None:
    """Busca uma página com retry e backoff. Retorna None em 204/404/erro."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(url, params=params)
            if r.status_code == 204:
                return None
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                wait = RETRY_BACKOFF * attempt * 2
                log.warning(f"    Rate limit (429) — aguardando {wait:.0f}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            text = r.text.strip()
            if not text:
                return None
            return json.loads(text, strict=False)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (400, 404):
                return None
            if attempt < MAX_RETRIES:
                log.warning(f"    HTTP {exc.response.status_code} (tentativa {attempt}/{MAX_RETRIES})")
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                log.warning(f"    Desistindo: {exc}")
                return None
        except httpx.HTTPError as exc:
            if attempt < MAX_RETRIES:
                log.warning(f"    Erro de rede (tentativa {attempt}/{MAX_RETRIES}): {exc}")
                time.sleep(RETRY_BACKOFF * attempt)
            else:
                log.warning(f"    Desistindo: {exc}")
                return None
    return None


def _fetch_all_pages(client: httpx.Client, url: str, base_params: dict,
                     page_workers: int = 1) -> list[dict]:
    """Busca todas as páginas de um endpoint paginado."""
    params = {**base_params, "pagina": 1}
    first = _fetch_page(client, url, params)
    if not first or not isinstance(first, dict):
        return []

    items = first.get("data", [])
    if not items or first.get("empty", True):
        return []

    records    = list(items)
    total_pgs  = int(first.get("totalPaginas", 1) or 1)
    if total_pgs <= 1:
        return records

    remaining = range(2, total_pgs + 1)

    def fetch_pg(pg: int) -> list[dict]:
        resp = _fetch_page(client, url, {**base_params, "pagina": pg})
        if not resp:
            return []
        pg_items = resp.get("data", [])
        return pg_items if pg_items and not resp.get("empty", False) else []

    if page_workers > 1:
        with ThreadPoolExecutor(max_workers=page_workers) as ex:
            futures = {ex.submit(fetch_pg, pg): pg for pg in remaining}
            for future in as_completed(futures):
                records.extend(future.result())
    else:
        for pg in remaining:
            records.extend(fetch_pg(pg))
            time.sleep(REQUEST_DELAY)

    return records


# ── Checkpoint ────────────────────────────────────────────────────────────────

def _load_ckpt(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        return set(path.read_text(encoding="utf-8").strip().splitlines())
    except OSError:
        return set()


def _save_ckpt(path: Path, key: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(key + "\n")


# ── Persistência mensal ───────────────────────────────────────────────────────

def _month_of(rec: dict, fallback: str) -> str:
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
            raw = json.loads(out_file.read_text(encoding="utf-8"), strict=False)
            existing = raw if isinstance(raw, list) else raw.get("data", [])
        except (json.JSONDecodeError, OSError):
            log.warning(f"    Arquivo corrompido {out_file.name} — sobrescrevendo")
    seen  = {str(r.get(id_field, "")) for r in existing}
    novos = [r for r in records if str(r.get(id_field, "")) not in seen]
    merged = existing + novos
    out_file.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")
    return len(merged)


# ── Janelas de datas ──────────────────────────────────────────────────────────

def _janelas(inicio: datetime, fim: datetime, max_days: int) -> list[tuple[str, str]]:
    result = []
    cur = inicio
    while cur <= fim:
        end = min(cur + timedelta(days=max_days - 1), fim)
        result.append((cur.strftime("%Y%m%d"), end.strftime("%Y%m%d")))
        cur = end + timedelta(days=1)
    return result


# ── Fase 1: Editais ───────────────────────────────────────────────────────────

def fase1_editais(out_dir: Path, periodos: list[tuple[datetime, datetime]],
                  modalidades: list[int], page_workers: int = 1) -> None:
    editais_dir = out_dir / "editais"
    editais_dir.mkdir(parents=True, exist_ok=True)
    ckpt     = out_dir / ".ckpt_editais"
    done     = _load_ckpt(ckpt)
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(f"  Modalidades: {[MODALIDADE_NOME.get(m, str(m)) for m in modalidades]}")

    # calcula total de combinações para o progresso
    todas_janelas = [(d, f, m)
                     for ini, fim in periodos
                     for d, f in _janelas(ini, fim, MAX_DAYS_EDITAIS)
                     for m in modalidades]
    total = len(todas_janelas)
    n = sum(1 for d, f, m in todas_janelas
            if f"editais_{d}_{f}_{m}" in done)
    log.info(f"  Total combinações: {total}  já concluídas: {n}")

    with _make_client() as client:
        for d_ini, d_fim, mod in todas_janelas:
            key = f"editais_{d_ini}_{d_fim}_{mod}"
            if key in done:
                continue

            n += 1
            log.info(f"  [{n}/{total}] editais {d_ini}→{d_fim} mod={mod} ({MODALIDADE_NOME.get(mod,'')})")

            records = _fetch_all_pages(client, API_EDITAIS, {
                "dataInicial":                 d_ini,
                "dataFinal":                   d_fim,
                "codigoModalidadeContratacao": mod,
                "tamanhoPagina":               MAX_PAGE_SIZE_EDITAIS,
            }, page_workers=page_workers)

            if records:
                for r in records:
                    r.update(FONTE)
                    r["_modalidade_nome"] = MODALIDADE_NOME.get(mod, "")
                    r["_coletado_em"]     = coletado

                by_month: dict[str, list[dict]] = defaultdict(list)
                for r in records:
                    by_month[_month_of(r, d_ini)].append(r)

                for mk, recs in by_month.items():
                    out_file  = editais_dir / f"pncp_editais_{mk}.json"
                    total_arq = _flush(out_file, recs, "numeroControlePNCP")
                    log.info(f"    {mk}: +{len(recs)} editais (arquivo: {total_arq})")

            _save_ckpt(ckpt, key)
            done.add(key)
            time.sleep(REQUEST_DELAY)


# ── Fase 2: Contratos ─────────────────────────────────────────────────────────

def fase2_contratos(out_dir: Path, periodos: list[tuple[datetime, datetime]],
                    page_workers: int = 1) -> None:
    contratos_dir = out_dir / "contratos"
    contratos_dir.mkdir(parents=True, exist_ok=True)
    ckpt     = out_dir / ".ckpt_contratos"
    done     = _load_ckpt(ckpt)
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    todas_janelas = [(d, f)
                     for ini, fim in periodos
                     for d, f in _janelas(ini, fim, MAX_DAYS_CONTRATOS)]
    total = len(todas_janelas)
    n = sum(1 for d, f in todas_janelas if f"contratos_{d}_{f}" in done)
    log.info(f"  Total janelas: {total}  já concluídas: {n}")

    with _make_client() as client:
        for d_ini, d_fim in todas_janelas:
            key = f"contratos_{d_ini}_{d_fim}"
            if key in done:
                continue

            n += 1
            log.info(f"  [{n}/{total}] contratos {d_ini}→{d_fim}")

            records = _fetch_all_pages(client, API_CONTRATOS, {
                "dataInicial":  d_ini,
                "dataFinal":    d_fim,
                "tamanhoPagina": MAX_PAGE_SIZE_CONTRATOS,
            }, page_workers=page_workers)

            if records:
                for r in records:
                    r.update(FONTE)
                    r["_coletado_em"] = coletado

                by_month: dict[str, list[dict]] = defaultdict(list)
                for r in records:
                    by_month[_month_of(r, d_ini)].append(r)

                for mk, recs in by_month.items():
                    out_file  = contratos_dir / f"pncp_contratos_{mk}.json"
                    total_arq = _flush(out_file, recs, "numeroContratoEmpenho")
                    log.info(f"    {mk}: +{len(recs)} contratos (arquivo: {total_arq})")

            _save_ckpt(ckpt, key)
            done.add(key)
            time.sleep(REQUEST_DELAY)


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(anos: list[int] | None = None,
        meses: list[int] | None = None,
        todas_modalidades: bool = False,
        page_workers: int = 1):
    """
    anos:              lista de anos. None = ano atual.
    meses:             lista de meses (1-12). None = todos do ano.
    todas_modalidades: False = mod 6,7,8,9 (Pregão+Dispensa+Inexigibilidade)
                       True  = todas as 13 modalidades (muito mais lento)
    page_workers:      threads para páginas paralelas dentro de cada janela.

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

    mods = ALL_MODALIDADES if todas_modalidades else DEFAULT_MODALIDADES
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    meses_str = [i.strftime("%Y-%m") for i, _ in periodos]
    log.info(f"[pncp] {len(periodos)} período(s): {meses_str}")
    log.info(f"[pncp] page_workers={page_workers}  todas_modalidades={todas_modalidades}")

    log.info("[pncp] Fase 1 — Editais/Licitações...")
    fase1_editais(DATA_DIR, periodos, mods, page_workers=page_workers)

    log.info("[pncp] Fase 2 — Contratos...")
    fase2_contratos(DATA_DIR, periodos, page_workers=page_workers)

    log.info("[pncp] Download concluído")
    log.info(f"  editais   → {DATA_DIR / 'editais'}")
    log.info(f"  contratos → {DATA_DIR / 'contratos'}")