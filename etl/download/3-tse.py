"""
Download 3 - TSE: Candidatos e Doações Eleitorais
Fonte: https://cdn.tse.jus.br/estatistica/sead/odsele/

Baixa e extrai para data/tse/:
  candidatos/{ano}.csv   ← consolidado nacional (arquivo BRASIL do ZIP)
  doacoes/{ano}.csv      ← doações consolidadas por ano

Tipos de eleição suportados:
  Municipais:  2024, 2020, 2016, 2012, 2008, 2004, 2000
  Gerais:      2022, 2018, 2014, 2010, 2006, 2002
  Nacionais:   incluídas nos anos gerais (presidente = cargo federal)

Uso via CLI:
  python main.py download tse                             # todas
  python main.py download tse --eleicao 2024              # só 2024
  python main.py download tse --eleicao 2024 --eleicao 2022  # 2024 e 2022
"""

import csv
import io
import logging
import os
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

TSE_CDN  = "https://cdn.tse.jus.br/estatistica/sead/odsele"
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "tse"

FONTE = {
    "fonte_nome":      "TSE — Tribunal Superior Eleitoral",
    "fonte_descricao": "Dados Eleitorais Abertos",
    "fonte_url":       "https://dadosabertos.tse.jus.br",
    "fonte_licenca":   "Dados Abertos",
}

# ── Anos disponíveis (municipais + gerais/nacionais) ─────────────────────────
# Municipais: anos pares não divisíveis por 4 a partir de 2000
# Gerais/nacionais: anos divisíveis por 4 a partir de 2002
# Presidente concorre nos anos gerais — já está incluso
ANOS_MUNICIPAIS  = [2024, 2020, 2016, 2012, 2008, 2004, 2000]
ANOS_GERAIS      = [2022, 2018, 2014, 2010, 2006, 2002]
ANOS_CANDIDATOS  = sorted(set(ANOS_MUNICIPAIS + ANOS_GERAIS), reverse=True)
ANOS_DOACOES     = sorted(ANOS_CANDIDATOS, reverse=True)  # mesmos anos

# ── Colunas de interesse (candidatos) ────────────────────────────────────────
COLUNAS_CAND = {
    "ANO_ELEICAO", "CD_TIPO_ELEICAO", "NM_TIPO_ELEICAO", "NR_TURNO",
    "CD_ELEICAO", "DS_ELEICAO", "DT_ELEICAO", "SG_UF", "SG_UE", "NM_UE",
    "CD_CARGO", "DS_CARGO", "SQ_CANDIDATO", "NR_CANDIDATO",
    "NM_CANDIDATO", "NM_URNA_CANDIDATO", "NR_PARTIDO", "SG_PARTIDO",
    "NM_PARTIDO", "SG_UF_NASCIMENTO", "DT_NASCIMENTO",
    "NR_TITULO_ELEITORAL_CANDIDATO", "DS_GENERO", "DS_GRAU_INSTRUCAO",
    "DS_ESTADO_CIVIL", "DS_COR_RACA", "CD_OCUPACAO", "DS_OCUPACAO",
    "CD_SIT_TOT_TURNO", "DS_SIT_TOT_TURNO",
}

# ── Mapeamento de colunas de doações (3 eras) ─────────────────────────────────
DOACAO_COLS_NEW = {       # 2018+
    "SQ_CANDIDATO":    "sq_candidato",
    "NR_CPF_CNPJ_DOADOR": "cpf_cnpj_doador",
    "NM_DOADOR":       "nome_doador",
    "VR_RECEITA":      "valor",
    "AA_ELEICAO":      "ano",
    "NM_CANDIDATO":    "nome_candidato",
    "SG_PARTIDO":      "partido",
    "NR_CANDIDATO":    "nr_candidato",
}
DOACAO_COLS_LEGACY = {    # 2010-2016
    "Sequencial Candidato": "sq_candidato",
    "CPF/CNPJ do doador":   "cpf_cnpj_doador",
    "Nome do doador":       "nome_doador",
    "Valor receita":        "valor",
    "Nome candidato":       "nome_candidato",
}
DOACAO_COLS_EARLY: dict[str, list[str]] = {   # 2002-2008
    "sq_candidato":   ["SEQUENCIAL_CANDIDATO"],
    "cpf_cnpj_doador":["CD_CPF_CNPJ_DOADOR","CD_CPF_CGC","CD_CPF_CGC_DOA","NUMERO_CPF_CGC_DOADOR"],
    "nome_doador":    ["NM_DOADOR","NO_DOADOR","NOME_DOADOR"],
    "valor":          ["VR_RECEITA","VALOR_RECEITA"],
    "nome_candidato": ["NM_CANDIDATO","NO_CAND","NOME_CANDIDATO"],
    "partido":        ["SG_PARTIDO","SG_PART","SIGLA_PARTIDO"],
}


# ── URL builders ──────────────────────────────────────────────────────────────

def _cand_url(year: int) -> str:
    return f"{TSE_CDN}/consulta_cand/consulta_cand_{year}.zip"


def _donation_url(year: int) -> str:
    if year >= 2018:
        return (f"{TSE_CDN}/prestacao_contas/"
                f"prestacao_de_contas_eleitorais_candidatos_{year}.zip")
    if year in (2012, 2014):
        return f"{TSE_CDN}/prestacao_contas/prestacao_final_{year}.zip"
    return f"{TSE_CDN}/prestacao_contas/prestacao_contas_{year}.zip"


def _election_years(n: int) -> list[int]:
    """Retorna os N ciclos eleitorais mais recentes (a cada 2 anos)."""
    hoje = date.today().year
    years = []
    y = hoje if hoje % 2 == 0 else hoje - 1
    while len(years) < n:
        years.append(y)
        y -= 2
    return years


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _get(url: str, retries: int = 3, delay: float = 5.0) -> bytes | None:
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(url, timeout=120,
                             headers={"User-Agent": "dados-abertos-etl/1.0"})
            if r.status_code == 404:
                log.warning(f"    404 — não disponível: {url}")
                return None
            r.raise_for_status()
            return r.content
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(delay)
    log.error(f"    Falha após {retries} tentativas: {url}")
    return None


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _add_fonte(row: dict, url: str, ano: int) -> dict:
    coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row.update({**FONTE,
                "fonte_url_origem":  url,
                "fonte_ano":         str(ano),
                "fonte_coletado_em": coletado})
    return row


def _open_zip_csv(content: bytes, filename_hint: str = "",
                  encoding: str = "latin-1", sep: str = ";"):
    """Abre o primeiro CSV dentro de um ZIP e retorna (reader, fieldnames)."""
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        # prefere arquivo que contenha o hint no nome
        names = [n for n in zf.namelist()
                 if n.lower().endswith(".csv") and "__macosx" not in n.lower()]
        if not names:
            return None, []
        target = next((n for n in names if filename_hint.lower() in n.lower()),
                      names[0])
        raw  = zf.read(target)
        text = raw.decode(encoding, errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter=sep)
        return reader, reader.fieldnames or []


class _CsvAppender:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f      = open(path, "w", newline="", encoding="utf-8-sig")
        self._writer = None
        self._total  = 0

    def write_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        if self._writer is None:
            self._writer = csv.DictWriter(self._f, fieldnames=list(rows[0].keys()),
                                          extrasaction="ignore")
            self._writer.writeheader()
        for row in rows:
            for f in self._writer.fieldnames:
                row.setdefault(f, "")
        self._writer.writerows(rows)
        self._total += len(rows)

    def close(self) -> int:
        self._f.close()
        return self._total


# ── Candidatos ────────────────────────────────────────────────────────────────

def _normalize_date(s: str) -> str:
    """DD/MM/YYYY → YYYY-MM-DD"""
    s = s.strip()
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        return f"{s[6:]}-{s[3:5]}-{s[:2]}"
    return s


def _process_candidatos(ano: int) -> None:
    url     = _cand_url(ano)
    content = _get(url)
    if not content:
        return

    out_dir = DATA_DIR / "candidatos"
    appender = _CsvAppender(out_dir / f"candidatos_{ano}.csv")
    hint     = f"consulta_cand_{ano}_BRASIL"

    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = [n for n in zf.namelist()
                     if n.lower().endswith(".csv") and "macos" not in n.lower()]
            # prefere BRASIL, senão processa todos os estados
            targets = [n for n in names if "BRASIL" in n.upper()] or names

        for target in targets:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                raw  = zf.read(target)
                text = raw.decode("latin-1", errors="replace")
                reader = csv.DictReader(io.StringIO(text), delimiter=";")
                batch = []
                for row in reader:
                    filtered = {k: v.strip() for k, v in row.items()
                                if k in COLUNAS_CAND}
                    # normaliza datas
                    for col in ("DT_ELEICAO", "DT_NASCIMENTO"):
                        if col in filtered:
                            filtered[col] = _normalize_date(filtered[col])
                    _add_fonte(filtered, url, ano)
                    batch.append(filtered)
                    if len(batch) >= 10_000:
                        appender.write_rows(batch)
                        batch = []
                if batch:
                    appender.write_rows(batch)
                log.info(f"    {target}")

    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido {url}: {exc}")
        return

    total = appender.close()
    log.info(f"    ✓ candidatos_{ano}.csv  ({total:,} linhas)")


# ── Doações ───────────────────────────────────────────────────────────────────

def _detect_doacao_mapping(fieldnames: list[str]) -> dict[str, str] | None:
    """Detecta qual era de formato e retorna mapeamento col_original→col_destino."""
    fset = set(fieldnames)
    if "SQ_CANDIDATO" in fset and "VR_RECEITA" in fset:
        return DOACAO_COLS_NEW
    if "Sequencial Candidato" in fset:
        m = dict(DOACAO_COLS_LEGACY)
        # partido tem espaçamento inconsistente
        for col in fieldnames:
            if "partido" in col.lower() or "Partido" in col:
                m[col] = "partido"
                break
        return m
    # era antiga — tenta variantes
    mapping = {}
    for dest, variants in DOACAO_COLS_EARLY.items():
        for v in variants:
            if v in fset:
                mapping[v] = dest
                break
    return mapping if mapping else None


def _process_doacoes(ano: int) -> None:
    url     = _donation_url(ano)
    content = _get(url)
    if not content:
        return

    out_dir  = DATA_DIR / "doacoes"
    appender = _CsvAppender(out_dir / f"doacoes_{ano}.csv")

    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_names = [n for n in zf.namelist()
                         if n.lower().endswith(".csv") and "macos" not in n.lower()]
    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido {url}: {exc}")
        return

    for name in csv_names:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            raw    = zf.read(name)
            text   = raw.decode("latin-1", errors="replace")
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            mapping = _detect_doacao_mapping(reader.fieldnames or [])
            if not mapping:
                log.warning(f"    Formato desconhecido em {name} — pulando")
                continue

            batch = []
            for row in reader:
                mapped = {dest: row.get(orig, "").strip()
                          for orig, dest in mapping.items()}
                mapped["ano"] = mapped.get("ano") or str(ano)
                _add_fonte(mapped, url, ano)
                batch.append(mapped)
                if len(batch) >= 10_000:
                    appender.write_rows(batch)
                    batch = []
            if batch:
                appender.write_rows(batch)
        log.info(f"    {name}")

    total = appender.close()
    log.info(f"    ✓ doacoes_{ano}.csv  ({total:,} linhas)")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(eleicoes: list[int] | None = None):
    """
    eleicoes: lista de anos a processar. None = todos os anos disponíveis.
    Passado pelo CLI via --eleicao 2024 --eleicao 2022.
    """
    anos_cand = sorted(eleicoes or ANOS_CANDIDATOS, reverse=True)
    anos_doac = sorted(eleicoes or ANOS_DOACOES,    reverse=True)

    log.info(f"[tse] Candidatos: {anos_cand}")
    log.info(f"[tse] Doações:    {anos_doac}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for ano in anos_cand:
        log.info(f"  === Candidatos {ano} ===")
        _process_candidatos(ano)
        time.sleep(1)

    for ano in anos_doac:
        log.info(f"  === Doações {ano} ===")
        _process_doacoes(ano)
        time.sleep(1)

    log.info("[tse] Download concluído")