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

Arquivos já baixados são pulados automaticamente (idempotente).

Uso via CLI:
  python main.py download tse                                # todas
  python main.py download tse --eleicao 2024                 # só 2024
  python main.py download tse --eleicao 2024 --eleicao 2022  # 2024 e 2022
"""

import csv
import io
import logging
import os
import shutil
import tempfile
import time
import zipfile
from datetime import datetime, timezone
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

# ── Anos disponíveis ──────────────────────────────────────────────────────────
ANOS_MUNICIPAIS = [2024, 2020, 2016, 2012, 2008, 2004, 2000]
ANOS_GERAIS     = [2022, 2018, 2014, 2010, 2006, 2002]
ANOS_TODOS      = sorted(set(ANOS_MUNICIPAIS + ANOS_GERAIS), reverse=True)

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

# ── Mapeamento de colunas — doações (3 eras) ──────────────────────────────────
DOACAO_COLS_NEW = {           # 2018+
    "SQ_CANDIDATO":       "sq_candidato",
    "NR_CPF_CNPJ_DOADOR": "cpf_cnpj_doador",
    "NM_DOADOR":          "nome_doador",
    "VR_RECEITA":         "valor",
    "AA_ELEICAO":         "ano",
    "NM_CANDIDATO":       "nome_candidato",
    "SG_PARTIDO":         "partido",
    "NR_CANDIDATO":       "nr_candidato",
}
DOACAO_COLS_LEGACY = {        # 2010-2016
    "Sequencial Candidato": "sq_candidato",
    "CPF/CNPJ do doador":   "cpf_cnpj_doador",
    "Nome do doador":       "nome_doador",
    "Valor receita":        "valor",
    "Nome candidato":       "nome_candidato",
}
DOACAO_COLS_EARLY: dict[str, list[str]] = {   # 2002-2008
    "sq_candidato":    ["SEQUENCIAL_CANDIDATO"],
    "cpf_cnpj_doador": ["CD_CPF_CNPJ_DOADOR", "CD_CPF_CGC",
                        "CD_CPF_CGC_DOA", "NUMERO_CPF_CGC_DOADOR"],
    "nome_doador":     ["NM_DOADOR", "NO_DOADOR", "NOME_DOADOR"],
    "valor":           ["VR_RECEITA", "VALOR_RECEITA"],
    "nome_candidato":  ["NM_CANDIDATO", "NO_CAND", "NOME_CANDIDATO"],
    "partido":         ["SG_PARTIDO", "SG_PART", "SIGLA_PARTIDO"],
}


# ── URL builders ──────────────────────────────────────────────────────────────

def _cand_url(ano: int) -> str:
    return f"{TSE_CDN}/consulta_cand/consulta_cand_{ano}.zip"


def _donation_url(ano: int) -> str:
    if ano >= 2018:
        return (f"{TSE_CDN}/prestacao_contas/"
                f"prestacao_de_contas_eleitorais_candidatos_{ano}.zip")
    if ano in (2012, 2014):
        return f"{TSE_CDN}/prestacao_contas/prestacao_final_{ano}.zip"
    return f"{TSE_CDN}/prestacao_contas/prestacao_contas_{ano}.zip"


# ── HTTP — download para arquivo temporário (streaming, sem RAM) ──────────────

def _download_to_tmp(url: str, retries: int = 3,
                     delay: float = 5.0) -> Path | None:
    """
    Faz download em streaming direto para um arquivo temporário em disco.
    Nunca carrega o ZIP inteiro na RAM — seguro para arquivos de vários GBs.
    Retorna o path do arquivo temporário ou None em caso de falha.
    O CHAMADOR é responsável por deletar o arquivo após o uso.
    """
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(url, timeout=120, stream=True,
                             headers={"User-Agent": "dados-abertos-etl/1.0"})
            if r.status_code == 404:
                log.warning(f"    404 — não disponível: {url}")
                return None
            r.raise_for_status()

            tmp = Path(tempfile.mktemp(suffix=".zip"))
            downloaded = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):  # 8 MB
                    f.write(chunk)
                    downloaded += len(chunk)
            log.info(f"    Baixado: {downloaded / 1e6:.1f} MB → {tmp.name}")
            return tmp

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


def _normalize_date(s: str) -> str:
    """DD/MM/YYYY → YYYY-MM-DD"""
    s = s.strip()
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        return f"{s[6:]}-{s[3:5]}-{s[:2]}"
    return s


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
            self._writer = csv.DictWriter(
                self._f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
            self._writer.writeheader()
        for row in rows:
            for f in self._writer.fieldnames:
                row.setdefault(f, "")
        self._writer.writerows(rows)
        self._total += len(rows)

    def close(self) -> int:
        self._f.close()
        return self._total


# ── Doações — detecção de formato ────────────────────────────────────────────

def _detect_doacao_mapping(fieldnames: list[str]) -> dict[str, str] | None:
    fset = set(fieldnames)
    if "SQ_CANDIDATO" in fset and "VR_RECEITA" in fset:
        return DOACAO_COLS_NEW
    if "Sequencial Candidato" in fset:
        m = dict(DOACAO_COLS_LEGACY)
        for col in fieldnames:
            if "partido" in col.lower():
                m[col] = "partido"
                break
        return m
    mapping = {}
    for dest, variants in DOACAO_COLS_EARLY.items():
        for v in variants:
            if v in fset:
                mapping[v] = dest
                break
    return mapping if mapping else None


# ── Candidatos ────────────────────────────────────────────────────────────────

def _process_candidatos(ano: int) -> None:
    out_path = DATA_DIR / "candidatos" / f"candidatos_{ano}.csv"
    if out_path.exists():
        log.info(f"    ✓ já existe ({out_path.stat().st_size/1e6:.1f} MB) — pulando")
        return

    url     = _cand_url(ano)
    tmp_zip = _download_to_tmp(url)
    if not tmp_zip:
        return

    appender = _CsvAppender(out_path)
    try:
        with zipfile.ZipFile(tmp_zip) as zf:
            names   = [n for n in zf.namelist()
                       if n.lower().endswith(".csv") and "__macosx" not in n.lower()]
            targets = [n for n in names if "BRASIL" in n.upper()] or names

        for target in targets:
            with zipfile.ZipFile(tmp_zip) as zf:
                raw    = zf.read(target)
                text   = raw.decode("latin-1", errors="replace")
                reader = csv.DictReader(io.StringIO(text), delimiter=";")
                batch  = []
                for row in reader:
                    filtered = {k: v.strip() for k, v in row.items()
                                if k in COLUNAS_CAND}
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
        log.error(f"    ZIP inválido: {exc}")
    finally:
        tmp_zip.unlink(missing_ok=True)

    total = appender.close()
    log.info(f"    ✓ candidatos_{ano}.csv  ({total:,} linhas)")


# ── Doações ───────────────────────────────────────────────────────────────────

def _process_doacoes(ano: int) -> None:
    out_path = DATA_DIR / "doacoes" / f"doacoes_{ano}.csv"
    if out_path.exists():
        log.info(f"    ✓ já existe ({out_path.stat().st_size/1e6:.1f} MB) — pulando")
        return

    url     = _donation_url(ano)
    tmp_zip = _download_to_tmp(url)
    if not tmp_zip:
        return

    appender = _CsvAppender(out_path)
    try:
        with zipfile.ZipFile(tmp_zip) as zf:
            todos = [n for n in zf.namelist()
                     if n.lower().endswith(".csv") and "__macosx" not in n.lower()]

        log.info(f"    ZIP: {len(todos)} CSV(s)  ex: {todos[:3]}")

        # prioridade: receitas consolidadas BRASIL → receitas por estado → todos
        receitas  = [n for n in todos if "receita" in n.lower()]
        brasil    = [n for n in receitas if "brasil" in n.lower()]
        csv_names = brasil or receitas or todos

        if not csv_names:
            log.warning(f"    ZIP vazio para {ano}")
            return

        log.info(f"    Processando {len(csv_names)} arquivo(s)"
                 + (" [BRASIL]" if brasil else
                    " [por estado]" if receitas else " [todos]"))

        for name in csv_names:
            with zipfile.ZipFile(tmp_zip) as zf:
                raw    = zf.read(name)
                text   = raw.decode("latin-1", errors="replace")
                reader = csv.DictReader(io.StringIO(text), delimiter=";")
                mapping = _detect_doacao_mapping(reader.fieldnames or [])
                if not mapping:
                    log.warning(f"    Formato desconhecido: {name}")
                    log.warning(f"    Colunas: {(reader.fieldnames or [])[:8]}")
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

    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido: {exc}")
    finally:
        tmp_zip.unlink(missing_ok=True)

    total = appender.close()
    log.info(f"    ✓ doacoes_{ano}.csv  ({total:,} linhas)")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(eleicoes: list[int] | None = None):
    """
    eleicoes: lista de anos a processar. None = todos os anos disponíveis.
    Passado pelo CLI via --eleicao 2024 --eleicao 2022.
    Arquivos já existentes são pulados automaticamente.
    """
    anos = sorted(eleicoes or ANOS_TODOS, reverse=True)

    log.info(f"[tse] Anos: {anos}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "candidatos").mkdir(exist_ok=True)
    (DATA_DIR / "doacoes").mkdir(exist_ok=True)

    for ano in anos:
        log.info(f"  === Candidatos {ano} ===")
        _process_candidatos(ano)
        time.sleep(1)

    for ano in anos:
        log.info(f"  === Doações {ano} ===")
        _process_doacoes(ano)
        time.sleep(1)

    log.info("[tse] Download concluído")