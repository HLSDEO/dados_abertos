"""
Download 11 - Câmara dos Deputados: Despesas CEAP
Fonte: https://dadosabertos.camara.leg.br/

As despesas são disponibilizadas por ano em arquivos ZIP (CSV, JSON, XML).
URL: https://www.camara.leg.br/cotas/Ano-{ano}.csv.zip

Saída:
  data/camara/despesas_{ano}.csv

Uso:
  python main.py download camara
  python main.py download camara --ano 2024
  python main.py download camara --ano 2023 --ano 2024
"""
import csv
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "camara"
BASE_URL = "https://www.camara.leg.br/cotas"

ANO_INICIO = 2008
ANO_FIM    = datetime.now().year

FONTE = {
    "fonte_nome":      "Câmara dos Deputados",
    "fonte_descricao": "Despesas CEAP — Cota para Exercício da Atividade Parlamentar",
    "fonte_url":       "https://dadosabertos.camara.leg.br",
    "fonte_licenca":   "Dados Abertos — http://www2.camara.leg.br/transparencia/licitacao-contrato-e-despesa/despesa-elenco",
}

# Colunas esperadas no CSV da Câmara (podem variar ligeiramente entre anos)
# Vamos usar o mapeamento flexível baseado no features.md
COL_DESPESA_ID    = ["nuDeputadoId", "ideCadastro", "codDeputado"]
COL_TIPO         = ["txtDescricao", "despesa", "tipoDespesa"]
COL_VALOR        = ["vlrLiquido", "valorLiquido", "vlrLiquid"]
COL_DATA        = ["datEmissao", "dataEmissao", "dataEmissão"]
COL_ANO          = ["numAno", "ano"]
COL_MES         = ["numMes", "mes"]
COL_CNPJ        = ["txtCNPJCPF", "cnpjCPF", "numCNPJ"]
COL_NOME_DEP    = ["txtFornecedor", "nomeFornecedor", "fornecedor"]
COL_SG_PARTIDO  = ["sgPartido", "partido"]
COL_UF           = ["sgUF", "uf"]


def _download_zip(url: str, retries: int = 3) -> Path | None:
    """Baixa ZIP para arquivo temporário."""
    import tempfile
    import os
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
            fd, tmp_str = tempfile.mkstemp(suffix=".zip")
            tmp = Path(tmp_str)
            downloaded = 0
            with open(fd, "wb") as f:  # Use o fd diretamente
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
            log.info(f"    {downloaded / 1e6:.1f} MB baixados")
            return tmp
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(5)
    return None


def _find_column(row: dict, candidates: list[str]) -> str:
    """Encontra a primeira coluna existente entre candidates."""
    for c in candidates:
        if c in row and row[c].strip():
            return c
    return ""


def _extract_and_process(tmp_zip: Path, ano: int, out_path: Path) -> int:
    """Extrai CSV do ZIP e processa as despesas."""
    try:
        csv_data = None
        with zipfile.ZipFile(tmp_zip) as zf:
            csvs = [n for n in zf.namelist()
                    if n.lower().endswith(".csv") and ".." not in n
                    and "__macosx" not in n.lower()]
            if not csvs:
                log.warning(f"    Nenhum CSV no ZIP do ano {ano}")
                return 0

            target = csvs[0]
            log.info(f"    Extraindo {target}")
            csv_data = zf.read(target)

        if csv_data is None:
            return 0

        text = csv_data.decode("latin-1", errors="replace")
        lines = [l for l in text.splitlines() if l.strip()]
        if not lines:
            return 0

        reader = csv.DictReader(lines, delimiter=";")
        if not reader.fieldnames:
            return 0

        log.info(f"    Colunas ({len(reader.fieldnames)}): {reader.fieldnames[:8]}...")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        rows_written = 0
        coletado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = None

            for row in reader:
                row = {k: (v or "").strip() for k, v in row.items() if k is not None}

                # Mapeia colunas de forma flexível
                dep_id_col     = _find_column(row, COL_DESPESA_ID)
                tipo_col       = _find_column(row, COL_TIPO)
                valor_col      = _find_column(row, COL_VALOR)
                data_col       = _find_column(row, COL_DATA)
                ano_col        = _find_column(row, COL_ANO)
                mes_col        = _find_column(row, COL_MES)
                cnpj_col       = _find_column(row, COL_CNPJ)
                nome_dep_col   = _find_column(row, COL_NOME_DEP)
                partido_col    = _find_column(row, COL_SG_PARTIDO)
                uf_col         = _find_column(row, COL_UF)

                if not dep_id_col or not valor_col:
                    continue

                mapped = {
                    "despesa_id":     row.get(dep_id_col, ""),
                    "tipo_despesa":   row.get(tipo_col, "") if tipo_col else "",
                    "valor_liquido":  row.get(valor_col, "").replace(",", "."),
                    "data_emissao":   row.get(data_col, "") if data_col else "",
                    "ano":            row.get(ano_col, str(ano)) if ano_col else str(ano),
                    "mes":            row.get(mes_col, "") if mes_col else "",
                    "cnpj_fornecedor": row.get(cnpj_col, "") if cnpj_col else "",
                    "nome_fornecedor": row.get(nome_dep_col, "") if nome_dep_col else "",
                    "partido":        row.get(partido_col, "") if partido_col else "",
                    "uf":             row.get(uf_col, "") if uf_col else "",
                    **FONTE,
                    "fonte_url_origem": f"{BASE_URL}/Ano-{ano}.csv.zip",
                    "fonte_coletado_em": coletado,
                }

                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(mapped.keys()),
                                            extrasaction="ignore")
                    writer.writeheader()
                writer.writerow(mapped)
                rows_written += 1

        log.info(f"    {rows_written:,} despesas processadas")
        return rows_written

    except zipfile.BadZipFile as exc:
        log.error(f"    ZIP inválido: {exc}")
        return 0


def run(anos: list[int] | None = None, meses: list[int] | None = None):
    """
    anos:  lista de anos. None = todos de 2008 até hoje.
    meses: filtro de meses (ainda não implementado no download, apenas pipeline).
    """
    log.info("[camara] Iniciando download de despesas CEAP")

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

        url = f"{BASE_URL}/Ano-{ano}.csv.zip"
        tmp_zip = _download_zip(url)

        if not tmp_zip:
            log.warning(f"  Ano {ano}: não disponível (404 ou erro)")
            continue

        try:
            total = _extract_and_process(tmp_zip, ano, out_path)
            if total > 0:
                log.info(f"  ✓ despesas_{ano}.csv  ({total:,} registros)")
                total_baixados += 1
            else:
                out_path.unlink(missing_ok=True)
        finally:
            if tmp_zip and tmp_zip.exists():
                try:
                    tmp_zip.unlink(missing_ok=True)
                except:
                    pass  # Ignora erro de unlink no Windows

        time.sleep(1)

    log.info(f"[camara] Concluído — baixados={total_baixados}  pulados={total_pulados}")
