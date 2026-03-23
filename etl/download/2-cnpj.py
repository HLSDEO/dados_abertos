"""
Download 2 - Receita Federal CNPJ
Não há API pública — os ZIPs são baixados manualmente em:
  https://dadosabertos.rfb.gov.br/CNPJ/

Estrutura esperada em data/cnpj/:
  data/cnpj/{YYYY-MM}/Empresas0.zip ... Empresas9.zip
  data/cnpj/{YYYY-MM}/Socios0.zip   ... Socios9.zip
  data/cnpj/{YYYY-MM}/Estabelecimentos0.zip ...
  data/cnpj/{YYYY-MM}/Cnaes.zip
  data/cnpj/{YYYY-MM}/Municipios.zip
  data/cnpj/{YYYY-MM}/Paises.zip
  data/cnpj/{YYYY-MM}/Naturezas.zip
  data/cnpj/{YYYY-MM}/Qualificacoes.zip
  data/cnpj/{YYYY-MM}/Motivos.zip
  data/cnpj/{YYYY-MM}/Simples.zip

Este script:
  1. Descobre todos os snapshots YYYY-MM disponíveis
  2. Extrai cada ZIP em um diretório temporário (um ZIP por vez — RAM segura)
  3. Processa em chunks de CHUNK_SIZE linhas e grava no CSV de saída
     incrementalmente (nunca carrega o arquivo inteiro em memória)
  4. Salva CSVs normalizados em data/cnpj/{YYYY-MM}/csv/ com cabeçalho
     e encoding utf-8-sig
"""

import csv
import logging
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

DATA_DIR   = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "cnpj"
_DEFAULT_CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "50000"))   # linhas por chunk
SNAPSHOT_FMT = "%Y-%m"

FONTE = {
    "fonte_nome":      "Receita Federal do Brasil",
    "fonte_descricao": "Dados Abertos CNPJ — Cadastro Nacional da Pessoa Jurídica",
    "fonte_url":       "https://dadosabertos.rfb.gov.br/CNPJ/",
    "fonte_licenca":   "Dados Abertos — https://www.gov.br/receitafederal",
}

# ── Schemas ───────────────────────────────────────────────────────────────────

DOMAIN_SCHEMAS = {
    "cnaes":         ["codigo_cnae",        "descricao_cnae"],
    "naturezas":     ["codigo_natureza",    "descricao_natureza"],
    "qualificacoes": ["codigo_qualificacao","descricao_qualificacao"],
    "motivos":       ["codigo_motivo",      "descricao_motivo"],
    "municipios_rf": ["codigo_municipio_rf","nome_municipio"],
    "paises":        ["codigo_pais",        "nome_pais"],
}

EMPRESAS_COLS = [
    "cnpj_basico", "razao_social", "natureza_juridica",
    "qualificacao_responsavel", "capital_social",
    "porte_empresa", "ente_federativo",
]

SOCIOS_COLS = [
    "cnpj_basico", "identificador_socio", "nome_socio",
    "cpf_cnpj_socio", "qualificacao_socio", "data_entrada",
    "pais", "representante_legal", "nome_representante",
    "qualificacao_representante", "faixa_etaria",
]

ESTABELECIMENTOS_COLS = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv",
    "identificador_matriz_filial", "nome_fantasia",
    "situacao_cadastral", "data_situacao_cadastral",
    "motivo_situacao_cadastral", "nome_cidade_exterior",
    "pais", "data_inicio_atividade",
    "cnae_principal", "cnae_secundaria",
    "tipo_logradouro", "logradouro", "numero", "complemento",
    "bairro", "cep", "uf", "municipio",
    "ddd1", "telefone1", "ddd2", "telefone2",
    "ddd_fax", "fax", "email",
    "situacao_especial", "data_situacao_especial",
]

SIMPLES_COLS = [
    "cnpj_basico", "opcao_simples", "data_opcao_simples",
    "data_exclusao_simples", "opcao_mei",
    "data_opcao_mei", "data_exclusao_mei",
]


# ── Helpers gerais ────────────────────────────────────────────────────────────

def _discover_snapshots() -> list[tuple[str, Path]]:
    result = []
    if not DATA_DIR.exists():
        return result
    for sub in sorted(DATA_DIR.iterdir()):
        if not sub.is_dir():
            continue
        try:
            datetime.strptime(sub.name, SNAPSHOT_FMT)
        except ValueError:
            continue
        if any(sub.glob("*.zip")):
            result.append((sub.name, sub))
    return result


def _extract_zip(zip_path: Path, dest: Path) -> list[Path]:
    """Extrai um ZIP e retorna os arquivos extraídos."""
    out = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            zf.extract(name, dest)
            out.append(dest / name)
    return out


def _normalize_date(s: str) -> str:
    s = s.strip()
    if not s or s in ("0", "00000000"):
        return ""
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def _normalize_capital(s: str) -> str:
    s = s.strip()
    if not s:
        return "0.00"
    return s.replace(".", "").replace(",", ".")


def _normalize_cnpj(basico: str, ordem: str, dv: str) -> str:
    return f"{str(basico).zfill(8)}{str(ordem).zfill(4)}{str(dv).zfill(2)}"


def _fonte_cols(url_origem: str, snapshot: str) -> dict:
    return {
        **FONTE,
        "fonte_url_origem":  url_origem,
        "fonte_snapshot":    snapshot,
        "fonte_coletado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _iter_rf_chunks(path: Path, columns: list[str], chunk_size: int = _DEFAULT_CHUNK_SIZE) -> "Iterator[pd.DataFrame]":
    """Gerador de chunks de um CSV da Receita Federal."""
    yield from pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        header=None,
        names=columns,
        dtype=str,
        keep_default_na=False,
        chunksize=chunk_size,
    )


class _CsvWriter:
    """Abre/cria o CSV de saída e grava chunks incrementalmente."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path    = path
        self._file    = open(path, "w", newline="", encoding="utf-8-sig")
        self._writer  = None
        self._rows    = 0

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        if self._writer is None:
            self._writer = csv.DictWriter(self._file, fieldnames=list(df.columns))
            self._writer.writeheader()
        self._writer.writerows(df.to_dict("records"))
        self._rows += len(df)

    def close(self) -> int:
        self._file.close()
        return self._rows


# ── Processadores ─────────────────────────────────────────────────────────────

def _process_domain(out_name: str, columns: list[str], zip_path: Path,
                    out_dir: Path, snapshot: str, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> None:
    tmp = Path(tempfile.mkdtemp())
    try:
        files = _extract_zip(zip_path, tmp)
        writer = _CsvWriter(out_dir / f"{out_name}.csv")
        fonte  = _fonte_cols(FONTE["fonte_url"], snapshot)
        for f in files:
            try:
                for chunk in _iter_rf_chunks(f, columns, chunk_size):
                    for k, v in fonte.items():
                        chunk[k] = v
                    writer.write(chunk)
            except Exception as exc:
                log.error(f"    ERRO ao ler {f.name}: {exc}", exc_info=True)
        total = writer.close()
        log.info(f"    → {out_name}.csv  ({total:,} linhas)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _process_main(out_name: str, columns: list[str], zip_paths: list[Path],
                  out_dir: Path, snapshot: str,
                  transform_fn=None, chunk_size: int = _DEFAULT_CHUNK_SIZE) -> None:
    """
    Processa arquivos numerados (Empresas0..N, Socios0..N, etc.)
    gravando diretamente no CSV de saída chunk a chunk.
    Nunca acumula mais de chunk_size linhas em memória.
    """
    writer = _CsvWriter(out_dir / f"{out_name}.csv")
    fonte  = _fonte_cols(FONTE["fonte_url"], snapshot)

    for zip_idx, zip_path in enumerate(zip_paths):
        log.info(f"    {zip_path.name}  ({zip_idx+1}/{len(zip_paths)})")
        tmp = Path(tempfile.mkdtemp())
        try:
            files = _extract_zip(zip_path, tmp)
            for f in files:
                chunk_num = 0
                try:
                    for chunk in _iter_rf_chunks(f, columns, chunk_size):
                        chunk_num += 1
                        if transform_fn:
                            chunk = transform_fn(chunk)
                        for k, v in fonte.items():
                            chunk[k] = v
                        writer.write(chunk)
                except Exception as exc:
                    log.error(
                        f"    ERRO em {zip_path.name} / {f.name} "
                        f"(chunk {chunk_num}): {exc}",
                        exc_info=True,
                    )
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    total = writer.close()
    log.info(f"    → {out_name}.csv  ({total:,} linhas)")


# ── Transformações específicas ────────────────────────────────────────────────

def _transform_empresas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["capital_social"] = df["capital_social"].apply(_normalize_capital)
    return df


def _transform_socios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["data_entrada"] = df["data_entrada"].apply(_normalize_date)
    return df


def _transform_estabelecimentos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["data_situacao_cadastral"] = df["data_situacao_cadastral"].apply(_normalize_date)
    df["data_inicio_atividade"]   = df["data_inicio_atividade"].apply(_normalize_date)
    df["data_situacao_especial"]  = df["data_situacao_especial"].apply(_normalize_date)
    df["cnpj"] = (
        df["cnpj_basico"].str.zfill(8)
        + df["cnpj_ordem"].str.zfill(4)
        + df["cnpj_dv"].str.zfill(2)
    )
    return df


def _transform_simples(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["data_opcao_simples", "data_exclusao_simples",
                "data_opcao_mei", "data_exclusao_mei"]:
        df[col] = df[col].apply(_normalize_date)
    return df


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(chunk_size: int = _DEFAULT_CHUNK_SIZE):
    log.info(f"[cnpj] Iniciando extração de ZIPs  (chunk_size={chunk_size:,})")
    snapshots = _discover_snapshots()
    if not snapshots:
        log.warning(f"  Nenhum snapshot encontrado em {DATA_DIR}")
        log.warning("  Baixe os ZIPs em: https://dadosabertos.rfb.gov.br/CNPJ/")
        log.warning("  e coloque em: data/cnpj/YYYY-MM/")
        return

    for snapshot, snap_dir in snapshots:
        log.info(f"  [snapshot {snapshot}] processando {snap_dir}")
        out_dir = snap_dir / "csv"
        out_dir.mkdir(exist_ok=True)

        zips = {z.stem.upper(): z for z in snap_dir.glob("*.zip")}

        # ── tabelas de domínio (pequenas, sem chunking especial) ──────────
        domain_map = {
            "CNAES":         ("cnaes",        DOMAIN_SCHEMAS["cnaes"]),
            "NATUREZAS":     ("naturezas",     DOMAIN_SCHEMAS["naturezas"]),
            "QUALIFICACOES": ("qualificacoes", DOMAIN_SCHEMAS["qualificacoes"]),
            "MOTIVOS":       ("motivos",       DOMAIN_SCHEMAS["motivos"]),
            "MUNICIPIOS":    ("municipios_rf", DOMAIN_SCHEMAS["municipios_rf"]),
            "PAISES":        ("paises",        DOMAIN_SCHEMAS["paises"]),
        }
        for zip_stem, (out_name, cols) in domain_map.items():
            zip_path = zips.get(zip_stem)
            if zip_path:
                log.info(f"    {zip_stem} → {out_name}.csv")
                _process_domain(out_name, cols, zip_path, out_dir, snapshot, chunk_size)
            else:
                log.warning(f"    ZIP não encontrado: {zip_stem}.zip")

        # ── arquivos numerados (grandes — processados chunk a chunk) ──────
        def _numbered(prefix: str) -> list[Path]:
            return sorted(snap_dir.glob(f"{prefix}*.zip"), key=lambda p: p.stem)

        emp_zips = _numbered("Empresas")
        if emp_zips:
            log.info(f"    Empresas ({len(emp_zips)} ZIPs, chunk={chunk_size:,})")
            _process_main("empresas", EMPRESAS_COLS, emp_zips, out_dir,
                          snapshot, _transform_empresas, chunk_size)

        soc_zips = _numbered("Socios")
        if soc_zips:
            log.info(f"    Socios ({len(soc_zips)} ZIPs, chunk={chunk_size:,})")
            _process_main("socios", SOCIOS_COLS, soc_zips, out_dir,
                          snapshot, _transform_socios, chunk_size)

        est_zips = _numbered("Estabelecimentos")
        if est_zips:
            log.info(f"    Estabelecimentos ({len(est_zips)} ZIPs, chunk={chunk_size:,})")
            _process_main("estabelecimentos", ESTABELECIMENTOS_COLS, est_zips, out_dir,
                          snapshot, _transform_estabelecimentos, chunk_size)

        sim_zip = zips.get("SIMPLES")
        if sim_zip:
            log.info(f"    Simples (chunk={chunk_size:,})")
            _process_main("simples", SIMPLES_COLS, [sim_zip], out_dir,
                          snapshot, _transform_simples, chunk_size)

        log.info(f"  [snapshot {snapshot}] CSVs salvos em {out_dir}")

    log.info("[cnpj] Extração concluída")