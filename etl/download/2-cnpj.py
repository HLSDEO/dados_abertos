"""
Download 2 - Receita Federal CNPJ
Não há API pública — os ZIPs são baixados manualmente em:
  https://dadosabertos.rfb.gov.br/CNPJ/

Estrutura esperada em data/cnpj/:
  data/cnpj/{YYYY-MM}/Empresas0.zip
  data/cnpj/{YYYY-MM}/Empresas1.zip
  ...
  data/cnpj/{YYYY-MM}/Socios0.zip
  data/cnpj/{YYYY-MM}/Estabelecimentos0.zip
  data/cnpj/{YYYY-MM}/Cnaes.zip
  data/cnpj/{YYYY-MM}/Municipios.zip   ← integrado com IBGE
  data/cnpj/{YYYY-MM}/Paises.zip       ← integrado com IBGE (futuro)
  data/cnpj/{YYYY-MM}/Naturezas.zip
  data/cnpj/{YYYY-MM}/Qualificacoes.zip
  data/cnpj/{YYYY-MM}/Motivos.zip
  data/cnpj/{YYYY-MM}/Simples.zip

Este script:
  1. Descobre todos os snapshots YYYY-MM disponíveis
  2. Extrai cada ZIP para um diretório temporário
  3. Lê os CSVs (sem cabeçalho, sep=";", encoding=latin-1)
  4. Salva CSVs normalizados em data/cnpj/{YYYY-MM}/csv/
     com cabeçalho e encoding utf-8-sig
"""

import csv
import io
import logging
import os
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[2] / "data")) / "cnpj"

FONTE = {
    "fonte_nome":      "Receita Federal do Brasil",
    "fonte_descricao": "Dados Abertos CNPJ — Cadastro Nacional da Pessoa Jurídica",
    "fonte_url":       "https://dadosabertos.rfb.gov.br/CNPJ/",
    "fonte_licenca":   "Dados Abertos — https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/cnpj/dados-publicos-cnpj",
}

# ── Schemas (sem cabeçalho na fonte) ─────────────────────────────────────────

DOMAIN_SCHEMAS = {
    # padrão: codigo | descricao
    "cnaes":          ["codigo_cnae",          "descricao_cnae"],
    "naturezas":      ["codigo_natureza",       "descricao_natureza"],
    "qualificacoes":  ["codigo_qualificacao",   "descricao_qualificacao"],
    "motivos":        ["codigo_motivo",         "descricao_motivo"],
    "municipios":     ["codigo_municipio_rf",   "nome_municipio"],   # cruzado com IBGE pelo nome
    "paises":         ["codigo_pais",           "nome_pais"],
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

# grupo → (padrão de arquivo, colunas)
MAIN_SCHEMAS = {
    "empresas":         ("*EMPRE*",    EMPRESAS_COLS),
    "socios":           ("*SOCIO*",    SOCIOS_COLS),
    "estabelecimentos": ("*ESTABELE*", ESTABELECIMENTOS_COLS),
    "simples":          ("*SIMPLES*",  SIMPLES_COLS),
}

SNAPSHOT_FMT = "%Y-%m"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _discover_snapshots() -> list[tuple[str, Path]]:
    """Retorna snapshots YYYY-MM que contenham pelo menos um ZIP."""
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
    """Extrai um ZIP e retorna lista de arquivos extraídos."""
    extracted = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            zf.extract(name, dest)
            extracted.append(dest / name)
    return extracted


def _read_rf_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    """Lê CSV da Receita Federal: sem cabeçalho, sep=;, latin-1."""
    return pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        header=None,
        names=columns,
        dtype=str,
        keep_default_na=False,
    )


def _normalize_date(s: str) -> str:
    """00000000 → '' | 20230115 → 2023-01-15"""
    s = s.strip()
    if not s or s == "0" or s == "00000000":
        return ""
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def _normalize_capital(s: str) -> str:
    """'120000000000,00' → '120000000000.00'"""
    s = s.strip()
    if not s:
        return "0.00"
    return s.replace(".", "").replace(",", ".")


def _normalize_cnpj(basico: str, ordem: str = "0001", dv: str = "00") -> str:
    return f"{basico.zfill(8)}{ordem.zfill(4)}{dv.zfill(2)}"


def _add_fonte(df: pd.DataFrame, url_origem: str, snapshot: str) -> pd.DataFrame:
    df = df.copy()
    coletado_em = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for k, v in FONTE.items():
        df[k] = v
    df["fonte_url_origem"]  = url_origem
    df["fonte_snapshot"]    = snapshot
    df["fonte_coletado_em"] = coletado_em
    return df


def _save_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"    → {out_path.name}  ({len(df):,} linhas, {len(df.columns)} colunas)")


# ── Processamento por grupo ───────────────────────────────────────────────────

def _process_domain(name: str, columns: list[str], zip_path: Path,
                    out_dir: Path, snapshot: str) -> None:
    tmp = Path(tempfile.mkdtemp())
    try:
        files = _extract_zip(zip_path, tmp)
        frames = []
        for f in files:
            try:
                df = _read_rf_csv(f, columns)
                frames.append(df)
            except Exception as exc:
                log.warning(f"    Erro ao ler {f.name}: {exc}")
        if not frames:
            return
        df = pd.concat(frames, ignore_index=True)
        df = _add_fonte(df, FONTE["fonte_url"], snapshot)
        _save_csv(df, out_dir / f"{name}.csv")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _process_empresas(zip_paths: list[Path], out_dir: Path, snapshot: str) -> None:
    frames = []
    for zip_path in zip_paths:
        tmp = Path(tempfile.mkdtemp())
        try:
            files = _extract_zip(zip_path, tmp)
            for f in files:
                try:
                    df = _read_rf_csv(f, EMPRESAS_COLS)
                    # normaliza capital social
                    df["capital_social"] = df["capital_social"].apply(_normalize_capital)
                    frames.append(df)
                except Exception as exc:
                    log.warning(f"    Erro ao ler {f.name}: {exc}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    df = _add_fonte(df, FONTE["fonte_url"], snapshot)
    _save_csv(df, out_dir / "empresas.csv")


def _process_socios(zip_paths: list[Path], out_dir: Path, snapshot: str) -> None:
    frames = []
    for zip_path in zip_paths:
        tmp = Path(tempfile.mkdtemp())
        try:
            files = _extract_zip(zip_path, tmp)
            for f in files:
                try:
                    df = _read_rf_csv(f, SOCIOS_COLS)
                    df["data_entrada"] = df["data_entrada"].apply(_normalize_date)
                    frames.append(df)
                except Exception as exc:
                    log.warning(f"    Erro ao ler {f.name}: {exc}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    df = _add_fonte(df, FONTE["fonte_url"], snapshot)
    _save_csv(df, out_dir / "socios.csv")


def _process_estabelecimentos(zip_paths: list[Path], out_dir: Path, snapshot: str) -> None:
    frames = []
    for zip_path in zip_paths:
        tmp = Path(tempfile.mkdtemp())
        try:
            files = _extract_zip(zip_path, tmp)
            for f in files:
                try:
                    df = _read_rf_csv(f, ESTABELECIMENTOS_COLS)
                    df["data_situacao_cadastral"] = df["data_situacao_cadastral"].apply(_normalize_date)
                    df["data_inicio_atividade"]   = df["data_inicio_atividade"].apply(_normalize_date)
                    df["data_situacao_especial"]  = df["data_situacao_especial"].apply(_normalize_date)
                    df["cnpj"] = df.apply(
                        lambda r: _normalize_cnpj(r["cnpj_basico"], r["cnpj_ordem"], r["cnpj_dv"]),
                        axis=1,
                    )
                    frames.append(df)
                except Exception as exc:
                    log.warning(f"    Erro ao ler {f.name}: {exc}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    if not frames:
        return
    df = pd.concat(frames, ignore_index=True)
    df = _add_fonte(df, FONTE["fonte_url"], snapshot)
    _save_csv(df, out_dir / "estabelecimentos.csv")


def _process_simples(zip_path: Path, out_dir: Path, snapshot: str) -> None:
    tmp = Path(tempfile.mkdtemp())
    try:
        files = _extract_zip(zip_path, tmp)
        frames = []
        for f in files:
            try:
                df = _read_rf_csv(f, SIMPLES_COLS)
                for col in ["data_opcao_simples", "data_exclusao_simples",
                            "data_opcao_mei", "data_exclusao_mei"]:
                    df[col] = df[col].apply(_normalize_date)
                frames.append(df)
            except Exception as exc:
                log.warning(f"    Erro ao ler {f.name}: {exc}")
        if not frames:
            return
        df = pd.concat(frames, ignore_index=True)
        df = _add_fonte(df, FONTE["fonte_url"], snapshot)
        _save_csv(df, out_dir / "simples.csv")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ── Entry-point ───────────────────────────────────────────────────────────────

def run():
    log.info("[cnpj] Iniciando extração de ZIPs")
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

        # ── tabelas de domínio ────────────────────────────────────────────
        domain_map = {
            "CNAES":         ("cnaes",         DOMAIN_SCHEMAS["cnaes"]),
            "NATUREZAS":     ("naturezas",      DOMAIN_SCHEMAS["naturezas"]),
            "QUALIFICACOES": ("qualificacoes",  DOMAIN_SCHEMAS["qualificacoes"]),
            "MOTIVOS":       ("motivos",        DOMAIN_SCHEMAS["motivos"]),
            "MUNICIPIOS":    ("municipios_rf",  DOMAIN_SCHEMAS["municipios"]),
            "PAISES":        ("paises",         DOMAIN_SCHEMAS["paises"]),
        }
        for zip_stem, (out_name, cols) in domain_map.items():
            zip_path = zips.get(zip_stem)
            if zip_path:
                log.info(f"    {zip_stem} → {out_name}.csv")
                _process_domain(out_name, cols, zip_path, out_dir, snapshot)
            else:
                log.warning(f"    ZIP não encontrado: {zip_stem}.zip")

        # ── arquivos numerados ────────────────────────────────────────────
        def _numbered(prefix: str) -> list[Path]:
            return sorted(snap_dir.glob(f"{prefix}*.zip"), key=lambda p: p.stem)

        emp_zips = _numbered("Empresas")
        if emp_zips:
            log.info(f"    Empresas ({len(emp_zips)} ZIPs)")
            _process_empresas(emp_zips, out_dir, snapshot)

        soc_zips = _numbered("Socios")
        if soc_zips:
            log.info(f"    Socios ({len(soc_zips)} ZIPs)")
            _process_socios(soc_zips, out_dir, snapshot)

        est_zips = _numbered("Estabelecimentos")
        if est_zips:
            log.info(f"    Estabelecimentos ({len(est_zips)} ZIPs)")
            _process_estabelecimentos(est_zips, out_dir, snapshot)

        sim_zip = zips.get("SIMPLES")
        if sim_zip:
            log.info("    Simples")
            _process_simples(sim_zip, out_dir, snapshot)

        log.info(f"  [snapshot {snapshot}] CSVs em {out_dir}")

    log.info("[cnpj] Extração concluída")