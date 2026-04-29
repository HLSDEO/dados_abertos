"""
Download 6 - Servidores Públicos Federais / Portal da Transparência
Fonte: https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores/

Baixa os arquivos mensais de três categorias:
  SIAPE     → servidores civis do executivo federal
  BACEN     → servidores do Banco Central
  Militares → militares das Forças Armadas

Cada mês gera 2 ZIPs por categoria (Cadastro + Remuneração), totalizando 6 ZIPs/mês.
Extrai e salva CSVs normalizados em:
  data/servidores/{ano}/{mes:02d}/cadastro.csv
  data/servidores/{ano}/{mes:02d}/remuneracao.csv

Arquivos já existentes são pulados (idempotente).

Uso:
  python main.py download servidores_cgu                          # mês atual
  python main.py download servidores_cgu --ano 2025               # todos os meses de 2025
  python main.py download servidores_cgu --ano 2025 --mes 1       # jan/2025
  python main.py download servidores_cgu --ano 2025 --mes 1 --mes 6  # jan e jun/2025
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

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "servidores"
CGU_BASE = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores"

FONTE = {
    "fonte_nome":      "CGU — Controladoria-Geral da União",
    "fonte_descricao": "Portal da Transparência — Servidores Públicos Federais",
    "fonte_url":       "https://portaldatransparencia.gov.br/servidores",
    "fonte_licenca":   "Dados Abertos — https://creativecommons.org/licenses/by/4.0/",
}

# Nomes exatos dos ZIPs no CGU (sem o prefixo YYYYMM_)
# URL real: {CGU_BASE}/{YYYYMM}_{nome}.zip
TIPOS = {
    "SIAPE":     ["Servidores_SIAPE"],
    "BACEN":     ["Servidores_BACEN"],
    "Militares": ["Militares"],
}

# Colunas de interesse — Cadastro
CADASTRO_COLS = {
    "Id_SERVIDOR_PORTAL":             "id_servidor",
    "CPF":                            "cpf",
    "NOME":                           "nome",
    "CARGO_ATIVIDADE_DENTRO_ORGAO":   "cargo",
    "CLASSE_CARGO_ATIVIDADE":         "classe",
    "REFERENCIA_CARGO_ATIVIDADE":     "referencia",
    "PADRAO_CARGO_ATIVIDADE":         "padrao",
    "NIVEL_CARGO_ATIVIDADE":          "nivel",
    "ORG_LOTACAO":                    "org_lotacao",
    "ORG_EXERCICIO":                  "org_exercicio",
    "UORG_LOTACAO":                   "uorg_lotacao",
    "UORG_EXERCICIO":                 "uorg_exercicio",
    "DATA_INGRESSO_CARGOFUNCAO":      "data_ingresso_cargo",
    "DATA_NOMEACAO_CARGOFUNCAO":      "data_nomeacao_cargo",
    "DATA_INGRESSO_ORGAO":            "data_ingresso_orgao",
    "DOCUMENTO_INGRESSO_SERVICO_PUBLICO": "doc_ingresso",
    "DATA_DIPLOMA_INGRESSO_SERVICO_PUBLICO": "data_ingresso_servico",
    "DIPLOMA_INGRESSO_CARGOFUNCAO":   "diploma_ingresso",
    "DIPLOMA_INGRESSO_ORGAO":         "diploma_ingresso_orgao",
    "DIPLOMA_INGRESSO_SERVICO_PUBLICO": "diploma_ingresso_servico",
    "UF_EXERCICIO":                   "uf_exercicio",
    "MUNICIPIO_EXERCICIO":            "municipio_exercicio",
    "SITUACAO_VINCULO":               "situacao_vinculo",
    "REGIME_JURIDICO":                "regime_juridico",
    "JORNADA_DE_TRABALHO":            "jornada_trabalho",
    "TIPO_VINCULO":                   "tipo_vinculo",
    "ATIVIDADE":                      "atividade",
    "OPCAO_PARCIAL":                  "opcao_parcial",
    "COD_UASG":                       "cd_uasg",
    "SIGLA_FUNCAO":                   "sigla_funcao",
    "NIVEL_FUNCAO":                   "nivel_funcao",
    "FUNCAO":                         "funcao",
    "CODIGO_ATIVIDADE":               "codigo_atividade",
    "PARTIDO_POLITICO":               "partido_politico",
    "PORTARIA":                       "portaria",
}

# Colunas de interesse — Remuneração
REMUNERACAO_COLS = {
    "Id_SERVIDOR_PORTAL":                       "id_servidor",
    "ANO":                                      "ano",
    "MES":                                      "mes",
    "REMUNERAÇÃO BÁSICA BRUTA (R$)":            "remuneracao_bruta",
    "REMUNERAÇÃO BÁSICA BRUTA (U$)":            "remuneracao_bruta_usd",
    "ABATE-TETO (R$)":                          "abate_teto",
    "GRATIFICAÇÃO NATALINA (R$)":               "gratificacao_natalina",
    "FÉRIAS (R$)":                              "ferias",
    "OUTRAS REMUNERAÇÕES EVENTUAIS (R$)":       "outras_remuneracoes",
    "IRRF (R$)":                                "irrf",
    "PSS/RPPS (R$)":                            "pss_rpps",
    "DEMAIS DEDUÇÕES (R$)":                     "demais_deducoes",
    "PENSÃO MILITAR (R$)":                      "pensao_militar",
    "FUNDO DE SAÚDE (R$)":                      "fundo_saude",
    "TCU/CGU (R$)":                             "tcu_cgu",
    "REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)": "remuneracao_liquida",
    "VERBAS INDENIZATÓRIAS REGISTRADAS NO SISTEMA DE REMUNERAÇÃO FIXA": "verbas_indenizatorias",
    "OUTRAS VERBAS INDENIZATÓRIAS, PROVISÓRIAS E EVENTUAIS": "outras_verbas",
    "TOTAL DE CRÉDITOS DECORRENTES DE DECISÃO JUDICIAL": "total_judicial",
    "GRATIFICAÇÕES E ADICIONAIS NÃO INCLUÍDOS NA REMUNERAÇÃO BÁSICA": "gratificacoes_adicionais",
    "SUBSTITUIÇÕES":                            "substituicoes",
    "TOTAL BRUTO":                              "total_bruto",
    "OBSERVAÇÕES":                              "observacoes",
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _download_to_tmp(url: str, retries: int = 3,
                     delay: float = 5.0) -> Path | None:
    import tempfile
    for attempt in range(1, retries + 1):
        try:
            log.info(f"    GET {url}  ({attempt}/{retries})")
            r = requests.get(url, timeout=180, stream=True,
                             headers={"User-Agent": "dados-abertos-etl/1.0"})
            if r.status_code == 404:
                log.warning(f"    404 — não disponível: {url}")
                return None
            r.raise_for_status()
            tmp = Path(tempfile.mktemp(suffix=".zip"))
            downloaded = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
            log.info(f"    {downloaded/1e6:.1f} MB baixados")
            return tmp
        except requests.RequestException as exc:
            log.warning(f"    Erro: {exc}")
            if attempt < retries:
                time.sleep(delay)
    return None


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _add_fonte(row: dict, url: str, categoria: str, ano: int, mes: int) -> dict:
    row.update({
        **FONTE,
        "fonte_url_origem":  url,
        "fonte_categoria":   categoria,
        "fonte_ano":         str(ano),
        "fonte_mes":         f"{mes:02d}",
        "fonte_coletado_em": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    return row


def _normalize_valor(s: str) -> str:
    s = (s or "").strip().replace("\xa0", "").replace(" ", "")
    if not s or s in ("-", "*", "**"):
        return "0"
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        float(s)
        return s
    except ValueError:
        return "0"


COLS_VALOR = {
    "remuneracao_bruta", "remuneracao_bruta_usd", "abate_teto",
    "gratificacao_natalina", "ferias", "outras_remuneracoes",
    "irrf", "pss_rpps", "demais_deducoes", "pensao_militar",
    "fundo_saude", "tcu_cgu", "remuneracao_liquida",
    "verbas_indenizatorias", "outras_verbas", "total_judicial",
    "gratificacoes_adicionais", "substituicoes", "total_bruto",
}


def _extract_csv_member(tmp_zip: Path, member: str, col_map: dict,
                        url: str, categoria: str, ano: int, mes: int) -> list[dict]:
    """Extrai um CSV específico do ZIP, filtra colunas e normaliza."""
    with zipfile.ZipFile(tmp_zip) as zf:
        raw = zf.read(member)

    # detecta encoding
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw.decode("utf-16").lstrip("\ufeff")
    elif raw[:3] == b"\xef\xbb\xbf":
        text = raw.decode("utf-8-sig")
    else:
        text = raw.decode("latin-1", errors="replace")

    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return []

    # detecta separador
    sep = ";"
    if lines[0] and ";" not in lines[0] and "\t" in lines[0]:
        sep = "\t"

    reader = csv.DictReader(lines, delimiter=sep)
    if not reader.fieldnames:
        return []

    rows = []
    for row in reader:
        row = {k: (v or "").strip() for k, v in row.items() if k is not None}
        # remapeia colunas
        mapped = {}
        for orig, dest in col_map.items():
            if orig in row:
                val = row[orig]
                if dest in COLS_VALOR:
                    val = _normalize_valor(val)
                mapped[dest] = val
        if not mapped:
            continue
        _add_fonte(mapped, url, categoria, ano, mes)
        rows.append(mapped)

    return rows


class _CsvAppender:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f      = open(path, "a", newline="", encoding="utf-8-sig")
        self._writer = None
        self._total  = 0
        self._new    = path.stat().st_size == 0 if path.exists() else True

    def write(self, rows: list[dict]) -> None:
        if not rows:
            return
        if self._writer is None:
            self._writer = csv.DictWriter(
                self._f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
            if self._new:
                self._writer.writeheader()
        for row in rows:
            for k in self._writer.fieldnames:
                row.setdefault(k, "")
        self._writer.writerows(rows)
        self._total += len(rows)

    def close(self) -> int:
        self._f.close()
        return self._total


# ── Processamento por mês ─────────────────────────────────────────────────────

def _process_mes(ano: int, mes: int) -> None:
    mes_dir  = DATA_DIR / str(ano) / f"{mes:02d}"
    cad_path = mes_dir / "cadastro.csv"
    rem_path = mes_dir / "remuneracao.csv"

    if cad_path.exists() and rem_path.exists():
        log.info(f"  {ano}/{mes:02d} — já existe ({cad_path.stat().st_size/1e6:.1f} MB) — pulando")
        return

    mes_dir.mkdir(parents=True, exist_ok=True)
    cad_writer = _CsvAppender(cad_path)
    rem_writer = _CsvAppender(rem_path)
    cad_total  = rem_total = 0
    date_str   = f"{ano}{mes:02d}"

    for categoria, nomes in TIPOS.items():
        for nome in nomes:
            # URL exata: YYYYMM_Servidores_SIAPE.zip / YYYYMM_Servidores_BACEN.zip / YYYYMM_Militares.zip
            filename = f"{date_str}_{nome}.zip"
            url      = f"{CGU_BASE}/{filename}"
            tmp_zip  = _download_to_tmp(url)
            if not tmp_zip:
                continue

            try:
                # Cada ZIP contém múltiplos CSVs — separamos Cadastro de Remuneracao pelo nome
                with zipfile.ZipFile(tmp_zip) as zf:
                    members = [m for m in zf.namelist()
                               if m.lower().endswith(".csv") and "__macosx" not in m.lower()]

                log.info(f"    {filename}: {len(members)} CSV(s) → {[m for m in members[:4]]}")

                for member in members:
                    is_cad = "cadastro" in member.lower()
                    is_rem = "remuner" in member.lower()
                    if not is_cad and not is_rem:
                        log.warning(f"    Arquivo não reconhecido: {member} — pulando")
                        continue

                    col_map = CADASTRO_COLS if is_cad else REMUNERACAO_COLS
                    rows    = _extract_csv_member(tmp_zip, member, col_map,
                                                  url, categoria, ano, mes)
                    if rows:
                        if is_cad:
                            cad_writer.write(rows)
                            cad_total += len(rows)
                        else:
                            rem_writer.write(rows)
                            rem_total += len(rows)
                        log.info(f"    {member}: {len(rows):,} linhas")
            finally:
                tmp_zip.unlink(missing_ok=True)

            time.sleep(0.5)

    cad_writer.close()
    rem_writer.close()

    if cad_total == 0:
        cad_path.unlink(missing_ok=True)
    if rem_total == 0:
        rem_path.unlink(missing_ok=True)

    if cad_total or rem_total:
        log.info(f"  {ano}/{mes:02d} ✓  cadastro={cad_total:,}  remuneração={rem_total:,}")
    else:
        log.warning(f"  {ano}/{mes:02d} — nenhum dado obtido")


# ── Entry-point ───────────────────────────────────────────────────────────────

def run(anos: list[int] | None = None, meses: list[int] | None = None):
    """
    anos:  lista de anos a processar. None = ano atual.
    meses: lista de meses (1-12). None = todos os meses do ano.

    Passado pelo CLI via:
      --ano 2025
      --ano 2025 --mes 1 --mes 6
    """
    hoje = datetime.now()

    if not anos:
        anos = [hoje.year]

    if not meses:
        meses = list(range(1, 13))

    log.info(f"[servidores] anos={anos}  meses={meses}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for ano in sorted(anos):
        for mes in sorted(meses):
            # não tenta baixar meses futuros
            if ano == hoje.year and mes > hoje.month:
                continue
            log.info(f"  === {ano}/{mes:02d} ===")
            _process_mes(ano, mes)
            time.sleep(1)

    log.info("[servidores] Download concluído")