"""
Download 8 - PNCP & ComprasNet Contratos

Baixa CSVs do Portal Nacional de Contratações Públicas (PNCP)
e ComprasNet Contratos (contratos e empenhos).

Arquivos baixados para data/pncp_csv/:
  - itens.csv          (PNCP_ITEM_RESULTADO)
  - contratos.csv      (comprasnet-contratos-anual-contratos)
  - empenhos.csv       (comprasnet-contratos-anual-empenhos)

Exemplo:
    python etl/download/8-pncp.py
"""

import csv
import logging
import os
import time
from pathlib import Path
from typing import List, Dict, Tuple

import requests

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data")) / "pncp_csv"

# URLs dos CSVs (ano 2026)
URLS = {
    "itens":
        "https://repositorio.dados.gov.br/seges/comprasgov/anual/2026/comprasGOV-anual-VW_DM_PNCP_ITEM_RESULTADO-2026.csv",
    "contratos":
        "https://repositorio.dados.gov.br/seges/comprasnet_contratos/anual/2026/comprasnet-contratos-anual-contratos-2026.csv",
    "empenhos":
        "https://repositorio.dados.gov.br/seges/comprasnet_contratos/anual/2026/comprasnet-contratos-anual-empenhos-2026.csv",
}

HEADERS = {"User-Agent": "dados-abertos-etl/1.0 (+https://github.com/dadosabertos)"}


def _download(url: str, dest: Path, retries: int = 3, delay: float = 2.0) -> bool:
    """Baixa arquivo e salva em dest. Retorna True se OK."""
    for attempt in range(1, retries + 1):
        try:
            log.info(f"  GET {dest.name}  ({attempt}/{retries})")
            r = requests.get(url, headers=HEADERS, timeout=120)
            if r.status_code == 404:
                log.warning(f"  404 — não disponível")
                return False
            r.raise_for_status()
            content = r.content
            if len(content) < 100:
                log.warning(f"  Arquivo muito pequeno ({len(content)} bytes)")
                return False
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            log.info(f"  OK: {len(content):,} bytes")
            return True
        except requests.RequestException as exc:
            log.warning(f"  Erro: {exc}")
            if attempt < retries:
                time.sleep(delay * attempt)
    log.error(f"  Falha após {retries} tentativas")
    return False


def run() -> None:
    """Baixa os 3 CSVs para data/pncp_csv/."""
    log.info("[pncp] Iniciando downloads")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"[pncp] Destino: {DATA_DIR}")

    ok = 0
    for nome, url in URLS.items():
        dest = DATA_DIR / f"{nome}.csv"
        if dest.exists():
            size = dest.stat().st_size
            log.info(f"  [skip] {dest.name} já existe ({size:,} bytes)")
            ok += 1
            continue
        if _download(url, dest):
            ok += 1

    total = len(URLS)
    log.info(f"[pncp] Concluído: {ok}/{total} arquivos")


# =============================================================================
# Parsers - Transformação de CSV em estruturas normalizadas
# =============================================================================

def _safe_str(v) -> str:
    return str(v or "").strip()


def _safe_float(v) -> str:
    try:
        s = str(v or "").strip().replace(",", ".")
        return str(float(s)) if s else "0"
    except (ValueError, TypeError):
        return "0"


def _safe_int(v) -> str:
    try:
        s = str(v or "").strip()
        return str(int(float(s))) if s else "0"
    except (ValueError, TypeError):
        return "0"


def parse_itens(chunk: List[Dict]) -> Tuple[
    List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Parse do CSV PNCP_ITEM_RESULTADO.

    Campos extraídos (dos solicitados):
      - ni_fornecedor, tipo_pessoa, data_inclusao_pncp,
      - numero_item_pncp, nome_razao_social_fornecedor,
      - codigo_pais, quantidade_homologada,
      - valor_unitario_homologado,
      - orgao_entidade_cnpj,
      - unidade_orgao_codigo_unidade,
      - unidade_orgao_uf_sigla

    Returns:
        (itens, fornecedores, orgaos, municipios, ids_link)
    """
    itens, fornecedores, orgaos, municipios, ids_link = [], [], [], [], []
    seen_forn = set()
    seen_org = set()

    for r in chunk:
        id_contrat = _safe_str(
            r.get("id_contratacao_pncp") or
            r.get("numero_contratacao") or
            r.get("numero_compra") or
            ""
        )
        if not id_contrat:
            continue

        ni_forn = _safe_str(
            r.get("ni_fornecedor") or
            r.get("documento_fornecedor") or
            ""
        )
        tipo_pessoa = _safe_str(r.get("tipo_pessoa") or "")
        if tipo_pessoa.upper() in ("", "N/A"):
            tipo_pessoa = "PJ" if len(ni_forn) >= 14 else \
                          "PF" if len(ni_forn) >= 11 else "PE"

        data_inc = _safe_str(
            r.get("data_inclusao_pncp") or
            r.get("data_inclusao") or
            ""
        )
        numero_item = _safe_str(
            r.get("numero_item_pncp") or
            r.get("numero_item") or
            ""
        )
        nome_forn = _safe_str(
            r.get("nome_razao_social_fornecedor") or
            r.get("razao_social") or
            ""
        )
        cod_pais = _safe_str(r.get("codigo_pais") or "")
        qtd = _safe_float(
            r.get("quantidade_homologada") or
            r.get("quantidade") or
            0
        )
        val_unit = _safe_float(
            r.get("valor_unitario_homologado") or
            r.get("valor_unitario") or
            0
        )

        orgao_cnpj = _safe_str(
            r.get("orgao_entidade_cnpj") or
            r.get("cnpj_orgao") or
            ""
        )
        cod_uni = _safe_str(
            r.get("unidade_orgao_codigo_unidade") or
            r.get("codigo_unidade") or
            ""
        )
        uf_sigla = _safe_str(
            r.get("unidade_orgao_uf_sigla") or
            r.get("uf_sigla") or
            r.get("uf") or
            ""
        )
        nome_uni = _safe_str(
            r.get("unidade_orgao_nome") or
            r.get("nome_unidade") or
            ""
        )
        nome_mun = _safe_str(r.get("municipio_nome") or "")

        item_id = f"{id_contrat}_{numero_item}" if numero_item else id_contrat

        itens.append({
            "item_id":                          item_id,
            "id_contratacao_pncp":              id_contrat,
            "numero_item":                      numero_item,
            "ni_fornecedor":                    ni_forn,
            "tipo_pessoa":                      tipo_pessoa.upper() if tipo_pessoa else "PJ",
            "data_inclusao_pncp":               data_inc[:19] if data_inc else "",
            "nome_razao_social_fornecedor":     nome_forn[:500] if nome_forn else "",
            "codigo_pais":                      cod_pais,
            "quantidade_homologada":            qtd,
            "valor_unitario_homologado":        val_unit,
            "orgao_entidade_cnpj":              orgao_cnpj,
            "unidade_orgao_codigo_unidade":     cod_uni,
            "unidade_orgao_uf_sigla":           uf_sigla,
            "nome_unidade":                     nome_uni[:300],
            "nome_municipio":                   nome_mun[:300],
            "fonte_nome":                       "PNCP — Portal Nacional de Contratações Públicas",
            "fonte_url":                        "https://pncp.gov.br",
        })

        if ni_forn and ni_forn not in seen_forn:
            seen_forn.add(ni_forn)
            fornecedores.append({
                "ni_fornecedor":     ni_forn,
                "doc_fornecedor":    ni_forn,
                "tipo_pessoa":       tipo_pessoa.upper() if tipo_pessoa else "PJ",
                "nome":              nome_forn[:500] if nome_forn else "",
                "fonte_nome":        "PNCP — Portal Nacional de Contratações Públicas",
            })

        if orgao_cnpj and orgao_cnpj not in seen_org:
            seen_org.add(orgao_cnpj)
            orgaos.append({
                "cnpj":              orgao_cnpj,
                "nome":              nome_uni or nome_forn or "Órgão",
                "uf":                uf_sigla,
                "fonte_nome":        "PNCP — Portal Nacional de Contratações Públicas",
            })

        if nome_mun or uf_sigla:
            municipios.append({
                "item_id":           item_id,
                "nome_municipio":    nome_mun or "Não especificado",
                "uf_sigla":          uf_sigla,
            })

        ids_link.append({
            "item_id":              item_id,
            "id_contratacao_pncp":  id_contrat,
        })

    return itens, fornecedores, orgaos, municipios, ids_link


def parse_contratos(chunk: List[Dict]) -> Tuple[
    List[Dict], List[Dict], List[Dict], List[Dict], List[Dict]
]:
    """
    Parse do CSV comprasnet-contratos-anual-contratos.
    """
    contratos, fornecedores, orgaos, vinculos, municipios = [], [], [], [], []
    seen_forn = set()
    seen_org = set()

    for r in chunk:
        cnpj_org = _safe_str(
            r.get("cnpj_ente") or
            r.get("cnpj_orgao") or
            r.get("cnpj") or
            ""
        )
        if not cnpj_org:
            continue

        ano = _safe_int(
            r.get("ano_contrato") or
            r.get("ano") or
            ""
        )
        num_contrato = _safe_str(r.get("numero_contrato") or "")
        seq = _safe_str(r.get("sequencial") or "1")
        valor = _safe_float(
            r.get("valor_global") or
            r.get("valor") or
            0
        )
        objeto = _safe_str(r.get("objeto") or "")
        data_ass = _safe_str(r.get("data_assinatura") or "")
        data_pub = _safe_str(r.get("data_publicacao") or "")

        cnpj_forn = _safe_str(
            r.get("cnpj_fornecedor") or
            r.get("cnpj_contratado") or
            ""
        )
        nome_forn = _safe_str(
            r.get("nome_fornecedor") or
            r.get("razao_social_fornecedor") or
            r.get("nome_razao_social") or
            ""
        )

        contrato_id = f"{cnpj_org}_{ano}_{seq}_{num_contrato}"

        contratos.append({
            "contrato_id":             contrato_id,
            "cnpj_orgao":              cnpj_org,
            "ano_contrato":            ano,
            "numero_contrato":         num_contrato,
            "sequencial":              seq,
            "valor_global":            _safe_float(valor),
            "objeto":                  objeto[:2000],
            "data_assinatura":         data_ass[:19] if data_ass else "",
            "data_publicacao":         data_pub[:19] if data_pub else "",
            "fonte_nome":              "ComprasNet Contratos",
            "fonte_url":               "https://comprasnet.gov.br",
        })

        nome_org = _safe_str(
            r.get("nome_ente") or
            r.get("nome_orgao") or
            cnpj_org
        )
        uf_org = _safe_str(
            r.get("uf_ente") or
            r.get("uf_orgao") or
            ""
        )
        if cnpj_org not in seen_org:
            seen_org.add(cnpj_org)
            orgaos.append({
                "cnpj":              cnpj_org,
                "nome":              nome_org[:300],
                "uf":                uf_org,
                "fonte_nome":        "ComprasNet Contratos",
            })

        if cnpj_forn:
            if cnpj_forn not in seen_forn:
                seen_forn.add(cnpj_forn)
                fornecedores.append({
                    "ni_fornecedor":     cnpj_forn,
                    "doc_fornecedor":    cnpj_forn,
                    "tipo_pessoa":       "PJ",
                    "nome":              nome_forn[:500] if nome_forn else "",
                    "fonte_nome":        "ComprasNet Contratos",
                })
            vinculos.append({
                "contrato_id":       contrato_id,
                "ni_fornecedor":     cnpj_forn,
            })

    return contratos, fornecedores, orgaos, vinculos, municipios


def parse_empenhos(chunk: List[Dict]) -> Tuple[
    List[Dict], List[Dict], List[Dict]
]:
    """
    Parse do CSV comprasnet-contratos-anual-empenhos.
    """
    empenhos, orgaos, vinculos = [], [], []
    seen_org = set()

    for r in chunk:
        cnpj = _safe_str(
            r.get("cnpj_ente") or
            r.get("cnpj_orgao") or
            r.get("cnpj") or
            ""
        )
        if not cnpj:
            continue

        ano = _safe_int(
            r.get("ano_empenho") or
            r.get("ano") or
            ""
        )
        num_emp = _safe_str(r.get("numero_empenho") or "")
        valor = _safe_float(
            r.get("valor_empenho") or
            r.get("valor") or
            0
        )
        num_contrato = _safe_str(r.get("numero_contrato") or "")

        if not num_emp:
            continue

        empenho_id = f"{cnpj}_{ano}_{num_emp}"

        empenhos.append({
            "empenho_id":          empenho_id,
            "cnpj_orgao":          cnpj,
            "ano_empenho":         ano,
            "numero_empenho":      num_emp,
            "valor_empenho":       valor,
            "numero_contrato":     num_contrato,
            "fonte_nome":          "ComprasNet Contratos",
            "fonte_url":           "https://comprasnet.gov.br",
        })

        if num_contrato and cnpj and ano:
            contrato_id = f"{cnpj}_{ano}_1_{num_contrato}"
            vinculos.append({
                "empenho_id":       empenho_id,
                "contrato_id":      contrato_id,
            })

        if cnpj not in seen_org:
            seen_org.add(cnpj)
            nome_org = _safe_str(
                r.get("nome_ente") or
                r.get("nome_orgao") or
                cnpj
            )
            uf_org = _safe_str(
                r.get("uf_ente") or
                r.get("uf_orgao") or
                ""
            )
            orgaos.append({
                "cnpj":              cnpj,
                "nome":              nome_org[:300],
                "uf":                uf_org,
                "fonte_nome":        "ComprasNet Contratos",
            })

    return empenhos, orgaos, vinculos


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    run()
