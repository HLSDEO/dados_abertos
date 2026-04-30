from fastapi import APIRouter, Query, HTTPException
from deps import get_driver
from patterns import PATTERNS, PATTERN_INDEX

router = APIRouter(prefix="/patterns", tags=["patterns"])


def _run_pattern(session, pattern: dict, cnpj: str) -> dict:
    try:
        row = session.run(pattern["cypher"], cnpj=cnpj).single()
    except Exception as exc:
        return {
            "id":          pattern["id"],
            "name_pt":     pattern["name_pt"],
            "risk_level":  pattern["risk_level"],
            "triggered":   False,
            "count":       0,
            "valor_total": None,
            "evidence":    [],
            "error":       str(exc),
        }

    count = int(row["count"]) if row and row["count"] else 0
    return {
        "id":          pattern["id"],
        "name_pt":     pattern["name_pt"],
        "risk_level":  pattern["risk_level"],
        "triggered":   count > 0,
        "count":       count,
        "valor_total": float(row["valor_total"]) if row and row["valor_total"] else None,
        "evidence":    list(row["evidence"]) if row and row["evidence"] else [],
    }


@router.get("/empresa/{cnpj_basico}")
def get_patterns(cnpj_basico: str):
    """
    Executa todos os padrões de corrupção/irregularidade para uma empresa.
    Retorna apenas os padrões disparados (triggered=true) mais os metadados de todos.
    """
    driver = get_driver()

    empresa_nome = None
    with driver.session() as s:
        row = s.run(
            "MATCH (e:Empresa {cnpj_basico: $cnpj}) RETURN e.razao_social AS nome",
            cnpj=cnpj_basico,
        ).single()
        if row:
            empresa_nome = row["nome"]

    results = []
    with driver.session() as s:
        for pattern in PATTERNS:
            results.append(_run_pattern(s, pattern, cnpj_basico))

    triggered = [r for r in results if r["triggered"]]

    return {
        "cnpj_basico":  cnpj_basico,
        "empresa":      empresa_nome,
        "triggered_count": len(triggered),
        "patterns":     results,
    }


@router.get("/empresa/{cnpj_basico}/{pattern_id}")
def get_single_pattern(cnpj_basico: str, pattern_id: str):
    """Executa um padrão específico."""
    pattern = PATTERN_INDEX.get(pattern_id)
    if not pattern:
        raise HTTPException(404, f"Padrão não encontrado: {pattern_id}")

    driver = get_driver()
    with driver.session() as s:
        result = _run_pattern(s, pattern, cnpj_basico)

    return {"cnpj_basico": cnpj_basico, **result}


@router.get("/estado/{uf}")
def get_state_patterns(uf: str, quantidade: int = Query(10, ge=1, le=100)):
    """
    Retorna as top empresas de um estado (UF) com maior número de padrões suspeitos.
    quantidade: número de empresas a retornar (default 10)
    """
    driver = get_driver()
    empresas_stats = []

    with driver.session() as s:
        # Busca empresas no estado usando index
        result = s.run(
            """
            MATCH (e:Empresa)
            WHERE e.uf = toUpper($uf)
            RETURN e.cnpj_basico AS cnpj, e.razao_social AS nome
            LIMIT 500
            """,
            uf=uf,
        ).data()

        if not result:
            return {"uf": uf, "total": 0, "empresas": []}

        # Para cada empresa, conta padrões disparados (limita para evitar timeout)
        for idx, emp in enumerate(result):
            if idx >= 500:  # Limita a 500 empresas para evitar timeout
                break
            cnpj = emp["cnpj"]
            nome = emp["nome"]
            triggered_count = 0

            for pattern in PATTERNS:
                r = _run_pattern(s, pattern, cnpj)
                if r["triggered"]:
                    triggered_count += 1

            if triggered_count > 0:
                empresas_stats.append({
                    "cnpj_basico": cnpj,
                    "empresa":    nome,
                    "triggered_count": triggered_count,
                })

    # Ordena por número de padrões disparados
    empresas_stats.sort(key=lambda x: x["triggered_count"], reverse=True)

    return {
        "uf":          uf,
        "total":       len(empresas_stats),
        "empresas":   empresas_stats[:quantidade],
    }
