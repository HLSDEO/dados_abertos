from fastapi import APIRouter
from deps import get_driver, run_query

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("/status")
def get_pipeline_status():
    driver = get_driver()
    with driver.session() as s:
        result = run_query(
            s,
            """
            MATCH (r:IngestionRun)
            WITH r.source_id AS source, max(r.started_at) AS last_run
            MATCH (r:IngestionRun {source_id: source, started_at: last_run})
            RETURN source,
                   r.status AS status,
                   r.started_at AS started_at,
                   r.finished_at AS finished_at,
                   r.rows_in AS rows_in,
                   r.rows_out AS rows_out,
                   r.error AS error
            ORDER BY source
            """,
        ).data()

    counts = {"ok": 0, "running": 0, "error": 0}
    items = []

    for row in result:
        status = (row.get("status") or "").lower()
        if status in {"completed", "loaded", "success", "ok"}:
            normalized = "ok"
        elif status in {"running", "started", "processing"}:
            normalized = "running"
        else:
            normalized = "error"

        counts[normalized] += 1
        items.append(
            {
                "source": row.get("source"),
                "status": row.get("status"),
                "status_group": normalized,
                "started_at": row.get("started_at"),
                "finished_at": row.get("finished_at"),
                "rows_in": row.get("rows_in") or 0,
                "rows_out": row.get("rows_out") or 0,
                "error": row.get("error"),
            }
        )

    return {
        "summary": {
            "total": len(items),
            "ok": counts["ok"],
            "running": counts["running"],
            "error": counts["error"],
        },
        "items": items,
    }
