from typing import Any

import httpx

from .config import settings


async def invoke_restate_workflow(
    *,
    workflow_name: str,
    workflow_id: str,
    handler_name: str,
    payload: dict[str, Any],
    arq_job_id: str | None = None,
    job_try: int | None = None,
) -> dict[str, Any]:
    """Invoke a Restate workflow via ARQ
      POST {ingress_base_url}/{WorkflowName}/{workflowId}/{handlerName}/send
    """
    if not settings.RESTATE_INGRESS_BASE_URL:
        raise RuntimeError("RESTATE_INGRESS_BASE_URL is empty; Restate workflows disabled")

    url = (
        f"{settings.RESTATE_INGRESS_BASE_URL.rstrip('/')}/"
        f"{workflow_name}/{workflow_id}/{handler_name}/send"
    )

    body: dict[str, Any] = dict(payload)
    if arq_job_id is not None:
        body["arq_job_id"] = arq_job_id
    if job_try is not None:
        body["arq_job_try"] = job_try

    headers: dict[str, str] = {}
    if arq_job_id is not None:
        headers["X-Arq-Job-Id"] = arq_job_id
    if job_try is not None:
        headers["X-Arq-Job-Try"] = str(job_try)

    async with httpx.AsyncClient(timeout=settings.RESTATE_INVOKE_TIMEOUT_SECONDS) as client:
        resp = await client.post(url, json=body, headers=headers)

    # If the workflow is already accepted (same workflow_id), Restate returns an error.
    if resp.status_code == 409:
        return {
            "ok": True,
            "already_accepted": True,
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
        }

    resp.raise_for_status()
    # Restate returns JSON - otherwise if it doesn't, fall back to something we can work with
    try:
        body = resp.json()
        if isinstance(body, dict):
            body.setdefault("ok", True)
            body.setdefault("workflow_name", workflow_name)
            body.setdefault("workflow_id", workflow_id)
        return body
    except ValueError:
        return {
            "ok": True,
            "workflow_name": workflow_name,
            "workflow_id": workflow_id,
        }

