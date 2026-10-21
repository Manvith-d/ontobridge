"""Pipeline service: run the alignment agents, review their decisions, generate star schemas,
and read the audit log and lineage.

    uvicorn ontobridge.services.pipeline_api:app --port 8000
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Response, status
from pydantic import BaseModel

from ..agents import align
from ..dimensions import define_dimensions
from ..lineage import mapping_graph
from ..llm import Adjudicator, chat_model
from ..models import AuditEntry, ReviewAction, RunSummary, StoredDecision
from ..sources import discover
from ..starschema import build_all
from ..store import Store
from . import OntologyDep, SettingsDep, StoreDep

app = FastAPI(title="OntoBridge pipeline", version="0.1.0",
              description="Alignment runs, human review of agent decisions, star schema generation, audit and lineage.")


class RunRequest(BaseModel):
    sources: list[str] | None = None  # source ids; default is every source


def _run(run_id: str, st: Store) -> RunSummary:
    run = st.run(run_id)
    if not run:
        raise HTTPException(404, f"run {run_id} not found")
    return run


@app.get("/health")
def health(onto: OntologyDep) -> dict:
    return {"status": "ok", "service": "pipeline", "properties": len(onto.properties)}


@app.post("/runs", status_code=status.HTTP_201_CREATED)
def create_run(req: RunRequest, st: StoreDep, onto: OntologyDep,
               cfg: SettingsDep) -> RunSummary:
    available = discover(cfg.sources_dir)
    chosen = [s for s in available if req.sources is None or s.id in req.sources]
    unknown = set(req.sources or []) - {s.id for s in available}
    if unknown or not chosen:
        raise HTTPException(422, f"unknown sources: {sorted(unknown)}" if unknown else "no sources selected")
    model = chat_model(cfg.llm)  # the server's configured model only; clients cannot pick a paid one
    return align(chosen, st, onto, cfg, Adjudicator(model) if model else None, actor="api")


@app.get("/runs")
def list_runs(st: StoreDep) -> list[RunSummary]:
    return st.runs()


@app.get("/runs/{run_id}")
def get_run(run_id: str, st: StoreDep) -> RunSummary:
    return _run(run_id, st)


@app.get("/runs/{run_id}/decisions")
def list_decisions(run_id: str, st: StoreDep,
                   status: Literal["accepted", "needs_review", "unmapped"] | None = None,
                   source: str | None = None) -> list[StoredDecision]:
    _run(run_id, st)
    return st.decisions(run_id, status, source)


@app.get("/review")
def review_queue(st: StoreDep, run_id: str | None = None) -> list[StoredDecision]:
    run = _run(run_id, st) if run_id else st.latest_run()
    return st.decisions(run.run_id, "needs_review") if run else []


@app.post("/decisions/{decision_id}/review")
def review(decision_id: int, action: ReviewAction, st: StoreDep,
           onto: OntologyDep) -> StoredDecision:
    d = st.decision(decision_id)
    if not d:
        raise HTTPException(404, f"decision {decision_id} not found")
    if action.action == "approve" and not d.property:
        raise HTTPException(422, "nothing to approve: the agent proposed no property; use override")
    if action.action == "override" and action.property not in onto.properties:
        raise HTTPException(422, f"override needs a known ontology property, got {action.property!r}")
    prop = {"approve": d.property, "reject": None, "override": action.property}[action.action]
    verb = {"approve": "approved", "reject": "rejected", "override": "overrode"}[action.action]
    updated = d.model_copy(update={
        "property": prop, "status": "unmapped" if prop is None else "accepted", "method": "human",
        "reviewed_by": action.reviewer,
        "reason": f"{action.reviewer} {verb}" + (f": {action.note}" if action.note else "") + f" (agent: {d.reason})",
    })
    st.update_decision(updated)
    st.audit(f"reviewer:{action.reviewer}", f"review.{action.action}", f"{d.source_id}#{d.column}", {
        "decision_id": d.id, "run_id": d.run_id, "note": action.note,
        "before": {"status": d.status, "property": d.property, "method": d.method},
        "after": {"status": updated.status, "property": updated.property},
    })
    return updated


@app.get("/runs/{run_id}/lineage", response_class=Response,
         responses={200: {"content": {"text/turtle": {}}}})
def lineage(run_id: str, st: StoreDep) -> Response:
    """PROV-O for the run as it stands now, reviews included."""
    run = _run(run_id, st)
    g = mapping_graph(run.run_id, run.started_at, st.decisions(run.run_id))
    return Response(g.serialize(format="turtle"), media_type="text/turtle")


@app.post("/runs/{run_id}/model")
def model(run_id: str, st: StoreDep, onto: OntologyDep,
          cfg: SettingsDep, target: Literal["fabric", "local"] = "fabric") -> dict:
    """Generate the star schemas for review. Nothing is executed or written to disk."""
    run = _run(run_id, st)
    decisions = st.decisions(run.run_id)
    pending = sum(d.status == "needs_review" for d in decisions)
    dims = define_dimensions(onto, decisions)
    models = build_all(discover(cfg.sources_dir), decisions, onto, dims, target,
                       "Files/raw" if target == "fabric" else str(cfg.sources_dir))
    st.audit("api", "model.generated", run.run_id, {"target": target, "pending_reviews": pending})
    return {
        "run_id": run.run_id, "target": target, "pending_reviews": pending,
        "dimensions": [d.as_dict() for d in dims],
        "domains": {m.domain: {"tables": m.tables, "checks": [n for n, _ in m.checks], "sql": m.sql()}
                    for m in models},
    }


@app.get("/audit")
def audit_log(st: StoreDep, subject: str | None = None,
              limit: Annotated[int, Query(ge=1, le=1000)] = 100) -> list[AuditEntry]:
    return st.audit_log(subject, limit)


@app.get("/audit/verify")
def audit_verify(st: StoreDep) -> dict:
    return st.verify()
