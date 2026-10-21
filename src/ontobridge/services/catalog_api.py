"""Catalog service: browse the ontology, the reusable dimension definitions, and query the graph.

    uvicorn ontobridge.services.catalog_api:app --port 8001
"""

from __future__ import annotations

from dataclasses import asdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from rdflib.plugins.sparql import prepareQuery

from ..dimensions import define_dimensions
from ..ontology import PREFIXES
from ..profiler import ColumnProfile, infer_type
from ..retrieval import Retriever
from . import OntologyDep, StoreDep

app = FastAPI(title="OntoBridge catalog", version="0.1.0",
              description="Ontology properties, conformed dimension definitions, candidate retrieval and SPARQL.")
MAX_ROWS = 1000


class RetrieveRequest(BaseModel):
    column: str = Field(min_length=1, max_length=200)
    domain: str = Field(pattern="^(core|insurance|retail|healthcare|finance)$")
    samples: list[str] = Field(default_factory=list, max_length=200)


class SparqlRequest(BaseModel):
    query: str = Field(min_length=1, max_length=10_000)


@app.get("/health")
def health(onto: OntologyDep) -> dict:
    return {"status": "ok", "service": "catalog", "triples": len(onto.graph)}


@app.get("/properties")
def properties(onto: OntologyDep, area: str | None = None, kind: str | None = None) -> list[dict]:
    return [asdict(p) for p in sorted(onto.properties.values(), key=lambda p: p.name)
            if (area is None or p.area == area) and (kind is None or p.kind == kind)]


@app.get("/properties/{name}")
def property_detail(name: str, onto: OntologyDep) -> dict:
    p = onto.properties.get(name)
    if not p:
        raise HTTPException(404, f"ob:{name} is not a property in the ontology")
    return asdict(p) | {"context": onto.context([name])}


@app.get("/dimensions")
def dimensions(onto: OntologyDep, st: StoreDep, run_id: str | None = None) -> list[dict]:
    run = st.run(run_id) if run_id else st.latest_run()
    if not run:
        raise HTTPException(404, "no alignment run yet; POST /runs on the pipeline service first")
    return [d.as_dict() for d in define_dimensions(onto, st.decisions(run.run_id))]


@app.post("/retrieve")
def retrieve(req: RetrieveRequest, onto: OntologyDep) -> dict:
    """What the retriever agent sees for one column: SPARQL terms, scored candidates and the RAG context."""
    present = [s for s in req.samples if s.strip()]
    profile = ColumnProfile(req.column, infer_type(present), 1 - len(present) / max(len(req.samples), 1),
                            len(set(present)) / max(len(present), 1), present[:50])
    r = Retriever(onto).retrieve(profile, req.domain)
    return {"column": r.column, "terms": r.terms, "profile": profile.as_dict(),
            "candidates": [c.as_dict() for c in r.candidates], "context": r.context}


@app.post("/sparql")
def sparql(req: SparqlRequest, onto: OntologyDep) -> dict:
    """Read-only: SELECT and ASK only (updates do not parse as queries). Common prefixes are predeclared."""
    try:
        query = prepareQuery(PREFIXES + req.query)
    except Exception as e:
        raise HTTPException(400, f"SPARQL parse error: {e}") from e
    if query.algebra.name not in ("SelectQuery", "AskQuery"):
        raise HTTPException(400, "only SELECT and ASK queries are allowed")
    result = onto.graph.query(query)
    if query.algebra.name == "AskQuery":
        return {"boolean": bool(result.askAnswer)}
    variables = [str(v) for v in result.vars]
    rows = [[None if v is None else str(v) for v in row] for _, row in zip(range(MAX_ROWS + 1), result, strict=False)]
    return {"vars": variables, "rows": rows[:MAX_ROWS], "truncated": len(rows) > MAX_ROWS}
