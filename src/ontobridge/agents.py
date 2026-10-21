"""The alignment graph: LangGraph agents that map one source's columns to the ontology.

    profiler -> retriever -> mapper -> validator -+-> review_queue -+-> lineage -> END
                                                  +-----------------+

profiler      infers each column's XSD type, null rate and sample values
retriever     SPARQL keyword search over the ontology, scores candidates, builds RAG context
mapper        decides accepted / needs_review / unmapped; consults the LLM (if configured) on uncertain columns
validator     ontology consistency checks: one column per property, type compatibility
review_queue  only runs when something needs a human; lists what to look at and why
lineage       records every decision as PROV-O
"""

from __future__ import annotations

import operator
from collections import defaultdict
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph
from rdflib import Graph

from .config import Settings
from .lineage import mapping_graph, new_graph
from .llm import Adjudicator
from .models import CandidateOut, MappingDecision, RunSummary
from .ontology import Ontology
from .profiler import ColumnProfile, profile_source
from .retrieval import Retrieval, Retriever
from .sources import Source
from .store import Store, now

MARGIN = 0.03  # an accept needs this lead over the runner-up, otherwise two properties are too close to call
LLM_FLOOR = 0.40  # below this nothing retrieved is plausible enough to ask a model about
LLM_CONFIDENT = 0.80


class AlignState(TypedDict, total=False):
    run_id: str
    started_at: str
    source: Source
    profiles: list[ColumnProfile]
    retrievals: list[Retrieval]
    decisions: list[MappingDecision]
    review: list[dict]
    lineage: Graph
    log: Annotated[list[str], operator.add]


def heuristic(source_id: str, profile: ColumnProfile, r: Retrieval, s: Settings) -> MappingDecision:
    cands = [CandidateOut(**c.as_dict()) for c in r.candidates]
    base = {"source_id": source_id, "column": profile.name, "candidates": cands, "profile": profile.as_dict()}
    if not cands:
        return MappingDecision(**base, property=None, status="unmapped", score=0.0,
                               reason="no ontology property shares a meaningful term with this column")
    top = cands[0]
    runner_up = cands[1].score if len(cands) > 1 else 0.0
    if top.score < s.abstain_threshold:
        return MappingDecision(**base, property=None, status="unmapped", score=top.score,
                               reason=f"best candidate ob:{top.property} scored {top.score:.2f} < {s.abstain_threshold}")
    if top.score >= s.accept_threshold and top.score - runner_up >= MARGIN:
        return MappingDecision(**base, property=top.property, status="accepted", score=top.score,
                               reason=f"ob:{top.property} scored {top.score:.2f}, "
                               + (f"{top.score - runner_up:.2f} ahead of ob:{cands[1].property}" if len(cands) > 1
                                  else "the only candidate retrieved"))
    why = (f"ob:{top.property} {top.score:.2f} vs ob:{cands[1].property} {runner_up:.2f} is too close"
           if top.score >= s.accept_threshold else f"ob:{top.property} scored {top.score:.2f}, in the review band")
    return MappingDecision(**base, property=top.property, status="needs_review", score=top.score, reason=why)


def build_alignment_graph(ontology: Ontology, settings: Settings, adjudicator: Adjudicator | None = None):
    retriever = Retriever(ontology)

    def profiler(state: AlignState) -> dict:
        profiles = profile_source(state["source"])
        return {"profiles": profiles, "log": [f"profiler: {len(profiles)} columns"]}

    def retriever_agent(state: AlignState) -> dict:
        domain = state["source"].domain
        retrievals = [retriever.retrieve(p, domain) for p in state["profiles"]]
        hits = sum(bool(r.candidates) for r in retrievals)
        return {"retrievals": retrievals, "log": [f"retriever: candidates for {hits}/{len(retrievals)} columns"]}

    def mapper(state: AlignState) -> dict:
        src = state["source"]
        decisions, consulted = [], 0
        for p, r in zip(state["profiles"], state["retrievals"], strict=True):
            d = heuristic(src.id, p, r, settings)
            if adjudicator and d.status != "accepted" and r.candidates and r.candidates[0].score >= LLM_FLOOR:
                consulted += 1
                d = apply_verdict(d, adjudicator(src.id, src.domain, p, r), settings)
            decisions.append(d)
        return {"decisions": decisions, "log": [f"mapper: {consulted} columns sent to the LLM"] if adjudicator else []}

    def validator(state: AlignState) -> dict:
        decisions = [d.model_copy() for d in state["decisions"]]
        notes = []
        by_property: dict[str, list[MappingDecision]] = defaultdict(list)
        for d in decisions:
            if d.property and d.status != "unmapped":
                by_property[d.property].append(d)
        for prop, ds in by_property.items():
            ds.sort(key=lambda d: -d.score)
            for d in ds[1:]:  # a property describes one column per source; the weaker claims go to a human
                d.status = "needs_review"
                d.reason += f"; ob:{prop} is also claimed by {ds[0].column} ({ds[0].score:.2f})"
                notes.append(f"{d.column} duplicates ob:{prop}")
        for d in decisions:
            chosen = next((c for c in d.candidates if c.property == d.property), None)
            if d.status == "accepted" and chosen and chosen.components.get("type", 1) == 0:
                d.status = "needs_review"
                d.reason += f"; inferred type {d.profile.get('type')} does not fit the range of ob:{d.property}"
                notes.append(f"{d.column} type conflict")
        return {"decisions": decisions, "log": [f"validator: {len(notes)} issues" + (f" ({'; '.join(notes)})" if notes else "")]}

    def review_queue(state: AlignState) -> dict:
        queue = [{"column": d.column, "suggested": d.property, "score": round(d.score, 3), "reason": d.reason}
                 for d in state["decisions"] if d.status == "needs_review"]
        return {"review": queue, "log": [f"review_queue: {len(queue)} columns wait for a reviewer"]}

    def lineage(state: AlignState) -> dict:
        g = mapping_graph(state["run_id"], state["started_at"], state["decisions"])
        return {"lineage": g, "log": [f"lineage: {len(g)} PROV-O triples"]}

    def route(state: AlignState) -> str:
        return "review_queue" if any(d.status == "needs_review" for d in state["decisions"]) else "lineage"

    g = StateGraph(AlignState)
    g.add_node("profiler", profiler)
    g.add_node("retriever", retriever_agent)
    g.add_node("mapper", mapper)
    g.add_node("validator", validator)
    g.add_node("review_queue", review_queue)
    g.add_node("lineage", lineage)
    g.add_edge(START, "profiler")
    g.add_edge("profiler", "retriever")
    g.add_edge("retriever", "mapper")
    g.add_edge("mapper", "validator")
    g.add_conditional_edges("validator", route, ["review_queue", "lineage"])
    g.add_edge("review_queue", "lineage")
    g.add_edge("lineage", END)
    return g.compile()


def apply_verdict(d: MappingDecision, v, settings: Settings) -> MappingDecision:
    """Fold an LLM verdict into a heuristic decision. The model can promote, never silently drop."""
    if v is None:
        return d.model_copy(update={"reason": d.reason + "; LLM reply unusable, heuristic kept"})
    if v.property is None:
        return d.model_copy(update={"reason": d.reason + f"; LLM found no fit ({v.reason})"})
    score = next(c.score for c in d.candidates if c.property == v.property)
    confident = v.confidence >= LLM_CONFIDENT and score >= settings.abstain_threshold
    return d.model_copy(update={
        "property": v.property, "score": score, "method": "llm",
        "status": "accepted" if confident else "needs_review",
        "reason": f"LLM chose ob:{v.property} ({v.confidence:.2f}): {v.reason}",
    })


def align(sources: list[Source], store: Store, ontology: Ontology, settings: Settings,
          adjudicator: Adjudicator | None = None, actor: str = "pipeline") -> RunSummary:
    """Run the alignment graph over each source and persist decisions, audit entries and lineage."""
    llm = settings.llm if adjudicator else "none"
    run_id = store.create_run(llm, [s.id for s in sources])
    started = now()
    store.audit(actor, "run.started", run_id, {"sources": [s.id for s in sources], "llm": llm})
    graph = build_alignment_graph(ontology, settings, adjudicator)
    lineage = new_graph()
    for source in sources:
        out = graph.invoke({"run_id": run_id, "started_at": started, "source": source, "log": []})
        store.save_decisions(run_id, out["decisions"])
        lineage += out["lineage"]
        counts = {k: sum(d.status == k for d in out["decisions"]) for k in ("accepted", "needs_review", "unmapped")}
        store.audit("agent:alignment-graph", "source.aligned", source.id,
                    {"run_id": run_id, "counts": counts, "trace": out["log"], "review": out.get("review", [])})
    path = settings.runtime_dir / "lineage" / f"{run_id}.ttl"
    path.parent.mkdir(parents=True, exist_ok=True)
    lineage.serialize(path, format="turtle")
    summary = store.finish_run(run_id)
    store.audit(actor, "run.finished", run_id, {"counts": summary.counts, "lineage": path.name})
    return summary
