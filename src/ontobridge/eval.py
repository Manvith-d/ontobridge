"""Measure alignment against the gold key, on the original columns and on held-out renamed columns.

The ontology's synonyms were written alongside the synthetic sources, so the original columns
(`tuning`) are an optimistic number. `holdout` renames 54 columns to names written after the
ontology was frozen, which is the honest estimate of how alignment generalizes.
"""

from __future__ import annotations

import json
from dataclasses import replace

from .agents import build_alignment_graph
from .config import Settings
from .llm import Adjudicator
from .models import MappingDecision
from .ontology import Ontology
from .sources import Source, discover


def metrics(pairs: list[tuple[MappingDecision, str | None]]) -> dict:
    n = len(pairs)
    accepted = [(d, e) for d, e in pairs if d.status == "accepted"]
    review = [(d, e) for d, e in pairs if d.status == "needs_review"]
    unmapped = [(d, e) for d, e in pairs if d.status == "unmapped"]
    wrong = [(d, e) for d, e in pairs if (d.property if d.status != "unmapped" else None) != e]
    return {
        "columns": n,
        "accepted": len(accepted),
        "accepted_precision": round(sum(d.property == e for d, e in accepted) / max(len(accepted), 1), 4),
        "needs_review": len(review),
        "review_suggestion_correct": sum(d.property == e for d, e in review),
        "unmapped": len(unmapped),
        "unmapped_correct": sum(e is None for _, e in unmapped),
        "top1_accuracy": round((n - len(wrong)) / max(n, 1), 4),
        "silent_errors": sum(d.property != e for d, e in accepted),  # wrong and nobody was asked
        "errors": [{"source": d.source_id, "column": d.column, "expected": e, "status": d.status,
                    "predicted": d.property, "score": round(d.score, 3)} for d, e in wrong],
    }


def evaluate(ontology: Ontology, settings: Settings, adjudicator: Adjudicator | None = None) -> dict:
    sources = {s.id: s for s in discover(settings.sources_dir)}
    gold = json.loads(settings.gold_path.read_text())["sources"]
    holdout = json.loads((settings.gold_path.parent / "holdout.json").read_text())["columns"]
    graph = build_alignment_graph(ontology, settings, adjudicator)

    def decide(source: Source) -> dict[str, MappingDecision]:
        out = graph.invoke({"run_id": "eval", "started_at": "1970-01-01T00:00:00Z", "source": source, "log": []})
        return {d.column: d for d in out["decisions"]}

    tuning = []
    for sid, columns in gold.items():
        decided = decide(sources[sid])
        tuning += [(decided[c], e) for c, e in columns.items()]

    renamed: dict[str, dict[str, tuple[str, str | None]]] = {}
    for h in holdout:
        renamed.setdefault(h["source"], {})[h["column"]] = (h["renamed"], h["expected"])
    held = []
    for sid, cols in renamed.items():
        src = sources[sid]
        records = [{cols.get(k, (k,))[0]: v for k, v in r.items()} for r in src.records]
        decided = decide(replace(src, records=records))
        held += [(decided[new], e) for new, e in cols.values()]
    return {"llm": settings.llm if adjudicator else "none",
            "tuning": metrics(tuning), "holdout": metrics(held)}
