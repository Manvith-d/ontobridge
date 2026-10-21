"""Command line: `ontobridge align | eval | model | audit | sparql | serve`."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import ROOT, get_settings
from .ontology import Ontology
from .sources import discover
from .store import Store


def _adjudicator(spec: str):
    from .llm import Adjudicator, chat_model

    model = chat_model(spec)
    return Adjudicator(model) if model else None


def cmd_align(args, settings, ontology, store) -> int:
    from .agents import align

    sources = [s for s in discover(settings.sources_dir) if not args.source or s.id in args.source]
    if not sources:
        print("no sources matched", file=sys.stderr)
        return 1
    run = align(sources, store, ontology, settings, _adjudicator(args.llm or settings.llm), actor="cli")
    print(f"{run.run_id}: {run.counts}")
    for d in store.decisions(run.run_id):
        target = f"ob:{d.property}" if d.property else "-"
        print(f"  {d.status:<12} {d.score:.2f}  {d.source_id:<34} {d.column:<24} {target}")
    return 0


def cmd_eval(args, settings, ontology, store) -> int:
    from .eval import evaluate

    report = evaluate(ontology, settings, _adjudicator(args.llm or settings.llm))
    for split in ("tuning", "holdout"):
        m = report[split]
        print(f"{split:<8} columns={m['columns']} accepted={m['accepted']} precision={m['accepted_precision']:.1%} "
              f"review={m['needs_review']} unmapped={m['unmapped']} top1={m['top1_accuracy']:.1%} "
              f"silent_errors={m['silent_errors']}")
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2))
    return 0 if report["holdout"]["silent_errors"] == 0 or args.no_fail else 1


def cmd_model(args, settings, ontology, store) -> int:
    from .dimensions import define_dimensions
    from .llm import chat_model
    from .starschema import build_all, run_local, write

    run = store.run(args.run) if args.run else store.latest_run()
    if not run:
        print("no finished alignment run; run `ontobridge align` first", file=sys.stderr)
        return 1
    decisions = store.decisions(run.run_id)
    dims = define_dimensions(ontology, decisions, chat_model(args.llm or "none"))
    sources = discover(settings.sources_dir)
    raw = "Files/raw" if args.target == "fabric" else str(settings.sources_dir)
    models = build_all(sources, decisions, ontology, dims, args.target, raw)
    for path in write(models, dims, Path(args.out), run.run_id):
        print(f"wrote {path}")
    store.audit("cli", "model.generated", run.run_id, {"target": args.target, "out": args.out,
                                                       "tables": sum(len(m.tables) for m in models)})
    if args.execute:
        local = models if args.target == "local" else build_all(sources, decisions, ontology, dims, "local",
                                                                str(settings.sources_dir))
        report = run_local(local, settings.runtime_dir / "spark-warehouse")
        failed = {k: v for r in report.values() for k, v in r["checks"].items() if v}
        for domain, r in report.items():
            print(f"{domain}: " + ", ".join(f"{t.split('.')[1]}={n}" for t, n in r["tables"].items()))
        print(f"checks: {sum(len(r['checks']) for r in report.values())} run, {len(failed)} failed {failed or ''}")
        store.audit("cli", "model.executed", run.run_id, {"checks_failed": failed, "report": report})
        return 1 if failed else 0
    return 0


def cmd_audit(args, settings, ontology, store) -> int:
    if args.verify:
        result = store.verify()
        print(json.dumps(result))
        return 0 if result["ok"] else 1
    for e in reversed(store.audit_log(limit=args.limit)):
        print(f"{e.seq:>5} {e.ts} {e.actor:<24} {e.action:<16} {e.subject}")
    return 0


def cmd_sparql(args, settings, ontology, store) -> int:
    from .ontology import PREFIXES

    for row in ontology.graph.query(PREFIXES + args.query):
        print(" | ".join(str(v) for v in row))
    return 0


def cmd_serve(args, settings, ontology, store) -> int:
    import uvicorn

    uvicorn.run(f"ontobridge.services.{args.service}_api:app", host=args.host, port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ontobridge", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("align", help="run the LangGraph alignment agents over the sources")
    a.add_argument("--source", action="append", help="source id such as insurance/policy_admin.csv (repeatable)")
    a.add_argument("--llm", help="none | ollama:<model> | anthropic:<model> (default: $ONTOBRIDGE_LLM)")
    e = sub.add_parser("eval", help="score alignment against the gold key and held-out renames")
    e.add_argument("--llm")
    e.add_argument("--json", help="also write the full report here")
    e.add_argument("--no-fail", action="store_true", help="exit 0 even if a wrong mapping was auto-accepted")
    m = sub.add_parser("model", help="generate star schemas (Spark SQL) from the latest alignment")
    m.add_argument("--target", choices=["fabric", "local"], default="fabric")
    m.add_argument("--out", default=str(ROOT / "fabric"))
    m.add_argument("--run", help="alignment run id (default: latest)")
    m.add_argument("--llm", help="model for dimension definitions (default: ontology template)")
    m.add_argument("--execute", action="store_true", help="also run the SQL on local Spark and validate it")
    au = sub.add_parser("audit", help="show or verify the hash-chained audit log")
    au.add_argument("--verify", action="store_true")
    au.add_argument("--limit", type=int, default=50)
    q = sub.add_parser("sparql", help="run a SPARQL query over the ontology (prefixes ob:, qb:, skos: ... preset)")
    q.add_argument("query")
    sv = sub.add_parser("serve", help="run one of the FastAPI services")
    sv.add_argument("service", choices=["pipeline", "catalog"])
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8000)
    args = p.parse_args(argv)

    settings = get_settings()
    ontology = Ontology(settings.ontology_dir)
    store = Store(settings.runtime_dir / "ontobridge.db")
    handler = {"align": cmd_align, "eval": cmd_eval, "model": cmd_model, "audit": cmd_audit,
               "sparql": cmd_sparql, "serve": cmd_serve}[args.cmd]
    return handler(args, settings, ontology, store)


if __name__ == "__main__":
    raise SystemExit(main())
