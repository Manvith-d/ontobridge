from conftest import make_source
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from ontobridge.agents import build_alignment_graph
from ontobridge.config import get_settings
from ontobridge.eval import evaluate
from ontobridge.llm import Adjudicator

RECORDS = [{"coverage_start": f"2024-0{i % 9 + 1}-1{i % 9}", "intro_date": f"2022-0{i % 9 + 1}-0{i % 9 + 1}",
            "annual_prem": str(100 + i * 3.5), "prem_amount": str(90 + i), "ETL_BATCH_ID": f"B{i:05d}"}
           for i in range(30)]


def run(ontology, adjudicator=None, domain="insurance"):
    graph = build_alignment_graph(ontology, get_settings(), adjudicator)
    out = graph.invoke({"run_id": "t", "started_at": "2026-01-01T00:00:00Z",
                        "source": make_source(domain, RECORDS), "log": []})
    return {d.column: d for d in out["decisions"]}, out


def test_gold_and_holdout_quality(ontology):
    report = evaluate(ontology, get_settings())
    tuning, holdout = report["tuning"], report["holdout"]
    assert tuning["columns"] == 105 and tuning["accepted_precision"] == 1.0
    # On column names never used to build the ontology, a wrong answer must go to a reviewer or abstain
    assert holdout["columns"] == 54 and holdout["silent_errors"] == 0
    assert holdout["accepted_precision"] >= 0.95 and holdout["top1_accuracy"] >= 0.8


def test_graph_outputs(ontology):
    d, out = run(ontology)
    assert d["annual_prem"].status == "accepted" and d["annual_prem"].property == "premiumAmount"
    assert d["ETL_BATCH_ID"].status == "unmapped" and d["ETL_BATCH_ID"].property is None
    # the validator: one property per source, so the weaker claim goes to review
    assert d["prem_amount"].status == "needs_review" and "also claimed by annual_prem" in d["prem_amount"].reason
    assert {q["column"] for q in out["review"]} == {"coverage_start", "prem_amount"}
    assert len(out["lineage"]) > 0
    assert [line.split(":")[0] for line in out["log"]] == \
        ["profiler", "retriever", "validator", "review_queue", "lineage"]


def test_llm_can_resolve_review_band(ontology):
    reply = '{"property": "policyEffectiveDate", "confidence": 0.9, "reason": "dates; ob:policyEffectiveDate"}'
    d, _ = run(ontology, Adjudicator(FakeListChatModel(responses=[reply])))
    assert d["coverage_start"].status == "accepted" and d["coverage_start"].method == "llm"
    assert d["coverage_start"].property == "policyEffectiveDate"
    # the same reply for intro_date names a property the retriever never offered it, so it is ignored
    assert d["intro_date"].status == "unmapped" and d["intro_date"].method == "heuristic"


def test_llm_low_evidence_goes_to_review(ontology):
    reply = '{"property": "productLaunchDate", "confidence": 0.95, "reason": "ob:productLaunchDate"}'
    d, _ = run(ontology, Adjudicator(FakeListChatModel(responses=[reply])), domain="retail")
    assert d["intro_date"].property == "productLaunchDate"
    assert d["intro_date"].status == "needs_review" and d["intro_date"].method == "llm"


def test_llm_guardrails(ontology):
    for reply in ('{"property": "madeUpProperty", "confidence": 1}', "I think it is a date.", '{"property": "none"}'):
        d, _ = run(ontology, Adjudicator(FakeListChatModel(responses=[reply])))
        assert d["coverage_start"].status == "needs_review" and d["coverage_start"].method == "heuristic"
        assert d["coverage_start"].property == "lineOfBusiness"
