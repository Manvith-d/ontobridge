import shutil

import pytest

from ontobridge.agents import build_alignment_graph
from ontobridge.config import ROOT, get_settings
from ontobridge.dimensions import define_dimensions
from ontobridge.sources import discover
from ontobridge.starschema import build_all, lineage_graph, notebook
from ontobridge.text import snake


@pytest.fixture(scope="module")
def aligned(ontology):
    settings = get_settings()
    graph = build_alignment_graph(ontology, settings)
    sources = discover(ROOT / "data" / "sources")
    decisions = [d for s in sources
                 for d in graph.invoke({"run_id": "t", "started_at": "2026-01-01T00:00:00Z", "source": s})["decisions"]]
    return sources, decisions, define_dimensions(ontology, decisions)


def test_dimensions_are_conformed_and_reused(ontology, aligned):
    sources, decisions, dims = aligned
    customer = next(d for d in dims if d.table == "dim_customer")
    assert customer.natural_key == "customerId" and customer.domains == ["finance", "insurance", "retail"]
    models = build_all(sources, decisions, ontology, dims)
    with_customer = [m.domain for m in models if f"{m.domain}.dim_customer" in m.tables]
    assert with_customer == ["finance", "insurance", "retail"]


def test_star_design_comes_from_the_ontology(ontology, aligned):
    sources, decisions, dims = aligned
    models = {m.domain: m for m in build_all(sources, decisions, ontology, dims)}
    assert sorted(models) == ["finance", "healthcare", "insurance", "retail"]
    facts = sorted(t for m in models.values() for t in m.tables if ".fact_" in t)
    assert len(facts) == 8  # every source with a qb:MeasureProperty; the 4 reference sources become dimensions
    policy = models["insurance"].tables["insurance.fact_policy_admin"]
    assert policy[:4] == ["agent_key", "customer_key", "policy_key", "location_key"]
    assert {"policy_effective_date_key", "policy_expiration_date_key", "premium_amount"} <= set(policy)
    assert "customer_name" not in policy  # attributes live on their dimension
    assert "agent_name" in models["insurance"].tables["insurance.dim_agent"]  # from the agents_crm reference source
    sql = models["insurance"].sql()
    assert "USING DELTA" in sql and "Files/raw/insurance/claims_events.json" in sql
    assert "CAST(`claimant`.`customerRef` AS STRING) AS customer_id" in sql


def test_lineage_and_notebook(ontology, aligned):
    sources, decisions, dims = aligned
    models = build_all(sources, decisions, ontology, dims)
    g = lineage_graph(models)
    derived = {str(o) for s, o in g.subject_objects(predicate=None) if "lakehouse/insurance.fact_policy_admin#premium_amount" in str(s)}
    assert any(o.endswith("policy_admin.csv#PREM_AMT") for o in derived)
    nb = notebook(models, "# test")
    assert sum(c["cell_type"] == "code" for c in nb["cells"]) == sum(len(m.statements) + 1 for m in models)


def test_unknown_member_and_snake_names(ontology, aligned):
    sources, decisions, dims = aligned
    sql = build_all(sources, decisions, ontology, dims, target="local")[0].sql()
    assert "USING PARQUET" in sql and "SELECT -1, 'UNKNOWN'" in sql
    assert snake("claimReportedTimestamp") == "claim_reported_timestamp"


@pytest.mark.spark
def test_generated_sql_runs_on_spark(ontology, aligned, tmp_path):
    pytest.importorskip("pyspark")
    if not shutil.which("java"):
        pytest.skip("Spark needs Java 17+")
    from ontobridge.starschema import run_local

    sources, decisions, dims = aligned
    models = build_all(sources, decisions, ontology, dims, target="local", raw_root=str(ROOT / "data" / "sources"))
    report = run_local(models, tmp_path / "warehouse")
    checks = {k: v for r in report.values() for k, v in r["checks"].items()}
    assert len(checks) == 65 and not any(checks.values()), {k: v for k, v in checks.items() if v}
    rows = {t: n for r in report.values() for t, n in r["tables"].items()}
    assert all(rows[t] == 200 for t in rows if ".fact_" in t)
    assert rows["insurance.dim_agent"] == 26  # 25 agents + the UNKNOWN member
