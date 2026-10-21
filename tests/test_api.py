from fastapi.testclient import TestClient
from rdflib import Graph

from ontobridge.services import catalog_api, pipeline_api

SOURCES = ["insurance/policy_admin.csv", "insurance/claims_events.json", "insurance/agents_crm.csv"]


def test_pipeline_run_review_audit_lineage():
    api = TestClient(pipeline_api.app)
    assert api.get("/health").json()["properties"] == 80
    assert api.post("/runs", json={"sources": ["nope.csv"]}).status_code == 422

    run = api.post("/runs", json={"sources": SOURCES}).json()
    assert run["counts"] == {"accepted": 27, "unmapped": 2}
    decisions = api.get(f"/runs/{run['run_id']}/decisions").json()
    by_col = {d["column"]: d for d in decisions}
    assert by_col["PREM_AMT"]["property"] == "premiumAmount" and by_col["PREM_AMT"]["candidates"]

    batch = by_col["ETL_BATCH_ID"]
    bad = api.post(f"/decisions/{batch['id']}/review", json={"action": "override", "reviewer": "ana", "property": "x"})
    assert bad.status_code == 422
    assert api.post(f"/decisions/{batch['id']}/review", json={"action": "approve", "reviewer": "ana"}).status_code == 422

    name = by_col["HOLDER_NM"]
    r = api.post(f"/decisions/{name['id']}/review", json={"action": "reject", "reviewer": "ana", "note": "PII"})
    assert r.json()["status"] == "unmapped" and r.json()["method"] == "human" and r.json()["reviewed_by"] == "ana"
    assert api.get(f"/runs/{run['run_id']}").json()["counts"] == {"accepted": 26, "unmapped": 3}

    entries = api.get("/audit").json()
    review = next(e for e in entries if e["action"] == "review.reject")
    assert review["payload"]["before"]["property"] == "customerName" and review["payload"]["after"]["property"] is None
    assert api.get("/audit/verify").json()["ok"] is True

    ttl = api.get(f"/runs/{run['run_id']}/lineage")
    assert ttl.headers["content-type"].startswith("text/turtle")
    g = Graph().parse(data=ttl.text, format="turtle")
    assert any("reviewer/ana" in str(s) for s in g.subjects())

    model = api.post(f"/runs/{run['run_id']}/model").json()
    sql, tables = model["domains"]["insurance"]["sql"], model["domains"]["insurance"]["tables"]
    assert "USING DELTA" in sql and "insurance.fact_policy_admin" in tables
    # the rejected column is gone from the generated SQL; claims still supplies the customer name
    assert "`HOLDER_NM`" not in sql and "customer_name" in tables["insurance.dim_customer"]


def test_catalog():
    api = TestClient(catalog_api.app)
    insurance = api.get("/properties", params={"area": "insurance"}).json()
    assert insurance and all(p["area"] == "insurance" for p in insurance)
    assert "premium" in api.get("/properties/premiumAmount").json()["context"].lower()
    assert api.get("/properties/nope").status_code == 404

    r = api.post("/retrieve", json={"column": "PREM_AMT", "domain": "insurance", "samples": ["120.50", "88.10"]}).json()
    assert r["candidates"][0]["property"] == "premiumAmount" and r["profile"]["type"] == "decimal"

    q = api.post("/sparql", json={"query": "SELECT (COUNT(?d) AS ?n) WHERE { ?d a ob:ConformedDimension }"}).json()
    assert q["rows"] == [["14"]]
    assert api.post("/sparql", json={"query": "ASK { ob:premiumAmount a qb:MeasureProperty }"}).json() == {"boolean": True}
    assert api.post("/sparql", json={"query": "INSERT DATA { ob:a ob:b ob:c }"}).status_code == 400
    assert api.post("/sparql", json={"query": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"}).status_code == 400

    assert api.get("/dimensions").status_code == 404
    TestClient(pipeline_api.app).post("/runs", json={"sources": SOURCES})
    dims = {d["table"]: d for d in api.get("/dimensions").json()}
    assert dims["dim_customer"]["natural_key"] == "customerId" and dims["dim_agent"]["described_by"] == "ontology"
