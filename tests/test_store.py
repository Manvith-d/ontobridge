from ontobridge.models import MappingDecision
from ontobridge.store import Store


def test_audit_chain_detects_tampering(tmp_path):
    st = Store(tmp_path / "t.db")
    for i in range(5):
        st.audit("tester", "step", f"s{i}", {"i": i})
    assert st.verify() == {"ok": True, "entries": 5, "broken_at": None}
    st.db.execute("UPDATE audit SET payload = '{\"i\": 99}' WHERE seq = 3")
    assert st.verify() == {"ok": False, "entries": 5, "broken_at": 3}


def test_audit_chain_detects_deletion(tmp_path):
    st = Store(tmp_path / "t.db")
    for i in range(4):
        st.audit("tester", "step", f"s{i}")
    st.db.execute("DELETE FROM audit WHERE seq = 2")
    assert st.verify()["broken_at"] == 3


def test_decisions_round_trip(tmp_path):
    st = Store(tmp_path / "t.db")
    run = st.create_run("none", ["a/b.csv"])
    d = MappingDecision(source_id="a/b.csv", column="x", property="claimId", status="accepted", score=0.9, reason="r")
    st.save_decisions(run, [d])
    summary = st.finish_run(run)
    assert summary.counts == {"accepted": 1}
    stored = st.decisions(run)[0]
    assert stored.property == "claimId" and stored.run_id == run
    st.update_decision(stored.model_copy(update={"status": "unmapped", "property": None}))
    assert st.run(run).counts == {"unmapped": 1}
