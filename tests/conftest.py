from pathlib import Path

import pytest

from ontobridge import services
from ontobridge.config import ROOT
from ontobridge.ontology import Ontology
from ontobridge.sources import Source


@pytest.fixture(autouse=True)
def home(tmp_path, monkeypatch):
    """Every test gets its own runtime directory (SQLite store, lineage files) and no LLM."""
    monkeypatch.setenv("ONTOBRIDGE_HOME", str(tmp_path))
    monkeypatch.setenv("ONTOBRIDGE_LLM", "none")
    services.reset()
    yield tmp_path
    services.reset()


@pytest.fixture(scope="session")
def ontology() -> Ontology:
    return Ontology(ROOT / "ontology")


def make_source(domain: str, records: list[dict]) -> Source:
    return Source(id=f"{domain}/test.csv", domain=domain, name="test", path=Path("test.csv"), format="csv",
                  records=records)
