"""Runtime settings, read from environment variables each time they are requested."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    ontology_dir: Path
    runtime_dir: Path
    llm: str
    accept_threshold: float
    abstain_threshold: float

    @property
    def sources_dir(self) -> Path:
        return self.data_dir / "sources"

    @property
    def gold_path(self) -> Path:
        return self.data_dir / "gold" / "mappings.json"


def get_settings() -> Settings:
    runtime = Path(os.environ.get("ONTOBRIDGE_HOME", ROOT / "runtime"))
    runtime.mkdir(parents=True, exist_ok=True)
    return Settings(
        data_dir=Path(os.environ.get("ONTOBRIDGE_DATA", ROOT / "data")),
        ontology_dir=Path(os.environ.get("ONTOBRIDGE_ONTOLOGY", ROOT / "ontology")),
        runtime_dir=runtime,
        # none | ollama:<model> | anthropic:<model> | openai:<model>
        llm=os.environ.get("ONTOBRIDGE_LLM", "none"),
        accept_threshold=float(os.environ.get("ONTOBRIDGE_ACCEPT", "0.80")),
        abstain_threshold=float(os.environ.get("ONTOBRIDGE_ABSTAIN", "0.55")),
    )
