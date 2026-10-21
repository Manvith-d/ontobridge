"""Load CSV and JSON sources into flat records. Nested JSON objects become dotted column paths."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Source:
    id: str  # "<domain>/<file>", e.g. "insurance/policy_admin.csv"
    domain: str
    name: str  # file stem, e.g. "policy_admin"
    path: Path
    format: str  # "csv" | "json"
    records: list[dict[str, Any]] = field(repr=False)

    @property
    def columns(self) -> list[str]:
        seen: dict[str, None] = {}
        for record in self.records:
            seen.update(dict.fromkeys(record))
        return list(seen)


def flatten(obj: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in obj.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(flatten(value, path + "."))
        else:
            out[path] = value
    return out


def load_source(path: Path, root: Path) -> Source:
    rel = path.relative_to(root).as_posix()
    domain = rel.split("/")[0]
    if path.suffix == ".csv":
        with path.open(newline="") as f:
            records = [{k: (v if v != "" else None) for k, v in row.items()} for row in csv.DictReader(f)]
        fmt = "csv"
    elif path.suffix == ".json":
        data = json.loads(path.read_text())
        if not isinstance(data, list):
            raise ValueError(f"{rel}: expected a JSON array of records")
        records = [flatten(r) for r in data]
        fmt = "json"
    else:
        raise ValueError(f"{rel}: unsupported format {path.suffix}")
    return Source(id=rel, domain=domain, name=path.stem, path=path, format=fmt, records=records)


def discover(sources_dir: Path) -> list[Source]:
    files = sorted(p for p in sources_dir.rglob("*") if p.suffix in {".csv", ".json"})
    return [load_source(p, sources_dir) for p in files]
