"""Profile each column: inferred XSD type, null rate, distinctness and sample values."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .sources import Source

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$")
_INT = re.compile(r"^[+-]?\d+$")
_DEC = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
_BOOL = {"true", "false", "yes", "no", "y", "n", "t", "f"}


@dataclass
class ColumnProfile:
    name: str
    inferred_type: str  # string | integer | decimal | boolean | date | dateTime
    null_rate: float
    distinct_ratio: float
    samples: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "type": self.inferred_type, "null_rate": round(self.null_rate, 3),
            "distinct_ratio": round(self.distinct_ratio, 3), "samples": self.samples[:5],
        }


def _kind(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "decimal"
    s = str(value).strip()
    if s.lower() in _BOOL:
        return "boolean"
    if _INT.match(s):
        # Long digit strings (NPIs, card digits, codes with leading zeros) are identifiers, not numbers
        return "string" if len(s.lstrip("+-")) >= 8 or (s.startswith("0") and len(s) > 1) else "integer"
    if _DEC.match(s):
        return "decimal"
    if _DATE.match(s):
        return "date"
    if _DATETIME.match(s):
        return "dateTime"
    return "string"


def infer_type(values: list[Any]) -> str:
    if not values:
        return "string"
    counts: dict[str, int] = {}
    for v in values:
        k = _kind(v)
        counts[k] = counts.get(k, 0) + 1
    top, n = max(counts.items(), key=lambda kv: kv[1])
    if n / len(values) >= 0.95:
        return top
    # Mixed integers and decimals are decimals; anything else ambiguous is a string
    if set(counts) <= {"integer", "decimal"}:
        return "decimal"
    return "string"


def profile_column(source: Source, column: str, sample_size: int = 50) -> ColumnProfile:
    raw = [r.get(column) for r in source.records]
    present = [v for v in raw if v is not None and str(v).strip() != ""]
    distinct = {str(v) for v in present}
    return ColumnProfile(
        name=column,
        inferred_type=infer_type(present),
        null_rate=1 - len(present) / max(len(raw), 1),
        distinct_ratio=len(distinct) / max(len(present), 1),
        samples=[str(v) for v in present[:sample_size]],
    )


def profile_source(source: Source) -> list[ColumnProfile]:
    return [profile_column(source, c) for c in source.columns]
