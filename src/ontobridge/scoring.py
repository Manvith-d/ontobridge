"""Score how well a profiled column matches an ontology property.

score = 0.62 * lexical + 0.16 * type + 0.10 * value pattern + 0.12 * domain
Each component is in [0, 1] and kept on the candidate, so every decision can be explained.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from rapidfuzz import fuzz

from .ontology import PropertyInfo
from .profiler import ColumnProfile
from .text import expand, meaningful, overlaps, same_word, tokens

WEIGHTS = {"lexical": 0.62, "type": 0.16, "pattern": 0.10, "domain": 0.12}

# How well an inferred column type fits a property's XSD range.
_TYPE_FIT = {
    ("string", "string"): 1.0, ("integer", "integer"): 1.0, ("decimal", "decimal"): 1.0,
    ("boolean", "boolean"): 1.0, ("date", "date"): 1.0, ("dateTime", "dateTime"): 1.0,
    ("integer", "decimal"): 0.85, ("decimal", "integer"): 0.5, ("date", "dateTime"): 0.7, ("dateTime", "date"): 0.8,
    ("integer", "string"): 0.65, ("decimal", "string"): 0.3, ("boolean", "string"): 0.4,
    ("date", "string"): 0.3, ("dateTime", "string"): 0.3, ("string", "boolean"): 0.2,
}


@dataclass
class Candidate:
    property: str
    score: float
    components: dict[str, float] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"property": self.property, "score": round(self.score, 4),
                "components": {k: round(v, 3) for k, v in self.components.items()}}


def lexical(column: str, prop: PropertyInfo) -> float:
    raw = tokens(column)
    variants = {" ".join(raw), " ".join(expand(raw))}
    best = 0.0
    for cv in variants:
        ctoks = set(cv.split())
        for lv in prop.phrases:
            if cv == lv:
                return 1.0
            ltoks = lv.split()
            fuzzy = (0.6 * fuzz.token_set_ratio(cv, lv) + 0.4 * fuzz.token_sort_ratio(cv, lv)) / 100
            # Share of the label's words the column covers (compound heads count: "holder" ~ "policyholder")
            covered = sum(any(same_word(t, c) for c in ctoks) for t in ltoks) / len(ltoks)
            best = max(best, fuzzy, covered)
    # A column with meaningful words that shares none of them with the property is not a match,
    # however similar the generic words are ("etl batch id" vs "claim id").
    col_meaning = meaningful(raw) | meaningful(expand(raw))
    prop_meaning = set().union(*(meaningful(p.split()) for p in prop.phrases))
    if col_meaning and not overlaps(col_meaning, prop_meaning):
        best = min(best, 0.3)
    return best


def type_fit(profile: ColumnProfile, prop: PropertyInfo) -> float:
    return _TYPE_FIT.get((profile.inferred_type, prop.range), 0.0)


def pattern_fit(profile: ColumnProfile, prop: PropertyInfo) -> float:
    if not prop.pattern:
        return 0.5  # no evidence either way
    if not profile.samples:
        return 0.0
    rx = re.compile(prop.pattern)
    return sum(bool(rx.match(s)) for s in profile.samples) / len(profile.samples)


def domain_fit(source_domain: str, prop: PropertyInfo) -> float:
    if prop.area == source_domain:
        return 1.0
    return 0.7 if prop.area == "core" else 0.2


def score(profile: ColumnProfile, source_domain: str, prop: PropertyInfo) -> Candidate:
    parts = {
        "lexical": lexical(profile.name, prop),
        "type": type_fit(profile, prop),
        "pattern": pattern_fit(profile, prop),
        "domain": domain_fit(source_domain, prop),
    }
    return Candidate(prop.name, sum(WEIGHTS[k] * v for k, v in parts.items()), parts)


def rank(profile: ColumnProfile, source_domain: str, props: list[PropertyInfo]) -> list[Candidate]:
    return sorted((score(profile, source_domain, p) for p in props), key=lambda c: -c.score)
