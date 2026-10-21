"""Turn identifiers like `PREM_AMT`, `claimant.customerRef` or `last4` into comparable token lists."""

from __future__ import annotations

import re

# Common abbreviations in operational extracts. Expansion runs alongside the raw tokens,
# so `qty` can match both "qty" and "quantity".
ABBREVIATIONS: dict[str, str] = {
    "acct": "account", "addr": "address", "agt": "agent", "amt": "amount", "auth": "authorization",
    "cd": "code", "cnt": "count", "cr": "credit", "ctr": "center", "cust": "customer", "desc": "description",
    "disc": "discount", "dob": "date of birth", "dr": "debit", "dt": "date", "dx": "diagnosis", "eff": "effective",
    "exp": "expiration", "fnol": "first notice of loss", "je": "journal entry", "lob": "line of business",
    "lvl": "level", "mcc": "merchant category code", "mrn": "medical record number", "ndc": "national drug code",
    "nm": "name", "no": "number", "npi": "national provider identifier", "nbr": "number", "num": "number",
    "pol": "policy", "prem": "premium", "qty": "quantity", "ref": "reference", "rx": "prescription",
    "st": "state", "ts": "timestamp", "txn": "transaction",
}

# Tokens that carry little meaning on their own. A column whose only overlap with a label is
# one of these ("etl batch id" vs "claim id") is not evidence of a match.
GENERIC = {
    "a", "an", "and", "at", "by", "code", "date", "flag", "id", "identifier", "is", "name", "number", "of",
    "on", "the", "time", "timestamp", "to", "total", "type", "value", "amount", "level", "count",
}

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_ALNUM = re.compile(r"(?<=[a-zA-Z])(?=\d)|(?<=\d)(?=[a-zA-Z])")


def tokens(identifier: str) -> list[str]:
    """`claimant.customerRef` -> ['claimant', 'customer', 'ref']; `category_lvl1` -> ['category', 'lvl', '1']."""
    spaced = _ALNUM.sub(" ", _CAMEL.sub(" ", identifier))
    return [t for t in re.split(r"[^A-Za-z0-9]+", spaced.lower()) if t]


def expand(toks: list[str]) -> list[str]:
    out: list[str] = []
    for t in toks:
        out.extend(ABBREVIATIONS.get(t, t).split())
    return out


def phrase(identifier: str, expanded: bool = False) -> str:
    toks = tokens(identifier)
    return " ".join(expand(toks) if expanded else toks)


def meaningful(toks: list[str]) -> set[str]:
    return {t for t in toks if t not in GENERIC and not t.isdigit()}


def same_word(x: str, y: str) -> bool:
    """Equal, or one is the head of the other's compound ("holder" ~ "policyholder", not "claim" ~ "claimant")."""
    return x == y or (min(len(x), len(y)) >= 4 and (x.endswith(y) or y.endswith(x)))


def overlaps(a: set[str], b: set[str]) -> bool:
    return any(same_word(x, y) for x in a for y in b)


def snake(name: str) -> str:
    return "_".join(tokens(name))
