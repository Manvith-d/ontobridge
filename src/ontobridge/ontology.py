"""The ontology as an RDF graph, queried with SPARQL.

Everything the agents need from the ontology (the property catalog, keyword retrieval of candidates,
and the context block that grounds LLM prompts) comes from SPARQL over this graph.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

from rdflib import Graph, Namespace
from rdflib.namespace import OWL, RDFS, SKOS, XSD

from .text import phrase

OB = Namespace("https://manvith-d.github.io/ontobridge/ontology#")
QB = Namespace("http://purl.org/linked-data/cube#")
PROV = Namespace("http://www.w3.org/ns/prov#")

PREFIXES = f"""
PREFIX ob:   <{OB}>
PREFIX qb:   <{QB}>
PREFIX owl:  <{OWL}>
PREFIX rdfs: <{RDFS}>
PREFIX skos: <{SKOS}>
PREFIX xsd:  <{XSD}>
PREFIX prov: <{PROV}>
"""

CATALOG_QUERY = PREFIXES + """
SELECT ?p ?label (GROUP_CONCAT(DISTINCT STR(?alt); separator="|") AS ?alts)
       ?comment ?range ?kind ?dim ?table ?nk ?area ?pattern
WHERE {
  ?p a owl:DatatypeProperty ; rdfs:label ?label ; rdfs:range ?range ; ob:domainArea ?area ; a ?kind .
  FILTER(?kind IN (ob:IdentifierProperty, qb:AttributeProperty, qb:DimensionProperty, qb:MeasureProperty))
  OPTIONAL { ?p skos:altLabel ?alt }
  OPTIONAL { ?p rdfs:comment ?comment }
  OPTIONAL { ?p ob:conformedDimension ?dim . OPTIONAL { ?dim ob:tableName ?table } }
  OPTIONAL { ?p ob:naturalKey ?nk }
  OPTIONAL { ?p ob:valuePattern ?pattern }
}
GROUP BY ?p ?label ?comment ?range ?kind ?dim ?table ?nk ?area ?pattern
"""

# Keyword retrieval: properties whose label or synonym contains any of the column's terms.
KEYWORD_QUERY = PREFIXES + """
SELECT DISTINCT ?p WHERE {{
  VALUES ?term {{ {terms} }}
  ?p a owl:DatatypeProperty ; (rdfs:label|skos:altLabel) ?l .
  FILTER(CONTAINS(LCASE(STR(?l)), ?term))
}}
"""

# Grounding context for one or more candidate properties, including the conformed dimension
# each belongs to and that dimension's other members (the reusable dimension definition).
CONTEXT_QUERY = PREFIXES + """
SELECT ?p ?label ?comment ?range ?kind ?dimLabel
       (GROUP_CONCAT(DISTINCT STR(?alt); separator=", ") AS ?alts)
       (GROUP_CONCAT(DISTINCT STR(?sibLabel); separator=", ") AS ?siblings)
WHERE {{
  VALUES ?p {{ {props} }}
  ?p rdfs:label ?label ; rdfs:range ?range ; a ?kind .
  FILTER(?kind IN (ob:IdentifierProperty, qb:AttributeProperty, qb:DimensionProperty, qb:MeasureProperty))
  OPTIONAL {{ ?p rdfs:comment ?comment }}
  OPTIONAL {{ ?p skos:altLabel ?alt }}
  OPTIONAL {{
    ?p ob:conformedDimension ?dim . ?dim rdfs:label ?dimLabel .
    OPTIONAL {{ ?sib ob:conformedDimension ?dim ; rdfs:label ?sibLabel . FILTER(?sib != ?p) }}
  }}
}}
GROUP BY ?p ?label ?comment ?range ?kind ?dimLabel
"""

KINDS = {
    str(OB.IdentifierProperty): "identifier", str(QB.AttributeProperty): "attribute",
    str(QB.DimensionProperty): "dimension", str(QB.MeasureProperty): "measure",
}
_SAFE_TERM = re.compile(r"^[a-z0-9]+$")


@dataclass(frozen=True)
class PropertyInfo:
    name: str  # local name, e.g. "premiumAmount"
    iri: str
    label: str
    alt_labels: tuple[str, ...]
    comment: str
    range: str  # XSD local name
    kind: str  # identifier | attribute | dimension | measure
    dimension: str | None  # e.g. "CustomerDimension"
    dimension_table: str | None  # e.g. "dim_customer"
    natural_key: bool
    area: str  # core | insurance | retail | healthcare | finance
    pattern: str | None

    @property
    def phrases(self) -> list[str]:
        return [phrase(x) for x in (self.label, *self.alt_labels)]


def _local(iri: str) -> str:
    return re.split(r"[#/]", iri)[-1]


class Ontology:
    def __init__(self, ontology_dir: Path):
        self.graph = Graph()
        for ttl in sorted(Path(ontology_dir).glob("*.ttl")):
            self.graph.parse(ttl, format="turtle")
        self.graph.bind("ob", OB)
        self.graph.bind("qb", QB)
        self.graph.bind("prov", PROV)

    @cached_property
    def properties(self) -> dict[str, PropertyInfo]:
        out: dict[str, PropertyInfo] = {}
        for row in self.graph.query(CATALOG_QUERY):
            name = _local(str(row.p))
            out[name] = PropertyInfo(
                name=name, iri=str(row.p), label=str(row.label),
                alt_labels=tuple(a for a in str(row.alts or "").split("|") if a),
                comment=str(row.comment or ""), range=_local(str(row.range)), kind=KINDS[str(row.kind)],
                dimension=_local(str(row.dim)) if row.dim else None,
                dimension_table=str(row.table) if row.table else None,
                natural_key=bool(row.nk and row.nk.toPython()), area=str(row.area),
                pattern=str(row.pattern) if row.pattern else None,
            )
        return out

    def keyword_candidates(self, terms: set[str]) -> set[str]:
        safe = sorted(t for t in terms if _SAFE_TERM.match(t))
        if not safe:
            return set()
        query = KEYWORD_QUERY.format(terms=" ".join(f'"{t}"' for t in safe))
        return {_local(str(row.p)) for row in self.graph.query(query)}

    def context(self, names: list[str]) -> str:
        """A compact, citation-friendly description of candidate properties for an LLM prompt."""
        if not names:
            return ""
        query = CONTEXT_QUERY.format(props=" ".join(f"ob:{n}" for n in names))
        lines = []
        for row in self.graph.query(query):
            dim = f"; member of the {row.dimLabel} dimension with {row.siblings}" if row.dimLabel else ""
            lines.append(
                f"- ob:{_local(str(row.p))} \"{row.label}\" ({KINDS[str(row.kind)]}, xsd:{_local(str(row.range))})"
                f"{dim}. {row.comment} Synonyms: {row.alts or 'none'}."
            )
        return "\n".join(sorted(lines))

    def dimension_members(self, dimension: str) -> list[PropertyInfo]:
        return [p for p in self.properties.values() if p.dimension == dimension]
