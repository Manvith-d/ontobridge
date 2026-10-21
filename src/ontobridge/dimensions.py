"""Reusable conformed dimension definitions, derived from the ontology and what the agents mapped.

A definition is written once (key, attributes, contributing sources, business description) and every
star schema that touches that dimension builds its table from it, so `dim_customer` means the same
thing in insurance, retail and finance.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from langchain_core.language_models import BaseChatModel

from .llm import describe_dimension
from .models import MappingDecision
from .ontology import PREFIXES, Ontology, _local

DIMENSIONS_QUERY = PREFIXES + """
SELECT ?dim ?label ?table WHERE { ?dim a ob:ConformedDimension ; rdfs:label ?label ; ob:tableName ?table }
ORDER BY ?table
"""


@dataclass
class DimensionDefinition:
    name: str  # e.g. "CustomerDimension"
    label: str
    table: str  # e.g. "dim_customer"
    natural_key: str | None  # property local name; None for generated dimensions (date, location)
    attributes: list[str]  # member properties some source carries
    sources: list[str]  # sources that contribute at least one member column
    domains: list[str]
    description: str
    described_by: str  # "ontology" (template over rdfs:comment) or "llm"

    def as_dict(self) -> dict:
        return asdict(self)


def define_dimensions(ontology: Ontology, decisions: list[MappingDecision],
                      model: BaseChatModel | None = None) -> list[DimensionDefinition]:
    props = ontology.properties
    accepted = [d for d in decisions if d.status == "accepted" and d.property in props]
    out = []
    for row in ontology.graph.query(DIMENSIONS_QUERY):
        name = _local(str(row.dim))
        members = ontology.dimension_members(name)
        used = [d for d in accepted if props[d.property].dimension == name]
        if not used:
            continue
        mapped = {d.property for d in used}
        key = next((m for m in members if m.natural_key), None)
        attributes = [m.name for m in sorted(members, key=lambda m: m.name) if m.name in mapped and m is not key]
        sources = sorted({d.source_id for d in used})
        domains = sorted({s.split("/")[0] for s in sources})
        description, by = _template(str(row.label), key, attributes, ontology, domains, sources), "ontology"
        if model:
            context = ontology.context(([key.name] if key else []) + attributes)
            text = describe_dimension(model, str(row.label), str(row.table), domains, context)
            if text:
                description, by = text, "llm"
        out.append(DimensionDefinition(name, str(row.label), str(row.table), key.name if key else None,
                                       attributes, sources, domains, description, by))
    return out


def _template(label, key, attributes, ontology, domains, sources) -> str:
    labels = [ontology.properties[a].label for a in attributes]
    grain = f"One row per {key.label} (ob:{key.name}): {key.comment.rstrip('.')}." if key else \
        "Generated from the values the sources carry."
    return (f"{label.capitalize()} dimension, conformed across {', '.join(domains)} "
            f"from {len(sources)} source{'s' if len(sources) != 1 else ''}. {grain}"
            + (f" Attributes: {', '.join(labels)}." if labels else ""))
