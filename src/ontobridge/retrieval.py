"""Graph retrieval for RAG: SPARQL keyword search over the ontology, then scoring and grounding context."""

from __future__ import annotations

from dataclasses import dataclass

from .ontology import Ontology
from .profiler import ColumnProfile
from .scoring import Candidate, rank
from .text import expand, meaningful, tokens


@dataclass
class Retrieval:
    column: str
    terms: list[str]
    candidates: list[Candidate]  # ranked, best first
    context: str  # SPARQL-built description of the top candidates, used to ground LLM prompts


class Retriever:
    def __init__(self, ontology: Ontology, top_k: int = 5):
        self.ontology = ontology
        self.top_k = top_k

    @staticmethod
    def terms_for(column: str) -> list[str]:
        raw = tokens(column)
        words = set(raw) | set(expand(raw))
        strong = {t for t in meaningful(list(words)) if len(t) >= 3}
        # A column made only of generic words ("amount.value") still needs something to search on
        return sorted(strong or {t for t in words if len(t) >= 3})

    def retrieve(self, profile: ColumnProfile, domain: str) -> Retrieval:
        terms = self.terms_for(profile.name)
        hits = self.ontology.keyword_candidates(set(terms))
        props = [self.ontology.properties[n] for n in sorted(hits) if n in self.ontology.properties]
        ranked = rank(profile, domain, props)[: self.top_k]
        return Retrieval(profile.name, terms, ranked, self.ontology.context([c.property for c in ranked]))
