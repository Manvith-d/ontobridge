"""Optional LLM reasoning, grounded in ontology context retrieved with SPARQL (RAG).

OntoBridge runs without a model by default (ONTOBRIDGE_LLM=none). Set ONTOBRIDGE_LLM=ollama:<model>
for a free local model, or anthropic:<model> with ANTHROPIC_API_KEY set.

Guardrails: the model only sees properties the retriever found and may only pick one of them, its
reply must be JSON, and anything malformed falls back to the deterministic heuristic.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from langchain_core.language_models import BaseChatModel
from langchain_core.prompts import ChatPromptTemplate

from .profiler import ColumnProfile
from .retrieval import Retrieval

MAPPING_PROMPT = ChatPromptTemplate.from_messages([
    ("system", (
     "You align columns from operational data extracts to an OWL ontology. Use only the ontology context "
     "you are given. Choose one of the candidate properties, or \"none\" if no candidate fits. Reply with "
     "JSON only: {{\"property\": \"<local name or none>\", \"confidence\": <0 to 1>, \"reason\": "
     "\"<one sentence that cites the ob: terms you relied on>\"}}")),
    ("human", (
     "Source: {source} (domain: {domain})\n"
     "Column: {column}\n"
     "Inferred type: {type}; null rate: {null_rate}; sample values: {samples}\n\n"
     "Candidate properties, retrieved from the ontology graph with SPARQL:\n{context}\n\n"
     "Heuristic scores: {scores}\n\n"
     "Which property does this column carry?")),
])

DIMENSION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", (
     "You write data catalog entries. Use only the ontology context you are given. Write at most two "
     "sentences defining the dimension for business users, and cite the properties you mention as ob:<name>.")),
    ("human", (
     "Dimension: {label} (table {table}), used in the {domains} domains.\n"
     "Ontology context, retrieved with SPARQL:\n{context}\n\nWrite the definition.")),
])


@dataclass
class Verdict:
    property: str | None
    confidence: float
    reason: str


def chat_model(spec: str) -> BaseChatModel | None:
    """`none`, `ollama:<model>` or `anthropic:<model>`. Provider packages are optional extras."""
    provider, _, model = spec.partition(":")
    if provider in ("", "none"):
        return None
    if provider == "ollama":
        from langchain_ollama import ChatOllama

        return ChatOllama(model=model or "llama3.1", temperature=0)
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=model or "claude-haiku-5-5", max_tokens=400)
    raise ValueError(f"unknown LLM provider {provider!r}; use none, ollama:<model> or anthropic:<model>")


def parse_json(text: str) -> dict:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("no JSON object in reply")
    return json.loads(match.group(0))


class Adjudicator:
    """Asks a chat model to choose between retrieved candidates for one column."""

    def __init__(self, model: BaseChatModel):
        self.chain = MAPPING_PROMPT | model

    def __call__(self, source_id: str, domain: str, profile: ColumnProfile, retrieval: Retrieval) -> Verdict | None:
        names = {c.property for c in retrieval.candidates}
        try:
            reply = self.chain.invoke({
                "source": source_id, "domain": domain, "column": profile.name, "type": profile.inferred_type,
                "null_rate": round(profile.null_rate, 2), "samples": ", ".join(profile.samples[:8]),
                "context": retrieval.context,
                "scores": ", ".join(f"ob:{c.property}={c.score:.2f}" for c in retrieval.candidates),
            })
            data = parse_json(reply.text)
            choice = str(data.get("property") or "none").removeprefix("ob:")
            confidence = min(max(float(data.get("confidence", 0)), 0.0), 1.0)
        except Exception:  # noqa: BLE001 - unreachable model or malformed output: the heuristic decision stands
            return None
        if choice.lower() == "none":
            return Verdict(None, confidence, str(data.get("reason", ""))[:300])
        if choice not in names:  # the model may not invent or reach outside the retrieved candidates
            return None
        return Verdict(choice, confidence, str(data.get("reason", ""))[:300])


def describe_dimension(model: BaseChatModel, label: str, table: str, domains: list[str], context: str) -> str | None:
    try:
        reply = (DIMENSION_PROMPT | model).invoke(
            {"label": label, "table": table, "domains": ", ".join(domains), "context": context})
    except Exception:  # noqa: BLE001 - no definition beats a failed one; the template is used
        return None
    text = reply.text.strip()
    # Grounded or nothing: the definition must cite the ontology and stay short
    return text if "ob:" in text and len(text) <= 600 else None
