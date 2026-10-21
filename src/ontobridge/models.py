"""Typed outputs shared by the agents, the store and the APIs.

Every decision an agent makes is a `MappingDecision`, so tests, reviewers and the audit log
all see the same schema, including the evidence behind it.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

Status = Literal["accepted", "needs_review", "unmapped"]
Method = Literal["heuristic", "llm", "human"]


class CandidateOut(BaseModel):
    property: str
    score: float
    components: dict[str, float]


class MappingDecision(BaseModel):
    source_id: str
    column: str
    property: str | None  # ontology local name, e.g. "premiumAmount"
    status: Status
    score: float  # score of the chosen (or best) candidate
    method: Method = "heuristic"
    reason: str
    candidates: list[CandidateOut] = Field(default_factory=list)
    profile: dict[str, Any] = Field(default_factory=dict)
    reviewed_by: str | None = None


class StoredDecision(MappingDecision):
    id: int
    run_id: str


class RunSummary(BaseModel):
    run_id: str
    started_at: str
    finished_at: str | None
    llm: str
    sources: list[str]
    counts: dict[str, int]


class ReviewAction(BaseModel):
    action: Literal["approve", "reject", "override"]
    reviewer: str = Field(min_length=1, max_length=80)
    property: str | None = None  # required for "override"
    note: str = Field(default="", max_length=500)


class AuditEntry(BaseModel):
    seq: int
    ts: str
    actor: str
    action: str
    subject: str
    payload: dict[str, Any]
    prev_hash: str
    hash: str
