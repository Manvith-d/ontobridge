"""SQLite persistence for runs, mapping decisions and a hash-chained audit log.

Each audit entry stores the SHA-256 of its own content plus the previous entry's hash, so editing
or deleting any past entry breaks the chain and `verify()` reports where.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import threading
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import AuditEntry, MappingDecision, RunSummary, StoredDecision

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
  llm TEXT NOT NULL, sources TEXT NOT NULL, counts TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS decisions (
  id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
  source_id TEXT NOT NULL, column_name TEXT NOT NULL, status TEXT NOT NULL, body TEXT NOT NULL,
  UNIQUE (run_id, source_id, column_name)
);
CREATE TABLE IF NOT EXISTS audit (
  seq INTEGER PRIMARY KEY, ts TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
  subject TEXT NOT NULL, payload TEXT NOT NULL, prev_hash TEXT NOT NULL, hash TEXT NOT NULL
);
"""
GENESIS = "0" * 64


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def entry_hash(prev: str, seq: int, ts: str, actor: str, action: str, subject: str, payload: dict) -> str:
    body = json.dumps([prev, seq, ts, actor, action, subject, payload], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


class Store:
    def __init__(self, path: Path | str):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        # ponytail: one lock serializes all access; move to Postgres if several writers need throughput
        self.lock = threading.Lock()

    # ---- runs -------------------------------------------------------------------------
    def create_run(self, llm: str, sources: list[str]) -> str:
        run_id = f"run-{datetime.now(UTC):%Y%m%dT%H%M%S}-{secrets.token_hex(2)}"
        with self.lock, self.db:
            self.db.execute("INSERT INTO runs (id, started_at, llm, sources) VALUES (?, ?, ?, ?)",
                            (run_id, now(), llm, json.dumps(sources)))
        return run_id

    def finish_run(self, run_id: str) -> RunSummary:
        with self.lock, self.db:
            rows = self.db.execute("SELECT status FROM decisions WHERE run_id = ?", (run_id,)).fetchall()
            counts = dict(Counter(r["status"] for r in rows))
            self.db.execute("UPDATE runs SET finished_at = ?, counts = ? WHERE id = ?",
                            (now(), json.dumps(counts), run_id))
        return self.run(run_id)

    def run(self, run_id: str) -> RunSummary | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return _run(row) if row else None

    def runs(self) -> list[RunSummary]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM runs ORDER BY started_at DESC, id DESC").fetchall()
        return [_run(r) for r in rows]

    def latest_run(self) -> RunSummary | None:
        done = [r for r in self.runs() if r.finished_at]
        return done[0] if done else None

    # ---- decisions --------------------------------------------------------------------
    def save_decisions(self, run_id: str, decisions: list[MappingDecision]) -> None:
        with self.lock, self.db:
            self.db.executemany(
                "INSERT INTO decisions (run_id, source_id, column_name, status, body) VALUES (?, ?, ?, ?, ?)",
                [(run_id, d.source_id, d.column, d.status, d.model_dump_json()) for d in decisions],
            )

    def decisions(self, run_id: str, status: str | None = None, source_id: str | None = None) -> list[StoredDecision]:
        sql, args = "SELECT * FROM decisions WHERE run_id = ?", [run_id]
        if status:
            sql, args = sql + " AND status = ?", [*args, status]
        if source_id:
            sql, args = sql + " AND source_id = ?", [*args, source_id]
        with self.lock:
            rows = self.db.execute(sql + " ORDER BY id", args).fetchall()
        return [_decision(r) for r in rows]

    def decision(self, decision_id: int) -> StoredDecision | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM decisions WHERE id = ?", (decision_id,)).fetchone()
        return _decision(row) if row else None

    def update_decision(self, decision: StoredDecision) -> None:
        body = MappingDecision(**decision.model_dump(exclude={"id", "run_id"}))
        with self.lock, self.db:
            self.db.execute("UPDATE decisions SET status = ?, body = ? WHERE id = ?",
                            (body.status, body.model_dump_json(), decision.id))
            rows = self.db.execute("SELECT status FROM decisions WHERE run_id = ?", (decision.run_id,)).fetchall()
            self.db.execute("UPDATE runs SET counts = ? WHERE id = ?",
                            (json.dumps(dict(Counter(r["status"] for r in rows))), decision.run_id))

    # ---- audit ------------------------------------------------------------------------
    def audit(self, actor: str, action: str, subject: str, payload: dict[str, Any] | None = None) -> AuditEntry:
        payload = json.loads(json.dumps(payload or {}, default=str))  # store exactly what gets hashed
        with self.lock, self.db:
            last = self.db.execute("SELECT seq, hash FROM audit ORDER BY seq DESC LIMIT 1").fetchone()
            seq, prev = (last["seq"] + 1, last["hash"]) if last else (1, GENESIS)
            ts = now()
            digest = entry_hash(prev, seq, ts, actor, action, subject, payload)
            self.db.execute("INSERT INTO audit VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (seq, ts, actor, action, subject, json.dumps(payload), prev, digest))
        return AuditEntry(seq=seq, ts=ts, actor=actor, action=action, subject=subject,
                          payload=payload, prev_hash=prev, hash=digest)

    def audit_log(self, subject: str | None = None, limit: int = 200) -> list[AuditEntry]:
        sql, args = "SELECT * FROM audit", []
        if subject:
            sql, args = sql + " WHERE subject = ?", [subject]
        with self.lock:
            rows = self.db.execute(sql + " ORDER BY seq DESC LIMIT ?", [*args, limit]).fetchall()
        return [_entry(r) for r in rows]

    def verify(self) -> dict[str, Any]:
        """Recompute the chain. Returns {"ok": bool, "entries": n, "broken_at": seq | None}."""
        with self.lock:
            rows = self.db.execute("SELECT * FROM audit ORDER BY seq").fetchall()
        prev = GENESIS
        for r in rows:
            e = _entry(r)
            if e.prev_hash != prev or entry_hash(prev, e.seq, e.ts, e.actor, e.action, e.subject, e.payload) != e.hash:
                return {"ok": False, "entries": len(rows), "broken_at": e.seq}
            prev = e.hash
        return {"ok": True, "entries": len(rows), "broken_at": None}


def _run(row: sqlite3.Row) -> RunSummary:
    return RunSummary(run_id=row["id"], started_at=row["started_at"], finished_at=row["finished_at"],
                      llm=row["llm"], sources=json.loads(row["sources"]), counts=json.loads(row["counts"]))


def _decision(row: sqlite3.Row) -> StoredDecision:
    return StoredDecision(id=row["id"], run_id=row["run_id"], **json.loads(row["body"]))


def _entry(row: sqlite3.Row) -> AuditEntry:
    return AuditEntry(**{**dict(row), "payload": json.loads(row["payload"])})
