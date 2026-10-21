"""Star schemas per business domain, emitted as Spark SQL for a Fabric Lakehouse (Delta) or local Spark (Parquet).

    bronze.<domain>_<source>   the raw file as loaded
    silver.<domain>_<source>   columns renamed to ontology terms and cast to their XSD types
    <domain>.dim_*             conformed dimensions with xxhash64 surrogate keys and an UNKNOWN (-1) member
    <domain>.fact_<source>     one fact per source that carries measures, keyed to every dimension it references

Whether a column becomes a dimension key, a date key, a degenerate dimension or a measure comes from
the ontology (ob:naturalKey, ob:conformedDimension, qb:MeasureProperty), not from per-source code.
"""

from __future__ import annotations

import json
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from rdflib import RDF, RDFS, BNode, Graph, Literal

from .dimensions import DimensionDefinition
from .lineage import ID, column_iri, new_graph
from .models import MappingDecision
from .ontology import OB, PROV, QB, Ontology
from .sources import Source
from .text import snake

SPARK_TYPES = {"string": "STRING", "integer": "INT", "decimal": "DECIMAL(18,2)", "boolean": "BOOLEAN",
               "date": "DATE", "dateTime": "TIMESTAMP"}
DATE_DIM, LOCATION_DIM = "DateDimension", "LocationDimension"
FORMATS = {"fabric": "DELTA", "local": "PARQUET"}


@dataclass
class DomainModel:
    domain: str
    target: str
    statements: list[str] = field(default_factory=list)
    checks: list[tuple[str, str]] = field(default_factory=list)  # (name, SQL returning one count that must be 0)
    tables: dict[str, list[str]] = field(default_factory=dict)  # gold table -> columns
    lineage: dict[tuple[str, str], set[tuple[str, str]]] = field(default_factory=lambda: defaultdict(set))
    cube: dict[str, list[tuple[str, str]]] = field(default_factory=dict)  # fact -> [(qb role, property)]

    def create(self, table: str, select: str) -> None:
        self.statements += [f"DROP TABLE IF EXISTS {table}",
                            f"CREATE TABLE {table} USING {FORMATS[self.target]} AS\n{select}"]

    def sql(self, header: str = "") -> str:
        return header + ";\n\n".join(self.statements) + ";\n"

    def checks_sql(self) -> str:
        return "\nUNION ALL\n".join(f"SELECT '{name}' AS check, ({q}) AS violations" for name, q in self.checks)


def quote(source: Source, column: str) -> str:
    parts = column.split(".") if source.format == "json" else [column]  # dotted JSON paths are struct fields
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def build_domain(domain: str, sources: list[Source], decisions: list[MappingDecision], ontology: Ontology,
                 dims: list[DimensionDefinition], target: str = "fabric", raw_root: str = "Files/raw") -> DomainModel:
    P = ontology.properties
    defs = {d.name: d for d in dims}
    m = DomainModel(domain, target)
    m.statements.append("SET spark.sql.session.timeZone = UTC")
    m.statements += [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in ("bronze", "silver", domain)]

    # ---- bronze and silver ---------------------------------------------------------------
    cols: dict[str, dict[str, str]] = {}  # source id -> {property: source column}
    silver: dict[str, str] = {}
    for s in sources:
        cols[s.id] = {d.property: d.column for d in decisions
                      if d.source_id == s.id and d.status == "accepted" and d.property in P}
        view, bronze, silver[s.id] = f"raw_{domain}_{s.name}", f"bronze.{domain}_{s.name}", f"silver.{domain}_{s.name}"
        options = "header 'true'" if s.format == "csv" else "multiLine 'true'"
        m.statements.append(f"CREATE OR REPLACE TEMPORARY VIEW {view} USING {s.format} "
                            f"OPTIONS (path '{raw_root}/{s.id}', {options})")
        m.create(bronze, f"SELECT * FROM {view}")
        select = [f"CAST({quote(s, c)} AS {SPARK_TYPES[P[p].range]}) AS {snake(p)}" for p, c in cols[s.id].items()]
        m.create(silver[s.id], "SELECT\n  " + ",\n  ".join([*select, f"'{s.id}' AS _source"]) + f"\nFROM {bronze}")

    def origin(table: str, column: str, prop: str, among: list[Source]) -> None:
        for s in among:
            if prop in cols[s.id]:
                m.lineage[(table, column)].add((s.id, cols[s.id][prop]))

    facts = [s for s in sources if any(P[p].kind == "measure" for p in cols[s.id])]

    # ---- conformed dimensions with a natural key -----------------------------------------
    keyed: dict[str, tuple[str, str, str]] = {}  # dimension -> (fk column, key column, table)
    for d in dims:
        contributors = [s for s in sources if d.natural_key and d.natural_key in cols[s.id]]
        if not contributors:
            continue
        key, fk, table = snake(d.natural_key), d.table.removeprefix("dim_") + "_key", f"{domain}.{d.table}"
        attrs = [a for a in d.attributes if any(a in cols[s.id] for s in contributors)]
        # A reference source (no measures) describes its dimension, so its other columns are attributes too
        for s in contributors:
            if s not in facts:
                attrs += [p for p in cols[s.id] if p not in attrs and p != d.natural_key and not P[p].natural_key]
        parts = []
        for s in contributors:
            sel = [key] + [snake(a) if a in cols[s.id] else f"CAST(NULL AS {SPARK_TYPES[P[a].range]}) AS {snake(a)}"
                           for a in attrs]
            parts.append(f"  SELECT {', '.join(sel)} FROM {silver[s.id]}")
        names = [snake(a) for a in attrs]
        # ponytail: max() picks one value per key when sources disagree; add survivorship rules if that matters
        m.create(table, f"SELECT xxhash64({key}) AS {fk}, {key}" + "".join(f", max({c}) AS {c}" for c in names)
                 + "\nFROM (\n" + "\n  UNION ALL\n".join(parts) + f"\n) src\nWHERE {key} IS NOT NULL\nGROUP BY {key}"
                 + "\nUNION ALL\nSELECT -1, 'UNKNOWN'" + ", NULL" * len(names))
        m.tables[table] = [fk, key, *names]
        origin(table, key, d.natural_key, contributors)
        for a, c in zip(attrs, names, strict=True):
            origin(table, c, a, contributors)
        m.checks.append((f"unique {table}.{fk}", f"SELECT count(*) - count(DISTINCT {fk}) FROM {table}"))
        keyed[d.name] = (fk, key, table)

    # ---- location: a composite-key dimension over the location columns facts carry --------
    loc = list(dict.fromkeys(p for s in facts for p in cols[s.id] if P[p].dimension == LOCATION_DIM))

    def location_key(values: list[str | None]) -> str:
        """Hash of every location column in a fixed order; a column the source lacks hashes as ''."""
        hashed = ", ".join(f"coalesce(CAST({v} AS STRING), '')" if v else "''" for v in values)
        present = ", ".join(v for v in values if v)
        return f"CASE WHEN coalesce({present}) IS NULL THEN -1 ELSE xxhash64({hashed}) END"

    if loc:
        table, names = f"{domain}.dim_location", [snake(p) for p in loc]
        parts = [f"  SELECT {', '.join(snake(p) if p in cols[s.id] else f'CAST(NULL AS STRING) AS {snake(p)}' for p in loc)}"
                 f" FROM {silver[s.id]}" for s in facts if any(p in cols[s.id] for p in loc)]
        m.create(table, f"SELECT DISTINCT {location_key(names)} AS location_key, {', '.join(names)}\nFROM (\n"
                 + "\n  UNION ALL\n".join(parts) + f"\n) src\nWHERE coalesce({', '.join(names)}) IS NOT NULL"
                 + "\nUNION ALL\nSELECT -1" + ", NULL" * len(names))
        m.tables[table] = ["location_key", *names]
        for p, c in zip(loc, names, strict=True):
            origin(table, c, p, facts)
        m.checks.append((f"unique {table}.location_key",
                         f"SELECT count(*) - count(DISTINCT location_key) FROM {table}"))

    # ---- date: a calendar spanning every date the facts carry -----------------------------
    dated = [(s, p) for s in facts for p in cols[s.id] if P[p].dimension == DATE_DIM]
    if dated:
        table = f"{domain}.dim_date"
        union = "\n    UNION ALL\n".join(f"    SELECT CAST({snake(p)} AS DATE) AS d FROM {silver[s.id]}" for s, p in dated)
        m.create(table,
                 "SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date, year(d) AS year,\n"
                 "  quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,\n"
                 "  dayofmonth(d) AS day_of_month, date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend\n"
                 "FROM (SELECT explode(sequence(lo, hi, INTERVAL 1 DAY)) AS d\n"
                 f"  FROM (SELECT min(d) AS lo, max(d) AS hi FROM (\n{union}\n  ) dates) bounds)\n"
                 "UNION ALL\nSELECT -1" + ", NULL" * 8)
        m.tables[table] = ["date_key", "calendar_date", "year", "quarter", "month", "month_name",
                           "day_of_month", "day_name", "is_weekend"]
        m.checks.append((f"unique {table}.date_key", f"SELECT count(*) - count(DISTINCT date_key) FROM {table}"))

    # ---- facts -----------------------------------------------------------------------------
    for s in facts:
        c, table = cols[s.id], f"{domain}.fact_{s.name}"
        keys, dates, inline, measures, cube = [], [], [], [], []
        refs: list[tuple[str, str]] = []  # (fk, dimension table) for orphan checks
        for name, (fk, key, dim_table) in keyed.items():
            if defs[name].natural_key in c:
                keys.append(f"CASE WHEN {key} IS NULL THEN -1 ELSE xxhash64({key}) END AS {fk}")
                origin(table, fk, defs[name].natural_key, [s])
                refs.append((fk, dim_table))
                cube.append(("dimension", defs[name].natural_key))
        if any(p in c for p in loc):
            keys.append(location_key([snake(p) if p in c else None for p in loc]) + " AS location_key")
            for p in loc:
                origin(table, "location_key", p, [s])
                cube += [("dimension", p)] if p in c else []
            refs.append(("location_key", f"{domain}.dim_location"))
        for p in c:
            info, col = P[p], snake(p)
            if info.dimension in keyed and (info.natural_key or defs[info.dimension].natural_key in c):
                continue  # the key is above; attributes live on the dimension
            if info.dimension == LOCATION_DIM:
                continue
            if info.dimension == DATE_DIM:
                dk = f"{col}_key" if info.range == "date" else f"{col.removesuffix('_timestamp')}_date_key"
                dates.append(f"coalesce(CAST(date_format({col}, 'yyyyMMdd') AS INT), -1) AS {dk}")
                origin(table, dk, p, [s])
                refs.append((dk, f"{domain}.dim_date"))
                cube.append(("dimension", p))
                if info.range == "dateTime":  # keep the event time itself next to its date key
                    inline.append(col)
                    origin(table, col, p, [s])
                continue
            (measures if info.kind == "measure" else inline).append(col)
            origin(table, col, p, [s])
            cube.append(({"measure": "measure", "attribute": "attribute"}.get(info.kind, "dimension"), p))
        m.create(table, "SELECT\n  " + ",\n  ".join([*keys, *dates, *inline, *measures, "_source"])
                 + f"\nFROM {silver[s.id]}")
        m.tables[table] = [x.rsplit(" AS ", 1)[-1] for x in keys + dates] + inline + measures + ["_source"]
        m.cube[table] = cube
        m.checks.append((f"rows {table}", f"SELECT (SELECT count(*) FROM {table}) - (SELECT count(*) FROM {silver[s.id]})"))
        for fk, dim_table in refs:
            key_col = "date_key" if dim_table.endswith("dim_date") else fk
            m.checks.append((f"orphans {table}.{fk}",
                             f"SELECT count(*) FROM {table} f LEFT ANTI JOIN {dim_table} d ON f.{fk} = d.{key_col}"))
    return m


def build_all(sources: list[Source], decisions: list[MappingDecision], ontology: Ontology,
              dims: list[DimensionDefinition], target: str = "fabric", raw_root: str = "Files/raw") -> list[DomainModel]:
    by_domain: dict[str, list[Source]] = defaultdict(list)
    for s in sources:
        by_domain[s.domain].append(s)
    return [build_domain(d, by_domain[d], decisions, ontology, dims, target, raw_root) for d in sorted(by_domain)]


def lineage_graph(models: list[DomainModel]) -> Graph:
    """Gold column -> source column (PROV-O), plus a W3C Data Cube structure definition per fact."""
    g = new_graph()
    g.bind("qb", QB)
    activity = ID["activity/star-schema"]
    g.add((activity, RDF.type, PROV.Activity))
    for m in models:
        for (table, column), origins in sorted(m.lineage.items()):
            lc = ID[f"lakehouse/{table}#{column}"]
            g.add((lc, RDF.type, OB.LakehouseColumn))
            g.add((lc, RDFS.label, Literal(f"{table}.{column}")))
            g.add((lc, PROV.wasGeneratedBy, activity))
            for source_id, source_column in origins:
                g.add((lc, PROV.wasDerivedFrom, column_iri(source_id, source_column)))
        for table, components in m.cube.items():
            dsd = ID[f"dsd/{table}"]
            g.add((dsd, RDF.type, QB.DataStructureDefinition))
            g.add((dsd, RDFS.label, Literal(table)))
            for role, prop in components:
                spec = BNode()
                g.add((dsd, QB.component, spec))
                g.add((spec, QB[role], OB[prop]))
    return g


def notebook(models: list[DomainModel], header: str) -> dict:
    """A Fabric notebook: one %%sql cell per statement, then a validation cell per domain."""
    def cell(kind: str, text: str) -> dict:
        base = {"cell_type": kind, "metadata": {}, "source": text.splitlines(keepends=True)}
        return base | ({"execution_count": None, "outputs": []} if kind == "code" else {})

    cells = [cell("markdown", header)]
    for m in models:
        cells.append(cell("markdown", f"## {m.domain.capitalize()}"))
        cells += [cell("code", "%%sql\n" + s) for s in m.statements]
        cells.append(cell("markdown", f"Validation for {m.domain}: every count must be 0."))
        cells.append(cell("code", "%%sql\n" + m.checks_sql()))
    return {"nbformat": 4, "nbformat_minor": 5, "cells": cells, "metadata": {
        "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
        "language_info": {"name": "python"}}}


def write(models: list[DomainModel], dims: list[DimensionDefinition], out: Path, run_id: str) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    target = models[0].target if models else "fabric"
    where = "Fabric Lakehouse (Delta); upload data/sources to Files/raw first" if target == "fabric" else "local Spark (Parquet)"
    files = []
    for m in models:
        header = (f"-- OntoBridge {m.domain} star schema for {where}.\n"
                  f"-- Generated from alignment {run_id} by `ontobridge model`. Regenerate rather than edit.\n\n")
        path = out / f"{m.domain}.sql"
        path.write_text(m.sql(header) + "\n-- Validation: every count must be 0\n" + m.checks_sql() + ";\n")
        files.append(path)
    nb = out / "ontobridge_lakehouse.ipynb"
    nb.write_text(json.dumps(notebook(models, f"# OntoBridge lakehouse build\n\nGenerated from alignment `{run_id}` "
                                              f"for a {where}."), indent=1))
    dj = out / "dimensions.json"
    dj.write_text(json.dumps([d.as_dict() for d in dims], indent=2))
    ttl = out / "lineage.ttl"
    lineage_graph(models).serialize(ttl, format="turtle")
    return [*files, nb, dj, ttl]


def run_local(models: list[DomainModel], warehouse: Path) -> dict:
    """Execute the generated SQL on a local Spark session and return row counts and check results."""
    from pyspark.sql import SparkSession

    shutil.rmtree(warehouse, ignore_errors=True)  # the local catalog is in-memory; stale table folders block CTAS
    spark = (SparkSession.builder.master("local[2]").appName("ontobridge")
             .config("spark.sql.warehouse.dir", str(warehouse))
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    report = {}
    for m in models:
        for statement in m.statements:
            spark.sql(statement)
        report[m.domain] = {
            "tables": {t: spark.table(t).count() for t in m.tables},
            "checks": {name: int(spark.sql(q).first()[0]) for name, q in m.checks},
        }
    return report
