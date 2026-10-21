"""PROV-O lineage: which agent mapped which source column to which ontology property, in which run.

Column IRIs are shared with the star-schema lineage (`starschema.lineage_graph`), so a gold table
column can be traced back through its mapping to the raw file it came from.
"""

from __future__ import annotations

from urllib.parse import quote

from rdflib import RDF, RDFS, XSD, Graph, Literal, Namespace, URIRef

from .models import MappingDecision
from .ontology import OB, PROV

ID = Namespace("https://manvith-d.github.io/ontobridge/id/")
AGENTS = {
    "heuristic": ID["agent/mapper"],
    "llm": ID["agent/llm-adjudicator"],
}


def dataset_iri(source_id: str) -> URIRef:
    return ID[f"source/{quote(source_id)}"]


def column_iri(source_id: str, column: str) -> URIRef:
    return URIRef(f"{dataset_iri(source_id)}#{quote(column, safe='')}")


def new_graph() -> Graph:
    g = Graph()
    for prefix, ns in (("ob", OB), ("prov", PROV), ("id", ID)):
        g.bind(prefix, ns)
    return g


def mapping_graph(run_id: str, started_at: str, decisions: list[MappingDecision]) -> Graph:
    g = new_graph()
    run = ID[f"run/{run_id}"]
    g.add((run, RDF.type, PROV.Activity))
    g.add((run, RDFS.label, Literal(f"alignment {run_id}")))
    g.add((run, PROV.startedAtTime, Literal(started_at, datatype=XSD.dateTime)))
    for d in decisions:
        ds, col = dataset_iri(d.source_id), column_iri(d.source_id, d.column)
        mapping = ID[f"mapping/{run_id}/{quote(d.source_id)}/{quote(d.column, safe='')}"]
        human = d.method == "human"
        agent = ID[f"reviewer/{quote(d.reviewed_by or 'unknown')}"] if human else AGENTS[d.method]
        g.add((ds, RDF.type, OB.SourceDataset))
        g.add((ds, RDFS.label, Literal(d.source_id)))
        g.add((run, PROV.used, ds))
        g.add((col, RDF.type, OB.SourceColumn))
        g.add((col, OB.columnName, Literal(d.column)))
        g.add((col, OB.inDataset, ds))
        g.add((agent, RDF.type, PROV.Person if human else PROV.SoftwareAgent))
        g.add((run, PROV.wasAssociatedWith, agent))
        g.add((mapping, RDF.type, OB.ColumnMapping))
        g.add((mapping, OB.sourceColumn, col))
        g.add((mapping, OB.status, Literal(d.status)))
        g.add((mapping, OB.method, Literal(d.method)))
        g.add((mapping, OB.score, Literal(round(d.score, 4), datatype=XSD.decimal)))
        g.add((mapping, RDFS.comment, Literal(d.reason)))
        g.add((mapping, PROV.wasGeneratedBy, run))
        g.add((mapping, PROV.wasDerivedFrom, col))
        g.add((mapping, PROV.wasAttributedTo, agent))
        if d.property:
            g.add((mapping, OB.targetProperty, OB[d.property]))
            if d.status == "accepted":
                g.add((col, OB.alignedTo, OB[d.property]))
    return g
