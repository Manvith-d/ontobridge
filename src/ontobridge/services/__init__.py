"""State shared by the FastAPI services: settings, the ontology graph and the store, built once per process."""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from ..config import Settings, get_settings
from ..ontology import Ontology
from ..store import Store


@lru_cache
def settings() -> Settings:
    return get_settings()


@lru_cache
def ontology() -> Ontology:
    return Ontology(settings().ontology_dir)


@lru_cache
def store() -> Store:
    return Store(settings().runtime_dir / "ontobridge.db")


def reset() -> None:
    """Forget cached state (tests point ONTOBRIDGE_HOME somewhere new, then call this)."""
    for f in (settings, ontology, store):
        f.cache_clear()


SettingsDep = Annotated[Settings, Depends(settings)]
OntologyDep = Annotated[Ontology, Depends(ontology)]
StoreDep = Annotated[Store, Depends(store)]
