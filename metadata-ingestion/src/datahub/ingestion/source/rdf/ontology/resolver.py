"""Resolve ontology configuration to a loaded TBox."""

from typing import Optional

from datahub.ingestion.source.rdf.ontology.loader import (
    default_ontology_path,
    load_datahub_ontology,
)
from datahub.ingestion.source.rdf.ontology.models import DataHubOntology


def resolve_ontology(ontology_config: Optional[str]) -> DataHubOntology:
    """
    Resolve the ontology config value to a DataHubOntology instance.

    Args:
        ontology_config: "default" for bundled TBox, None/"" for empty, else a file path.

    Returns:
        Loaded ontology (possibly empty).
    """
    if ontology_config is None or ontology_config.strip() == "":
        return DataHubOntology()
    if ontology_config.strip().lower() == "default":
        return load_datahub_ontology(str(default_ontology_path()))
    return load_datahub_ontology(ontology_config.strip())
