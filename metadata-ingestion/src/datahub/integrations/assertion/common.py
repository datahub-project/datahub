from functools import lru_cache
from typing import List, Optional, Tuple, TypedDict

from datahub.api.entities.assertion.assertion import BaseEntityAssertion
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.utilities.urns.urn import Urn


class ColumnDict(TypedDict):
    col: str
    native_type: str


@lru_cache
def get_qualified_name_from_datahub(urn: str) -> Optional[str]:
    with get_default_graph(ClientMode.CLI) as graph:
        props: Optional[DatasetProperties] = graph.get_aspect(urn, DatasetProperties)
        if props is not None:
            return props.qualifiedName
    return None


@lru_cache
def get_schema_from_datahub(urn: str) -> Optional[List[ColumnDict]]:
    with get_default_graph(ClientMode.INGESTION) as graph:
        schema: Optional[SchemaMetadata] = graph.get_aspect(urn, SchemaMetadata)
        if schema is not None:
            return [
                {"col": field.fieldPath, "native_type": field.nativeDataType}
                for field in schema.fields
            ]
    return None


def get_entity_name(assertion: BaseEntityAssertion) -> Tuple[str, str, str]:
    if assertion.meta and assertion.meta.get("entity_qualified_name"):
        parts = assertion.meta["entity_qualified_name"].split(".")
    else:
        qualified_name = get_qualified_name_from_datahub(assertion.entity)
        if qualified_name is not None:
            parts = qualified_name.split(".")
        else:
            urn_id = Urn.from_string(assertion.entity).entity_ids[1]
            parts = urn_id.split(".")
            if len(parts) > 3:
                parts = parts[-3:]
    assert len(parts) == 3
    database = parts[-3]
    schema = parts[-2]
    table = parts[-1]
    return database, schema, table


def get_entity_schema(assertion: BaseEntityAssertion) -> Optional[List[ColumnDict]]:
    if assertion.meta and assertion.meta.get("entity_schema"):
        return assertion.meta.get("entity_schema")
    elif get_schema_from_datahub(assertion.entity):
        return get_schema_from_datahub(assertion.entity)
    return None
