from typing import List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.qlik_sense.data_classes import (
    FIELD_TYPE_MAPPING,
    App,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.file_lineage import qri_part_id
from datahub.ingestion.source.qlik_sense.qvd_lineage import QvdWrite
from datahub.ingestion.source.qlik_sense.script_lineage import ScriptTableLineage
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import NullType, SchemaField
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.sqlglot_lineage import ColumnRef
from datahub.utilities.urns.error import InvalidUrnError


def qvd_fields(writes: List[QvdWrite], reads: List[str]) -> Optional[List[str]]:
    """A QVD's fields: what its writers store, or failing that what its readers
    load, which may be only some of them. None when unknown."""
    if writes:
        if any(write.fields is None for write in writes):
            return None
        return list(
            dict.fromkeys(
                field.name for write in writes for field in write.fields or []
            )
        )
    return reads or None


def schema_field_parent(urn: str) -> Optional[str]:
    # Lineage another tool wrote may hold any kind of URN.
    try:
        return SchemaFieldUrn.from_string(urn).parent
    except InvalidUrnError:
        return None


def app_field_names(app: App) -> List[str]:
    names = [field.name for table in app.tables for field in table.datasetSchema]
    names.extend(
        field.name for table in app.data_model_tables for field in table.fields
    )
    names.extend(field for table in app.lineage_tables for field in table.fields)
    return list(dict.fromkeys(names))


def qlik_field_id(name: str) -> str:
    """The last part of a field node's QRI in Qlik's lineage graph, which is the
    unpadded base64url SHA-256 of the field name. Not documented by Qlik, so it
    is only used to confirm a name, never to make one up."""
    return qri_part_id(name)


def refs_of_field_sources(
    refs: List[ColumnRef], field_name: str, field_ids: Set[str]
) -> List[ColumnRef]:
    """The columns whose name Qlik's graph hashes into one of `field_ids`.
    Qlik hashes the name the database returned, whose case the SQL and DataHub
    may not share, so a column also matches the Qlik field it loads when that
    field kept the source's name, unless another column differs from it only
    in case."""
    exact = [ref for ref in refs if qlik_field_id(ref.column) in field_ids]
    if exact or qlik_field_id(field_name) not in field_ids:
        return exact
    folded = [ref for ref in refs if ref.column.lower() == field_name.lower()]
    return folded if len({ref.column for ref in folded}) == 1 else []


def gen_schema_field(field: QlikDatasetSchemaField) -> SchemaField:
    return SchemaField(
        fieldPath=field.name,
        type=SchemaFieldDataTypeClass(
            type=(
                FIELD_TYPE_MAPPING.get(field.dataType, NullType)()
                if field.dataType
                else NullType()
            )
        ),
        nativeDataType=field.dataType if field.dataType else "",
        nullable=field.nullable,
        isPartOfKey=field.primaryKey,
    )


def script_column_lineage(
    dataset_urn: str, table: ScriptTableLineage
) -> Optional[List[FineGrainedLineage]]:
    return [
        FineGrainedLineage(
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=upstreams,
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[builder.make_schema_field_urn(dataset_urn, column)],
        )
        for column, upstreams in table.column_upstreams.items()
        if upstreams
    ] or None


def stale_field_lineage(
    current: List[FineGrainedLineage],
    stored: List[FineGrainedLineage],
    owned: Set[str],
) -> List[FineGrainedLineage]:
    """The column edges from owned tables that this run no longer stores,
    each as a lineage holding only those upstream fields."""
    stored_edges = {
        (downstream, upstream)
        for lineage in stored
        for downstream in lineage.downstreams or []
        for upstream in lineage.upstreams or []
    }
    stale = []
    for lineage in current:
        # A column edge can only be removed by patch through its single
        # downstream field; one another tool wrote with none or several
        # is not this connector's.
        if len(lineage.downstreams or []) != 1:
            continue
        upstreams = [
            upstream
            for upstream in lineage.upstreams or []
            if schema_field_parent(upstream) in owned
            and not any(
                (downstream, upstream) in stored_edges
                for downstream in lineage.downstreams or []
            )
        ]
        if upstreams:
            stale_lineage = FineGrainedLineage.from_obj(lineage.to_obj())
            stale_lineage.upstreams = upstreams
            stale.append(stale_lineage)
    return stale
