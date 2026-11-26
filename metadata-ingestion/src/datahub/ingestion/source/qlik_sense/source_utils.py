from collections import Counter
from typing import Dict, Iterable, List, Mapping, Optional, Set, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.constants import (
    ENTITY_CHECK_BATCH_SIZE,
    QLIK_APP_QRI_RE,
    QVD_EXTENSION,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    FIELD_TYPE_MAPPING,
    App,
    ListedDataset,
    QlikDataset,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.file_lineage import qri_part_id
from datahub.ingestion.source.qlik_sense.lib_path import _lib_path_parts, _path_key
from datahub.ingestion.source.qlik_sense.qvd_lineage import QvdWrite
from datahub.ingestion.source.qlik_sense.script_lineage import (
    LIB_PATH_RE,
    SPACE_SEPARATOR,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.schema import NullType, SchemaField
from datahub.metadata.schema_classes import SchemaFieldDataTypeClass
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.sqlglot_lineage import ColumnRef
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import guess_entity_type

MappingT = TypeVar("MappingT")


def _get_ignoring_case(
    mapping: Mapping[str, MappingT], name: str
) -> Optional[MappingT]:
    if name in mapping:
        return mapping[name]
    # Qlik resolves connection names regardless of case. A case variant is
    # taken only when a single key matches it.
    matches = [key for key in mapping if key.lower() == name.lower()]
    return mapping[matches[0]] if len(matches) == 1 else None


def _is_qvd(dataset: Union[QlikDataset, ListedDataset]) -> bool:
    return dataset.type.upper() == "QVD" or dataset.name.lower().endswith(QVD_EXTENSION)


def _dataset_subtype(dataset: QlikDataset) -> str:
    return (
        DatasetSubTypes.QLIK_QVD if _is_qvd(dataset) else DatasetSubTypes.QLIK_DATASET
    )


def _dataset_display_name(dataset: QlikDataset) -> str:
    # The URN keeps the file name, so `sales.qvd` and `sales.csv` in one space
    # stay apart and QVDs ingested before keep their URNs.
    if _is_qvd(dataset) and dataset.name.lower().endswith(QVD_EXTENSION):
        return dataset.name[: -len(QVD_EXTENSION)]
    return dataset.name


def _qvd_fields(writes: List[QvdWrite], reads: List[str]) -> Optional[List[str]]:
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


def _own_table_urns(app_id: str, urns: Dict[str, str]) -> Dict[str, str]:
    return {
        qri: urn for qri, urn in urns.items() if _qlik_app_id(qri) in (None, app_id)
    }


def _qlik_app_id(qri: str) -> Optional[str]:
    match = QLIK_APP_QRI_RE.match(qri)
    return match.group(1) if match else None


def _schema_field_parent(urn: str) -> Optional[str]:
    # Lineage another tool wrote may hold any kind of URN.
    try:
        return SchemaFieldUrn.from_string(urn).parent
    except InvalidUrnError:
        return None


def _app_field_names(app: App) -> List[str]:
    names = [field.name for table in app.tables for field in table.datasetSchema]
    names.extend(
        field.name for table in app.data_model_tables for field in table.fields
    )
    names.extend(field for table in app.lineage_tables for field in table.fields)
    return list(dict.fromkeys(names))


def _qlik_field_id(name: str) -> str:
    """The last part of a field node's QRI in Qlik's lineage graph, which is the
    unpadded base64url SHA-256 of the field name. Not documented by Qlik, so it
    is only used to confirm a name, never to make one up."""
    return qri_part_id(name)


def _refs_of_field_sources(
    refs: List[ColumnRef], field_name: str, field_ids: Set[str]
) -> List[ColumnRef]:
    """The columns whose name Qlik's graph hashes into one of `field_ids`.
    Qlik hashes the name the database returned, whose case the SQL and DataHub
    may not share, so a column also matches the Qlik field it loads when that
    field kept the source's name, unless another column differs from it only
    in case."""
    exact = [ref for ref in refs if _qlik_field_id(ref.column) in field_ids]
    if exact or _qlik_field_id(field_name) not in field_ids:
        return exact
    folded = [ref for ref in refs if ref.column.lower() == field_name.lower()]
    return folded if len({ref.column for ref in folded}) == 1 else []


def _patch_workunits(
    patch: MetadataPatchProposal, is_primary_source: bool
) -> Iterable[MetadataWorkUnit]:
    for mcp in patch.build():
        yield MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(mcp),
            mcp_raw=mcp,
            is_primary_source=is_primary_source,
        )


def _bounded_list(items: List[str], limit: int = 10) -> str:
    more = f" (and {len(items) - limit} more)" if len(items) > limit else ""
    return ", ".join(items[:limit]) + more


def _describe_error(error: Exception) -> str:
    # The type tells failures apart when the message is empty or generic.
    return f"{type(error).__name__}: {error}" if str(error) else type(error).__name__


def _error_type(error: Optional[Exception]) -> str:
    # SQL parser errors quote the statement, which can hold credentials when a
    # script statement was split wrongly, so only the type goes in the report.
    return type(error).__name__ if error else "unknown"


def _gen_schema_field(field: QlikDatasetSchemaField) -> SchemaField:
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


def _qlik_dataset_identifier(space_id: str, name: str) -> str:
    return f"{space_id}.{name}".lower()


def _voted_segments(
    path: str, spellings: Dict[str, Counter[str]]
) -> Optional[List[str]]:
    """The segments of a path whose space and connection can't be told
    apart, when exactly one way of splitting them is spelled elsewhere."""
    match = LIB_PATH_RE.match(path)
    if not match or "$(" in path:
        return None
    connection = match.group("connection")
    splits = [
        [connection[:i].strip(), connection[i + 1 :].strip()]
        for i, char in enumerate(connection)
        if char == SPACE_SEPARATOR
    ]
    known = [split for split in splits if _path_key("/".join(split)) in spellings]
    if len(known) != 1:
        return None
    return [*known[0], *_lib_path_parts(match.group("path"))]


def _entities_present(graph: DataHubGraph, urns: List[str]) -> bool:
    """Whether every entity the app's last read produced is in DataHub and
    not soft-deleted, which also catches entities the sink failed to write
    and hard-deleted ones. Entities stale removal deleted after they left
    Qlik are not in the checkpoint."""
    by_type: Dict[str, List[str]] = {}
    for urn in urns:
        by_type.setdefault(guess_entity_type(urn), []).append(urn)
    for entity_type, typed_urns in by_type.items():
        for start in range(0, len(typed_urns), ENTITY_CHECK_BATCH_SIZE):
            batch = typed_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
            entities = graph.get_entities(
                entity_type, batch, aspects=[Status.ASPECT_NAME]
            )
            for urn in batch:
                status = entities.get(urn, {}).get(Status.ASPECT_NAME)
                if status is None or getattr(status[0], "removed", True):
                    return False
    return True


def _read_errors_context(app: App) -> str:
    first, *rest = app.read_errors
    more = f" (and {len(rest)} more)" if rest else ""
    return f"{app.id}: {first}{more}"
