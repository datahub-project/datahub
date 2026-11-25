from typing import Dict, Iterable, List, Mapping, Optional, Set, TypeVar, Union

from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.constants import QLIK_APP_QRI_RE, QVD_EXTENSION
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    ListedDataset,
    QlikDataset,
)
from datahub.ingestion.source.qlik_sense.file_lineage import qri_part_id
from datahub.ingestion.source.qlik_sense.qvd_lineage import QvdWrite
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.sqlglot_lineage import ColumnRef

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
    except Exception:
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


def _error_type(error: Optional[Exception]) -> str:
    # SQL parser errors quote the statement, which can hold credentials when a
    # script statement was split wrongly, so only the type goes in the report.
    return type(error).__name__ if error else "unknown"
