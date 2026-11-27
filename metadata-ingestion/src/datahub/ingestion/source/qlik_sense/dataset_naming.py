from typing import (
    Dict,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.qlik_sense.app_state import UnmappedFileStore
from datahub.ingestion.source.qlik_sense.constants import (
    QLIK_APP_QRI_RE,
    QVD_EXTENSION,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    ListedDataset,
    QlikDataset,
)

MappingT = TypeVar("MappingT")


def get_ignoring_case(mapping: Mapping[str, MappingT], name: str) -> Optional[MappingT]:
    if name in mapping:
        return mapping[name]
    # Qlik resolves connection names regardless of case. A case variant is
    # taken only when a single key matches it.
    matches = [key for key in mapping if key.lower() == name.lower()]
    return mapping[matches[0]] if len(matches) == 1 else None


def file_display_name(name: str) -> str:
    return name[: -len(QVD_EXTENSION)] if name.lower().endswith(QVD_EXTENSION) else name


def is_stored_qvd(store: UnmappedFileStore) -> bool:
    return (
        store.path.lower().endswith(QVD_EXTENSION) if store.qvd is None else store.qvd
    )


def is_qvd_dataset(dataset: Union[QlikDataset, ListedDataset]) -> bool:
    return dataset.type.upper() == "QVD" or dataset.name.lower().endswith(QVD_EXTENSION)


def dataset_subtype(dataset: QlikDataset) -> str:
    return (
        DatasetSubTypes.QLIK_QVD
        if is_qvd_dataset(dataset)
        else DatasetSubTypes.QLIK_DATASET
    )


def dataset_display_name(dataset: QlikDataset) -> str:
    # The URN keeps the file name, so `sales.qvd` and `sales.csv` in one space
    # stay apart and QVDs ingested before keep their URNs.
    if is_qvd_dataset(dataset) and dataset.name.lower().endswith(QVD_EXTENSION):
        return dataset.name[: -len(QVD_EXTENSION)]
    return dataset.name


def own_table_urns(app_id: str, urns: Dict[str, str]) -> Dict[str, str]:
    return {qri: urn for qri, urn in urns.items() if qlik_app_id(qri) in (None, app_id)}


def qlik_app_id(qri: str) -> Optional[str]:
    match = QLIK_APP_QRI_RE.match(qri)
    return match.group(1) if match else None


def qlik_dataset_identifier(space_id: str, name: str) -> str:
    return f"{space_id}.{name}".lower()
