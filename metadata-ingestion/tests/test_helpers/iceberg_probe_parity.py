"""Parity listings for the Iceberg probe, shared by its unit tests (fake and
SQL catalogs) and its integration test (docker REST catalog)."""

from typing import List

from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import EmittedIndex, FanOut, ParityListing


def _table_names(index: EmittedIndex) -> List[str]:
    # `<namespace>.<table>`, the identity a fanned-out table listing gets.
    return [DatasetUrn.from_string(u).name for u in index.urns("dataset")]


LISTINGS: List[ParityListing] = [
    ParityListing(
        "namespaces",
        "namespaces",
        lambda index: index.container_names(DatasetContainerSubTypes.NAMESPACE),
    ),
    ParityListing(
        "tables",
        "tables",
        lambda index: set(_table_names(index)),
        fan_out=FanOut("namespaces", "namespace"),
    ),
]
