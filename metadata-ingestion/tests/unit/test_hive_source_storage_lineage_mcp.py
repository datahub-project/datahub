from typing import Iterable, Union
from unittest.mock import patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource, SqlWorkUnit
from datahub.metadata.schema_classes import MetadataChangeEventClass
from tests.unit.test_hive_source_storage_lineage import (
    HIVE_TABLE,
    STORAGE,
    assert_storage_lineage,
    rows,
    run_scenario,
)

_parent_process_table = SQLAlchemySource._process_table


def _parent_emitting_mcps(
    self: SQLAlchemySource, *args: object, **kwargs: object
) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
    """Splits any MCE from the parent into one MCP per aspect (a no-op once the parent emits MCPs)."""
    for wu in _parent_process_table(self, *args, **kwargs):  # type: ignore[arg-type]
        if isinstance(wu.metadata, MetadataChangeEventClass):
            snapshot = wu.metadata.proposedSnapshot
            for aspect in snapshot.aspects:
                yield MetadataChangeProposalWrapper(
                    entityUrn=snapshot.urn, aspect=aspect
                ).as_workunit()
        else:
            yield wu


@pytest.mark.parametrize("direction", ["upstream", "downstream"])
def test_storage_lineage_when_parent_emits_mcps(direction: str) -> None:
    with patch.object(SQLAlchemySource, "_process_table", _parent_emitting_mcps):
        source, workunits = run_scenario(direction)
    assert not any(
        isinstance(wu.metadata, MetadataChangeEventClass) for wu in workunits
    )
    assert_storage_lineage(source, workunits, direction)


def test_unexpected_storage_lineage_error_keeps_table_aspects() -> None:
    with patch.object(
        HiveStorageLineage, "get_lineage_mcp", side_effect=RuntimeError("boom")
    ):
        source, workunits = run_scenario("upstream")
    found = rows(workunits)

    assert not source.report.failures
    assert len(source.report.warnings) == 1
    assert not [n for u, n, _ in found if u == STORAGE]
    assert [n for u, n, _ in found if u == HIVE_TABLE] == [
        "container",
        "status",
        "datasetProperties",
        "schemaMetadata",
        "subTypes",
    ]
