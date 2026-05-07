"""Regression tests for `_extend_lineage` config-mutation safety.

The user-supplied `sources_to_platform_instance` and
`destination_to_platform_instance` mappings on `FivetranSourceConfig` are
shared across connectors within an ingest. A previous bug at
`fivetran.py:187/199` mutated `PlatformDetail.platform` in place after
`dict.get(...)` returned the *configured* instance for hits, poisoning
the user's config object. The fix uses `model_copy(update=...)`. This
test pins the immutability invariant: a regression that reverts to
`source_details.platform = ...` would silently re-introduce the bug,
because each integration test runs one pipeline against a fresh config
dict and would not surface cross-connector contamination.
"""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranLogConfig,
    FivetranSourceConfig,
    PlatformDetail,
)
from datahub.ingestion.source.fivetran.data_classes import Connector
from datahub.ingestion.source.fivetran.fivetran import FivetranSource


@pytest.fixture
def source_with_postgres_override():
    """A FivetranSource with a partial `sources_to_platform_instance` override
    that pins `database` but leaves `platform` for `_extend_lineage` to
    auto-detect from the connector_type. This is the exact shape that
    triggered the original mutation bug.
    """
    config = FivetranSourceConfig(
        fivetran_log_config=FivetranLogConfig(
            destination_platform="snowflake",
            snowflake_destination_config={
                "host_port": "test.snowflakecomputing.com",
                "username": "u",
                "password": "p",
                "database": "log_db",
                "warehouse": "wh",
                "role": "r",
                "log_schema": "fivetran_log",
            },
        ),
        api_config=FivetranAPIConfig(api_key="k", api_secret="s"),
        # Partial override: user pins `database` for "c1" but expects the
        # connector to auto-detect platform from connector_type.
        sources_to_platform_instance={"c1": PlatformDetail(database="user_db")},
        destination_to_platform_instance={
            "test_destination": {"platform": "snowflake"}
        },
    )
    ctx = PipelineContext(run_id="test_run")

    # Patch `create_engine` (not `FivetranLogDbReader`) so the real
    # `FivetranLogDbReader` class is preserved — the source's
    # `isinstance(self.log_reader, FivetranLogDbReader)` check needs the
    # log_reader to be an actual instance, not a MagicMock.
    with patch(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
    ):
        source = FivetranSource(config, ctx)
        source.api_client = None  # DB-only mode for this test
        yield source


def _make_connector(connector_id: str, connector_type: str = "postgres") -> Connector:
    return Connector(
        connector_id=connector_id,
        connector_name=connector_id,
        connector_type=connector_type,
        paused=False,
        sync_frequency=1440,
        destination_id="test_destination",
        user_id="",
        lineage=[],
        jobs=[],
    )


def test_extend_lineage_does_not_mutate_user_source_platform_detail(
    source_with_postgres_override,
):
    """The user supplied `PlatformDetail(database="user_db")` with no
    platform pinned. After `_extend_lineage` runs, the *configured* object
    must still have `platform=None` — auto-detection must go through
    `model_copy`, not in-place assignment, so subsequent ingests / config
    inspections see the user's original input untouched.
    """
    source = source_with_postgres_override
    datajob = Mock()

    # Snapshot before.
    pinned_detail = source.config.sources_to_platform_instance["c1"]
    assert pinned_detail.platform is None
    assert pinned_detail.database == "user_db"

    source._extend_lineage(_make_connector("c1"), datajob)

    # The configured object must be byte-for-byte identical after the call.
    assert pinned_detail.platform is None, (
        "C1 regression: `_extend_lineage` mutated the user's "
        "sources_to_platform_instance entry. Auto-detection of platform "
        "must use `model_copy(update=...)`, not in-place assignment."
    )
    assert pinned_detail.database == "user_db"
    # Confirm the dict still holds the same instance (not replaced wholesale).
    assert source.config.sources_to_platform_instance["c1"] is pinned_detail


def test_extend_lineage_uses_user_database_with_resolved_platform(
    source_with_postgres_override,
):
    """The mutation fix must also preserve the user's `database` field
    while filling in the auto-detected `platform`. Walk through one
    connector's lineage edge to confirm both halves of the merged
    `PlatformDetail` reach the URN-construction site.
    """
    source = source_with_postgres_override

    # Lineage list with one row so _extend_lineage has something to iterate.
    from datahub.ingestion.source.fivetran.data_classes import (
        ColumnLineage,
        TableLineage,
    )

    connector = _make_connector("c1")
    connector.lineage = [
        TableLineage(
            source_table="public.employee",
            destination_table="schema.employee",
            column_lineage=[
                ColumnLineage(source_column="id", destination_column="id"),
            ],
        )
    ]

    datajob = Mock()

    source._extend_lineage(connector, datajob)

    # `_extend_lineage` calls `datajob.set_inlets(...)` with the input URN
    # list. The source URN should reflect both: platform="postgres"
    # (auto-detected from connector_type) AND database="user_db" (from
    # the user's override).
    datajob.set_inlets.assert_called_once()
    inlet_urns = datajob.set_inlets.call_args.args[0]
    assert len(inlet_urns) == 1, "Expected exactly one input dataset URN"
    urn_str = str(inlet_urns[0])
    assert "postgres" in urn_str
    assert "user_db" in urn_str


def test_extend_lineage_two_connectors_share_pinned_detail(
    source_with_postgres_override,
):
    """Cross-connector contamination check: two connectors that both look
    up the same `sources_to_platform_instance["c1"]` entry must each get a
    fresh resolved PlatformDetail. A regression to in-place mutation would
    produce identical *first* connector behavior but break the *second*
    one (it would see the platform left over from the first call).
    """
    source = source_with_postgres_override
    datajob = Mock()

    source._extend_lineage(_make_connector("c1", connector_type="postgres"), datajob)
    # Pin still untouched after first run.
    assert source.config.sources_to_platform_instance["c1"].platform is None

    # Second connector with the same id but a different connector_type. If
    # the previous call had mutated the pinned detail, this run would see
    # `platform="postgres"` already set and skip auto-detection — wrong
    # behavior. With the fix, each call goes through model_copy fresh.
    source._extend_lineage(_make_connector("c1", connector_type="snowflake"), datajob)
    assert source.config.sources_to_platform_instance["c1"].platform is None
