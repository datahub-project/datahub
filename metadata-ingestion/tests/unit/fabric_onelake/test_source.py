"""Unit tests for Fabric OneLake source key hierarchy."""

from typing import List, Optional
from unittest.mock import MagicMock, patch

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    datahub_guid,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.source.fabric.common.models import (
    FABRIC_WORKSPACE_PLATFORM,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.onelake.models import FabricColumn, FabricTable
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.ingestion.source.fabric.onelake.source import (
    PLATFORM,
    FabricOneLakeSource,
    LakehouseKey,
    LakehouseSchemaKey,
    WarehouseKey,
    WarehouseSchemaKey,
    _ProfileCandidate,
)
from datahub.ingestion.source.profiling.common import ProfilerRequest
from datahub.metadata.schema_classes import DatasetProfileClass


def _profiling_source_stub(
    *, enabled: bool = True, profile_pattern: Optional[AllowDenyPattern] = None
) -> MagicMock:
    """A MagicMock standing in for FabricOneLakeSource with the attributes the
    profiling helpers read. Bypassing __init__ keeps the test off the network."""
    src = MagicMock()
    src.config.is_profiling_enabled.return_value = enabled
    src.config.profile_pattern = profile_pattern or AllowDenyPattern.allow_all()
    src.config.platform_instance = None
    src.config.env = "PROD"
    src.config.profiling.max_workers = 1
    src.report = FabricOneLakeSourceReport()
    # Lowercase like the real _norm with convert_urns_to_lowercase=True.
    src._norm = lambda value: value.lower()
    return src


def test_lakehouse_parent_workspace_uses_fabric_platform() -> None:
    key = LakehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
    )

    parent = key.parent_key()
    assert isinstance(parent, WorkspaceKey)
    assert parent.platform == FABRIC_WORKSPACE_PLATFORM
    assert parent.workspace_id == "ws-123"


def test_warehouse_parent_workspace_uses_fabric_platform() -> None:
    key = WarehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
    )

    parent = key.parent_key()
    assert isinstance(parent, WorkspaceKey)
    assert parent.platform == FABRIC_WORKSPACE_PLATFORM
    assert parent.workspace_id == "ws-123"


def test_schema_parent_chain_keeps_onelake_then_fabric() -> None:
    schema_key = LakehouseSchemaKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
        schema_name="dbo",
    )

    lakehouse_parent = schema_key.parent_key()
    assert isinstance(lakehouse_parent, LakehouseKey)
    assert lakehouse_parent.platform == PLATFORM

    workspace_parent = lakehouse_parent.parent_key()
    assert isinstance(workspace_parent, WorkspaceKey)
    assert workspace_parent.platform == FABRIC_WORKSPACE_PLATFORM


def test_warehouse_schema_parent_chain_keeps_onelake_then_fabric() -> None:
    schema_key = WarehouseSchemaKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
        schema_name="sales",
    )

    warehouse_parent = schema_key.parent_key()
    assert isinstance(warehouse_parent, WarehouseKey)
    assert warehouse_parent.platform == PLATFORM

    workspace_parent = warehouse_parent.parent_key()
    assert isinstance(workspace_parent, WorkspaceKey)
    assert workspace_parent.platform == FABRIC_WORKSPACE_PLATFORM


def test_lakehouse_key_guid_uses_fabric_onelake_platform() -> None:
    key = LakehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
    )

    expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
            "lakehouse_id": "lh-456",
        }
    )
    assert key.guid() == expected


def test_warehouse_key_guid_uses_fabric_onelake_platform() -> None:
    key = WarehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
    )

    expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
            "warehouse_id": "wh-456",
        }
    )
    assert key.guid() == expected


def test_norm_respects_convert_urns_to_lowercase() -> None:
    """_norm lowercases identifiers iff convert_urns_to_lowercase=True.

    _norm gates URN-bound identifier casing so the dataset URNs match what
    sqlglot emits during view-lineage parsing. Bypassing __init__ is fine here
    because _norm only reads self.config.convert_urns_to_lowercase.
    """
    src = MagicMock()

    src.config.convert_urns_to_lowercase = True
    assert FabricOneLakeSource._norm(src, "Sales") == "sales"
    assert FabricOneLakeSource._norm(src, "CUSTOMERS") == "customers"
    assert FabricOneLakeSource._norm(src, "already_lower") == "already_lower"

    src.config.convert_urns_to_lowercase = False
    assert FabricOneLakeSource._norm(src, "Sales") == "Sales"
    assert FabricOneLakeSource._norm(src, "CUSTOMERS") == "CUSTOMERS"


def _table(name: str = "Customers", schema: str = "dbo") -> FabricTable:
    return FabricTable(name=name, schema_name=schema, item_id="it1", workspace_id="ws1")


def test_maybe_queue_profiling_happy_path() -> None:
    src = _profiling_source_stub()
    workspace = MagicMock(id="ws1")
    candidates: List[_ProfileCandidate] = []

    FabricOneLakeSource._maybe_queue_profiling(
        src,
        workspace,
        "it1",
        "dbo",
        _table(),
        [FabricColumn(name="id", data_type="int", is_nullable=False)],
        candidates,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    # Reflection identifiers keep source case; the URN is normalized.
    assert candidate.schema_name == "dbo"
    assert candidate.table_name == "Customers"
    assert candidate.pretty_name == "dbo.Customers"
    assert "customers" in candidate.dataset_urn
    assert "Customers" not in candidate.dataset_urn


def test_maybe_queue_profiling_disabled() -> None:
    src = _profiling_source_stub(enabled=False)
    candidates: List[_ProfileCandidate] = []
    FabricOneLakeSource._maybe_queue_profiling(
        src,
        MagicMock(id="ws1"),
        "it1",
        "dbo",
        _table(),
        [FabricColumn(name="id", data_type="int", is_nullable=False)],
        candidates,
    )
    assert candidates == []


def test_maybe_queue_profiling_skips_tables_without_columns() -> None:
    src = _profiling_source_stub()
    candidates: List[_ProfileCandidate] = []
    FabricOneLakeSource._maybe_queue_profiling(
        src, MagicMock(id="ws1"), "it1", "dbo", _table(), [], candidates
    )
    assert candidates == []


def test_maybe_queue_profiling_respects_profile_pattern() -> None:
    src = _profiling_source_stub(
        profile_pattern=AllowDenyPattern(allow=["^sales\\..*"])
    )
    candidates: List[_ProfileCandidate] = []
    FabricOneLakeSource._maybe_queue_profiling(
        src,
        MagicMock(id="ws1"),
        "it1",
        "dbo",
        _table(),
        [FabricColumn(name="id", data_type="int", is_nullable=False)],
        candidates,
    )
    assert candidates == []
    assert src.report.profiling_skipped_profile_pattern == 1


def test_profile_item_emits_profile_workunits() -> None:
    src = _profiling_source_stub()
    workspace = MagicMock(id="ws1")
    schema_client = MagicMock()
    schema_client.get_engine.return_value = MagicMock()

    dataset_urn = make_dataset_urn_with_platform_instance(
        PLATFORM, "ws1.it1.dbo.customers", None, "PROD"
    )
    candidate = _ProfileCandidate(
        pretty_name="dbo.customers",
        dataset_urn=dataset_urn,
        schema_name="dbo",
        table_name="customers",
    )
    profile = DatasetProfileClass(timestampMillis=1)

    profiler_instance = MagicMock()
    profiler_instance.generate_profiles.return_value = [
        (ProfilerRequest(pretty_name="dbo.customers", batch_kwargs={}), profile)
    ]

    with patch(
        "datahub.ingestion.source.sqlalchemy_profiler.sqlalchemy_profiler.SQLAlchemyProfiler",
        return_value=profiler_instance,
    ):
        workunits = list(
            FabricOneLakeSource._profile_item(
                src, workspace, "it1", schema_client, [candidate]
            )
        )

    assert len(workunits) == 1
    assert workunits[0].metadata.entityUrn == dataset_urn
    assert src.report.tables_profiled == 1


def test_profile_item_noop_without_schema_client() -> None:
    src = _profiling_source_stub()
    candidate = _ProfileCandidate(
        pretty_name="dbo.customers",
        dataset_urn="urn:li:dataset:x",
        schema_name="dbo",
        table_name="customers",
    )
    workunits = list(
        FabricOneLakeSource._profile_item(
            src, MagicMock(id="ws1"), "it1", None, [candidate]
        )
    )
    assert workunits == []
