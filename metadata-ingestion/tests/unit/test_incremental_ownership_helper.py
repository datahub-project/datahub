import json

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_ownership_helper import (
    auto_incremental_ownership,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SystemMetadataClass,
)

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
)
OWNERS = [
    OwnerClass(owner="urn:li:corpuser:user1", type=OwnershipTypeClass.DATAOWNER),
    OwnerClass(owner="urn:li:corpuser:user2", type=OwnershipTypeClass.DEVELOPER),
]
SYS_META = SystemMetadataClass(lastObserved=1700000000000, runId="test-run-id")


def _ownership_wu(
    urn: str = DATASET_URN, entity_type: str = "dataset"
) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=urn, entityType=entity_type, aspect=OwnershipClass(owners=OWNERS)
    ).as_workunit()


def test_disabled_passes_through_as_upsert():
    """When incremental_ownership=False, ownership emits as standard UPSERT."""
    result = list(auto_incremental_ownership(False, [_ownership_wu()]))

    assert len(result) == 1
    assert isinstance(result[0].metadata, MetadataChangeProposalWrapper)
    assert result[0].metadata.changeType == "UPSERT"


def test_enabled_converts_dataset_ownership_to_patch():
    """When incremental_ownership=True, dataset ownership becomes a JSON Patch MCP
    with the correct owner URNs in the payload."""
    result = list(auto_incremental_ownership(True, [_ownership_wu()]))

    assert len(result) == 1
    mcp = result[0].metadata
    assert isinstance(mcp, MetadataChangeProposalClass)
    assert mcp.changeType == "PATCH"
    assert mcp.aspectName == "ownership"
    assert mcp.aspect is not None

    payload = json.loads(mcp.aspect.value)
    paths = [op["path"] for op in payload["patch"]]
    assert any("user1" in p for p in paths)
    assert any("user2" in p for p in paths)


def test_enabled_does_not_convert_container_ownership():
    """Container ownership passes through as UPSERT even when incremental is on —
    only dataset entities should be converted to patches."""
    result = list(
        auto_incremental_ownership(
            True,
            [_ownership_wu(urn="urn:li:container:abc123", entity_type="container")],
        )
    )

    assert len(result) == 1
    assert isinstance(result[0].metadata, MetadataChangeProposalWrapper)
    assert result[0].metadata.changeType == "UPSERT"


def test_enabled_with_empty_owners_drops_workunit():
    """An OwnershipClass with no owners produces zero patch ops, so the
    workunit is dropped (no-op for incremental semantics). See module docstring."""
    empty_wu = MetadataChangeProposalWrapper(
        entityUrn=DATASET_URN, aspect=OwnershipClass(owners=[])
    ).as_workunit()

    result = list(auto_incremental_ownership(True, [empty_wu]))

    assert result == []


def test_enabled_preserves_system_metadata():
    """systemMetadata on the input MCPW flows through to the PATCH MCP."""
    wu = MetadataChangeProposalWrapper(
        entityUrn=DATASET_URN,
        aspect=OwnershipClass(owners=OWNERS),
        systemMetadata=SYS_META,
    ).as_workunit()

    result = list(auto_incremental_ownership(True, [wu]))

    assert len(result) == 1
    mcp = result[0].metadata
    assert isinstance(mcp, MetadataChangeProposalClass)
    assert mcp.systemMetadata is not None
    assert mcp.systemMetadata.runId == "test-run-id"
    assert mcp.systemMetadata.lastObserved == 1700000000000


def test_enabled_passes_through_non_ownership_aspects():
    """Non-ownership MCPWs (e.g. Status) take the else branch and pass through."""
    status_wu = MetadataChangeProposalWrapper(
        entityUrn=DATASET_URN, aspect=StatusClass(removed=False)
    ).as_workunit()

    result = list(auto_incremental_ownership(True, [status_wu]))

    assert len(result) == 1
    assert isinstance(result[0].metadata, MetadataChangeProposalWrapper)
    assert isinstance(result[0].metadata.aspect, StatusClass)
    assert result[0].metadata.changeType == "UPSERT"


def test_enabled_converts_dataset_ownership_in_mce_to_patch():
    """Dataset MCEs with an Ownership aspect have it stripped from the snapshot
    and emitted as a separate PATCH MCP. The remaining snapshot (with other
    aspects) is still yielded."""
    mce = MetadataChangeEventClass(
        proposedSnapshot=DatasetSnapshotClass(
            urn=DATASET_URN,
            aspects=[StatusClass(removed=False), OwnershipClass(owners=OWNERS)],
        ),
        systemMetadata=SYS_META,
    )
    wu = MetadataWorkUnit(id="mce-test", mce=mce)

    result = list(auto_incremental_ownership(True, [wu]))

    assert len(result) == 2

    # The MCE remains, but Ownership has been stripped.
    mce_out = result[0].metadata
    assert isinstance(mce_out, MetadataChangeEventClass)
    aspect_types = {type(a).__name__ for a in mce_out.proposedSnapshot.aspects}
    assert aspect_types == {"StatusClass"}

    # The Ownership patch was emitted separately, with the original systemMetadata.
    patch_mcp = result[1].metadata
    assert isinstance(patch_mcp, MetadataChangeProposalClass)
    assert patch_mcp.changeType == "PATCH"
    assert patch_mcp.aspectName == "ownership"
    assert patch_mcp.systemMetadata is not None
    assert patch_mcp.systemMetadata.runId == "test-run-id"
