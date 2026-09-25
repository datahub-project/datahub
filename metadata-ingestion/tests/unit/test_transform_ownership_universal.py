"""Tests for universal ownership transformers.

Verifies that the universal ownership transformers (SimpleAddOwnership,
PatternAddOwnership, AddOwnership) work on all supported entity types,
while the legacy *Dataset* variants preserve their original entity type set.
"""

import json
from unittest import mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
)


class TestCallbackBasedOwnershipTransformers:
    """Test the generic callback-based ownership transformer (AddOwnership)."""

    CONTAINER_URN = "urn:li:container:test_container_123"

    def test_add_ownership_callback_on_container(self) -> None:
        from datahub.ingestion.transformer.add_ownership import AddOwnership

        owner_urn = "urn:li:corpuser:alice"

        graph = mock.MagicMock()
        pipeline_context = PipelineContext(run_id="test_add_ownership_callback")
        pipeline_context.graph = graph

        transformer = AddOwnership.create(
            {
                "get_owners_to_add": lambda urn: [
                    OwnerClass(owner=owner_urn, type="DATAOWNER")
                ],
            },
            pipeline_context,
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.CONTAINER_URN,
            aspect=OwnershipClass(owners=[]),
        )
        outputs = list(
            transformer.transform(
                [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
            )
        )

        owner_found = False
        for envelope in outputs:
            if isinstance(envelope.record, MetadataChangeProposalWrapper):
                if isinstance(envelope.record.aspect, OwnershipClass):
                    if any(o.owner == owner_urn for o in envelope.record.aspect.owners):
                        owner_found = True
        assert owner_found, "Owner should be added to container"

    def test_container_rollup_dedupes_owners(self) -> None:
        """Container ownership rollup must emit one op per unique owner."""
        from datahub.ingestion.transformer.add_ownership import PatternAddOwnership

        owner_urn = "urn:li:corpGroup:shared"

        graph = mock.MagicMock()
        graph.get_aspect.return_value = BrowsePathsV2Class(
            path=[BrowsePathEntryClass(id=self.CONTAINER_URN, urn=self.CONTAINER_URN)]
        )
        pipeline_context = PipelineContext(run_id="test_container_rollup_dedupe")
        pipeline_context.graph = graph

        transformer = PatternAddOwnership.create(
            {
                "owner_pattern": {"rules": {".*": [owner_urn]}},
                "ownership_type": "TECHNICAL_OWNER",
                "is_container": True,
            },
            pipeline_context,
        )

        records = [
            RecordEnvelope(r, metadata={})
            for r in [
                *[
                    MetadataChangeProposalWrapper(
                        entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:looker,explore.{i},PROD)",
                        aspect=OwnershipClass(owners=[]),
                    )
                    for i in range(50)
                ],
                EndOfStream(),
            ]
        ]

        outputs = list(transformer.transform(records))

        container_patch = next(
            env.record
            for env in outputs
            if isinstance(env.record, MetadataChangeProposalClass)
            and env.record.entityUrn == self.CONTAINER_URN
            and env.record.aspectName == "ownership"
        )
        assert container_patch.aspect is not None
        envelope = json.loads(container_patch.aspect.value)
        assert len(envelope["patch"]) == 1
        assert envelope["patch"][0]["value"]["owner"] == owner_urn


class TestOwnershipEntityTypesConfigRestriction:
    """Verify that the entity_types config field restricts ownership processing."""

    DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)"
    CONTAINER_URN = "urn:li:container:test_container_123"

    def test_simple_add_ownership_restricted_to_dataset_only(self) -> None:
        from datahub.ingestion.transformer.add_ownership import SimpleAddOwnership

        owner_urn = "urn:li:corpuser:alice"
        graph = mock.MagicMock()
        pipeline_context = PipelineContext(run_id="test_ownership_entity_types")
        pipeline_context.graph = graph

        transformer = SimpleAddOwnership.create(
            {
                "owner_urns": [owner_urn],
                "entity_types": ["dataset"],
            },
            pipeline_context,
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.DATASET_URN,
            aspect=OwnershipClass(owners=[]),
        )
        outputs = list(
            transformer.transform(
                [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
            )
        )
        owner_found = any(
            isinstance(e.record, MetadataChangeProposalWrapper)
            and isinstance(e.record.aspect, OwnershipClass)
            and any(o.owner == owner_urn for o in e.record.aspect.owners)
            for e in outputs
        )
        assert owner_found

    def test_simple_add_ownership_restricted_skips_excluded_type(self) -> None:
        from datahub.ingestion.transformer.add_ownership import SimpleAddOwnership

        owner_urn = "urn:li:corpuser:alice"
        graph = mock.MagicMock()
        pipeline_context = PipelineContext(run_id="test_ownership_entity_types_skip")
        pipeline_context.graph = graph

        transformer = SimpleAddOwnership.create(
            {
                "owner_urns": [owner_urn],
                "entity_types": ["dataset"],
            },
            pipeline_context,
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=self.CONTAINER_URN,
            aspect=OwnershipClass(owners=[]),
        )
        outputs = list(
            transformer.transform(
                [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
            )
        )
        owner_found = any(
            isinstance(e.record, MetadataChangeProposalWrapper)
            and isinstance(e.record.aspect, OwnershipClass)
            and any(o.owner == owner_urn for o in e.record.aspect.owners)
            for e in outputs
        )
        assert not owner_found

    def test_add_dataset_ownership_alias_only_processes_legacy_types(self) -> None:
        """Regression: AddDatasetOwnership must not silently add container."""
        from datahub.ingestion.transformer.add_dataset_ownership import (
            AddDatasetOwnership,
        )

        owner_urn = "urn:li:corpuser:alice"

        graph = mock.MagicMock()
        pipeline_context = PipelineContext(run_id="test_ownership_alias_entity_types")
        pipeline_context.graph = graph

        transformer = AddDatasetOwnership.create(
            {
                "get_owners_to_add": lambda urn: [
                    OwnerClass(owner=owner_urn, type="DATAOWNER")
                ],
            },
            pipeline_context,
        )

        assert "container" not in transformer.entity_types()
        assert "dataset" in transformer.entity_types()
