"""Tests for domain transformer support across multiple entity types.

Verifies that the new generic domain transformers (SimpleAddDomain, PatternAddDomain)
work on all supported entity types (dataset, container, chart, dashboard, dataJob,
dataFlow), while the legacy *Dataset* variants remain dataset-only.
"""

from typing import List, Optional, Type
from unittest import mock

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.dataset_domain import (
    AddDatasetDomain,
    PatternAddDomain,
    SimpleAddDatasetDomain,
    SimpleAddDomain,
)

# --- Test helpers ---


def _make_domain_mcp(
    entity_urn: str,
    aspect: Optional[models.DomainsClass] = None,
) -> MetadataChangeProposalWrapper:
    if aspect is None:
        aspect = models.DomainsClass(domains=[])
    return MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect)


def _run_domain_transformer(
    transformer_type: Type[AddDatasetDomain],
    config: dict,
    entity_urn: str,
    existing_domains: Optional[models.DomainsClass] = None,
    graph: Optional[mock.MagicMock] = None,
) -> List[RecordEnvelope]:
    """Run a domain transformer against a single entity MCP."""
    if graph is None:
        graph = mock.MagicMock()
        graph.get_domain.return_value = None

    pipeline_context = PipelineContext(run_id="test_domain_multi_entity")
    pipeline_context.graph = graph

    transformer = transformer_type.create(config, pipeline_context)

    mcp = _make_domain_mcp(entity_urn, existing_domains)
    return list(
        transformer.transform(
            [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
        )
    )


def _has_domain_in_output(output: List[RecordEnvelope], domain_urn: str) -> bool:
    for envelope in output:
        if isinstance(envelope.record, MetadataChangeProposalWrapper):
            if isinstance(envelope.record.aspect, models.DomainsClass):
                if domain_urn in envelope.record.aspect.domains:
                    return True
    return False


# --- Legacy *Dataset* transformers remain dataset-only ---


class TestDatasetDomainTransformersDatasetOnly:
    """The *Dataset* variants should only process dataset entities."""

    DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)"
    CONTAINER_URN = "urn:li:container:test_container_123"
    DASHBOARD_URN = "urn:li:dashboard:(powerbi,my_dashboard)"

    def test_simple_dataset_domain_works_on_dataset(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        output = _run_domain_transformer(
            SimpleAddDatasetDomain,
            {"domains": [domain]},
            self.DATASET_URN,
            models.DomainsClass(domains=[]),
        )
        assert _has_domain_in_output(output, domain)

    def test_simple_dataset_domain_ignores_container(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        output = _run_domain_transformer(
            SimpleAddDatasetDomain,
            {"domains": [domain]},
            self.CONTAINER_URN,
            models.DomainsClass(domains=[]),
        )
        assert not _has_domain_in_output(output, domain)

    def test_simple_dataset_domain_ignores_dashboard(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        output = _run_domain_transformer(
            SimpleAddDatasetDomain,
            {"domains": [domain]},
            self.DASHBOARD_URN,
            models.DomainsClass(domains=[]),
        )
        assert not _has_domain_in_output(output, domain)


# --- New generic transformers work on all entity types ---


class TestGenericDomainTransformers:
    """The new generic variants should process all supported entity types."""

    DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:bigquery,example1,PROD)"
    CONTAINER_URN = "urn:li:container:test_container_123"
    DASHBOARD_URN = "urn:li:dashboard:(powerbi,my_dashboard)"
    CHART_URN = "urn:li:chart:(powerbi,my_chart)"
    DATAFLOW_URN = "urn:li:dataFlow:(airflow,my_flow,PROD)"
    DATAJOB_URN = "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_flow,PROD),my_job)"

    ALL_URNS = [
        DATASET_URN,
        CONTAINER_URN,
        DASHBOARD_URN,
        CHART_URN,
        DATAFLOW_URN,
        DATAJOB_URN,
    ]

    def test_simple_add_domain_on_all_types(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        for urn in self.ALL_URNS:
            output = _run_domain_transformer(
                SimpleAddDomain,
                {"domains": [domain]},
                urn,
                models.DomainsClass(domains=[]),
            )
            assert _has_domain_in_output(output, domain), f"Failed for {urn}"

    def test_simple_add_domain_preserves_existing(self) -> None:
        new_domain = builder.make_domain_urn("new.io")
        existing_domain = builder.make_domain_urn("existing.io")

        output = _run_domain_transformer(
            SimpleAddDomain,
            {"domains": [new_domain]},
            self.DASHBOARD_URN,
            models.DomainsClass(domains=[existing_domain]),
        )

        assert _has_domain_in_output(output, new_domain)
        assert _has_domain_in_output(output, existing_domain)

    def test_pattern_add_domain_on_dashboard(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        output = _run_domain_transformer(
            PatternAddDomain,
            {"domain_pattern": {"rules": {".*powerbi.*": [domain]}}},
            self.DASHBOARD_URN,
            models.DomainsClass(domains=[]),
        )
        assert _has_domain_in_output(output, domain)

    def test_pattern_add_domain_on_container(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        output = _run_domain_transformer(
            PatternAddDomain,
            {"domain_pattern": {"rules": {".*container.*": [domain]}}},
            self.CONTAINER_URN,
            models.DomainsClass(domains=[]),
        )
        assert _has_domain_in_output(output, domain)

    def test_pattern_add_domain_no_match(self) -> None:
        domain = builder.make_domain_urn("acryl.io")
        output = _run_domain_transformer(
            PatternAddDomain,
            {"domain_pattern": {"rules": {".*snowflake.*": [domain]}}},
            self.DASHBOARD_URN,
            models.DomainsClass(domains=[]),
        )
        assert not _has_domain_in_output(output, domain)


# --- is_container propagation from non-dataset entities ---


class TestIsContainerPropagationFromDashboard:
    """Test that is_container=true propagates domains from dashboards to parent containers."""

    DASHBOARD_URN = "urn:li:dashboard:(powerbi,my_dashboard)"

    def test_pattern_add_domain_propagates_to_containers(self) -> None:
        domain = builder.make_domain_urn("analytics")

        graph = mock.MagicMock()
        graph.get_domain.return_value = None
        graph.get_aspect.return_value = models.BrowsePathsV2Class(
            path=[
                models.BrowsePathEntryClass(
                    id="workspace", urn="urn:li:container:workspace_123"
                ),
            ]
        )

        pipeline_context = PipelineContext(run_id="test_is_container_dashboard")
        pipeline_context.graph = graph

        transformer = PatternAddDomain.create(
            {
                "domain_pattern": {"rules": {".*powerbi.*": [domain]}},
                "is_container": True,
            },
            pipeline_context,
        )

        mcp = _make_domain_mcp(self.DASHBOARD_URN, models.DomainsClass(domains=[]))
        outputs = list(
            transformer.transform(
                [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
            )
        )

        # Should have: transformed dashboard MCP + container domain MCP + EndOfStream
        container_domain_found = False
        for envelope in outputs:
            if isinstance(envelope.record, MetadataChangeProposalWrapper):
                if (
                    envelope.record.entityUrn == "urn:li:container:workspace_123"
                    and isinstance(envelope.record.aspect, models.DomainsClass)
                ):
                    assert domain in envelope.record.aspect.domains
                    container_domain_found = True

        assert container_domain_found, "Domain should propagate to parent container"


# --- Callback-based (add_domain / add_ownership) tests ---


class TestCallbackBasedTransformers:
    """Test the generic callback-based transformers (AddDomain, AddOwnership)."""

    DASHBOARD_URN = "urn:li:dashboard:(powerbi,my_dashboard)"
    CONTAINER_URN = "urn:li:container:test_container_123"

    def test_add_domain_callback_on_dashboard(self) -> None:
        from datahub.ingestion.transformer.dataset_domain import AddDomain

        domain = builder.make_domain_urn("analytics")

        graph = mock.MagicMock()
        graph.get_domain.return_value = None

        pipeline_context = PipelineContext(run_id="test_add_domain_callback")
        pipeline_context.graph = graph

        transformer = AddDomain.create(
            {
                "get_domains_to_add": lambda urn: models.DomainsClass(domains=[domain]),
            },
            pipeline_context,
        )

        mcp = _make_domain_mcp(self.DASHBOARD_URN, models.DomainsClass(domains=[]))
        outputs = list(
            transformer.transform(
                [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
            )
        )

        assert _has_domain_in_output(outputs, domain)

    def test_add_ownership_callback_on_container(self) -> None:
        from datahub.ingestion.transformer.add_dataset_ownership import AddOwnership
        from datahub.metadata.schema_classes import OwnerClass, OwnershipClass

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
