from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dremio.dremio_aspects import DremioAspects
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioDataset,
    DremioDatasetType,
    DremioSpace,
)
from datahub.metadata.schema_classes import DomainsClass
from datahub.utilities.registries.domain_registry import DomainRegistry


def _make_aspects(domain=None, domain_registry=None):
    return DremioAspects(
        platform="dremio",
        ui_url="http://dremio.example.com",
        env="PROD",
        ingest_owner=False,
        domain=domain,
        domain_registry=domain_registry,
    )


def _fake_registry(mapping):
    # Skip __init__ so we don't pay a graph round-trip in tests.
    registry = DomainRegistry.__new__(DomainRegistry)
    registry.domain_registry = dict(mapping)
    return registry


def _make_container(name="src1", path=None):
    # Concrete subclass: DremioContainer.subclass has no default.
    api_ops = MagicMock()
    api_ops.get_description_for_resource.return_value = None
    return DremioSpace(
        container_name=name,
        location_id="loc-1",
        path=path or [],
        api_operations=api_ops,
    )


def _make_dataset():
    api_ops = MagicMock()
    api_ops.get_description_for_resource.return_value = None
    api_ops.get_tags_for_resource.return_value = []
    # Bypass __init__: it expects a full live-API payload. Only stub the
    # attributes touched by the aspect builders under test.
    dataset = DremioDataset.__new__(DremioDataset)
    dataset.resource_id = "ds-1"
    dataset.resource_name = "table1"
    dataset.path = ["src1", "schema1"]
    dataset.location_id = "loc-1"
    dataset.columns = []
    dataset.sql_definition = None
    dataset.dataset_type = DremioDatasetType.TABLE
    dataset.default_schema = None
    dataset.owner = None
    dataset.owner_type = None
    dataset.created = "2024-01-01 00:00:00.000"
    dataset.parents = None
    dataset.description = None
    dataset.format_type = None
    dataset.glossary_terms = []
    return dataset


class TestCreateDomainAspect:
    def test_returns_none_when_domain_unset(self):
        assert _make_aspects(domain=None).create_domain_aspect() is None

    def test_passes_through_full_urn(self):
        aspect = _make_aspects(domain="urn:li:domain:marketing").create_domain_aspect()
        assert isinstance(aspect, DomainsClass)
        assert aspect.domains == ["urn:li:domain:marketing"]

    def test_bare_name_resolved_via_domain_registry(self):
        registry = _fake_registry({"marketing": "urn:li:domain:abc-123"})
        aspect = _make_aspects(
            domain="marketing", domain_registry=registry
        ).create_domain_aspect()
        assert isinstance(aspect, DomainsClass)
        assert aspect.domains == ["urn:li:domain:abc-123"]

    def test_bare_name_without_registry_in_production_path_is_impossible(self):
        # The bare-name + no-graph case is gated at DomainRegistry.__init__
        # in DremioSource — verify the registry layer raises clearly so
        # the assertion in create_domain_aspect can never fire in prod.
        with pytest.raises(ValueError, match="server-side resolution"):
            DomainRegistry(cached_domains=["marketing"], graph=None)


def _collect_domain_aspects(workunits):
    # MCE workunits expose no .aspect — narrow to MCPW before access.
    aspects = []
    for wu in workunits:
        if not isinstance(wu.metadata, MetadataChangeProposalWrapper):
            continue
        if isinstance(wu.metadata.aspect, DomainsClass):
            aspects.append(wu.metadata.aspect)
    return aspects


@pytest.mark.parametrize(
    "domain,expected_count",
    [(None, 0), ("urn:li:domain:eng", 1)],
)
def test_populate_container_mcp_emits_domain_when_configured(domain, expected_count):
    aspects = _make_aspects(domain=domain)
    container = _make_container()
    workunits = list(aspects.populate_container_mcp("urn:li:container:c1", container))
    domain_aspects = _collect_domain_aspects(workunits)
    assert len(domain_aspects) == expected_count
    if expected_count:
        assert domain_aspects[0].domains == ["urn:li:domain:eng"]


@pytest.mark.parametrize(
    "domain,expected_count",
    [(None, 0), ("urn:li:domain:eng", 1)],
)
def test_populate_dataset_mcp_emits_domain_when_configured(domain, expected_count):
    aspects = _make_aspects(domain=domain)
    dataset = _make_dataset()
    workunits = list(
        aspects.populate_dataset_mcp(
            "urn:li:dataset:(urn:li:dataPlatform:dremio,db.t,PROD)", dataset
        )
    )
    domain_aspects = _collect_domain_aspects(workunits)
    assert len(domain_aspects) == expected_count
    if expected_count:
        assert domain_aspects[0].domains == ["urn:li:domain:eng"]


def test_add_mapping_removed():
    # Regression guard: add_mapping mutated class state; replaced by the
    # per-instance source_type_mappings config field. If reintroduced,
    # route callers to the config instead.
    assert not hasattr(DremioToDataHubSourceTypeMapping, "add_mapping")
