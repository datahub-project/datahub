from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.dremio.dremio_aspects import DremioAspects
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioContainer,
    DremioDataset,
    DremioDatasetType,
)
from datahub.metadata.schema_classes import DomainsClass


def _make_aspects(domain=None):
    return DremioAspects(
        platform="dremio",
        ui_url="http://dremio.example.com",
        env="PROD",
        ingest_owner=False,
        domain=domain,
    )


def _make_container(name="src1", path=None):
    api_ops = MagicMock()
    api_ops.get_description_for_resource.return_value = None
    return DremioContainer(
        container_name=name,
        location_id="loc-1",
        path=path or [],
        api_operations=api_ops,
    )


def _make_dataset():
    api_ops = MagicMock()
    api_ops.get_description_for_resource.return_value = None
    api_ops.get_tags_for_resource.return_value = []
    # Bypass __init__ — it expects a fully shaped dataset_details dict
    # from the live API. Set only the attributes touched by the aspect
    # builders under test.
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
        aspect = _make_aspects(
            domain="urn:li:domain:marketing"
        ).create_domain_aspect()
        assert isinstance(aspect, DomainsClass)
        assert aspect.domains == ["urn:li:domain:marketing"]

    def test_hashes_bare_name_to_stable_urn(self):
        # Stability matters: consecutive ingest runs must address the
        # same domain URN, otherwise recipes leak orphan domains.
        aspect_a = _make_aspects(domain="marketing").create_domain_aspect()
        aspect_b = _make_aspects(domain="marketing").create_domain_aspect()
        assert aspect_a is not None and aspect_b is not None
        assert aspect_a.domains == aspect_b.domains
        assert aspect_a.domains[0].startswith("urn:li:domain:")


def _collect_domain_aspects(workunits):
    return [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata.aspect, DomainsClass)
    ]


@pytest.mark.parametrize(
    "domain,expected_count",
    [(None, 0), ("urn:li:domain:eng", 1)],
)
def test_populate_container_mcp_emits_domain_when_configured(
    domain, expected_count
):
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
def test_populate_dataset_mcp_emits_domain_when_configured(
    domain, expected_count
):
    aspects = _make_aspects(domain=domain)
    dataset = _make_dataset()
    workunits = list(
        aspects.populate_dataset_mcp("urn:li:dataset:(urn:li:dataPlatform:dremio,db.t,PROD)", dataset)
    )
    domain_aspects = _collect_domain_aspects(workunits)
    assert len(domain_aspects) == expected_count
    if expected_count:
        assert domain_aspects[0].domains == ["urn:li:domain:eng"]


def test_add_mapping_removed():
    # Regression guard: `add_mapping` mutated class-level state and was
    # replaced by the per-instance `source_type_mappings` config field
    # (see test_dremio_source_type_overrides.py). If it reappears, route
    # users to the config field instead.
    from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
        DremioToDataHubSourceTypeMapping,
    )

    assert not hasattr(DremioToDataHubSourceTypeMapping, "add_mapping")
