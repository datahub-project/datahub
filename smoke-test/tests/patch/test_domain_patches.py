import logging
import time
import uuid
from typing import Generator, cast

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DomainAssociationClass,
    DomainsClass,
    MetadataAttributionClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

logger = logging.getLogger(__name__)


def _make_domain_urn() -> str:
    return f"urn:li:domain:{uuid.uuid4()}"


def _make_domain_association(
    domain_urn: str,
    source: str = "",
    propagated: bool = False,
) -> DomainAssociationClass:
    now_ms = int(time.time() * 1000)
    source_detail = {"propagated": "true"} if propagated else None
    attribution = MetadataAttributionClass(
        time=now_ms,
        actor="urn:li:corpuser:datahub",
        source=source if source else "urn:li:dataHubAction:default",
        sourceDetail=source_detail,
    )
    if not source and not propagated:
        return DomainAssociationClass(domain=domain_urn)
    return DomainAssociationClass(
        domain=domain_urn, context="context", attribution=attribution
    )


def _get_domains(graph_client: DataHubGraph, entity_urn: str) -> DomainsClass:
    aspect = graph_client.get_aspect(
        entity_urn=entity_urn,
        aspect_type=DomainsClass,
    )
    assert aspect is not None
    return aspect


@pytest.fixture(params=["graph_client", "openapi_graph_client"])
def patch_dataset(request) -> Generator[tuple[DataHubGraph, str], None, None]:
    """Yields (graph_client, dataset_urn). The dataset is hard-deleted after the test."""
    graph_client = cast(DataHubGraph, request.getfixturevalue(request.param))
    dataset_urn = make_dataset_urn(
        platform="hive", name=f"SampleHiveDataset-{uuid.uuid4()}", env="PROD"
    )
    yield graph_client, dataset_urn
    graph_client.hard_delete_entity(dataset_urn)


def test_add_remove_via_domains_updates_domain_associations(patch_dataset):
    """Adding/removing entries in `domains` (legacy field) should sync to domainAssociations."""
    graph_client, dataset_urn = patch_dataset

    domain_a = _make_domain_urn()
    domain_b = _make_domain_urn()

    # Seed with one domain.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_a]),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_a]
    assert aspect.domainAssociations is not None
    assoc_domains = {a.domain for a in aspect.domainAssociations}
    assert assoc_domains == {domain_a}

    # Add a second domain via full-replace of the domains field.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_a, domain_b]),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_a, domain_b]
    assert aspect.domainAssociations is not None
    assoc_domains = {a.domain for a in aspect.domainAssociations}
    assert assoc_domains == {domain_a, domain_b}

    # Remove domain_a by writing only domain_b.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_b]),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_b]
    assert aspect.domainAssociations is not None
    assoc_domains = {a.domain for a in aspect.domainAssociations}
    assert assoc_domains == {domain_b}


def test_add_remove_via_domain_associations_patch_updates_domains(patch_dataset):
    """Adding/removing entries via domainAssociations patch
    should sync to the domains field and preserve attribution."""
    graph_client, dataset_urn = patch_dataset

    source = "urn:li:dataHubAction:testSource"
    domain_a = _make_domain_urn()
    domain_b = _make_domain_urn()

    # Add domain_a via patch with a source.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_domain(_make_domain_association(domain_a, source=source))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_a]
    assert aspect.domainAssociations is not None and len(aspect.domainAssociations) == 1
    assoc_a = aspect.domainAssociations[0]
    assert assoc_a.domain == domain_a
    assert assoc_a.attribution is not None
    assert assoc_a.attribution.source == source
    assert assoc_a.context == "context"

    # Add domain_b via patch
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .add_domain(_make_domain_association(domain_b))
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = _get_domains(graph_client, dataset_urn)
    expected_domains = [domain_a, domain_b]
    assert aspect.domains == expected_domains
    assert {a.domain for a in (aspect.domainAssociations or [])} == set(
        expected_domains
    )

    # Remove domain_a via patch.
    for mcp in (
        DatasetPatchBuilder(dataset_urn)
        .remove_domain(domain_a, attribution_source=source)
        .build()
    ):
        graph_client.emit_mcp(mcp)

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_b]
    assert aspect.domainAssociations is not None
    assert {a.domain for a in (aspect.domainAssociations or [])} == {domain_b}


def test_add_remove_via_domain_associations_upsert_updates_domains(patch_dataset):
    """Adding/removing entries via full-replace domainAssociations
    should sync to the domains field and preserve attribution."""
    graph_client, dataset_urn = patch_dataset

    source = "urn:li:dataHubAction:testSource"
    domain_a = _make_domain_urn()
    domain_b = _make_domain_urn()

    assoc_a = _make_domain_association(domain_a, source=source)

    # Write domain_a via full-replace with domainAssociations.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[], domainAssociations=[assoc_a]),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_a]
    assert aspect.domainAssociations is not None and len(aspect.domainAssociations) == 1
    read_assoc = aspect.domainAssociations[0]
    assert read_assoc.domain == domain_a
    assert read_assoc.attribution is not None
    assert read_assoc.attribution.source == source
    assert read_assoc.context == "context"

    # Add domain_b via full-replace, keeping domain_a.
    assoc_b = _make_domain_association(domain_b)
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[], domainAssociations=[assoc_a, assoc_b]),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)
    expected_domains = [domain_a, domain_b]
    assert aspect.domains == expected_domains
    assert {a.domain for a in (aspect.domainAssociations or [])} == set(
        expected_domains
    )

    # Remove domain_a by writing only domain_b.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[], domainAssociations=[assoc_b]),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_b]
    assert aspect.domainAssociations is not None
    assert {a.domain for a in (aspect.domainAssociations or [])} == {domain_b}


def test_updating_both_domains_and_domain_associations_errors(patch_dataset):
    """Writing a Domains aspect where both `domains` and `domainAssociations`
    actually changed from previous and are inconsistent should result in an error."""
    graph_client, dataset_urn = patch_dataset

    domain_a = _make_domain_urn()
    domain_b = _make_domain_urn()

    # Seed with domain_a.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_a]),
        )
    )

    # Both fields differ from previous and are inconsistent with each other:
    # domains says [domain_b] but domainAssociations says [domain_c].
    inconsistent_aspect = DomainsClass(
        domains=[domain_b],
        domainAssociations=[],
    )

    with pytest.raises(OperationalError, match="Cannot update both"):
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=inconsistent_aspect
            )
        )


def test_read_modify_write_domains_updates_associations(patch_dataset):
    """Read-modify-write on `domains` should sync domainAssociations,
    even though the read-back aspect has domainAssociations populated."""
    graph_client, dataset_urn = patch_dataset

    domain_a = _make_domain_urn()
    domain_b = _make_domain_urn()

    # Seed with domain_a.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_a]),
        )
    )

    # Read back the full aspect — both fields are now populated.
    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_a]
    assert aspect.domainAssociations is not None
    assert len(aspect.domainAssociations) == 1

    # Simulate read-modify-write: change only domains, leave domainAssociations stale.
    aspect.domains = [domain_a, domain_b]

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=aspect,
        )
    )

    result = _get_domains(graph_client, dataset_urn)
    assert set(result.domains) == {domain_a, domain_b}
    assert result.domainAssociations is not None
    assoc_domains = {a.domain for a in result.domainAssociations}
    assert assoc_domains == {domain_a, domain_b}


def test_read_modify_write_associations_updates_domains(patch_dataset):
    """Read-modify-write on `domainAssociations` should sync domains,
    even though the read-back aspect has domains populated."""
    graph_client, dataset_urn = patch_dataset

    domain_a = _make_domain_urn()
    domain_b = _make_domain_urn()

    # Seed with domain_a.
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(domains=[domain_a]),
        )
    )

    # Read back the full aspect — both fields are now populated.
    aspect = _get_domains(graph_client, dataset_urn)
    assert aspect.domains == [domain_a]
    assert aspect.domainAssociations is not None

    # Simulate read-modify-write: change only domainAssociations, leave domains stale.
    aspect.domainAssociations = [
        DomainAssociationClass(domain=domain_a),
        DomainAssociationClass(domain=domain_b),
    ]

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=aspect,
        )
    )

    result = _get_domains(graph_client, dataset_urn)
    assert set(result.domains) == {domain_a, domain_b}
    assert result.domainAssociations is not None
    assoc_domains = {a.domain for a in result.domainAssociations}
    assert assoc_domains == {domain_a, domain_b}


def test_propagated_domains_ordered_after_manual(patch_dataset):
    """Propagated domain associations should always appear after manual ones
    in both domainAssociations and domains."""
    graph_client, dataset_urn = patch_dataset

    domain_manual = _make_domain_urn()
    domain_propagated = _make_domain_urn()

    # Write both a propagated and manual association via full-replace.
    # Intentionally put the propagated one first to verify reordering.
    assoc_propagated = _make_domain_association(domain_propagated, propagated=True)
    assoc_manual = _make_domain_association(domain_manual)

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DomainsClass(
                domains=[domain_manual, domain_propagated],
                domainAssociations=[assoc_propagated, assoc_manual],
            ),
        )
    )

    aspect = _get_domains(graph_client, dataset_urn)

    # domains field should have manual first, then propagated
    assert aspect.domains == [domain_manual, domain_propagated]

    # domainAssociations should have both, with manual first
    assert aspect.domainAssociations is not None
    assert len(aspect.domainAssociations) == 2
    assert aspect.domainAssociations[0].domain == domain_manual
    assert aspect.domainAssociations[1].domain == domain_propagated
    assert (
        aspect.domainAssociations[1].attribution is not None
        and aspect.domainAssociations[1].attribution.sourceDetail is not None
        and aspect.domainAssociations[1].attribution.sourceDetail.get("propagated")
        == "true"
    )
