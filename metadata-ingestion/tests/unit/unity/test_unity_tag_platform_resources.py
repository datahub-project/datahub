"""Resolution of Unity Catalog tag platform-resource ids against DataHub.

The platform resource repository and the graph are the boundary to DataHub and
are mocked (with spec); the id derivation and state transitions under test are
real."""

from typing import Optional
from unittest.mock import MagicMock

import pytest

from datahub.api.entities.external.external_entities import LinkedResourceSet
from datahub.api.entities.external.unity_catalog_external_entites import (
    UnityCatalogTag,
)
from datahub.api.entities.platformresource.platform_resource import PlatformResource
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.platform_resource_repository import (
    UnityCatalogPlatformResourceRepository,
)
from datahub.ingestion.source.unity.tag_entities import (
    UnityCatalogTagPlatformResource,
    UnityCatalogTagPlatformResourceId,
    UnityCatalogTagSyncContext,
)

CONTEXT = UnityCatalogTagSyncContext(platform_instance="ws1")


def _repository(
    existing: Optional[UnityCatalogTagPlatformResourceId] = None,
) -> MagicMock:
    repository = MagicMock(spec=UnityCatalogPlatformResourceRepository)
    repository.platform_instance = "ws1"
    repository.search_entity_by_urn.return_value = existing
    return repository


@pytest.fixture
def graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


def test_tag_id_reuses_the_resource_already_registered_for_the_datahub_tag() -> None:
    """A tag already known to DataHub keeps its persisted id (and its
    exists_in_unity_catalog / persisted state) instead of getting a fresh one."""
    existing = UnityCatalogTagPlatformResourceId(
        tag_key="pii",
        tag_value="email",
        platform_instance="ws1",
        exists_in_unity_catalog=True,
        persisted=True,
    )

    resolved = UnityCatalogTagPlatformResourceId.get_or_create_from_tag(
        UnityCatalogTag(key="pii", value="email"), _repository(existing)
    )

    assert resolved is existing


def test_datahub_tag_urn_resolves_to_a_new_id_marked_as_unknown_in_unity(
    graph: MagicMock,
) -> None:
    repository = _repository()
    repository.get.return_value = None

    resolved = UnityCatalogTagPlatformResourceId.from_datahub_urn(
        "urn:li:tag:pii:email", CONTEXT, repository, graph=graph
    )

    assert (resolved.tag_key, resolved.tag_value) == ("pii", "email")
    assert resolved.platform_instance == "ws1"
    assert not resolved.exists_in_unity_catalog


def test_datahub_tag_urn_already_persisted_as_a_resource_is_marked_existing(
    graph: MagicMock,
) -> None:
    """A platform resource stored under the derived key means the tag was seen
    in Unity before, so the new id must say it exists there."""
    repository = _repository()
    repository.get.return_value = MagicMock(spec=PlatformResource)

    resolved = UnityCatalogTagPlatformResourceId.from_datahub_urn(
        "urn:li:tag:pii", CONTEXT, repository, graph=graph
    )

    assert resolved.tag_key == "pii" and resolved.tag_value is None
    assert resolved.exists_in_unity_catalog


def test_non_tag_urn_cannot_become_a_unity_tag(graph: MagicMock) -> None:
    with pytest.raises(ValueError):
        UnityCatalogTagPlatformResourceId.from_datahub_urn(
            "urn:li:glossaryTerm:pii", CONTEXT, _repository(), graph=graph
        )


def test_default_resource_starts_unlinked_and_not_persisted() -> None:
    """create_default resets the seed id's state: a brand-new resource has not
    been persisted and is not yet known to exist in Unity, whatever the seed
    said."""
    seed = UnityCatalogTagPlatformResourceId(
        tag_key="tier",
        tag_value="gold",
        platform_instance="ws1",
        exists_in_unity_catalog=True,
        persisted=True,
    )

    resource = UnityCatalogTagPlatformResource.create_default(
        seed, managed_by_datahub=True
    )

    assert resource.managed_by_datahub
    assert resource.datahub_linked_resources() == LinkedResourceSet(urns=[])
    assert (resource.id.tag_key, resource.id.tag_value) == ("tier", "gold")
    assert not resource.id.exists_in_unity_catalog
    assert not resource.id.persisted
