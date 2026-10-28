"""Resolution of Unity Catalog tag platform-resource ids against DataHub.

The platform resource repository is the boundary to the DataHub graph and is
mocked; the id derivation and state transitions under test are real."""

from unittest.mock import MagicMock

import pytest

from datahub.api.entities.external.external_entities import LinkedResourceSet
from datahub.api.entities.external.unity_catalog_external_entites import (
    UnityCatalogTag,
)
from datahub.ingestion.source.unity.tag_entities import (
    UnityCatalogTagPlatformResource,
    UnityCatalogTagPlatformResourceId,
    UnityCatalogTagSyncContext,
)

CONTEXT = UnityCatalogTagSyncContext(platform_instance="ws1")


def _repository(existing: object = None) -> MagicMock:
    repository = MagicMock()
    repository.platform_instance = "ws1"
    repository.search_entity_by_urn.return_value = existing
    return repository


def test_tag_id_reuses_the_resource_already_registered_for_the_datahub_tag() -> None:
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


def test_datahub_tag_urn_resolves_to_a_new_id_marked_as_unknown_in_unity() -> None:
    repository = _repository()
    repository.get.return_value = None

    resolved = UnityCatalogTagPlatformResourceId.from_datahub_urn(
        "urn:li:tag:pii:email", CONTEXT, repository, graph=MagicMock()
    )

    assert (resolved.tag_key, resolved.tag_value) == ("pii", "email")
    assert resolved.platform_instance == "ws1"
    assert not resolved.exists_in_unity_catalog


def test_datahub_tag_urn_already_persisted_as_a_resource_is_marked_existing() -> None:
    repository = _repository()
    repository.get.return_value = MagicMock(name="persisted platform resource")

    resolved = UnityCatalogTagPlatformResourceId.from_datahub_urn(
        "urn:li:tag:pii", CONTEXT, repository, graph=MagicMock()
    )

    assert resolved.tag_key == "pii" and resolved.tag_value is None
    assert resolved.exists_in_unity_catalog


def test_non_tag_urn_cannot_become_a_unity_tag() -> None:
    with pytest.raises(ValueError):
        UnityCatalogTagPlatformResourceId.from_datahub_urn(
            "urn:li:glossaryTerm:pii", CONTEXT, _repository(), graph=MagicMock()
        )


def test_default_resource_starts_unlinked_and_not_persisted() -> None:
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
