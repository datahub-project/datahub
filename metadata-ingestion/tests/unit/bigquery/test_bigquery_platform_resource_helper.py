"""BigQueryPlatformResourceHelper: reconciling BigQuery labels with the
PlatformResource entities that back DataHub tags."""

from unittest.mock import create_autospec, patch

import pytest
from pydantic import BaseModel

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper import (
    BigQueryLabel,
    BigQueryLabelInfo,
    BigQueryPlatformResourceHelper,
)
from datahub.metadata.urns import TagUrn

LABEL = BigQueryLabel(key="team", value="data")
TAG = TagUrn("team:data")
OTHER_TAG = TagUrn("other")
MODULE = "datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper"


def label_key() -> PlatformResourceKey:
    return PlatformResourceKey(
        platform="bigquery",
        resource_type="BigQueryLabelInfo",
        platform_instance=None,
        primary_key=LABEL.primary_key(),
    )


def existing_resource(
    tag: TagUrn = TAG, managed_by_datahub: bool = False
) -> PlatformResource:
    return PlatformResource.create(
        key=label_key(),
        value=BigQueryLabelInfo(
            datahub_urn=tag.urn(),
            managed_by_datahub=managed_by_datahub,
            key=LABEL.key,
            value=LABEL.value or "",
        ),
    )


def label_info(resource: PlatformResource) -> BigQueryLabelInfo:
    assert resource.resource_info is not None
    assert resource.resource_info.value is not None
    return resource.resource_info.value.as_pydantic_object(BigQueryLabelInfo)


def helper() -> BigQueryPlatformResourceHelper:
    return BigQueryPlatformResourceHelper(
        "my-project", graph=create_autospec(DataHubGraph, instance=True)
    )


def test_without_graph_a_new_resource_is_created_and_cached() -> None:
    no_graph = BigQueryPlatformResourceHelper("my-project", graph=None)

    first = no_graph.generate_label_platform_resource(LABEL, TAG)
    second = no_graph.generate_label_platform_resource(LABEL, TAG)

    assert label_info(first).datahub_urn == TAG.urn()
    assert label_info(first).managed_by_datahub is True
    assert second.id == first.id
    assert no_graph.get_platform_resource(label_key()) is None


def test_existing_resource_with_same_mapping_is_reused() -> None:
    with patch(
        f"{MODULE}.PlatformResource.from_datahub", return_value=existing_resource()
    ) as from_datahub:
        resource = helper().generate_label_platform_resource(
            LABEL, TAG, managed_by_datahub=False
        )
        again = helper().generate_label_platform_resource(
            LABEL, TAG, managed_by_datahub=False
        )

    assert label_info(resource).datahub_urn == TAG.urn()
    assert again is resource
    # Second lookup is served from the cache.
    from_datahub.assert_called_once()


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="platform_resource_cache is a class attribute, so it is shared by every "
    "helper in the process: a later ingestion run (new helper, new graph) is "
    "served the previous run's PlatformResource instead of asking DataHub.",
)
def test_cache_is_per_helper_not_process_wide() -> None:
    with patch(f"{MODULE}.PlatformResource.from_datahub", return_value=None):
        helper().generate_label_platform_resource(LABEL, TAG)

    with patch(
        f"{MODULE}.PlatformResource.from_datahub",
        return_value=existing_resource(tag=OTHER_TAG),
    ):
        found = helper().get_platform_resource(label_key())

    assert found is not None
    assert label_info(found).datahub_urn == OTHER_TAG.urn()


def test_datahub_managed_resource_wins_over_a_conflicting_label() -> None:
    """A mapping DataHub itself created is authoritative: a BigQuery label that
    now points at a different tag does not overwrite it."""
    managed = existing_resource(tag=OTHER_TAG, managed_by_datahub=True)
    with patch(f"{MODULE}.PlatformResource.from_datahub", return_value=managed):
        resource = helper().generate_label_platform_resource(
            LABEL, TAG, managed_by_datahub=False
        )

    assert label_info(resource).datahub_urn == OTHER_TAG.urn()


def test_conflicting_unmanaged_resource_is_rejected() -> None:
    conflicting = existing_resource(tag=OTHER_TAG, managed_by_datahub=False)
    with (
        patch(f"{MODULE}.PlatformResource.from_datahub", return_value=conflicting),
        pytest.raises(ValueError),
    ):
        helper().generate_label_platform_resource(LABEL, TAG, managed_by_datahub=False)


def test_unreadable_existing_value_is_replaced_by_a_new_resource() -> None:
    """A resource written by an older connector with a different value schema
    must not abort tag extraction; it is replaced."""

    class LegacyShape(BaseModel):
        something_else: str

    stale = PlatformResource.create(
        key=label_key(), value=LegacyShape(something_else="x")
    )
    with patch(f"{MODULE}.PlatformResource.from_datahub", return_value=stale):
        resource = helper().generate_label_platform_resource(LABEL, TAG)

    assert label_info(resource).datahub_urn == TAG.urn()


def test_label_without_value_uses_bare_key_as_primary_key() -> None:
    assert BigQueryLabel(key="flag", value=None).primary_key() == "flag"
    assert LABEL.primary_key() == "team/data"
