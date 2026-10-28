"""BigQueryPlatformResourceHelper: reconciling BigQuery labels with the
PlatformResource entities that back DataHub tags."""

from typing import Iterator, Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from datahub.api.entities.platformresource.platform_resource import (
    PlatformResource,
    PlatformResourceKey,
)
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


@pytest.fixture(autouse=True)
def clear_shared_cache() -> Iterator[None]:
    # The cache is a class attribute shared by every helper instance.
    BigQueryPlatformResourceHelper.platform_resource_cache.clear()
    yield
    BigQueryPlatformResourceHelper.platform_resource_cache.clear()


def existing_resource(
    tag: TagUrn = TAG, managed_by_datahub: bool = False
) -> PlatformResource:
    return PlatformResource.create(
        key=PlatformResourceKey(
            platform="bigquery",
            resource_type="BigQueryLabelInfo",
            platform_instance=None,
            primary_key=LABEL.primary_key(),
        ),
        value=BigQueryLabelInfo(
            datahub_urn=tag.urn(),
            managed_by_datahub=managed_by_datahub,
            key=LABEL.key,
            value=LABEL.value or "",
        ),
    )


def label_info(resource: PlatformResource) -> BigQueryLabelInfo:
    assert resource.resource_info is not None
    return resource.resource_info.value.as_pydantic_object(BigQueryLabelInfo)  # type: ignore[union-attr]


def helper_with(found: Optional[PlatformResource]) -> BigQueryPlatformResourceHelper:
    helper = BigQueryPlatformResourceHelper("my-project", graph=MagicMock())
    patcher = patch(f"{MODULE}.PlatformResource.from_datahub", return_value=found)
    patcher.start()
    return helper


def test_without_graph_a_new_resource_is_created_and_cached() -> None:
    helper = BigQueryPlatformResourceHelper("my-project", graph=None)

    first = helper.generate_label_platform_resource(LABEL, TAG)
    second = helper.generate_label_platform_resource(LABEL, TAG)

    assert label_info(first).datahub_urn == TAG.urn()
    assert label_info(first).managed_by_datahub is True
    assert second.id == first.id
    assert helper.get_platform_resource(MagicMock()) is None


def test_existing_resource_with_same_mapping_is_reused() -> None:
    with patch(
        f"{MODULE}.PlatformResource.from_datahub", return_value=existing_resource()
    ) as from_datahub:
        helper = BigQueryPlatformResourceHelper("my-project", graph=MagicMock())

        resource = helper.generate_label_platform_resource(
            LABEL, TAG, managed_by_datahub=False
        )
        again = helper.generate_label_platform_resource(
            LABEL, TAG, managed_by_datahub=False
        )

    assert label_info(resource).datahub_urn == TAG.urn()
    assert again is resource
    # Second lookup is served from the cache.
    from_datahub.assert_called_once()


def test_datahub_managed_resource_wins_over_a_conflicting_label() -> None:
    managed = existing_resource(tag=OTHER_TAG, managed_by_datahub=True)
    with patch(f"{MODULE}.PlatformResource.from_datahub", return_value=managed):
        helper = BigQueryPlatformResourceHelper("my-project", graph=MagicMock())
        resource = helper.generate_label_platform_resource(
            LABEL, TAG, managed_by_datahub=False
        )

    assert label_info(resource).datahub_urn == OTHER_TAG.urn()


def test_conflicting_unmanaged_resource_is_rejected() -> None:
    conflicting = existing_resource(tag=OTHER_TAG, managed_by_datahub=False)
    with patch(f"{MODULE}.PlatformResource.from_datahub", return_value=conflicting):
        helper = BigQueryPlatformResourceHelper("my-project", graph=MagicMock())
        with pytest.raises(ValueError):
            helper.generate_label_platform_resource(
                LABEL, TAG, managed_by_datahub=False
            )


def test_unreadable_existing_value_is_replaced_by_a_new_resource() -> None:
    class LegacyShape(BaseModel):
        something_else: str

    stale = PlatformResource.create(
        key=PlatformResourceKey(
            platform="bigquery",
            resource_type="BigQueryLabelInfo",
            platform_instance=None,
            primary_key=LABEL.primary_key(),
        ),
        value=LegacyShape(something_else="x"),
    )
    with patch(f"{MODULE}.PlatformResource.from_datahub", return_value=stale):
        helper = BigQueryPlatformResourceHelper("my-project", graph=MagicMock())
        resource = helper.generate_label_platform_resource(LABEL, TAG)

    assert label_info(resource).datahub_urn == TAG.urn()


def test_label_without_value_uses_bare_key_as_primary_key() -> None:
    assert BigQueryLabel(key="flag", value=None).primary_key() == "flag"
    assert LABEL.primary_key() == "team/data"
