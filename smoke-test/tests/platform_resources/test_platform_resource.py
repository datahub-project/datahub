import logging
import random
import string
from typing import List

import pytest

from datahub.api.entities.platformresource.platform_resource import (
    ElasticPlatformResourceQuery,
    PlatformResource,
    PlatformResourceKey,
    PlatformResourceSearchFields,
)
from tests.utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)


def generate_random_id(length=8):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


@pytest.fixture
def test_id():
    return f"test_{generate_random_id()}"


@pytest.fixture(scope="function", autouse=True)
def cleanup_resources(graph_client):
    created_resources: List[PlatformResource] = []
    yield created_resources

    # Delete all generated platform resources after each test
    for resource in created_resources:
        try:
            resource.delete(graph_client)
        except Exception as e:
            logger.warning(f"Failed to delete resource: {e}")

    # Additional cleanup for any resources that might have been missed
    for resource in PlatformResource.search_by_filters(
        graph_client,
        ElasticPlatformResourceQuery.create_from().add_wildcard(
            PlatformResourceSearchFields.PRIMARY_KEY, "test_*"
        ),
    ):
        try:
            resource.delete(graph_client)
        except Exception as e:
            logger.warning(f"Failed to delete resource during final cleanup: {e}")


def test_platform_resource_read_write(graph_client, test_id, cleanup_resources):
    key = PlatformResourceKey(
        platform=f"test_platform_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_{test_id}",
    )
    platform_resource = PlatformResource.create(
        key=key,
        secondary_keys=[f"test_secondary_key_{test_id}"],
        value={"test_key": f"test_value_{test_id}"},
    )
    platform_resource.to_datahub(graph_client)
    cleanup_resources.append(platform_resource)

    wait_for_writes_to_sync()

    read_platform_resource = PlatformResource.from_datahub(graph_client, key)
    assert read_platform_resource == platform_resource


def test_platform_resource_search(graph_client, test_id, cleanup_resources):
    key = PlatformResourceKey(
        platform=f"test_platform_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_{test_id}",
    )
    platform_resource = PlatformResource.create(
        key=key,
        secondary_keys=[f"test_secondary_key_{test_id}"],
        value={"test_key": f"test_value_{test_id}"},
    )
    platform_resource.to_datahub(graph_client)
    cleanup_resources.append(platform_resource)

    wait_for_writes_to_sync()

    search_results = [
        r for r in PlatformResource.search_by_key(graph_client, key.primary_key)
    ]
    assert len(search_results) == 1
    assert search_results[0] == platform_resource

    search_results = [
        r
        for r in PlatformResource.search_by_key(
            graph_client, f"test_secondary_key_{test_id}", primary=False
        )
    ]
    assert len(search_results) == 1
    assert search_results[0] == platform_resource


def test_platform_resource_non_existent(graph_client, test_id):
    key = PlatformResourceKey(
        platform=f"test_platform_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_{test_id}",
    )
    platform_resource = PlatformResource.from_datahub(
        key=key,
        graph_client=graph_client,
    )
    assert platform_resource is None


def test_platform_resource_urn_secondary_key(graph_client, test_id, cleanup_resources):
    key = PlatformResourceKey(
        platform=f"test_platform_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_{test_id}",
    )
    dataset_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:test,test_secondary_key_{test_id},PROD)"
    )
    platform_resource = PlatformResource.create(
        key=key,
        value={"test_key": f"test_value_{test_id}"},
        secondary_keys=[dataset_urn],
    )
    platform_resource.to_datahub(graph_client)
    cleanup_resources.append(platform_resource)
    wait_for_writes_to_sync()

    read_platform_resources = [
        r
        for r in PlatformResource.search_by_key(
            graph_client, dataset_urn, primary=False
        )
    ]
    assert len(read_platform_resources) == 1
    assert read_platform_resources[0] == platform_resource


def test_platform_resource_listing_by_resource_type(
    graph_client, test_id, cleanup_resources
):
    # Generate two resources with the same resource type
    key1 = PlatformResourceKey(
        platform=f"test_platform_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_1_{test_id}",
    )
    platform_resource1 = PlatformResource.create(
        key=key1,
        value={"test_key": f"test_value_1_{test_id}"},
    )
    platform_resource1.to_datahub(graph_client)

    key2 = PlatformResourceKey(
        platform=f"test_platform_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_2_{test_id}",
    )
    platform_resource2 = PlatformResource.create(
        key=key2,
        value={"test_key": f"test_value_2_{test_id}"},
    )
    platform_resource2.to_datahub(graph_client)

    wait_for_writes_to_sync()

    search_results = [
        r
        for r in PlatformResource.search_by_filters(
            graph_client,
            query=ElasticPlatformResourceQuery.create_from(
                (PlatformResourceSearchFields.RESOURCE_TYPE, key1.resource_type)
            ),
        )
    ]
    assert len(search_results) == 2

    read_platform_resource_1 = next(r for r in search_results if r.id == key1.id)
    read_platform_resource_2 = next(r for r in search_results if r.id == key2.id)
    assert read_platform_resource_1 == platform_resource1
    assert read_platform_resource_2 == platform_resource2


def test_platform_resource_listing_complex_queries(graph_client, test_id):
    # Generate two resources with the same resource type
    key1 = PlatformResourceKey(
        platform=f"test_platform1_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_1_{test_id}",
    )
    platform_resource1 = PlatformResource.create(
        key=key1,
        value={"test_key": f"test_value_1_{test_id}"},
    )
    platform_resource1.to_datahub(graph_client)

    key2 = PlatformResourceKey(
        platform=f"test_platform2_{test_id}",
        resource_type=f"test_resource_type_{test_id}",
        primary_key=f"test_primary_key_2_{test_id}",
    )
    platform_resource2 = PlatformResource.create(
        key=key2,
        value={"test_key": f"test_value_2_{test_id}"},
    )
    platform_resource2.to_datahub(graph_client)

    wait_for_writes_to_sync()
    from datahub.api.entities.platformresource.platform_resource import (
        ElasticPlatformResourceQuery,
        LogicalOperator,
        PlatformResourceSearchFields,
    )

    query = (
        ElasticPlatformResourceQuery.create_from()
        .group(LogicalOperator.AND)
        .add_field_match(PlatformResourceSearchFields.RESOURCE_TYPE, key1.resource_type)
        .add_field_not_match(PlatformResourceSearchFields.PLATFORM, key1.platform)
        .end()
    )

    search_results = [
        r
        for r in PlatformResource.search_by_filters(
            graph_client,
            query=query,
        )
    ]
    assert len(search_results) == 1

    read_platform_resource = search_results[0]
    assert read_platform_resource == platform_resource2
