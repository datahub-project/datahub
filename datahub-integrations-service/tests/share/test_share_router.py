import json
from typing import Any, Optional
from unittest import mock
from unittest.mock import MagicMock, Mock, patch

import datahub.metadata.schema_classes as models
import pytest
from datahub.emitter.mce_builder import make_dataplatform_instance_urn
from datahub.metadata.schema_classes import (
    ShareClass,
    ShareResultClass,
    ShareResultStateClass,
)
from fastapi.testclient import TestClient
from freezegun import freeze_time

from datahub_integrations import server
from datahub_integrations.share import share_router
from datahub_integrations.share.share_agent import LineageDirection, ShareAgent
from datahub_integrations.share.share_settings import (
    RESTRICTED_SHARED_ASPECTS,
    SHARED_ASPECTS,
)


def test_determine_entities_to_sync_has_container() -> None:
    test_urn = "urn:li:dataset:1"
    test_container_urn = "urn:li:container:199e66d51aeb64f24dbf5a0af94baf7e"
    graph_mock = Mock()
    graph_mock.get_aspect.return_value = models.ContainerClass(
        container=test_container_urn
    )
    share_config = {
        "connection": {
            "server": "localhost:8080",
            "details": {
                "type": "JSON",
                "json": {"blob": '{"connection": {"server": "localhost:8080"}}'},
            },
        }
    }

    graph_mock.execute_graphql.return_value = share_config
    share_agent = ShareAgent(graph_mock, "dummy_connection")
    entities = share_agent.determine_entities_to_sync(test_urn)
    assert len(entities) == 2
    assert "urn:li:container:199e66d51aeb64f24dbf5a0af94baf7e" in entities
    assert test_urn in entities


def test_determine_entities_to_sync_without_container() -> None:
    share_config = {
        "connection": {
            "server": "localhost:8080",
            "details": {
                "type": "JSON",
                "json": {"blob": '{"connection": {"server": "localhost:8080"}}'},
            },
        }
    }

    graph_mock = Mock()
    graph_mock.execute_graphql.return_value = share_config

    test_urn = "urn:li:dataset:1"
    graph_mock.get_aspect.return_value = None
    share_agent = ShareAgent(graph_mock, "dummy_connection")
    entities = share_agent.determine_entities_to_sync(test_urn)
    assert len(entities) == 1
    assert test_urn in entities


def test_determine_entities_to_sync_has_container_in_container() -> None:
    test_urn = "urn:li:dataset:1"
    test_container_urn = "urn:li:container:199e66d51aeb64f24dbf5a0af94baf7e"
    test_container_2_urn = "urn:li:container:another_container"

    graph_mock = Mock()

    def get_aspect_side_effect(urn: str, model_class: Any) -> Any:
        if urn == test_urn and model_class == models.ContainerClass:
            return models.ContainerClass(container=test_container_urn)
        elif urn == test_container_urn and model_class == models.ContainerClass:
            return models.ContainerClass(container=test_container_2_urn)
        elif urn == test_container_2_urn and model_class == models.ContainerClass:
            return None
        return None

    graph_mock.get_aspect.side_effect = get_aspect_side_effect

    share_agent = ShareAgent(
        source_graph=graph_mock,
        share_connection_urn="dummy_connection",
        destination_graph=Mock(),
    )

    entities = share_agent.determine_entities_to_sync(test_urn)
    assert len(entities) == 3
    assert test_container_urn in entities
    assert test_container_2_urn in entities
    assert test_urn in entities


def test_share_non_restricted() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    sharer = "urn:li:corpuser:test_user"
    test_urn = "urn:li:dataset:1"
    destination_urn = "urn:li:datahub:destination_datahub_urn"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = None
    share_agent = ShareAgent(
        source_graph, destination_urn, destination_graph=destination_graph_mock
    )
    share_agent.source_platform_instance = make_dataplatform_instance_urn(
        "acryl", "my_platform_id"
    )

    share_agent.share_one_entity(
        test_urn,
        test_urn,
        sharer,
        False,
    )

    assert len(destination_graph_mock.method_calls) == 10  # +1 Origin aspect

    for method_call in destination_graph_mock.method_calls:
        mcp = method_call.args[0]
        assert mcp.aspectName in SHARED_ASPECTS
        if mcp.aspectName == "upstreamLineage":
            upstreamClass = json.loads(mcp.aspect.value)
            # FinegrainedLineage should be an empty list if upstreamLineage shared in restricted mode
            assert len(upstreamClass.get("fineGrainedLineages")) > 0

    source_graph.emit.assert_called_once()
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].implicitShareEntity
        is None
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].status == "SUCCESS"
    )


def test_share_explicit_share_should_not_remove_implicit_shares() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    sharer = "urn:li:corpuser:test_user"
    test_urn = "urn:li:dataset:1"
    destination_urn = "urn:li:datahub:destination_datahub_urn"
    referenced_entity = "urn:li:dataset:referenced"
    referenced_entity2 = "urn:li:dataset:referenced2"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=destination_urn,
                status="SUCCESS",
                implicitShareEntity=referenced_entity,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=destination_urn,
                status="SUCCESS",
                implicitShareEntity=referenced_entity2,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ],
        lastUnshareResults=[
            ShareResultClass(
                destination=destination_urn,
                status="SUCCESS",
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ],
    )

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_urn,
        destination_graph=destination_graph_mock,
    )
    share_agent.source_platform_instance = make_dataplatform_instance_urn(
        "acryl", "my_platform_id"
    )

    share_agent.share_one_entity(
        test_urn,
        test_urn,
        sharer,
        False,
    )

    assert len(destination_graph_mock.method_calls) == 10  # +1 Origin aspect

    for method_call in destination_graph_mock.method_calls:
        mcp = method_call.args[0]
        assert mcp.aspectName in SHARED_ASPECTS
        if mcp.aspectName == "upstreamLineage":
            upstreamClass = json.loads(mcp.aspect.value)
            # FinegrainedLineage should be an empty list if upstreamLineage shared in restricted mode
            assert len(upstreamClass.get("fineGrainedLineages")) > 0

    source_graph.emit.assert_called_once()
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].implicitShareEntity
        == referenced_entity
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].status == "SUCCESS"
    )

    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].implicitShareEntity
        == referenced_entity2
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].status == "SUCCESS"
    )

    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[2].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[2].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[2].implicitShareEntity
        is None
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[2].status == "SUCCESS"
    )

    # Make sure unshare results are not removed
    assert (
        source_graph.emit.call_args[0][0].aspect.lastUnshareResults[0].status
        == "SUCCESS"
    )


def test_share_implict_share_should_add_to_implicit_shares() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    sharer = "urn:li:corpuser:test_user"
    test_urn = "urn:li:dataset:1"
    destination_urn = "urn:li:datahub:destination_datahub_urn"
    referenced_entity = "urn:li:dataset:referenced"
    referenced_entity2 = "urn:li:dataset:referenced2"
    referenced_entity3 = "urn:li:dataset:referenced3"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=destination_urn,
                status="SUCCESS",
                implicitShareEntity=referenced_entity,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=destination_urn,
                status="SUCCESS",
                implicitShareEntity=referenced_entity2,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ]
    )

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_urn,
        destination_graph=destination_graph_mock,
    )

    share_agent.source_platform_instance = make_dataplatform_instance_urn(
        "acryl", "my_platform_id"
    )

    share_agent.share_one_entity(
        test_urn,
        referenced_entity3,
        sharer,
        False,
    )

    assert len(destination_graph_mock.method_calls) == 10  # +1 Origin aspect

    for method_call in destination_graph_mock.method_calls:
        mcp = method_call.args[0]
        assert mcp.aspectName in SHARED_ASPECTS
        if mcp.aspectName == "upstreamLineage":
            upstreamClass = json.loads(mcp.aspect.value)
            # FinegrainedLineage should be an empty list if upstreamLineage shared in restricted mode
            assert len(upstreamClass.get("fineGrainedLineages")) > 0

    source_graph.emit.assert_called_once()
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].implicitShareEntity
        == referenced_entity
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].implicitShareEntity
        == referenced_entity2
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[2].implicitShareEntity
        == referenced_entity3
    )

    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].status == "SUCCESS"
    )


def test_share_implict_share_should_be_added_if_earlier_it_was_explicitly_shared() -> (
    None
):
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    sharer = "urn:li:corpuser:test_user"
    test_urn = "urn:li:dataset:1"
    destination_urn = "urn:li:datahub:destination_datahub_urn"
    referenced_entity = "urn:li:dataset:referenced"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=destination_urn,
                status="SUCCESS",
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ]
    )

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_urn,
        destination_graph=destination_graph_mock,
    )
    share_agent.source_platform_instance = make_dataplatform_instance_urn(
        "acryl", "my_platform_id"
    )

    share_agent.share_one_entity(
        test_urn,
        referenced_entity,
        sharer,
        False,
    )

    assert len(destination_graph_mock.method_calls) == 10  # +1 origin aspect

    for method_call in destination_graph_mock.method_calls:
        mcp = method_call.args[0]
        assert mcp.aspectName in SHARED_ASPECTS
        if mcp.aspectName == "upstreamLineage":
            upstreamClass = json.loads(mcp.aspect.value)
            # FinegrainedLineage should be an empty list if upstreamLineage shared in restricted mode
            assert len(upstreamClass.get("fineGrainedLineages")) > 0

    source_graph.emit.assert_called_once()
    assert len(source_graph.emit.call_args[0][0].aspect.lastShareResults) == 2
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].implicitShareEntity
        is None
    )

    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].status == "SUCCESS"
    )

    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].implicitShareEntity
        == referenced_entity
    )

    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[1].status == "SUCCESS"
    )


def test_restricted_share_should_only_share_certain_aspects() -> None:
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    source_graph = Mock()
    destination_graph_mock = Mock()

    sharer = "urn:li:corpuser:test_user"
    test_urn = "urn:li:dataset:1"
    destination_urn = "urn:li:datahub:destination_datahub_urn"

    source_graph.get_entity_raw.return_value = response

    source_graph.get_aspect.return_value = None

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_urn,
        destination_graph=destination_graph_mock,
    )

    share_agent.source_platform_instance = make_dataplatform_instance_urn(
        "acryl", "my_platform_id"
    )

    share_agent.share_one_entity(
        test_urn,
        test_urn,
        sharer,
        True,
    )

    assert len(destination_graph_mock.method_calls) == 3

    for method_call in destination_graph_mock.method_calls:
        mcp = method_call.args[0]
        assert mcp.aspectName in RESTRICTED_SHARED_ASPECTS
        if mcp.aspectName == "upstreamLineage":
            upstreamClass = json.loads(mcp.aspect.value)
            # FinegrainedLineage should be an empty list if upstreamLineage shared in restricted mode
            assert upstreamClass.get("fineGrainedLineages") == []

    source_graph.emit.assert_called_once()
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].destination
        == destination_urn
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].created.actor
        == "urn:li:corpuser:test_user"
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].implicitShareEntity
        is None
    )
    assert (
        source_graph.emit.call_args[0][0].aspect.lastShareResults[0].status == "SUCCESS"
    )


def test_unshare() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    urn_to_unshare = "urn:li:dataset:1"
    another_destination_darahub_instance = "urn:li:datahub:another_destination_datahub"
    destination_datahub_instance = "urn:li:datahub:destination_datahub"
    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=another_destination_darahub_instance,
                status="SUCCESS",
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=destination_datahub_instance,
                status="SUCCESS",
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ]
    )

    destination_graph_mock.exists.return_value = True

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_datahub_instance,
        destination_graph=destination_graph_mock,
    )
    with patch.object(
        share_agent,
        "determine_entities_to_sync",
        return_value={urn_to_unshare},
    ):
        share_agent.unshare(
            urn_to_unshare,
        )

    destination_graph_mock.emit.assert_called_once()

    for method_call in destination_graph_mock.emit.call_args_list:
        mcp = method_call[0][0]
        assert mcp.aspectName == "status"

    assert (
        source_graph.get_aspect.call_count == 3
    )  # unshare checks the share aspect twice (first to determine lineage and second to update it) + 1 for the status set

    assert source_graph.get_aspect.call_args[0][0] == urn_to_unshare
    assert source_graph.get_aspect.call_args[0][1] == ShareClass

    call_arg = source_graph.emit.call_args_list[0]
    share_results = call_arg[0][0].aspect.lastShareResults
    assert len(share_results) == 1
    assert share_results[0].destination == another_destination_darahub_instance


def test_execute_share() -> None:
    urn_to_unshare = "urn:li:dataset:1"
    sharer_urn = "urn:li:corpuser:dummy_user"
    lineage = LineageDirection.DOWNSTREAM
    downstream_urn = "urn:li:dataset:2"

    share_mock = MagicMock(return_value=None)
    get_entities_across_lineage_mock = MagicMock(return_value={downstream_urn})

    source_graph = Mock()
    source_graph.get_ownership.return_value = None

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn="dummy_connection_url",
        destination_graph=Mock(),
    )

    with (
        patch.object(
            share_router, "get_or_create_share_agent", return_value=share_agent
        ),
        patch.object(share_agent, "share_one_entity", share_mock),
        patch.object(
            share_agent,
            "get_entities_across_lineage",
            get_entities_across_lineage_mock,
        ),
        patch.object(
            share_agent,
            "determine_entities_to_sync",
            return_value={urn_to_unshare},
        ),
    ):
        share_agent.share(urn_to_unshare, sharer_urn, lineage)

    assert share_mock.call_count == 2
    assert share_mock.call_args_list[0].kwargs.get("restricted") is False
    assert share_mock.call_args_list[1].kwargs.get("restricted") is False


def test_execute_share_when_owner_matches_and_share_is_non_restricted() -> None:
    source_graph = Mock()
    urn_to_unshare = "urn:li:dataset:1"
    sharer_urn = "urn:li:corpuser:dummy_user"
    lineage = LineageDirection.DOWNSTREAM
    downstream_urn = "urn:li:dataset:2"

    share_mock = MagicMock(return_value=None)
    get_entities_across_lineage_mock = MagicMock(return_value={downstream_urn})

    source_graph.get_ownership.return_value = models.OwnershipClass(
        owners=[
            models.OwnerClass(owner="not_the_sharer_urn", type="DATAOWNER"),
            models.OwnerClass(owner=sharer_urn, type="DATAOWNER"),
        ]
    )

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn="dummy_connection_url",
        destination_graph=Mock(),
    )

    with (
        patch.object(
            share_router, "get_or_create_share_agent", return_value=share_agent
        ),
        patch.object(share_agent, "share_one_entity", share_mock),
        patch.object(
            share_agent,
            "get_entities_across_lineage",
            get_entities_across_lineage_mock,
        ),
        patch.object(
            share_agent,
            "determine_entities_to_sync",
            return_value={urn_to_unshare},
        ),
    ):
        share_agent.share(urn_to_unshare, sharer_urn, lineage)

    assert share_mock.call_count == 2
    assert not share_mock.call_args_list[0].kwargs.get("restricted")
    assert not share_mock.call_args_list[1].kwargs.get("restricted")


def test_execute_share_when_owner_does_not_matches_and_share_is_restricted() -> None:
    source_graph = Mock()
    urn_to_unshare = "urn:li:dataset:1"
    sharer_urn = "urn:li:corpuser:dummy_user"
    lineage = LineageDirection.DOWNSTREAM
    downstream_urn = "urn:li:dataset:2"

    share_mock = MagicMock(return_value=None)
    get_entities_across_lineage_mock = MagicMock(return_value={downstream_urn})

    source_graph.get_ownership.return_value = models.OwnershipClass(
        owners=[
            models.OwnerClass(owner="not_the_sharer_urn", type="DATAOWNER"),
            models.OwnerClass(owner="anonther_owner", type="DATAOWNER"),
        ]
    )

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn="dummy_connection_url",
        destination_graph=Mock(),
    )

    with (
        patch.object(
            share_router, "get_or_create_share_agent", return_value=share_agent
        ),
        patch.object(share_agent, "share_one_entity", share_mock),
        patch.object(
            share_agent,
            "get_entities_across_lineage",
            get_entities_across_lineage_mock,
        ),
        patch.object(
            share_agent,
            "determine_entities_to_sync",
            return_value={urn_to_unshare},
        ),
    ):
        share_agent.share(urn_to_unshare, sharer_urn, lineage)

    assert share_mock.call_count == 2
    assert share_mock.call_args_list[0].kwargs.get("restricted") is True
    assert share_mock.call_args_list[1].kwargs.get("restricted") is True


def test_execute_share_should_ingest_structured_properties_first() -> None:
    urn_to_share = "urn:li:dataset:1"
    sharer_urn = "urn:li:corpuser:dummy_user"
    lineage = LineageDirection.DOWNSTREAM
    downstream_urn = "urn:li:dataset:2"
    downstream_urn2 = "urn:li:dataset:3"
    structured_property_1 = "urn:li:structuredProperty:1"
    structured_property_2 = "urn:li:structuredProperty:2"

    share_mock = MagicMock(return_value=None)
    get_entities_across_lineage_mock = MagicMock(
        return_value={
            downstream_urn,
            structured_property_1,
            downstream_urn2,
            structured_property_2,
        }
    )

    source_graph = Mock()
    source_graph.get_ownership.return_value = None

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn="dummy_connection_url",
        destination_graph=Mock(),
    )

    with (
        patch.object(
            share_router, "get_or_create_share_agent", return_value=share_agent
        ),
        patch.object(share_agent, "share_one_entity", share_mock),
        patch.object(
            share_agent,
            "get_entities_across_lineage",
            get_entities_across_lineage_mock,
        ),
        patch.object(
            share_agent,
            "determine_entities_to_sync",
            return_value={urn_to_share},
        ),
    ):
        execute_share_result = share_agent.share(urn_to_share, sharer_urn, lineage)

    assert share_mock.call_count == 5
    assert execute_share_result.entities_shared == [
        "urn:li:structuredProperty:1",
        "urn:li:structuredProperty:2",
        "urn:li:dataset:2",
        "urn:li:dataset:3",
        "urn:li:dataset:1",
    ]
    assert share_mock.call_args_list[0].kwargs.get("restricted") is False
    assert share_mock.call_args_list[1].kwargs.get("restricted") is False


def test_unshare_implicit_unshare_should_not_remove_entity_if_there_is_another_reference() -> (
    None
):
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    urn_to_unshare = "urn:li:dataset:1"
    another_destination_datahub_instance = "urn:li:datahub:another_destination_datahub"
    destination_datahub_instance = "urn:li:datahub:destination_datahub"

    referenced_entity = "urn:li:dataset:root"
    referenced_entity2 = "urn:li:dataset:root2"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=destination_datahub_instance,
                status="SUCCESS",
                implicitShareEntity=referenced_entity,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=destination_datahub_instance,
                status="SUCCESS",
                implicitShareEntity=referenced_entity2,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=another_destination_datahub_instance,
                status="SUCCESS",
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ]
    )

    destination_graph_mock.exists.return_value = True

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_datahub_instance,
        destination_graph=destination_graph_mock,
    )
    share_agent.unshare_one_entity(
        urn_to_unshare,
        referenced_entity,
    )

    assert not destination_graph_mock.emit.called, (
        "method should not have been called as it should not do soft-delete if there are still references"
    )

    assert source_graph.get_aspect.call_count == 1

    assert source_graph.get_aspect.call_args[0][0] == urn_to_unshare
    assert source_graph.get_aspect.call_args[0][1] == ShareClass

    call_arg = source_graph.emit.call_args_list[0]
    share_results = call_arg[0][0].aspect.lastShareResults
    assert len(share_results) == 2
    assert share_results[0].destination == destination_datahub_instance
    assert share_results[0].implicitShareEntity == referenced_entity2
    assert share_results[1].destination == another_destination_datahub_instance


def test_unshare_complex_explicit_share_should_force_unshare_the_entity() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    urn_to_unshare = "urn:li:dataset:1"
    another_destination_datahub_instance = "urn:li:datahub:another_destination_datahub"
    destination_datahub_instance = "urn:li:datahub:destination_datahub"

    referenced_entity = "urn:li:dataset:root"
    referenced_entity2 = "urn:li:dataset:root2"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=destination_datahub_instance,
                status="SUCCESS",
                implicitShareEntity=referenced_entity,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=destination_datahub_instance,
                status="SUCCESS",
                implicitShareEntity=referenced_entity2,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
            ShareResultClass(
                destination=another_destination_datahub_instance,
                status="SUCCESS",
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ]
    )

    destination_graph_mock.exists.return_value = True

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_datahub_instance,
        destination_graph=destination_graph_mock,
    )

    with patch.object(
        share_agent,
        "determine_entities_to_sync",
        return_value={urn_to_unshare},
    ):
        share_agent.unshare(
            urn_to_unshare,
        )

    destination_graph_mock.emit.assert_called_once()

    for method_call in destination_graph_mock.emit.call_args_list:
        mcp = method_call[0][0]
        assert mcp.aspectName == "status"

    assert (
        source_graph.get_aspect.call_count == 3
    )  # unshare checks the share aspect twice + 1 for the status set

    assert source_graph.get_aspect.call_args[0][0] == urn_to_unshare
    assert source_graph.get_aspect.call_args[0][1] == ShareClass

    call_arg = source_graph.emit.call_args_list[0]
    share_results = call_arg[0][0].aspect.lastShareResults
    assert len(share_results) == 1
    assert share_results[0].destination == another_destination_datahub_instance


def test_unshare_complex_explicit_share_should_not_be_unshared_with_implicit_unshare() -> (
    None
):
    source_graph = Mock()
    destination_graph_mock = Mock()
    response = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )

    urn_to_unshare = "urn:li:dataset:1"
    destination_datahub_instance = "urn:li:datahub:destination_datahub"
    referenced_entity = "urn:li:dataset:root"

    source_graph.get_entity_raw.return_value = response
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination=destination_datahub_instance,
                status="SUCCESS",
                implicitShareEntity=None,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=1234
                ),
                lastAttempt=models.AuditStampClass(
                    actor="urn:li:corpuser:test_user", time=2345
                ),
            ),
        ]
    )

    destination_graph_mock.exists.return_value = True

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_datahub_instance,
        destination_graph=destination_graph_mock,
    )
    share_agent.unshare_one_entity(
        urn_to_unshare,
        referenced_entity,
    )

    assert not destination_graph_mock.emit.called

    assert source_graph.get_aspect.call_count == 1

    assert source_graph.get_aspect.call_args[0][0] == urn_to_unshare
    assert source_graph.get_aspect.call_args[0][1] == ShareClass


@pytest.mark.parametrize(
    "scenario, shared_urn, existing_share_aspect, source_share_connection_urn, sharer_urn, implicit_share_entity, status, share_config, expected_result_share",
    [
        (
            "new aspect getting added",
            "urn:li:dataset:1",
            None,
            "urn:li:dataHubConnection:1",
            "urn:li:corpuser:testUser",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ]
            ),
        ),
        (
            "update from running to success",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastSuccess=None,
                        status=models.ShareResultStateClass.RUNNING,
                        implicitShareEntity="urn:li:dataset:2",
                    )
                ]
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:corpuser:testUser",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time (we don't update lastAttempt as part of update_share_aspect method),
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ]
            ),
        ),
        (
            "update to existing implicit",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                    )
                ]
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:corpuser:testUser",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time (we don't update lastAttempt as part of update_share_aspect method)
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ]
            ),
        ),
        (
            "upgrade from implicit to explicit",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                    )
                ]
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:corpuser:testUser",
            None,
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity=None,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ]
            ),
        ),
        (
            "downgrade from explicit to implicit",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity=None,
                    )
                ]
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:corpuser:testUser",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity=None,
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ]
            ),
        ),
        (
            "multiple implicits",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                    )
                ]
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:corpuser:testUser",
            "urn:li:dataset:3",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:3",
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ]
            ),
        ),
        (
            "different destination",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier than frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                    )
                ]
            ),
            "urn:li:dataHubConnection:2",
            "urn:li:corpuser:testUser",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1600080800000,  # earlier frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:2",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        implicitShareEntity="urn:li:dataset:2",
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ]
            ),
        ),
    ],
)
@freeze_time("2022-04-15")
def test_update_share_aspect(
    scenario: str,
    shared_urn: str,
    existing_share_aspect: Optional[ShareClass],
    source_share_connection_urn: str,
    sharer_urn: str,
    implicit_share_entity: str,
    status: models.ShareResultStateClass,
    share_config: Optional[Any],
    expected_result_share: ShareClass,
) -> None:
    source_graph = Mock()
    destination_graph = Mock()

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=source_share_connection_urn,
        destination_graph=destination_graph,
    )
    result_share = share_agent.update_share_aspect(
        shared_urn=shared_urn,
        existing_share_aspect=existing_share_aspect,
        source_share_connection_urn=source_share_connection_urn,
        sharer_urn=sharer_urn,
        implicit_share_entity=implicit_share_entity,
        status=status,
        share_config=share_config,
    )
    try:
        assert result_share == expected_result_share
    except AssertionError as e:
        print(f"Results Differ on scenario {scenario}!")
        print(result_share)
        print("Expected")
        print(expected_result_share)
        raise e


def test_failures_in_emission() -> None:
    destination_graph = Mock()
    source_graph = Mock()
    destination_graph.emit.side_effect = Exception("Failed to emit")

    source_graph.get_aspect.return_value = None
    source_graph.get_ownership.return_value = None
    source_graph.get_entity_raw.return_value = json.load(
        open("tests/share/sample_files/sample_entity_v2_response.json")
    )
    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn="dummy_connection_url",
        destination_graph=destination_graph,
    )
    share_agent.source_platform_instance = make_dataplatform_instance_urn(
        "acryl", "my_platform_id"
    )

    share_agent.share(
        "urn:li:dataset:1",
        "urn:li:corpuser:dummy_user",
    )

    call_arg = source_graph.emit.call_args_list[0]
    share_results = call_arg[0][0].aspect.lastShareResults
    assert len(share_results) == 1
    assert share_results[0].status == models.ShareResultStateClass.FAILURE
    assert share_results[0].destination == "dummy_connection_url"


@pytest.mark.parametrize(
    "scenario, unshared_urn, existing_share_aspect, source_share_connection_urn, implicit_share_entity, status, share_config, expected_result_share",
    [
        (
            "new unshare aspect getting added with state In Progress",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ]
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:dataset:2",
            models.ShareResultStateClass.RUNNING,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
            ),
        ),
        (
            "unshare aspect should not be deleted after unshare",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
            ),
        ),
        (
            "unshare aspect should not be deleted after unshare but set status success and it should keep the other in progress unshares",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:2",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ],
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:dataset:2",
            models.ShareResultStateClass.SUCCESS,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:2",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ],
            ),
        ),
        (
            "if you unshare a share in-progress it should update the unshare aspect",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:2",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ],
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:dataset:2",
            models.ShareResultStateClass.RUNNING,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:2",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    ),
                ],
            ),
        ),
        (
            "if you unshare a share and there was no lastUnshare then it should be added",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=None,
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:dataset:2",
            models.ShareResultStateClass.RUNNING,
            None,
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.RUNNING,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
            ),
        ),
        (
            "if you unshare an implicit share it should return with None as implict shares can't be unshared and we shouldn't do anything",
            "urn:li:dataset:1",
            ShareClass(
                lastShareResults=[
                    ShareResultClass(
                        destination="urn:li:dataHubConnection:1",
                        implicitShareEntity="urn:li:dataset:2",
                        created=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        lastAttempt=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        status=models.ShareResultStateClass.SUCCESS,
                        lastSuccess=models.AuditStampClass(
                            time=1649980800000,  # frozen time
                            actor="urn:li:corpuser:testUser",
                        ),
                        statusLastUpdated=1649980800000,
                    )
                ],
                lastUnshareResults=None,
            ),
            "urn:li:dataHubConnection:1",
            "urn:li:dataset:2",
            models.ShareResultStateClass.RUNNING,
            None,
            None,
        ),
    ],
)
@freeze_time("2022-04-15")
def test_update_unshare_aspect(
    scenario: str,
    unshared_urn: str,
    existing_share_aspect: ShareClass,
    source_share_connection_urn: str,
    implicit_share_entity: str,
    status: models.ShareResultStateClass,
    share_config: Optional[Any],
    expected_result_share: ShareClass,
) -> None:
    source_graph = Mock()
    destination_graph = Mock()

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=source_share_connection_urn,
        destination_graph=destination_graph,
    )
    result_share = share_agent.update_unshare_aspect(
        unshared_urn=unshared_urn,
        existing_share_aspect=existing_share_aspect,
        source_share_connection_urn=source_share_connection_urn,
        status=status,
    )
    try:
        assert result_share == expected_result_share
    except AssertionError as e:
        print(f"Results Differ on scenario {scenario}!")
        print(result_share)
        print("Expected")
        print(expected_result_share)
        raise e


def test_update_share_status() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()

    urn_to_unshare = "urn:li:dataset:1"
    destination_datahub_instance = "urn:li:dataHubConnection:1"

    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[],
        lastUnshareResults=[
            ShareResultClass(
                destination="urn:li:dataHubConnection:1",
                created=models.AuditStampClass(
                    time=1649980800000,  # frozen time
                    actor="urn:li:corpuser:testUser",
                ),
                lastAttempt=models.AuditStampClass(
                    time=1649980800000,  # frozen time
                    actor="urn:li:corpuser:testUser",
                ),
                status=models.ShareResultStateClass.RUNNING,
                lastSuccess=models.AuditStampClass(
                    time=1649980800000,  # frozen time
                    actor="urn:li:corpuser:testUser",
                ),
                statusLastUpdated=1649980800000,
            )
        ],
    )

    destination_graph_mock.exists.return_value = True

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_datahub_instance,
        destination_graph=destination_graph_mock,
    )
    share_agent.unshare_status_update(
        urn_to_unshare,
        ShareResultStateClass.SUCCESS,
    )
    source_graph.emit.assert_called_once()
    assert (
        source_graph.emit.call_args[0][0].aspect.lastUnshareResults[0].status
        == ShareResultStateClass.SUCCESS
    )


def test_determine_lineage_direction_from_share_aspect() -> None:
    source_graph = Mock()
    destination_graph_mock = Mock()
    urn_to_unshare = "urn:li:dataset:1"
    destination_datahub_instance = "urn:li:dataHubConnection:1"

    share_agent = ShareAgent(
        source_graph=source_graph,
        share_connection_urn=destination_datahub_instance,
        destination_graph=destination_graph_mock,
    )
    source_graph.get_aspect.return_value = ShareClass(
        lastShareResults=[
            ShareResultClass(
                destination="urn:li:dataHubConnection:1",
                implicitShareEntity=None,
                created=models.AuditStampClass(
                    time=1649980800000,  # frozen time
                    actor="urn:li:corpuser:testUser",
                ),
                lastAttempt=models.AuditStampClass(
                    time=1649980800000,  # frozen time
                    actor="urn:li:corpuser:testUser",
                ),
                status=models.ShareResultStateClass.SUCCESS,
                lastAttemptRequestId="ce3566fd-fa7f-45a2-957b-43a19f5341ff",
                shareConfig=models.ShareConfigClass(
                    enableUpstreamLineage=True,
                    enableDownstreamLineage=True,
                ),
                lastSuccess=models.AuditStampClass(
                    time=1649980800000,  # frozen time
                    actor="urn:li:corpuser:testUser",
                ),
                statusLastUpdated=1649980800000,
            )
        ]
    )
    lineage_direction = share_agent.get_lineage_direction_from_share_aspect(
        urn_to_unshare
    )
    assert lineage_direction == LineageDirection.BOTH


client = TestClient(server.app)


@mock.patch("datahub_integrations.share.share_agent.ShareAgent")
def test_share_without_lineage(share_agent_mock: Mock) -> None:
    share_agent_mock = MagicMock()
    share_router.get_or_create_share_agent = (
        lambda share_connection_urn: share_agent_mock
    )

    response = client.post(
        "/private/share/execute_share",
        params={
            "entity_urn": "urn:li:dataset:1",
            "sharer_urn": "urn:li:corpuser:testUser",
            "share_connection_urn": "urn:li:dataHubConnection:1",
        },
    )
    assert response.status_code == 200
    share_agent_mock.emit_share_result.assert_called_once()
    share_result_call = share_agent_mock.emit_share_result.call_args_list[0].kwargs
    share_result_call["share_config"].enableUpstreamLineage = False
    share_result_call["share_config"].enableDownstreamLineage = False
    resp_dict = response.json()
    assert resp_dict["status"] == "success"


@mock.patch("datahub_integrations.share.share_agent.ShareAgent")
def test_share_with_lineage(share_agent_mock: Mock) -> None:
    share_agent_mock = MagicMock()
    share_router.get_or_create_share_agent = (
        lambda share_connection_urn: share_agent_mock
    )

    response = client.post(
        "/private/share/execute_share",
        params={
            "entity_urn": "urn:li:dataset:1",
            "sharer_urn": "urn:li:corpuser:testUser",
            "share_connection_urn": "urn:li:dataHubConnection:1",
            "lineage_direction": "BOTH",
        },
    )
    assert response.status_code == 200
    share_agent_mock.emit_share_result.assert_called_once()
    share_result_call = share_agent_mock.emit_share_result.call_args_list[0].kwargs
    share_result_call["share_config"].enableUpstreamLineage = True
    share_result_call["share_config"].enableDownstreamLineage = True
    resp_dict = response.json()
    assert resp_dict["status"] == "success"


@mock.patch("datahub_integrations.share.share_agent.ShareAgent")
def test_share_with_lineage_from_resync(share_agent_mock: Mock) -> None:
    share_agent_mock = MagicMock()
    share_router.get_or_create_share_agent = (
        lambda share_connection_urn: share_agent_mock
    )
    share_agent_mock.get_lineage_direction_from_share_aspect.return_value = (
        LineageDirection.DOWNSTREAM
    )

    response = client.post(
        "/private/share/execute_share",
        params={
            "entity_urn": "urn:li:dataset:1",
            "sharer_urn": "urn:li:corpuser:testUser",
            "share_connection_urn": "urn:li:dataHubConnection:1",
        },
    )
    assert response.status_code == 200

    share_agent_mock.get_lineage_direction_from_share_aspect.assert_called_once()
    assert (
        share_agent_mock.get_lineage_direction_from_share_aspect.call_args_list[0][0][0]
        == "urn:li:dataset:1"
    )

    share_agent_mock.emit_share_result.assert_called_once()
    share_result_call = share_agent_mock.emit_share_result.call_args_list[0].kwargs
    share_result_call["share_config"].enableUpstreamLineage = False
    share_result_call["share_config"].enableDownstreamLineage = True
    resp_dict = response.json()
    assert resp_dict["status"] == "success"
