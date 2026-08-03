from datahub.ingestion.source.tibco_ems.models import (
    BridgeTarget,
    DestinationType,
    TibcoBridge,
    TibcoDestination,
)


def test_destination_type_parse_case_insensitive() -> None:
    assert DestinationType.parse("Queue") is DestinationType.QUEUE
    assert DestinationType.parse(" TOPIC ") is DestinationType.TOPIC
    assert DestinationType.parse("channel") is None


def test_destination_aliases_populate_from_json() -> None:
    destination = TibcoDestination.model_validate(
        {
            "name": "orders.new",
            "destination_type": DestinationType.QUEUE,
            "global": True,
            "maxMsgs": 500,
            "pendingMessageCount": 3,
        }
    )
    assert destination.is_global is True
    assert destination.max_msgs == 500
    assert destination.pending_message_count == 3


def test_bridge_target_type_normalised() -> None:
    target = BridgeTarget.model_validate({"name": "t", "type": "Topic"})
    assert target.destination_type is DestinationType.TOPIC

    unknown = BridgeTarget.model_validate({"name": "t", "type": "cluster"})
    assert unknown.destination_type is None


def test_bridge_parses_source_and_targets() -> None:
    bridge = TibcoBridge.model_validate(
        {
            "name": "orders.new",
            "type": "queue",
            "targets": [{"name": "events.audit", "type": "topic"}],
        }
    )
    assert bridge.source_name == "orders.new"
    assert bridge.source_type is DestinationType.QUEUE
    assert bridge.targets[0].name == "events.audit"
    assert bridge.targets[0].destination_type is DestinationType.TOPIC
