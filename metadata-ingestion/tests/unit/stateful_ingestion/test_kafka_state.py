from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.state.kafka_state import KafkaCheckpointState


def test_kafka_common_state() -> None:
    state1 = KafkaCheckpointState()
    test_topic_urn = make_dataset_urn("kafka", "test_topic1", "test")
    state1.add_topic_urn(test_topic_urn)

    state2 = KafkaCheckpointState()

    topic_urns_diff = list(state1.get_topic_urns_not_in(state2))
    assert len(topic_urns_diff) == 1 and topic_urns_diff[0] == test_topic_urn
