# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState


def test_kafka_common_state() -> None:
    state1 = GenericCheckpointState()
    test_topic_urn = make_dataset_urn("kafka", "test_topic1", "test")
    state1.add_checkpoint_urn(type="topic", urn=test_topic_urn)

    state2 = GenericCheckpointState()

    topic_urns_diff = list(
        state1.get_urns_not_in(type="topic", other_checkpoint_state=state2)
    )
    assert len(topic_urns_diff) == 1 and topic_urns_diff[0] == test_topic_urn


def test_kafka_state_migration() -> None:
    state = GenericCheckpointState.model_validate(
        {
            "encoded_topic_urns": [
                "kafka||test_topic1||test",
                "kafka||topic_2||DEV",
            ]
        }
    )
    assert state.urns == [
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test_topic1,TEST)",
        "urn:li:dataset:(urn:li:dataPlatform:kafka,topic_2,DEV)",
    ]
