import time
from typing import Any, Dict, List, Optional, cast

import pytest
from confluent_kafka.admin import NewTopic
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.kafka import KafkaSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.kafka_state import KafkaCheckpointState

from tests.utils import (create_kafka_topics, delete_kafka_topics,
                         run_and_get_pipeline,
                         validate_all_providers_have_committed_successfully)

GMS_ENDPOINT = "http://localhost:8080"
FRONTEND_ENDPOINT = "http://localhost:9002"
KAFKA_BROKER = "localhost:9092"


@pytest.fixture(scope="function")
def kafka_topics(request):
    topic_prefix: str = "stateful_ingestion_test"
    topic_names: List[str] = [f"{topic_prefix}_t1", f"{topic_prefix}_t2"]
    topics: List[NewTopic] = list()

    # Create a couple of topics
    for topic_name in topic_names:
        topics.append(
            NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
        )

    create_kafka_topics(topics)
    # Cleanup topics once the test is done
    request.addfinalizer(lambda: delete_kafka_topics(topic_names))
    return topic_names


def test_stateful_ingestion_kafka(wait_for_healthchecks, kafka_topics):
    def get_current_checkpoint_from_pipeline(
        pipeline: Pipeline,
    ) -> Optional[Checkpoint]:
        kafka_source = cast(KafkaSource, pipeline.source)
        return kafka_source.get_current_checkpoint(
            kafka_source.get_default_ingestion_job_id()
        )

    PLATFORM_INSTANCE = "smoke_test"

    source_config_dict: Dict[str, Any] = {
        "connection": {
            "bootstrap": f"{KAFKA_BROKER}",
        },
        "platform_instance": f"{PLATFORM_INSTANCE}",
        "topic_patterns": {"allow": ["stateful_ingestion_test.*"]},
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_ENDPOINT}},
            },
        },
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "kafka",
            "config": source_config_dict,
        },
        "sink": {
            "type": "datahub-rest",
            "config": {"server": GMS_ENDPOINT},
        },
        "pipeline_name": "kafka_stateful_ingestion_smoke_test_pipeline",
        "reporting": [
            {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_ENDPOINT}},
            }
        ],
    }

    # 1. Do the first run of the pipeline and get the default job's checkpoint.
    pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

    assert checkpoint1
    assert checkpoint1.state

    # 2. remove the first of the two topics
    delete_kafka_topics([kafka_topics[0]])
    # sleep to guarantee eventual consistency
    time.sleep(1)

    pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

    assert checkpoint2
    assert checkpoint2.state

    # 3. Perform all assertions on the states
    state1 = cast(KafkaCheckpointState, checkpoint1.state)
    state2 = cast(KafkaCheckpointState, checkpoint2.state)
    difference_urns = list(state1.get_topic_urns_not_in(state2))

    assert len(difference_urns) == 1
    assert (
        difference_urns[0]
        == f"urn:li:dataset:(urn:li:dataPlatform:kafka,{PLATFORM_INSTANCE}.{kafka_topics[0]},PROD)"
    )

    # 4. Perform all assertions on the config.
    assert checkpoint1.config == checkpoint2.config

    # 5. Validate that all providers have committed successfully.
    # NOTE: The following validation asserts for presence of state as well
    # and validates reporting.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=2
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=2
    )
