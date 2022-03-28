import time
from typing import Any, Dict, List, Optional, cast
from unittest.mock import patch

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.kafka import KafkaSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.kafka_state import KafkaCheckpointState
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.state_helpers import (
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
KAFKA_PORT = 59092
KAFKA_BOOTSTRAP_SERVER = f"localhost:{KAFKA_PORT}"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


class KafkaTopicsCxtManager:
    def __init__(self, topics: List[str], bootstrap_servers: str) -> None:
        self.topics = topics
        self.bootstrap_servers = bootstrap_servers

    def create_kafka_topics(self, topics: List[NewTopic]) -> None:
        """
        creates new kafka topics
        """
        admin_config: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        a = AdminClient(admin_config)

        fs = a.create_topics(topics, operation_timeout=3)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
                raise e

    def delete_kafka_topics(self, topics: List[str]) -> None:
        """
        delete a list of existing Kafka topics
        """
        admin_config: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
        }
        a = AdminClient(admin_config)

        fs = a.delete_topics(topics, operation_timeout=3)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                # this error should be ignored when we already deleted
                # the topic within the test code
                print("Failed to delete topic {}: {}".format(topic, e))

    def __enter__(self):
        topics = [
            NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
            for topic_name in self.topics
        ]
        self.create_kafka_topics(topics)
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.delete_kafka_topics(self.topics)


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint]:
    kafka_source = cast(KafkaSource, pipeline.source)
    return kafka_source.get_current_checkpoint(
        kafka_source.get_default_ingestion_job_id()
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_ingest_with_stateful(
    docker_compose_runner, pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka"
    topic_prefix: str = "stateful_ingestion_test"
    topic_names: List[str] = [f"{topic_prefix}_t1", f"{topic_prefix}_t2"]
    platform_instance = "test_platform_instance_1"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka"
    ) as docker_services:

        wait_for_port(docker_services, "test_broker", KAFKA_PORT, timeout=120)
        wait_for_port(docker_services, "test_schema_registry", 8081, timeout=120)

        source_config_dict: Dict[str, Any] = {
            "connection": {
                "bootstrap": KAFKA_BOOTSTRAP_SERVER,
            },
            "platform_instance": f"{platform_instance}",
            # enable stateful ingestion
            "stateful_ingestion": {
                "enabled": True,
                "remove_stale_metadata": True,
                "state_provider": {
                    "type": "datahub",
                    "config": {"datahub_api": {"server": GMS_SERVER}},
                },
            },
        }

        pipeline_config_dict: Dict[str, Any] = {
            "source": {
                "type": "kafka",
                "config": source_config_dict,
            },
            "sink": {
                # we are not really interested in the resulting events for this test
                "type": "console"
            },
            "pipeline_name": "test_pipeline",
            # enable reporting
            "reporting": [
                {
                    "type": "datahub",
                    "config": {"datahub_api": {"server": GMS_SERVER}},
                }
            ],
        }

        # topics will be automatically created and deleted upon test completion
        with KafkaTopicsCxtManager(
            topic_names, KAFKA_BOOTSTRAP_SERVER
        ) as kafka_ctx, patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint, patch(
            "datahub.ingestion.reporting.datahub_ingestion_reporting_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_reporting:

            # both checkpoint and reporting will use the same mocked graph instance
            mock_checkpoint.return_value = mock_datahub_graph
            mock_reporting.return_value = mock_datahub_graph

            # 1. Do the first run of the pipeline and get the default job's checkpoint.
            pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
            checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

            assert checkpoint1
            assert checkpoint1.state

            # 2. Drop the first topic created during step 1 + rerun the pipeline and get the checkpoint state.
            kafka_ctx.delete_kafka_topics([kafka_ctx.topics[0]])
            # sleep to guarantee eventual consistency for kafka topic deletion
            time.sleep(1)
            pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
            checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

            assert checkpoint2
            assert checkpoint2.state

            # 3. Perform all assertions on the states. The deleted topic should not be
            #    part of the second state
            state1 = cast(KafkaCheckpointState, checkpoint1.state)
            state2 = cast(KafkaCheckpointState, checkpoint2.state)
            difference_urns = list(state1.get_topic_urns_not_in(state2))

            assert len(difference_urns) == 1
            assert (
                difference_urns[0]
                == f"urn:li:dataset:(urn:li:dataPlatform:kafka,{platform_instance}.{kafka_ctx.topics[0]},PROD)"
            )

            # 4. Checkpoint configuration should be the same.
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
