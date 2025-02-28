import time
from typing import Any, Dict, List
from unittest.mock import patch

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from freezegun import freeze_time

from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2020-04-14 07:00:00"
KAFKA_PORT = 29092
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
                print(f"Topic {topic} created")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
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
                print(f"Topic {topic} deleted")
            except Exception as e:
                # this error should be ignored when we already deleted
                # the topic within the test code
                print(f"Failed to delete topic {topic}: {e}")

    def __enter__(self):
        topics = [
            NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
            for topic_name in self.topics
        ]
        self.create_kafka_topics(topics)
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.delete_kafka_topics(self.topics)


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
                "fail_safe_threshold": 100.0,
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
                }
            ],
        }

        # topics will be automatically created and deleted upon test completion
        with KafkaTopicsCxtManager(
            topic_names, KAFKA_BOOTSTRAP_SERVER
        ) as kafka_ctx, patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint:
            # both checkpoint and reporting will use the same mocked graph instance
            mock_checkpoint.return_value = mock_datahub_graph

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
            state1 = checkpoint1.state
            state2 = checkpoint2.state
            difference_urns = list(
                state1.get_urns_not_in(type="topic", other_checkpoint_state=state2)
            )

            assert len(difference_urns) == 1
            assert (
                difference_urns[0]
                == f"urn:li:dataset:(urn:li:dataPlatform:kafka,{platform_instance}.{kafka_ctx.topics[0]},PROD)"
            )

            # 4. Validate that all providers have committed successfully.
            # NOTE: The following validation asserts for presence of state as well
            # and validates reporting.
            validate_all_providers_have_committed_successfully(
                pipeline=pipeline_run1, expected_providers=1
            )
            validate_all_providers_have_committed_successfully(
                pipeline=pipeline_run1, expected_providers=1
            )
