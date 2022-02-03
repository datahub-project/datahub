import json
from typing import Any, Dict, List, cast

import requests
from confluent_kafka.admin import AdminClient, NewTopic
from datahub.cli import cli_utils
from datahub.ingestion.api.committable import StatefulCommittable
from datahub.ingestion.run.pipeline import Pipeline

GMS_ENDPOINT = "http://localhost:8080"
FRONTEND_ENDPOINT = "http://localhost:9002"
KAFKA_BROKER = "localhost:9092"


def ingest_file_via_rest(filename: str) -> None:
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "file",
                "config": {"filename": filename},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": GMS_ENDPOINT},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()


def delete_urns_from_file(filename: str) -> None:
    session = requests.Session()
    session.headers.update(
        {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
    )

    with open(filename) as f:
        d = json.load(f)
        for entry in d:
            is_mcp = "entityUrn" in entry
            urn = None
            # Kill Snapshot
            if is_mcp:
                urn = entry["entityUrn"]
            else:
                snapshot_union = entry["proposedSnapshot"]
                snapshot = list(snapshot_union.values())[0]
                urn = snapshot["urn"]
            payload_obj = {"urn": urn}

            cli_utils.post_delete_endpoint_with_session_and_url(
                session,
                GMS_ENDPOINT + "/entities?action=delete",
                payload_obj,
            )


def run_and_get_pipeline(pipeline_config_dict: Dict[str, Any]) -> Pipeline:
    pipeline = Pipeline.create(pipeline_config_dict)
    pipeline.run()
    pipeline.raise_from_status()
    return pipeline


def validate_all_providers_have_committed_successfully(
    pipeline: Pipeline, expected_providers: int
) -> None:
    provider_count: int = 0
    for _, provider in pipeline.ctx.get_committables():
        provider_count += 1
        assert isinstance(provider, StatefulCommittable)
        stateful_committable = cast(StatefulCommittable, provider)
        assert stateful_committable.has_successfully_committed()
        assert stateful_committable.state_to_commit
    assert provider_count == expected_providers


def create_kafka_topics(topics: List[NewTopic]) -> None:
    """
    creates new kafka topics
    """
    admin_config: Dict[str, Any] = {
        "bootstrap.servers": f"{KAFKA_BROKER}",
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


def delete_kafka_topics(topics: List[str]) -> None:
    """
    delete a list of existing Kafka topics
    """
    admin_config: Dict[str, Any] = {
        "bootstrap.servers": f"{KAFKA_BROKER}",
    }
    a = AdminClient(admin_config)

    fs = a.delete_topics(topics, operation_timeout=3)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))
