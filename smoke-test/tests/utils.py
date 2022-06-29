import json
import os
from typing import Any

import requests
from datahub.cli import cli_utils
from datahub.ingestion.run.pipeline import Pipeline


def get_gms_url():
    return os.getenv("DATAHUB_GMS_URL") or "http://localhost:8080"


def get_frontend_url():
    return os.getenv("DATAHUB_FRONTEND_URL") or "http://localhost:9002"


def get_kafka_broker_url():
    return os.getenv("DATAHUB_KAFKA_URL") or "localhost:9092"


def get_sleep_info():
    return (
        os.environ.get("DATAHUB_TEST_SLEEP_BETWEEN") or 60,
        os.environ.get("DATAHUB_TEST_SLEEP_TIMES") or 5,
    )


def ingest_file_via_rest(filename: str) -> Any:
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "file",
                "config": {"filename": filename},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": get_gms_url()},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    return pipeline


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
                get_gms_url() + "/entities?action=delete",
                payload_obj,
            )
