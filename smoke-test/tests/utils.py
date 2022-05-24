import json
from datetime import datetime, timedelta

import requests
from datahub.cli import cli_utils
from datahub.ingestion.run.pipeline import Pipeline

GMS_ENDPOINT = "http://localhost:8080"
FRONTEND_ENDPOINT = "http://localhost:9002"


def ingest_file_via_rest(filename: str) -> Pipeline:
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
                GMS_ENDPOINT + "/entities?action=delete",
                payload_obj,
            )


# Fixed now value
NOW: datetime = datetime.now()


def get_timestampmillis_at_start_of_day(relative_day_num: int) -> int:
    """
    Returns the time in milliseconds from epoch at the start of the day
    corresponding to `now + relative_day_num`

    """
    time: datetime = NOW + timedelta(days=float(relative_day_num))
    time = datetime(
        year=time.year,
        month=time.month,
        day=time.day,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return int(time.timestamp() * 1000)


def get_strftime_from_timestamp_millis(ts_millis: int) -> str:
    return datetime.fromtimestamp(ts_millis / 1000).strftime("%Y-%m-%d %H:%M:%S")
