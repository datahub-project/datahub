import pathlib

import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-01-01 00:00:00"

_RESOURCES_DIR = pathlib.Path(__file__).parent
_BASE_URL = "https://ems.example.com:8080"

_QUEUES = [
    {
        "name": "orders.new",
        "global": True,
        "maxMsgs": 1000,
        "consumerCount": 2,
        "server_group": "group1",
        "server_role": "primary",
    },
    {"name": "orders.processed", "server_group": "group1"},
    {"name": "$sys.admin", "server_group": "group1"},
    # Same destination name in a second server group: an independent queue.
    {"name": "orders.new", "server_group": "group2"},
]
_TOPICS = [
    {"name": "events.audit", "secure": True, "server_group": "group1"},
    # Shares a name with a queue to exercise collision-free naming.
    {"name": "orders.new", "server_group": "group1"},
]
_BRIDGES = [
    {
        "name": "orders.new",
        "type": "queue",
        "server_group": "group1",
        "targets": [{"name": "events.audit", "type": "topic"}],
    }
]


def _envelope(key: str, records: list, next_cursor: str = "") -> dict:
    # The on-prem REST Proxy wraps records in an envelope whose "errors" array is
    # serialised first, and pages the remainder behind the "next" cursor.
    return {
        "errors": [],
        "first": "cursor-first",
        "last": "cursor-last",
        "next": next_cursor,
        "prev": "",
        "self": "cursor-self",
        key: records,
    }


@time_machine.travel(FROZEN_TIME, tick=False)
def test_tibco_ems_ingest(pytestconfig, tmp_path, requests_mock):
    requests_mock.post(f"{_BASE_URL}/connect", json={})
    # Queues arrive over two pages so the cursor walk is exercised end to end.
    requests_mock.get(
        f"{_BASE_URL}/system/ems/queues",
        [
            {"json": _envelope("queues", _QUEUES[:2], next_cursor="queues-page-2")},
            {"json": _envelope("queues", _QUEUES[2:])},
        ],
    )
    requests_mock.get(
        f"{_BASE_URL}/system/ems/topics", json=_envelope("topics", _TOPICS)
    )
    requests_mock.get(
        f"{_BASE_URL}/system/ems/configuration/bridges",
        json=_envelope("bridges", _BRIDGES),
    )

    output_path = tmp_path / "tibco_ems_mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "tibco-ems-test",
            "source": {
                "type": "tibco-ems",
                "config": {
                    "base_url": _BASE_URL,
                    "username": "admin",
                    "password": "admin",
                    "stateful_ingestion": {"enabled": False},
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": str(output_path)},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig=pytestconfig,
        output_path=output_path,
        golden_path=_RESOURCES_DIR / "tibco_ems_mces_golden.json",
    )
