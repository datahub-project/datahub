import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import requests
from joblib import Parallel, delayed
from requests.structures import CaseInsensitiveDict

from datahub.cli import cli_utils, env_utils
from datahub.ingestion.run.pipeline import Pipeline
from tests.consistency_utils import wait_for_writes_to_sync

TIME: int = 1581407189000
logger = logging.getLogger(__name__)


def get_frontend_session():
    username, password = get_admin_credentials()
    return login_as(username, password)


def login_as(username: str, password: str):
    return cli_utils.get_frontend_session_login_as(
        username=username, password=password, frontend_url=get_frontend_url()
    )


def get_admin_username() -> str:
    return get_admin_credentials()[0]


def get_admin_credentials():
    return (
        os.getenv("ADMIN_USERNAME", "datahub"),
        os.getenv("ADMIN_PASSWORD", "datahub"),
    )


def get_root_urn():
    return "urn:li:corpuser:datahub"


def get_gms_url():
    return os.getenv("DATAHUB_GMS_URL") or "http://localhost:8080"


def get_frontend_url():
    return os.getenv("DATAHUB_FRONTEND_URL") or "http://localhost:9002"


def get_kafka_broker_url():
    return os.getenv("DATAHUB_KAFKA_URL") or "localhost:9092"


def get_kafka_schema_registry():
    #  internal registry "http://localhost:8080/schema-registry/api/"
    return os.getenv("DATAHUB_KAFKA_SCHEMA_REGISTRY_URL") or "http://localhost:8081"


def get_mysql_url():
    return os.getenv("DATAHUB_MYSQL_URL") or "localhost:3306"


def get_mysql_username():
    return os.getenv("DATAHUB_MYSQL_USERNAME") or "datahub"


def get_mysql_password():
    return os.getenv("DATAHUB_MYSQL_PASSWORD") or "datahub"


def get_sleep_info() -> Tuple[int, int]:
    return (
        int(os.getenv("DATAHUB_TEST_SLEEP_BETWEEN", 20)),
        int(os.getenv("DATAHUB_TEST_SLEEP_TIMES", 3)),
    )


def is_k8s_enabled():
    return os.getenv("K8S_CLUSTER_ENABLED", "false").lower() in ["true", "yes"]


def wait_for_healthcheck_util(auth_session):
    assert not check_endpoint(auth_session, f"{get_frontend_url()}/admin")
    assert not check_endpoint(auth_session, f"{get_gms_url()}/health")


def check_endpoint(auth_session, url):
    try:
        get = auth_session.get(url)
        if get.status_code == 200:
            return
        else:
            return f"{url}: is Not reachable, status_code: {get.status_code}"
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"{url}: is Not reachable \nErr: {e}")


def ingest_file_via_rest(auth_session, filename: str) -> Pipeline:
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "file",
                "config": {"filename": filename},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": auth_session.gms_url(),
                    "token": auth_session.gms_token(),
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    wait_for_writes_to_sync()
    return pipeline


def delete_urn(graph_client, urn: str) -> None:
    graph_client.hard_delete_entity(urn)


def delete_urns(graph_client, urns: List[str]) -> None:
    for urn in urns:
        delete_urn(graph_client, urn)


def delete_urns_from_file(
    graph_client, filename: str, shared_data: bool = False
) -> None:
    if not env_utils.get_boolean_env_variable("CLEANUP_DATA", True):
        print("Not cleaning data to save time")
        return

    def delete(entry):
        is_mcp = "entityUrn" in entry
        urn = None
        # Kill Snapshot
        if is_mcp:
            urn = entry["entityUrn"]
        else:
            snapshot_union = entry["proposedSnapshot"]
            snapshot = list(snapshot_union.values())[0]
            urn = snapshot["urn"]
        delete_urn(graph_client, urn)

    with open(filename) as f:
        d = json.load(f)
        Parallel(n_jobs=10)(delayed(delete)(entry) for entry in d)

    wait_for_writes_to_sync()


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
    return datetime.fromtimestamp(ts_millis / 1000, tz=timezone.utc).isoformat()


def create_datahub_step_state_aspect(
    username: str, onboarding_id: str
) -> Dict[str, Any]:
    entity_urn = f"urn:li:dataHubStepState:urn:li:corpuser:{username}-{onboarding_id}"
    print(f"Creating dataHubStepState aspect for {entity_urn}")
    return {
        "auditHeader": None,
        "entityType": "dataHubStepState",
        "entityUrn": entity_urn,
        "changeType": "UPSERT",
        "aspectName": "dataHubStepStateProperties",
        "aspect": {
            "value": f'{{"properties":{{}},"lastModified":{{"actor":"urn:li:corpuser:{username}","time":{TIME}}}}}',
            "contentType": "application/json",
        },
        "systemMetadata": None,
    }


def create_datahub_step_state_aspects(
    username: str, onboarding_ids: List[str], onboarding_filename: str
) -> None:
    """
    For a specific user, creates dataHubStepState aspects for each onboarding id in the list
    """
    aspects_dict: List[Dict[str, Any]] = [
        create_datahub_step_state_aspect(username, onboarding_id)
        for onboarding_id in onboarding_ids
    ]
    with open(onboarding_filename, "w") as f:
        json.dump(aspects_dict, f, indent=2)


class TestSessionWrapper:
    """
    Many of the tests do not consider async writes. This
    class intercepts mutations using the requests library
    to simulate sync requests.
    """

    def __init__(self, requests_session):
        self._upstream = requests_session
        self._frontend_url = get_frontend_url()
        self._gms_url = get_gms_url()
        self._gms_token_id, self._gms_token = self._generate_gms_token()

    def __getattr__(self, name):
        # Intercept method calls
        attr = getattr(self._upstream, name)

        if callable(attr):

            def wrapper(*args, **kwargs):
                # Pre-processing can be done here
                if name in ("get", "head", "post", "put", "delete", "option", "patch"):
                    if "headers" not in kwargs:
                        kwargs["headers"] = CaseInsensitiveDict()
                    kwargs["headers"].update(
                        {"Authorization": f"Bearer {self._gms_token}"}
                    )

                result = attr(*args, **kwargs)

                # Post-processing can be done here
                if name in ("post", "put"):
                    # Wait for sync if writing
                    # delete is excluded for efficient test clean-up
                    self._wait(*args, **kwargs)

                return result

            return wrapper

        return attr

    def gms_token(self):
        return self._gms_token

    def gms_token_id(self):
        return self._gms_token_id

    def frontend_url(self):
        return self._frontend_url

    def gms_url(self):
        return self._gms_url

    def _wait(self, *args, **kwargs):
        if "/logIn" not in args[0]:
            print("TestSessionWrapper sync wait.")
            wait_for_writes_to_sync()

    def _generate_gms_token(self):
        actor_urn = self._upstream.cookies["actor"]
        json = {
            "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
                createAccessToken(input: $input) {
                  accessToken
                  metadata {
                    id
                  }
                }
            }""",
            "variables": {
                "input": {
                    "type": "PERSONAL",
                    "actorUrn": actor_urn,
                    "duration": "ONE_DAY",
                    "name": "Test Session Token",
                    "description": "Token generated for smoke-tests",
                }
            },
        }

        response = self._upstream.post(
            f"{self._frontend_url}/api/v2/graphql", json=json
        )
        response.raise_for_status()
        return (
            response.json()["data"]["createAccessToken"]["metadata"]["id"],
            response.json()["data"]["createAccessToken"]["accessToken"],
        )

    def destroy(self):
        if self._gms_token_id:
            json = {
                "query": """mutation revokeAccessToken($tokenId: String!) {
                revokeAccessToken(tokenId: $tokenId)
            }""",
                "variables": {"tokenId": self._gms_token_id},
            }

            response = self._upstream.post(
                f"{self._frontend_url}/api/v2/graphql", json=json
            )
            response.raise_for_status()
