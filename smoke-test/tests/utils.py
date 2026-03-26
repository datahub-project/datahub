import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import click
import click.testing
import requests
import tenacity
from joblib import Parallel, delayed
from packaging import version
from requests.structures import CaseInsensitiveDict

from datahub.cli import cli_utils, env_utils
from datahub.entrypoints import datahub
from datahub.ingestion.run.pipeline import Pipeline
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities import env_vars

TIME: int = 1581407189000
logger = logging.getLogger(__name__)


def sync_elastic() -> None:
    wait_for_writes_to_sync()


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
        env_vars.get_admin_username(),
        env_vars.get_admin_password(),
    )


def get_base_path():
    base_path = env_vars.get_base_path()
    return "" if base_path == "/" else base_path


def get_gms_base_path():
    base_gms_path = env_vars.get_gms_base_path()
    return "" if base_gms_path == "/" else base_gms_path


def get_root_urn():
    return "urn:li:corpuser:datahub"


def get_gms_url():
    return env_vars.get_gms_url() or f"http://localhost:8080{get_gms_base_path()}"


def get_frontend_url():
    return env_vars.get_frontend_url() or f"http://localhost:9002{get_base_path()}"


def get_kafka_broker_url():
    return env_vars.get_kafka_url() or "localhost:9092"


def get_kafka_schema_registry():
    #  internal registry "http://localhost:8080/schema-registry/api/"
    return (
        env_vars.get_kafka_schema_registry_url()
        or f"http://localhost:8080{get_gms_base_path()}/schema-registry/api"
    )


def get_db_type():
    db_type = env_vars.get_db_type()
    if db_type:
        return db_type
    else:
        # infer from profile
        profile_name = env_vars.get_profile_name()
        if profile_name and "postgres" in profile_name:
            return "postgres"
        else:
            return "mysql"


def get_db_url():
    if get_db_type() == "mysql":
        return env_vars.get_mysql_url()
    else:
        return env_vars.get_postgres_url()


def get_db_username():
    if get_db_type() == "mysql":
        return env_vars.get_mysql_username()
    else:
        return env_vars.get_postgres_username()


def get_db_password():
    if get_db_type() == "mysql":
        return env_vars.get_mysql_password()
    else:
        return env_vars.get_postgres_password()


def get_sleep_info() -> Tuple[int, int]:
    return (
        env_vars.get_test_sleep_between(),
        env_vars.get_test_sleep_times(),
    )


def with_test_retry(
    max_attempts: Optional[int] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that retries with environment-based sleep settings from get_sleep_info().

    Returns a configured tenacity.retry decorator using DATAHUB_TEST_SLEEP_BETWEEN
    and DATAHUB_TEST_SLEEP_TIMES environment variables for eventual consistency.

    Args:
        max_attempts: Optional maximum number of retry attempts. If not provided,
                     uses DATAHUB_TEST_SLEEP_TIMES environment variable (default 3).

    Usage:
        @with_test_retry()
        def test_function():
            # Function will retry with configured sleep settings
            ...

        @with_test_retry(max_attempts=10)
        def test_with_more_retries():
            # Function will retry up to 10 times
            ...
    """
    sleep_sec, sleep_times = get_sleep_info()
    retry_count = max_attempts if max_attempts is not None else sleep_times
    return tenacity.retry(
        stop=tenacity.stop_after_attempt(retry_count),
        wait=tenacity.wait_fixed(sleep_sec),
        reraise=True,
    )


def is_k8s_enabled():
    return env_vars.get_k8s_cluster_enabled()


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


def delete_entity(auth_session, urn: str) -> None:
    delete_json = {"urn": urn}
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=delete", json=delete_json
    )

    response.raise_for_status()


def execute_graphql(
    auth_session,
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    expect_errors: bool = False,
) -> Dict[str, Any]:
    """Execute a GraphQL query with standard error handling.

    Args:
        auth_session: Authenticated session for making requests
        query: GraphQL query string
        variables: Optional dictionary of GraphQL variables

    Returns:
        Response data dictionary

    Example:
        >>> query = "query getDataset($urn: String!) { dataset(urn: $urn) { name } }"
        >>> variables = {"urn": "urn:li:dataset:(...)"}
        >>> res_data = execute_graphql(auth_session, query, variables)
        >>> dataset_name = res_data["data"]["dataset"]["name"]
    """
    json_payload: Dict[str, Any] = {"query": query}
    if variables:
        json_payload["variables"] = variables

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json_payload
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data, "GraphQL response is empty"
    assert res_data.get("data") is not None, "GraphQL response.data is None"
    assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"

    return res_data


def run_datahub_cmd(
    command: List[str],
    *,
    input: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> click.testing.Result:
    # TODO: Unify this with the run_datahub_cmd in the metadata-ingestion directory.
    click_version: str = click.__version__  # type: ignore
    if version.parse(click_version) >= version.parse("8.2.0"):
        runner = click.testing.CliRunner()
    else:
        # Once we're pinned to click >= 8.2.0, we can remove this.
        runner = click.testing.CliRunner(mix_stderr=False)  # type: ignore
    return runner.invoke(datahub, command, input=input, env=env)


def ingest_file_via_rest(
    auth_session, filename: str, mode: str = "ASYNC_BATCH"
) -> Pipeline:
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
                    "mode": mode,
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
        logger.info("Not cleaning data to save time")
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
    logger.info(f"Creating dataHubStepState aspect for {entity_urn}")
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
                    else:
                        # Clone the headers dict to prevent mutation of caller's data
                        # This fixes test pollution where shared header dicts get contaminated
                        kwargs["headers"] = dict(kwargs["headers"])

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
            logger.info("TestSessionWrapper sync wait.")
            wait_for_writes_to_sync()

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
        retry=tenacity.retry_if_exception_type(Exception),
        reraise=True,
    )
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
            # Clear the token ID after successful revocation to prevent double-call issues
            self._gms_token_id = None
