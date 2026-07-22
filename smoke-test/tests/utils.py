import importlib.metadata
import json
import logging
import os
import socket
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse

import click
import click.testing
import requests
import tenacity
from joblib import Parallel, delayed
from packaging import version
from requests.structures import CaseInsensitiveDict

from datahub.cli import cli_utils, env_utils
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.entrypoints import datahub
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sql.sqlalchemy_uri import parse_host_port
from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities import env_vars

TIME: int = 1581407189000
logger = logging.getLogger(__name__)


def sync_elastic() -> None:
    wait_for_writes_to_sync()


_TRANSIENT_LOGIN_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _is_transient_login_error(exception: BaseException) -> bool:
    if isinstance(exception, _TRANSIENT_LOGIN_EXCEPTIONS):
        return True
    if isinstance(exception, requests.HTTPError) and exception.response is not None:
        # 400/429: parallel /logIn under xdist can briefly fail with Bad Request
        # or rate limiting; treat as retryable like 5xx.
        return exception.response.status_code in (400, 429) or (
            exception.response.status_code >= 500
        )
    return False


def get_frontend_session():
    username, password = get_admin_credentials()
    return login_as(username, password)


@tenacity.retry(
    retry=tenacity.retry_if_exception(_is_transient_login_error),
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
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


def get_gms_prometheus_base_url():
    """Base URL for /actuator/prometheus.

    Docker images default to management on :4319 while GMS HTTP stays on :8080; when the GMS URL
    uses port 8080, assume Micrometer is on the same host at 4319 unless DATAHUB_GMS_MANAGEMENT_URL
    is set. For a local GMS with Actuator on the main port only, set DATAHUB_GMS_MANAGEMENT_URL
    to your GMS base URL.
    """
    mgmt = env_vars.get_gms_management_url()
    if mgmt:
        return mgmt.rstrip("/")
    base = get_gms_url().rstrip("/")
    parsed = urlparse(base)
    if parsed.port == 8080 and parsed.hostname is not None:
        return urlunparse((parsed.scheme, f"{parsed.hostname}:4319", "", "", "", ""))
    return base


def _prometheus_actuator_reachable(base_url: str, timeout: float = 2.0) -> bool:
    try:
        response = requests.get(
            f"{base_url.rstrip('/')}/actuator/prometheus", timeout=timeout
        )
        return response.status_code == 200
    except requests.RequestException:
        return False


def resolve_queue_ingest_prometheus_base_url() -> tuple[Optional[str], str]:
    """Resolve Prometheus base URL for queue-path ``metadata_ingest`` metrics.

    Standalone MCE consumer deployments expose metrics on the MCE management port; embedded
    consumer deployments (``quickstart`` without ``quickstart-consumers``) share the GMS JVM and
    fall back to the GMS management URL when no separate MCE actuator is reachable.

    Returns:
        (base_url, source) where source is ``mce``, ``gms``, or ``none``.
    """
    explicit_mce = env_vars.get_mce_management_url()
    if explicit_mce and explicit_mce.strip():
        base = explicit_mce.strip().rstrip("/")
        if _prometheus_actuator_reachable(base):
            return base, "mce"
        return None, "none"

    for candidate in ("http://datahub-mce-consumer:4319",):
        if _prometheus_actuator_reachable(candidate):
            return candidate.rstrip("/"), "mce"

    gms_base = get_gms_prometheus_base_url()
    if gms_base and _prometheus_actuator_reachable(gms_base):
        return gms_base.rstrip("/"), "gms"

    return None, "none"


def get_queue_ingest_prometheus_url() -> tuple[Optional[str], str]:
    """Full ``/actuator/prometheus`` URL for queue ingest metrics, plus source label."""
    base, source = resolve_queue_ingest_prometheus_base_url()
    if base is None:
        return None, source
    return f"{base}/actuator/prometheus", source


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


def _metadata_db_tcp_reachable(host_port: str, default_port: int) -> bool:
    """Return True if ``host[:port]`` accepts a TCP connection (short timeout)."""
    host, port = parse_host_port(host_port, default_port)
    if port is None:
        port = default_port
    try:
        with socket.create_connection((host, int(port)), timeout=0.75):
            return True
    except OSError:
        return False


def get_db_type():
    db_type = env_vars.get_db_type()
    if db_type:
        return db_type
    profile_name = env_vars.get_profile_name()
    if profile_name and "postgres" in profile_name:
        return "postgres"

    pg_url = env_vars.get_postgres_url()
    mysql_url = env_vars.get_mysql_url()
    pg_ok = _metadata_db_tcp_reachable(pg_url, 5432)
    mysql_ok = _metadata_db_tcp_reachable(mysql_url, 3306)

    if pg_ok and not mysql_ok:
        return "postgres"
    if mysql_ok and not pg_ok:
        return "mysql"
    if pg_ok and mysql_ok:
        logger.warning(
            "Both postgres (%s) and mysql (%s) ports accept connections; "
            "using postgres. Set DB_TYPE=mysql or DB_TYPE=postgres to override.",
            pg_url,
            mysql_url,
        )
        return "postgres"

    logger.warning(
        "Neither postgres (%s) nor mysql (%s) responded to a TCP probe; "
        "defaulting to mysql. Set DB_TYPE=postgres for PostgreSQL-only quickstart.",
        pg_url,
        mysql_url,
    )
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


class AdminCorpUserBootstrapError(Exception):
    """Admin corpUserInfo not yet visible or missing root-user bootstrap flags."""


class AdminCorpUserInfoMutatedError(AssertionError):
    """A smoke test overwrote privileged admin corpUserInfo fields."""


_ADMIN_CORPUSER_PRIVILEGED_FLAGS = ("system", "isSupportUser")


def get_admin_corpuser_urn() -> str:
    return f"urn:li:corpuser:{get_admin_username()}"


def fetch_admin_corpuser_info(auth_session) -> Dict[str, Any]:
    """Return corpUserInfo.value for the configured smoke admin."""
    expected_urn = get_admin_corpuser_urn()
    encoded = quote(expected_urn, safe="")
    response = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v3/entity/corpuser/{encoded}"
    )
    if response.status_code != 200:
        raise AdminCorpUserBootstrapError(
            f"Admin corpuser not readable: HTTP {response.status_code} for {expected_urn}"
        )
    corp_user_info = response.json().get("corpUserInfo")
    if not corp_user_info or corp_user_info.get("value") is None:
        raise AdminCorpUserBootstrapError(
            f"corpUserInfo aspect missing for {expected_urn}"
        )
    return dict(corp_user_info.get("value", {}))


def assert_admin_corpuser_info_preserved(
    auth_session,
    baseline: Dict[str, Any],
    *,
    context: str = "",
) -> None:
    """Fail if a test cleared privileged corpUserInfo flags on the smoke admin."""
    current = fetch_admin_corpuser_info(auth_session)
    cleared = {
        flag: baseline.get(flag)
        for flag in _ADMIN_CORPUSER_PRIVILEGED_FLAGS
        if baseline.get(flag) and not current.get(flag)
    }
    if cleared:
        suffix = f" after {context}" if context else ""
        raise AdminCorpUserInfoMutatedError(
            f"Privileged admin corpUserInfo flags were cleared{suffix}: "
            f"cleared={cleared!r} baseline={baseline!r} current={current!r}"
        )


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(AdminCorpUserBootstrapError),
    stop=tenacity.stop_after_attempt(40),
    wait=tenacity.wait_fixed(3),
    reraise=True,
)
def wait_for_admin_corpuser_system_bootstrap(auth_session) -> None:
    """Wait until the configured smoke admin has corpUserInfo with system=true.

    Quickstart relies on the root-user bootstrap MCP; GMS health can be ready before
    that aspect is readable on the entity API (or before the session actor matches).
    """
    expected_urn = get_admin_corpuser_urn()
    me = execute_graphql(auth_session, "query { me { corpUser { urn } } }")
    actual_urn = me["data"]["me"]["corpUser"]["urn"]
    if actual_urn != expected_urn:
        raise AdminCorpUserBootstrapError(
            f"Session corp user {actual_urn} != configured admin {expected_urn}"
        )

    info = fetch_admin_corpuser_info(auth_session)
    if not info.get("system"):
        raise AdminCorpUserBootstrapError(
            f"corpUserInfo.system is not true for {expected_urn}: {info!r}"
        )

    logger.info("Verified admin corpUserInfo bootstrap: %s system=true", expected_urn)


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
    if not expect_errors:
        assert res_data.get("data") is not None, (
            f"GraphQL response.data is None. Errors: {res_data.get('errors')}"
        )
        assert "errors" not in res_data, f"GraphQL errors: {res_data.get('errors')}"

    return res_data


def run_datahub_cmd(
    command: List[str],
    *,
    input: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> click.testing.Result:
    # TODO: Unify this with the run_datahub_cmd in the metadata-ingestion directory.
    click_version: str = importlib.metadata.version("click")
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


def unique_suffix() -> str:
    """Return a short, collision-resistant suffix for test-scoped entity names.

    Under pytest-xdist ``--dist=loadscope`` (see smoke-test/smoke.sh) different
    test modules run concurrently against the SAME GMS, so any entity URN
    hardcoded in two modules races and flakes. Append this suffix to
    the name of an entity a test creates/mutates/deletes so no other test —
    now or later — can collide with it. Mirrors the uuid4-hex slice already used
    in tests/metrics/test_queue_ingest_usage.py.
    """
    return uuid.uuid4().hex[:8]


def unique_dataset_urn(name: str, platform: str = "kafka", env: str = "PROD") -> str:
    """Build a dataset URN whose name is unique per test run.

    Prefer this over a hardcoded dataset URN whenever a test creates and then
    mutates or deletes the dataset. ``name`` is a human-readable prefix; a unique
    suffix (see :func:`unique_suffix`) is appended so parallel modules never
    share the URN.
    """
    return make_dataset_urn(
        platform=platform, name=f"{name}-{unique_suffix()}", env=env
    )


def materialize_with_unique_name(
    src_file: str, name: str, dest_dir: str
) -> Tuple[str, str]:
    """Copy ``src_file`` with every occurrence of ``name`` rewritten to a
    run-unique ``name-<suffix>``. Returns ``(dest_file, unique_name)``.

    Building the entity URN (dataset, tag, …) from ``unique_name`` is the
    caller's job. Substitution is a plain string replace over the whole file,
    so ``name`` must be a token that appears ONLY where a rename is intended —
    e.g. an entity key embedded in URNs. Do not pass a value that could also
    occur in a description or other free-text field, or it will be corrupted.
    """
    unique_name = f"{name}-{unique_suffix()}"
    with open(src_file) as f:
        content = f.read().replace(name, unique_name)
    dest_file = os.path.join(str(dest_dir), os.path.basename(src_file))
    with open(dest_file, "w") as f:
        f.write(content)
    return dest_file, unique_name


def materialize_unique_dataset(
    src_file: str,
    dataset_name: str,
    dest_dir: str,
    platform: str = "kafka",
    env: str = "PROD",
) -> Tuple[str, str]:
    """Copy ``src_file`` with ``dataset_name`` rewritten to a run-unique name.

    Returns ``(dest_file, dataset_urn)``. Use from a module-scoped fixture so a
    file-driven test owns a dataset no other test module can collide with under
    xdist ``--dist=loadscope``. Every occurrence of ``dataset_name`` in the file
    is replaced, so field-level and reference URNs stay consistent with the
    returned dataset URN.
    """
    dest_file, unique_name = materialize_with_unique_name(
        src_file, dataset_name, dest_dir
    )
    return dest_file, make_dataset_urn(platform=platform, name=unique_name, env=env)


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

    Two construction modes:

    1. Login-based (default): provide a ``requests_session`` obtained via
       ``get_frontend_session()``.  A short-lived GMS token is minted via GraphQL
       and revoked on ``destroy()``.

    2. Token-based: pass ``prebuilt_token`` directly (e.g. a PAT).  No login or
       token-generation round-trip is performed.  ``frontend_url()`` returns the
       GMS URL so GraphQL calls go to ``{gms_url}/api/graphql``.  ``destroy()``
       is a no-op — the externally-provided token is never revoked.
    """

    __test__ = False

    def __init__(self, requests_session, *, prebuilt_token: str | None = None):
        self._upstream = requests_session
        self._gms_url = get_gms_url()

        if prebuilt_token is not None:
            # Token-based auth: skip login and token generation entirely.
            # Route GraphQL calls through the GMS endpoint directly.
            self._gms_token = prebuilt_token
            self._gms_token_id = None  # externally-owned — never revoke
            self._frontend_url = self._gms_url  # /api/graphql works on GMS too
        else:
            self._frontend_url = get_frontend_url()
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
                f"{self._frontend_url}/api/v2/graphql",
                json=json,
                headers={"Authorization": f"Bearer {self._gms_token}"},
            )
            response.raise_for_status()
            # Clear the token ID after successful revocation to prevent double-call issues
            self._gms_token_id = None
