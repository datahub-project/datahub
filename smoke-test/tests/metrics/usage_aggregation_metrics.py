"""Helpers for GMS usage-aggregation Micrometer smoke tests."""

from __future__ import annotations

import logging
import re
import urllib.parse
from typing import Dict, List, Optional

import requests
import tenacity

from tests.utilities.metadata_operations import get_prometheus_metrics

logger = logging.getLogger(__name__)

# Legacy per-request counter (user_category tags). Aggregation flush also targets this name
# in Micrometer, but quickstart currently exports aggregation dimensions on byte counters.
REQUEST_COUNT_METRIC = "datahub_request_count"
INPUT_BYTES_METRIC = "datahub_usage_input_bytes"
OUTPUT_BYTES_METRIC = "datahub_usage_output_bytes"
ACTIVE_IDENTITIES_METRIC = "datahub_usage_active_identities"

AGGREGATION_TAG_KEYS = (
    "usage_operation",
    "actor_class",
    "agent_class",
    "request_api",
    "auth_channel",
)

_ME_QUERY = """
query smokeUsageAggregationMe {
  me {
    corpUser {
      urn
    }
  }
}
"""


def iter_metric_samples(content: str, metric_substring: str) -> List[str]:
    samples: List[str] = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if metric_substring in line:
            samples.append(line)
    return samples


def parse_prometheus_tags(line: str) -> Dict[str, str]:
    match = re.search(r"\{([^}]*)\}", line)
    if not match:
        return {}
    tags: Dict[str, str] = {}
    for part in match.group(1).split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, _, value = part.partition("=")
        tags[key.strip()] = value.strip().strip('"')
    return tags


def find_metric_samples(
    content: str,
    metric_substring: str,
    required_tags: Optional[Dict[str, str]] = None,
) -> List[str]:
    required_tags = required_tags or {}
    matched: List[str] = []
    for line in iter_metric_samples(content, metric_substring):
        tags = parse_prometheus_tags(line)
        if all(tags.get(key) == value for key, value in required_tags.items()):
            matched.append(line)
    return matched


def assert_samples_have_tag_keys(samples: List[str], tag_keys: tuple[str, ...]) -> None:
    assert samples, "Expected at least one Prometheus sample"
    for line in samples:
        tags = parse_prometheus_tags(line)
        missing = [key for key in tag_keys if key not in tags]
        assert not missing, f"Sample missing tags {missing}: {line}"


def assert_samples_lack_tag(samples: List[str], tag_key: str) -> None:
    for line in samples:
        tags = parse_prometheus_tags(line)
        assert tag_key not in tags, (
            f"Legacy tag {tag_key!r} should not be present on flush metrics: {line}"
        )


def parse_sample_value(line: str) -> float:
    """Parse the numeric sample value from a Prometheus text-format line."""
    try:
        return float(line.split()[-1])
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Could not parse metric value from line: {line}") from exc


def sum_matching_metric_values(
    content: str,
    metric_substring: str,
    required_tags: Optional[Dict[str, str]] = None,
) -> float:
    """Sum sample values for all series matching the metric name and tag filter."""
    return sum(
        parse_sample_value(line)
        for line in find_metric_samples(content, metric_substring, required_tags)
    )


def fetch_metric_total(
    auth_session,
    gms_url: str,
    metric_substring: str,
    required_tags: Optional[Dict[str, str]] = None,
) -> float:
    content = get_prometheus_metrics(auth_session, gms_url)
    return sum_matching_metric_values(content, metric_substring, required_tags)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(25),
    wait=tenacity.wait_fixed(3),
    reraise=True,
)
def wait_for_metric_delta(
    auth_session,
    gms_url: str,
    metric_substring: str,
    baseline: float,
    required_tags: Optional[Dict[str, str]] = None,
    min_delta: float = 1.0,
) -> float:
    """Wait until the aggregated counter value increases by at least min_delta."""
    total = fetch_metric_total(auth_session, gms_url, metric_substring, required_tags)
    delta = total - baseline
    assert delta >= min_delta, (
        f"Expected {metric_substring!r} delta>={min_delta}, got {delta} "
        f"(baseline={baseline}, total={total}, tags={required_tags or {}})"
    )
    logger.info(
        "Metric %s increased by %s (baseline=%s, total=%s, tags=%s)",
        metric_substring,
        delta,
        baseline,
        total,
        required_tags or {},
    )
    return delta


@tenacity.retry(
    stop=tenacity.stop_after_attempt(25),
    wait=tenacity.wait_fixed(3),
    reraise=True,
)
def wait_for_metric_at_least(
    auth_session,
    gms_url: str,
    metric_substring: str,
    min_value: float,
    required_tags: Optional[Dict[str, str]] = None,
) -> float:
    """Wait until aggregated gauge/counter samples reach at least min_value."""
    total = fetch_metric_total(auth_session, gms_url, metric_substring, required_tags)
    assert total >= min_value, (
        f"Expected {metric_substring!r} value>={min_value}, got {total} "
        f"(tags={required_tags or {}})"
    )
    logger.info(
        "Metric %s reached %s (min=%s, tags=%s)",
        metric_substring,
        total,
        min_value,
        required_tags or {},
    )
    return total


def generate_graphql_read_traffic(auth_session, *, repeat: int = 3) -> None:
    from tests.utils import execute_graphql

    for _ in range(repeat):
        execute_graphql(auth_session, _ME_QUERY)


def execute_graphql_raw(auth_session, query: str) -> str:
    """POST a GraphQL query and return the raw response body text."""
    url = f"{auth_session.frontend_url()}/api/v2/graphql"
    response = auth_session.post(
        url,
        json={"query": query, "operationName": "smokeUsageAggregationMe"},
    )
    response.raise_for_status()
    return response.text


def corpuser_urn(username: str) -> str:
    return f"urn:li:corpuser:{username}"


def configured_admin_urn() -> str:
    """URN for the smoke-test admin principal from env (DATAHUB_USERNAME / defaults)."""
    from tests.utils import get_admin_username

    return corpuser_urn(get_admin_username())


def session_corp_user_urn(auth_session) -> str:
    """Return the authenticated corp-user URN from a GraphQL ``me`` query."""
    from tests.utils import execute_graphql

    me = execute_graphql(auth_session, "query { me { corpUser { urn } } }")
    return me["data"]["me"]["corpUser"]["urn"]


def resolve_usage_actor_class(auth_session) -> str:
    """Resolve usage aggregation actor_class from CorpUserInfo flags (matches GMS resolver)."""
    urn = session_corp_user_urn(auth_session)
    encoded = urllib.parse.quote(urn, safe="")
    response = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v3/entity/corpuser/{encoded}"
    )
    response.raise_for_status()
    info = response.json().get("corpUserInfo", {}).get("value", {})
    if info.get("system"):
        return "system"
    if info.get("isSupportUser"):
        return "support"
    return "regular"


def expected_actor_class_for_admin_session(auth_session) -> str:
    return resolve_usage_actor_class(auth_session)


def aggregation_tags(
    actor_class: str,
    *,
    usage_operation: str,
    request_api: str,
) -> Dict[str, str]:
    return {
        "usage_operation": usage_operation,
        "request_api": request_api,
        "actor_class": actor_class,
    }


def graphql_metadata_query_tags(actor_class: str) -> Dict[str, str]:
    return aggregation_tags(
        actor_class, usage_operation="metadata_query", request_api="graphql"
    )


def try_mint_personal_access_token(auth_session, actor_urn: str) -> str | None:
    """Mint a short-lived PAT for ``actor_urn`` when the session is authorized."""
    from tests.utils import execute_graphql

    try:
        result = execute_graphql(
            auth_session,
            """
            mutation createAccessToken($input: CreateAccessTokenInput!) {
              createAccessToken(input: $input) {
                accessToken
                metadata { id actorUrn }
              }
            }
            """,
            {
                "input": {
                    "type": "PERSONAL",
                    "actorUrn": actor_urn,
                    "duration": "ONE_HOUR",
                    "name": "usage-aggregation-pat-probe",
                }
            },
            expect_errors=True,
        )
    except AssertionError:
        return None
    if result.get("errors"):
        return None
    token_payload = result.get("data", {}).get("createAccessToken")
    if not token_payload or not token_payload.get("accessToken"):
        return None
    return token_payload["accessToken"]


def generate_graphql_search_traffic(auth_session, *, repeat: int = 1) -> None:
    from tests.utils import execute_graphql

    query = """
    query smokeUsageAggregationSearch {
      search(input: { type: DATASET, query: "*", start: 0, count: 1 }) {
        total
      }
    }
    """
    for _ in range(repeat):
        execute_graphql(auth_session, query)


def generate_openapi_metadata_read_traffic(auth_session, *, repeat: int = 1) -> None:
    for _ in range(repeat):
        response = auth_session.get(
            f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
            params={"count": 1, "query": "*"},
        )
        response.raise_for_status()


def generate_openapi_search_traffic(auth_session, *, repeat: int = 1) -> None:
    body = {"entities": ["dataset"], "query": "*", "start": 0, "count": 1}
    for _ in range(repeat):
        response = auth_session.post(
            f"{auth_session.gms_url()}/openapi/v3/entity/scroll",
            json=body,
        )
        response.raise_for_status()


def can_provision_native_users(auth_session) -> bool:
    from tests.utils import execute_graphql

    privileges = execute_graphql(
        auth_session,
        "query { me { platformPrivileges { manageIdentities } } }",
    )
    return bool(privileges["data"]["me"]["platformPrivileges"]["manageIdentities"])


def set_corpuser_is_support_user(
    admin_session, corp_user_urn: str, *, is_support_user: bool
) -> None:
    """Set CorpUserInfo.isSupportUser (requires admin with system/support privileges)."""
    from tests.consistency_utils import wait_for_writes_to_sync

    encoded = urllib.parse.quote(corp_user_urn, safe="")
    gms_url = admin_session.gms_url()
    get_resp = admin_session.get(f"{gms_url}/openapi/v3/entity/corpuser/{encoded}")
    get_resp.raise_for_status()
    info = dict(get_resp.json().get("corpUserInfo", {}).get("value", {}))
    info["isSupportUser"] = is_support_user
    post_resp = admin_session.post(
        f"{gms_url}/openapi/v3/entity/corpuser/{encoded}/corpUserInfo",
        params={"createIfNotExists": "false"},
        json={"value": info},
    )
    if not post_resp.ok:
        raise requests.HTTPError(
            f"{post_resp.status_code} setting isSupportUser on {corp_user_urn}: "
            f"{post_resp.text}",
            response=post_resp,
        )
    wait_for_writes_to_sync()


def make_support_actor_user(admin_session, name: str):
    """Provision a native user with actor_class=support for isolated usage metrics."""
    from tests.utilities.multi_user import make_step_actor_user
    from tests.utils import TestSessionWrapper, get_admin_credentials, login_as

    user_urn, user_session = make_step_actor_user(admin_session, name)
    # create_user re-authenticates as admin; use a fresh wrapper so GMS PAT calls stay valid.
    admin_user, admin_pass = get_admin_credentials()
    refreshed_admin = TestSessionWrapper(login_as(admin_user, admin_pass))
    set_corpuser_is_support_user(refreshed_admin, user_urn, is_support_user=True)
    actor_class = resolve_usage_actor_class(user_session)
    if actor_class != "support":
        raise AssertionError(
            f"Expected actor_class=support for {user_urn}, got {actor_class!r}"
        )
    return user_urn, user_session


def corpuser_entity_exists(auth_session, corp_user_urn: str) -> bool:
    encoded = urllib.parse.quote(corp_user_urn, safe="")
    response = auth_session.get(
        f"{auth_session.gms_url()}/openapi/v3/entity/corpuser/{encoded}"
    )
    return response.status_code == 200


@tenacity.retry(
    stop=tenacity.stop_after_attempt(25),
    wait=tenacity.wait_fixed(3),
    reraise=True,
)
def wait_for_metric_samples(
    auth_session,
    gms_url: str,
    metric_substring: str,
    required_tags: Optional[Dict[str, str]] = None,
) -> List[str]:
    content = get_prometheus_metrics(auth_session, gms_url)
    samples = find_metric_samples(content, metric_substring, required_tags)
    assert samples, (
        f"No Prometheus samples yet for {metric_substring!r} "
        f"with tags {required_tags or {}}"
    )
    logger.info(
        "Found %s sample(s) for %s tags=%s",
        len(samples),
        metric_substring,
        required_tags or {},
    )
    for sample in samples[:3]:
        logger.info("  %s", sample)
    return samples
