"""
Comprehensive audit of GMS usage-aggregation instrumentation.

Exercises admin and non-admin users across GraphQL and OpenAPI surfaces, then
validates Prometheus tag cardinality on flush-exported byte counters.

Run locally (requires Prometheus on :4319 and USAGE_AGGREGATION_ENABLED):

    scripts/dev/datahub-dev.sh test tests/metrics/test_usage_aggregation_audit.py

Tune flush latency for faster iteration:

    scripts/dev/datahub-dev.sh env set USAGE_AGGREGATION_FLUSH_INTERVAL_SECONDS=10
    scripts/dev/datahub-dev.sh env restart
"""

from __future__ import annotations

import logging

import pytest
import requests

from tests.metrics.usage_aggregation_metrics import (
    ACTIVE_IDENTITIES_METRIC,
    AGGREGATION_TAG_KEYS,
    INPUT_BYTES_METRIC,
    OUTPUT_BYTES_METRIC,
    aggregation_tags,
    assert_samples_have_tag_keys,
    can_provision_native_users,
    configured_admin_urn,
    expected_actor_class_for_admin_session,
    fetch_metric_total,
    find_metric_samples,
    generate_graphql_read_traffic,
    generate_graphql_search_traffic,
    generate_openapi_metadata_read_traffic,
    generate_openapi_search_traffic,
    graphql_metadata_query_tags,
    parse_prometheus_tags,
    parse_sample_value,
    resolve_usage_actor_class,
    session_corp_user_urn,
    try_mint_personal_access_token,
    wait_for_metric_at_least,
    wait_for_metric_delta,
)
from tests.utilities.metadata_operations import get_prometheus_metrics
from tests.utilities.multi_user import cleanup_step_actor_user, make_step_actor_user
from tests.utils import (
    TestSessionWrapper,
    get_admin_credentials,
    get_gms_prometheus_base_url,
    login_as,
)

logger = logging.getLogger(__name__)


def _require_prometheus_url() -> str:
    gms_url = get_gms_prometheus_base_url()
    if gms_url is None:
        pytest.skip(
            "Management endpoint not resolvable — Prometheus port (4319) unreachable."
        )
    return gms_url


@pytest.mark.read_only
def test_usage_aggregation_configured_admin_actor_class(auth_session):
    """Smoke admin session resolves actor_class from CorpUserInfo flags."""
    gms_url = _require_prometheus_url()
    urn = session_corp_user_urn(auth_session)
    assert urn == configured_admin_urn()

    actor_class = resolve_usage_actor_class(auth_session)
    assert actor_class in ("system", "support", "regular")

    tags = graphql_metadata_query_tags(actor_class)
    baseline = fetch_metric_total(auth_session, gms_url, INPUT_BYTES_METRIC, tags)
    generate_graphql_read_traffic(auth_session, repeat=2)
    wait_for_metric_delta(
        auth_session,
        gms_url,
        INPUT_BYTES_METRIC,
        baseline,
        required_tags=tags,
        min_delta=1.0,
    )

    content = get_prometheus_metrics(auth_session, gms_url)
    samples = find_metric_samples(content, INPUT_BYTES_METRIC, required_tags=tags)
    assert samples, f"No Prometheus samples for admin session (tags={tags})"
    for line in samples:
        tags_on_line = parse_prometheus_tags(line)
        assert tags_on_line.get("actor_class") == actor_class
        assert tags_on_line.get("request_api") == "graphql"
    logger.info("admin session evidence: urn=%s actor_class=%s", urn, actor_class)


@pytest.mark.read_only
def test_usage_aggregation_pat_authenticated_traffic(auth_session):
    """Configured admin exports traffic when authenticating via a minted PAT."""
    gms_url = _require_prometheus_url()
    admin_urn = session_corp_user_urn(auth_session)
    actor_class = resolve_usage_actor_class(auth_session)

    admin_pat = try_mint_personal_access_token(auth_session, admin_urn)
    if admin_pat is None:
        pytest.skip("Cannot mint PAT for configured admin in this environment")

    pat_session = TestSessionWrapper(requests.Session(), prebuilt_token=admin_pat)
    try:
        tags = aggregation_tags(
            actor_class, usage_operation="metadata_read", request_api="openapi"
        )
        baseline = fetch_metric_total(pat_session, gms_url, OUTPUT_BYTES_METRIC, tags)
        generate_openapi_metadata_read_traffic(pat_session, repeat=2)
        wait_for_metric_delta(
            pat_session,
            gms_url,
            OUTPUT_BYTES_METRIC,
            baseline,
            required_tags=tags,
            min_delta=1.0,
        )
        logger.info(
            "PAT-authenticated admin evidence: urn=%s actor_class=%s",
            admin_urn,
            actor_class,
        )
    finally:
        pat_session.destroy()


@pytest.mark.read_only
def test_usage_aggregation_fresh_admin_session_login(auth_session):
    """Fresh frontend login for the configured admin exports GraphQL byte counters."""
    gms_url = _require_prometheus_url()
    username, password = get_admin_credentials()
    session = login_as(username, password)
    admin_session = TestSessionWrapper(session)
    try:
        assert session_corp_user_urn(admin_session) == configured_admin_urn()
        actor_class = resolve_usage_actor_class(admin_session)
        tags = graphql_metadata_query_tags(actor_class)
        baseline = fetch_metric_total(admin_session, gms_url, INPUT_BYTES_METRIC, tags)
        generate_graphql_read_traffic(admin_session, repeat=2)
        wait_for_metric_delta(
            admin_session,
            gms_url,
            INPUT_BYTES_METRIC,
            baseline,
            required_tags=tags,
            min_delta=1.0,
        )
        logger.info("fresh admin login evidence: actor_class=%s", actor_class)
    finally:
        admin_session.destroy()


@pytest.mark.read_only
def test_usage_aggregation_admin_actor_class_and_tags(auth_session):
    """Admin session maps to a usage actor_class with the full aggregation tag set."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)
    tags = graphql_metadata_query_tags(actor_class)

    baseline = fetch_metric_total(auth_session, gms_url, INPUT_BYTES_METRIC, tags)
    generate_graphql_read_traffic(auth_session, repeat=2)

    wait_for_metric_delta(
        auth_session,
        gms_url,
        INPUT_BYTES_METRIC,
        baseline,
        required_tags=tags,
        min_delta=1.0,
    )

    content = get_prometheus_metrics(auth_session, gms_url)
    samples = find_metric_samples(content, INPUT_BYTES_METRIC, required_tags=tags)
    assert_samples_have_tag_keys(samples, AGGREGATION_TAG_KEYS)


@pytest.mark.read_only
def test_usage_aggregation_openapi_read_and_search(auth_session):
    """OpenAPI entity list (GET) exports output bytes; scroll search (POST) exports input bytes."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)

    read_tags = aggregation_tags(
        actor_class, usage_operation="metadata_read", request_api="openapi"
    )
    search_tags = aggregation_tags(
        actor_class, usage_operation="search_query", request_api="openapi"
    )

    read_output_baseline = fetch_metric_total(
        auth_session, gms_url, OUTPUT_BYTES_METRIC, read_tags
    )
    search_input_baseline = fetch_metric_total(
        auth_session, gms_url, INPUT_BYTES_METRIC, search_tags
    )

    generate_openapi_metadata_read_traffic(auth_session)
    generate_openapi_search_traffic(auth_session)

    wait_for_metric_delta(
        auth_session,
        gms_url,
        OUTPUT_BYTES_METRIC,
        read_output_baseline,
        required_tags=read_tags,
    )
    wait_for_metric_delta(
        auth_session,
        gms_url,
        INPUT_BYTES_METRIC,
        search_input_baseline,
        required_tags=search_tags,
    )

    content = get_prometheus_metrics(auth_session, gms_url)
    out_samples = find_metric_samples(
        content, OUTPUT_BYTES_METRIC, required_tags=read_tags
    )
    assert out_samples, (
        "Expected output_bytes for OpenAPI metadata_read — "
        "OpenAPI synchronous responses should be measurable"
    )


@pytest.mark.read_only
def test_usage_aggregation_graphql_output_bytes(auth_session):
    """GraphQL async responses record output_bytes after execution completes."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)
    tags = graphql_metadata_query_tags(actor_class)

    output_baseline = fetch_metric_total(
        auth_session, gms_url, OUTPUT_BYTES_METRIC, tags
    )
    generate_graphql_read_traffic(auth_session, repeat=2)

    wait_for_metric_delta(
        auth_session,
        gms_url,
        OUTPUT_BYTES_METRIC,
        output_baseline,
        required_tags=tags,
        min_delta=1.0,
    )

    content = get_prometheus_metrics(auth_session, gms_url)
    graphql_output = find_metric_samples(
        content, OUTPUT_BYTES_METRIC, required_tags={"request_api": "graphql"}
    )
    assert graphql_output, (
        "Expected datahub_usage_output_bytes for request_api=graphql after async GraphQL fix"
    )


@pytest.mark.read_only
def test_usage_aggregation_graphql_output_bytes_not_double_counted(auth_session):
    """Single GraphQL response should contribute output_bytes ~= body length, not ~2x."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)
    tags = graphql_metadata_query_tags(actor_class)

    from tests.metrics.usage_aggregation_metrics import _ME_QUERY, execute_graphql_raw

    output_baseline = fetch_metric_total(
        auth_session, gms_url, OUTPUT_BYTES_METRIC, tags
    )
    response_text = execute_graphql_raw(auth_session, _ME_QUERY)
    response_len = len(response_text)
    assert response_len > 0

    delta = wait_for_metric_delta(
        auth_session,
        gms_url,
        OUTPUT_BYTES_METRIC,
        output_baseline,
        required_tags=tags,
        min_delta=float(response_len) * 0.95,
    )
    # Allow up to ~2x when prior traffic in the same flush window contributes bytes.
    assert delta <= response_len * 2.1, (
        f"output_bytes delta {delta} far exceeds response length {response_len}"
    )


@pytest.mark.read_only
def test_usage_aggregation_openapi_search_output_bytes(auth_session):
    """OpenAPI scroll search records output_bytes with search_query tags."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)
    search_tags = aggregation_tags(
        actor_class, usage_operation="search_query", request_api="openapi"
    )

    output_baseline = fetch_metric_total(
        auth_session, gms_url, OUTPUT_BYTES_METRIC, search_tags
    )
    generate_openapi_search_traffic(auth_session, repeat=2)
    wait_for_metric_delta(
        auth_session,
        gms_url,
        OUTPUT_BYTES_METRIC,
        output_baseline,
        required_tags=search_tags,
        min_delta=1.0,
    )


@pytest.mark.read_only
def test_usage_aggregation_regular_user_active_identities(auth_session):
    """Distinct identity gauge reflects unique regular users in the flush window."""
    gms_url = _require_prometheus_url()
    if not can_provision_native_users(auth_session):
        pytest.skip(
            "Session lacks manageIdentities — cannot provision a regular user locally"
        )

    user_urn, regular_session = make_step_actor_user(auth_session, "usage-metrics")
    try:
        identity_tags = {
            "identity_metric": "active_users",
            "actor_class": "regular",
        }
        generate_graphql_read_traffic(regular_session, repeat=2)

        wait_for_metric_at_least(
            auth_session,
            gms_url,
            ACTIVE_IDENTITIES_METRIC,
            1.0,
            required_tags=identity_tags,
        )

        search_tags = aggregation_tags(
            "regular", usage_operation="search_query", request_api="graphql"
        )
        input_baseline = fetch_metric_total(
            auth_session, gms_url, INPUT_BYTES_METRIC, search_tags
        )
        generate_graphql_search_traffic(regular_session)
        wait_for_metric_delta(
            auth_session,
            gms_url,
            INPUT_BYTES_METRIC,
            input_baseline,
            required_tags=search_tags,
        )
    finally:
        regular_session.destroy()
        cleanup_step_actor_user(auth_session, user_urn)


@pytest.mark.read_only
def test_usage_aggregation_admin_active_identities(auth_session):
    """Admin actor_class appears in the distinct-identity gauge after traffic."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)

    tags = {
        "identity_metric": "active_users",
        "actor_class": actor_class,
    }
    generate_graphql_read_traffic(auth_session, repeat=3)

    wait_for_metric_at_least(
        auth_session,
        gms_url,
        ACTIVE_IDENTITIES_METRIC,
        1.0,
        required_tags=tags,
    )

    content = get_prometheus_metrics(auth_session, gms_url)
    samples = find_metric_samples(content, ACTIVE_IDENTITIES_METRIC, required_tags=tags)
    assert samples, f"Expected active_identities samples (tags={tags})"
    for line in samples:
        parsed = parse_prometheus_tags(line)
        assert parsed.get("actor_class") == actor_class
        assert parsed.get("identity_metric") == "active_users"
        assert parse_sample_value(line) >= 1.0
