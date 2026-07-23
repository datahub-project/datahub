"""
Smoke tests for GMS usage-aggregation Micrometer export.

Verifies flush-based operational metrics (not per-request counters) when
USAGE_AGGREGATION_ENABLED is on — as in Docker quickstart dev profile.
"""

import logging

import pytest

from tests.metrics.usage_aggregation_metrics import (
    ACTIVE_IDENTITIES_METRIC,
    AGGREGATION_TAG_KEYS,
    INPUT_BYTES_METRIC,
    OUTPUT_BYTES_METRIC,
    assert_samples_have_tag_keys,
    assert_samples_lack_tag,
    can_provision_native_users,
    expected_actor_class_for_admin_session,
    fetch_metric_total,
    find_metric_samples,
    generate_graphql_read_traffic,
    graphql_metadata_query_tags,
    parse_prometheus_tags,
    wait_for_metric_at_least,
    wait_for_metric_delta,
)
from tests.utilities.metadata_operations import get_prometheus_metrics
from tests.utilities.multi_user import cleanup_step_actor_user, make_step_actor_user
from tests.utils import get_gms_prometheus_base_url

logger = logging.getLogger(__name__)


def _require_prometheus_url() -> str:
    gms_url = get_gms_prometheus_base_url()
    if gms_url is None:
        pytest.skip(
            "Management endpoint not resolvable in this environment — "
            "Prometheus port (4319) is cluster-internal only."
        )
    return gms_url


@pytest.mark.read_only
def test_usage_aggregation_micrometer_export(auth_session):
    """Aggregation flush exports byte and identity counters to Prometheus."""
    gms_url = _require_prometheus_url()
    actor_class = expected_actor_class_for_admin_session(auth_session)
    graphql_tags = graphql_metadata_query_tags(actor_class)

    input_baseline = fetch_metric_total(
        auth_session, gms_url, INPUT_BYTES_METRIC, graphql_tags
    )
    output_baseline = fetch_metric_total(
        auth_session, gms_url, OUTPUT_BYTES_METRIC, graphql_tags
    )

    generate_graphql_read_traffic(auth_session)

    wait_for_metric_delta(
        auth_session,
        gms_url,
        INPUT_BYTES_METRIC,
        input_baseline,
        required_tags=graphql_tags,
        min_delta=1.0,
    )
    wait_for_metric_delta(
        auth_session,
        gms_url,
        OUTPUT_BYTES_METRIC,
        output_baseline,
        required_tags=graphql_tags,
        min_delta=1.0,
    )

    if can_provision_native_users(auth_session):
        user_urn, regular_session = make_step_actor_user(auth_session, "metrics-smoke")
        try:
            regular_tags = {"identity_metric": "active_users", "actor_class": "regular"}
            generate_graphql_read_traffic(regular_session, repeat=2)
            wait_for_metric_at_least(
                auth_session,
                gms_url,
                ACTIVE_IDENTITIES_METRIC,
                1.0,
                required_tags=regular_tags,
            )
        finally:
            regular_session.destroy()
            cleanup_step_actor_user(auth_session, user_urn)
    else:
        pytest.skip(
            "Session lacks manageIdentities — cannot provision a regular user locally"
        )

    content = get_prometheus_metrics(auth_session, gms_url)

    input_samples = find_metric_samples(
        content, INPUT_BYTES_METRIC, required_tags=graphql_tags
    )
    assert_samples_have_tag_keys(input_samples, AGGREGATION_TAG_KEYS)
    assert_samples_lack_tag(input_samples, "user_category")

    output_samples = find_metric_samples(content, OUTPUT_BYTES_METRIC)
    assert_samples_have_tag_keys(output_samples, AGGREGATION_TAG_KEYS)

    identity_samples = find_metric_samples(content, ACTIVE_IDENTITIES_METRIC)
    for sample in identity_samples:
        assert "identity_metric" in parse_prometheus_tags(sample), (
            f"active_identities sample missing identity_metric label: {sample}"
        )

    logger.info(
        "Usage aggregation Micrometer counters increased after GraphQL read "
        "(actor_class=%s)",
        actor_class,
    )
