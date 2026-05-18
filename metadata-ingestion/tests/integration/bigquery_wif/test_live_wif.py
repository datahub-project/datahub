"""Live BigQuery Workload Identity Federation test.

Requires real GCP + GitHub OIDC infrastructure. The test is skipped unless
both BIGQUERY_WIF_CONFIG_PATH and BIGQUERY_WIF_PROJECT_ID are set, so it is
inert in normal CI and local runs.
"""

import os

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryAuthType,
    BigQueryConnectionConfig,
)

pytestmark = pytest.mark.integration

WIF_CONFIG_PATH = os.environ.get("BIGQUERY_WIF_CONFIG_PATH")
PROJECT_ID = os.environ.get("BIGQUERY_WIF_PROJECT_ID")

needs_wif = pytest.mark.skipif(
    not (WIF_CONFIG_PATH and PROJECT_ID),
    reason="Live WIF test requires BIGQUERY_WIF_CONFIG_PATH and BIGQUERY_WIF_PROJECT_ID.",
)


@needs_wif
def test_wif_connection_runs_real_bigquery_query() -> None:
    """End-to-end: the WIF config in BIGQUERY_WIF_CONFIG_PATH must produce a
    bigquery.Client that can run a real query against BIGQUERY_WIF_PROJECT_ID.
    Token exchange happens against Google STS — this is the only test in the
    suite that requires network access to *.googleapis.com.
    """
    config = BigQueryConnectionConfig(
        auth_type=BigQueryAuthType.WORKLOAD_IDENTITY_FEDERATION,
        gcp_wif_configuration=WIF_CONFIG_PATH,
        project_on_behalf=PROJECT_ID,
    )

    # Sanity: in-memory creds are populated, no env var leak.
    assert config._credentials is not None
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

    client = config.get_bigquery_client()
    rows = list(client.query("SELECT 1 AS one").result())
    assert len(rows) == 1
    assert rows[0].one == 1
