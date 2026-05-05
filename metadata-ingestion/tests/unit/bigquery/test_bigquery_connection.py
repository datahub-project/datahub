import json
import os
from pathlib import Path
from typing import Any, Dict, Iterator
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryAuthType,
    BigQueryConnectionConfig,
)


def _wif_dict() -> Dict[str, Any]:
    return {
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/provider",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {"url": "https://example.com/token"},
    }


def _service_account_dict() -> Dict[str, str]:
    return {
        "project_id": "p",
        "private_key_id": "kid",
        "private_key": "-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----\n",
        "client_email": "sa@example.iam.gserviceaccount.com",
        "client_id": "cid",
    }


@pytest.fixture(autouse=True)
def _isolate_google_credentials_env(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """BigQueryConnectionConfig mutates GOOGLE_APPLICATION_CREDENTIALS;
    snapshot and restore so it does not leak between tests."""
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    yield


class TestBigQueryConnectionConfigValidation:
    def test_wif_requires_a_wif_option(self) -> None:
        with pytest.raises(ValueError, match="One of gcp_wif_configuration"):
            BigQueryConnectionConfig(auth_type="workload_identity_federation")

    def test_wif_rejects_credential(self) -> None:
        with pytest.raises(
            ValueError, match="credential must not be set when auth_type is"
        ):
            BigQueryConnectionConfig(
                auth_type="workload_identity_federation",
                gcp_wif_configuration_json=_wif_dict(),
                credential=_service_account_dict(),
            )


class TestBigQueryWIFCredentialSetup:
    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.load_wif_credentials"
    )
    def test_wif_loads_credentials_and_writes_temp_file(
        self,
        mock_load: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        monkeypatch.setenv("TMPDIR", str(tmp_path))
        fake_creds = MagicMock()
        mock_load.return_value = (fake_creds, "wif-project")

        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )

        assert config._credentials is fake_creds
        assert config._credentials_path is not None

        with open(config._credentials_path) as f:
            assert json.load(f) == _wif_dict()

        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == config._credentials_path

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.load_wif_credentials"
    )
    def test_wif_clients_use_explicit_credentials(self, mock_load: MagicMock) -> None:
        fake_creds = MagicMock()
        mock_load.return_value = (fake_creds, None)

        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.bigquery.Client"
        ) as mock_bq_client:
            config.get_bigquery_client()
            assert mock_bq_client.call_args.kwargs["credentials"] is fake_creds

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.resourcemanager_v3.ProjectsClient"
        ) as mock_proj_client:
            config.get_projects_client()
            assert mock_proj_client.call_args.kwargs["credentials"] is fake_creds

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.datacatalog_v1.PolicyTagManagerClient"
        ) as mock_ptm_client:
            config.get_policy_tag_manager_client()
            assert mock_ptm_client.call_args.kwargs["credentials"] is fake_creds

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.GCPLoggingClient"
        ) as mock_log_client:
            config.make_gcp_logging_client()
            assert mock_log_client.call_args.kwargs["credentials"] is fake_creds

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.GCPLoggingClient"
        ) as mock_log_client:
            config.make_gcp_logging_client(project_id="my-project")
            assert mock_log_client.call_args.kwargs["credentials"] is fake_creds
            assert mock_log_client.call_args.kwargs["project"] == "my-project"

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.load_wif_credentials"
    )
    def test_wif_loader_failure_propagates(self, mock_load: MagicMock) -> None:
        mock_load.side_effect = ValueError("boom")
        with pytest.raises(ValueError, match="boom"):
            BigQueryConnectionConfig(
                auth_type="workload_identity_federation",
                gcp_wif_configuration_json=_wif_dict(),
            )

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.load_wif_credentials"
    )
    def test_wif_sql_alchemy_url(self, mock_load: MagicMock) -> None:
        mock_load.return_value = (MagicMock(), None)

        config_default = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )
        # Without project_on_behalf, the SQLAlchemy dialect picks up the
        # project from GOOGLE_APPLICATION_CREDENTIALS (which we set above).
        assert config_default.get_sql_alchemy_url() == "bigquery://"
        assert "GOOGLE_APPLICATION_CREDENTIALS" in os.environ

        config_with_project = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
            project_on_behalf="my-project",
        )
        assert config_with_project.get_sql_alchemy_url() == "bigquery://my-project"


class TestBigQueryServiceAccountCredentialSetup:
    """Regression coverage for the existing service-account path. The WIF
    refactor moved credential setup into a shared validator — these tests
    pin the behavior the SA path relied on before the refactor."""

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
    )
    def test_service_account_builds_explicit_credentials_and_temp_file(
        self, mock_from_info: MagicMock
    ) -> None:
        fake_creds = MagicMock()
        mock_from_info.return_value = fake_creds

        config = BigQueryConnectionConfig(credential=_service_account_dict())

        assert config.auth_type == BigQueryAuthType.SERVICE_ACCOUNT
        assert config._credentials is fake_creds
        assert config._credentials_path is not None
        assert os.path.exists(config._credentials_path)
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == config._credentials_path

        # Verify the explicit credentials were built from the SA info
        # (concurrent-config thread safety relies on this).
        mock_from_info.assert_called_once()
        passed_info = mock_from_info.call_args.args[0]
        assert passed_info["client_email"] == "sa@example.iam.gserviceaccount.com"

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
    )
    def test_service_account_clients_use_explicit_credentials(
        self, mock_from_info: MagicMock
    ) -> None:
        fake_creds = MagicMock()
        mock_from_info.return_value = fake_creds

        config = BigQueryConnectionConfig(credential=_service_account_dict())

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.bigquery.Client"
        ) as mock_bq_client:
            config.get_bigquery_client()
            assert mock_bq_client.call_args.kwargs["credentials"] is fake_creds

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.resourcemanager_v3.ProjectsClient"
        ) as mock_proj_client:
            config.get_projects_client()
            assert mock_proj_client.call_args.kwargs["credentials"] is fake_creds

    def test_no_credential_falls_back_to_adc(self) -> None:
        # No credential and no WIF means rely on Application Default Credentials —
        # _credentials stays None and clients pass credentials=None to GCP libs,
        # which then pick up ADC (e.g. metadata server on GCE/GKE).
        config = BigQueryConnectionConfig()

        assert config.auth_type == BigQueryAuthType.SERVICE_ACCOUNT
        assert config._credentials is None
        assert config._credentials_path is None
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
