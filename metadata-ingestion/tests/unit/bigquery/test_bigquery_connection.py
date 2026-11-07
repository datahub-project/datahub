import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List
from unittest.mock import MagicMock, patch

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from datahub.ingestion.source.bigquery_v2 import bigquery_connection
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryAuthType,
    BigQueryConnectionConfig,
)

try:
    from datahub.ingestion.source.fivetran.config import BigQueryDestinationConfig
except ImportError:
    BigQueryDestinationConfig = None  # type: ignore[assignment,misc]


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
    """Clear GOOGLE_APPLICATION_CREDENTIALS so tests can assert it is NOT set
    by BigQueryConnectionConfig (the config keeps credentials in memory)."""
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
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_wif_loads_credentials_in_memory(
        self,
        mock_load: MagicMock,
    ) -> None:
        fake_creds = MagicMock()
        mock_load.return_value = (fake_creds, "wif-project")

        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )

        # WIF credentials are held in memory only — no temp file, no env var.
        # SQLAlchemy callers receive the bigquery.Client via connect_args
        # (see profiler.py) rather than picking up creds from the environment.
        assert config._credentials is fake_creds
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
        mock_load.assert_called_once()
        passed_dict, _ = mock_load.call_args.args
        assert passed_dict == _wif_dict()

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
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

        # Imported lazily inside get_sharing_client, so patch it at its source module
        # rather than on bigquery_connection.
        with patch(
            "google.cloud.bigquery_analyticshub_v1.AnalyticsHubServiceClient"
        ) as mock_sharing_client:
            config.get_sharing_client()
            assert mock_sharing_client.call_args.kwargs["credentials"] is fake_creds

        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.GCPLoggingClient"
        ) as mock_log_client:
            config.make_gcp_logging_client(project_id="my-project")
            assert mock_log_client.call_args.kwargs["credentials"] is fake_creds
            assert mock_log_client.call_args.kwargs["project"] == "my-project"

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_wif_loader_failure_propagates(self, mock_load: MagicMock) -> None:
        mock_load.side_effect = ValueError("boom")
        with pytest.raises(ValueError, match="boom"):
            BigQueryConnectionConfig(
                auth_type="workload_identity_federation",
                gcp_wif_configuration_json=_wif_dict(),
            )

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_wif_json_string_field_builds_credentials(
        self,
        mock_load: MagicMock,
    ) -> None:
        """gcp_wif_configuration_json_string (secrets-manager injection path) must
        work identically to the dict variant at the BigQueryConnectionConfig level."""
        fake_creds = MagicMock()
        mock_load.return_value = (fake_creds, None)

        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json_string=json.dumps(_wif_dict()),
        )

        assert config._credentials is fake_creds
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
        mock_load.assert_called_once()
        passed_dict, _ = mock_load.call_args.args
        assert passed_dict == _wif_dict()

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_wif_sql_alchemy_url(self, mock_load: MagicMock) -> None:
        mock_load.return_value = (MagicMock(), None)

        # Without project_on_behalf, the URL is project-less; SQLAlchemy picks
        # up the project from the explicit bigquery.Client the profiler passes
        # via connect_args.
        config_default = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )
        assert config_default.get_sql_alchemy_url() == "bigquery://"

        config_with_project = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
            project_on_behalf="my-project",
        )
        assert config_with_project.get_sql_alchemy_url() == "bigquery://my-project"


class TestBigQueryWIFValidatorReentrance:
    """Document the re-validation behaviour of the mode='after' validator."""

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_model_copy_does_not_reinitialize_credentials(
        self, mock_load: MagicMock
    ) -> None:
        """model_copy() does not re-run Pydantic validators — load is called once."""
        mock_load.return_value = (MagicMock(), None)
        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )
        original_creds = config._credentials

        _ = config.model_copy()

        assert mock_load.call_count == 1
        assert config._credentials is original_creds

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_model_validate_reinitializes_credentials(
        self, mock_load: MagicMock
    ) -> None:
        """model_validate(dump) re-runs the validator because PrivateAttrs are not
        serialized, so _credentials is None on re-entry.  A second set of credentials
        is built.  Avoid round-tripping live configs through model_validate()."""
        mock_load.return_value = (MagicMock(), None)
        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
        )
        assert mock_load.call_count == 1

        config2 = BigQueryConnectionConfig.model_validate(config.model_dump())

        assert mock_load.call_count == 2
        assert config2._credentials is not None


class TestBigQueryServiceAccountCredentialSetup:
    """Regression coverage for the existing service-account path. The WIF
    refactor moved credential setup into a shared validator — these tests
    pin the behavior the SA path relied on before the refactor."""

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
    )
    def test_service_account_builds_explicit_credentials_in_memory(
        self, mock_from_info: MagicMock
    ) -> None:
        fake_creds = MagicMock()
        mock_from_info.return_value = fake_creds

        config = BigQueryConnectionConfig(credential=_service_account_dict())

        assert config.auth_type == BigQueryAuthType.SERVICE_ACCOUNT
        assert config._credentials is fake_creds
        # SA path keeps credentials only in memory — no env var leak.
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

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

        assert config._credentials is None
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_service_account_auth_type_ignores_wif_fields(
        self, mock_build: MagicMock
    ) -> None:
        """Regression: WIF fields supplied alongside the default service_account
        auth_type must be silently ignored, not consumed as WIF credentials.
        Credentials must be None (ADC fallback) since no credential block is set."""
        mock_build.return_value = (MagicMock(), None)
        config = BigQueryConnectionConfig(
            auth_type="service_account",
            gcp_wif_configuration_json=_wif_dict(),
        )
        assert config._credentials is None  # credential=None → ADC, not WIF
        mock_build.assert_not_called()  # WIF builder must not have been invoked


class TestBigQueryWIFFilePathSetup:
    """Verify the gcp_wif_configuration (file path) variant works end-to-end
    through BigQueryConnectionConfig, not just through GCPWIFConfig.to_wif_dict."""

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_wif_file_path_reads_and_builds_credentials(
        self,
        mock_build: MagicMock,
        tmp_path: Path,
    ) -> None:
        fake_creds = MagicMock()
        mock_build.return_value = (fake_creds, None)

        wif_file = tmp_path / "wif.json"
        wif_file.write_text(json.dumps(_wif_dict()))

        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration=str(wif_file),
        )

        assert config._credentials is fake_creds
        # The dict passed to the credential builder must match the source file,
        # proving the file is read and parsed correctly.
        mock_build.assert_called_once()
        passed_dict = mock_build.call_args.args[0]
        assert passed_dict == _wif_dict()


class TestBigQueryExtraClientOptions:
    """Verify extra_client_options and the _use_grpc override invariant."""

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_extra_client_options_passed_to_bigquery_client(
        self, mock_build: MagicMock
    ) -> None:
        mock_build.return_value = (MagicMock(), None)
        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
            extra_client_options={"timeout": 30},
        )
        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.bigquery.Client"
        ) as mock_client:
            config.get_bigquery_client()
            assert mock_client.call_args.kwargs.get("timeout") == 30

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_logging_client_always_disables_grpc(self, mock_build: MagicMock) -> None:
        """_use_grpc=False must always be set on the logging client, even when a
        caller passes _use_grpc=True via extra_client_options."""
        mock_build.return_value = (MagicMock(), None)
        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
            extra_client_options={"_use_grpc": True},
        )
        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.GCPLoggingClient"
        ) as mock_log:
            config.make_gcp_logging_client()
            assert mock_log.call_args.kwargs["_use_grpc"] is False


@pytest.mark.skipif(
    BigQueryDestinationConfig is None,
    reason="fivetran extra not installed",
)
class TestFivetranBigQueryDestinationConfig:
    """Regression coverage: BigQueryDestinationConfig inherits BigQueryConnectionConfig,
    so it also gains WIF support.  Verify the inherited validator fires correctly."""

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_fivetran_bigquery_destination_supports_wif(
        self, mock_build: MagicMock
    ) -> None:
        fake_creds = MagicMock()
        mock_build.return_value = (fake_creds, None)

        config = BigQueryDestinationConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
            dataset="my_log_dataset",
        )

        assert config._credentials is fake_creds
        mock_build.assert_called_once()

    @patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict"
    )
    def test_fivetran_bigquery_destination_service_account_still_works(
        self, mock_build: MagicMock
    ) -> None:
        with patch(
            "datahub.ingestion.source.bigquery_v2.bigquery_connection.service_account.Credentials.from_service_account_info"
        ) as mock_sa:
            mock_sa.return_value = MagicMock()
            config = BigQueryDestinationConfig(
                credential=_service_account_dict(),
                dataset="my_log_dataset",
            )
            assert config._credentials is not None
            mock_build.assert_not_called()  # SA path does not use build_credentials_from_wif_dict


_JOB_REF = {"projectId": "my-project", "jobId": "job_1", "location": "US"}
_QUERY_CONFIG = {"query": {"query": "select 1"}}
_REPLIES: Dict[str, Dict[str, Any]] = {
    "insert": {
        "jobReference": _JOB_REF,
        "status": {"state": "RUNNING"},
        "configuration": _QUERY_CONFIG,
    },
    "jobs": {
        "jobReference": _JOB_REF,
        "status": {"state": "DONE"},
        "configuration": _QUERY_CONFIG,
    },
    "queries": {
        "jobReference": _JOB_REF,
        "jobComplete": True,
        "totalRows": "1",
        "schema": {"fields": [{"name": "x", "type": "INTEGER"}]},
        "rows": [{"f": [{"v": "1"}]}],
    },
    "tables": {"tables": []},
    "projects": {"projects": []},
}


class _FakeBigQueryApi:
    """Answers like the BigQuery REST API, except that the first request of kind
    `lose` never gets a reply, as when the network drops it."""

    def __init__(self, lose: str) -> None:
        self.calls: List[str] = []
        release = self._release = threading.Event()
        calls = self.calls

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _reply(self) -> None:
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                path = self.path.split("?")[0]
                if self.command == "POST":
                    kind = "insert"
                elif path.endswith("/tables"):
                    kind = "tables"
                elif "/queries/" in path:
                    kind = "queries"
                elif "/jobs/" in path:
                    kind = "jobs"
                else:
                    kind = "projects"
                calls.append(kind)
                if kind == lose and calls.count(kind) == 1:
                    release.wait(30)
                    self.close_connection = True
                    return
                body = json.dumps(_REPLIES[kind]).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = _reply

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._server.server_port}"

    def __enter__(self) -> "_FakeBigQueryApi":
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._release.set()
        self._server.shutdown()
        self._server.server_close()


def _client(url: str) -> bigquery.Client:
    with patch(
        "datahub.ingestion.source.bigquery_v2.bigquery_connection.build_credentials_from_wif_dict",
        return_value=(AnonymousCredentials(), None),
    ):
        config = BigQueryConnectionConfig(
            auth_type="workload_identity_federation",
            gcp_wif_configuration_json=_wif_dict(),
            project_on_behalf="my-project",
            extra_client_options={"client_options": {"api_endpoint": url}},
        )
    return config.get_bigquery_client()


def _run_with_deadline(fn: Callable[[], Any], seconds: float = 15) -> Dict[str, Any]:
    outcome: Dict[str, Any] = {}

    def target() -> None:
        try:
            outcome["value"] = fn()
        except Exception as e:
            outcome["error"] = e

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(seconds)
    assert not worker.is_alive(), "still waiting for a reply that will never come"
    return outcome


class TestBigQueryRequestTimeout:
    @pytest.fixture(autouse=True)
    def _short_default_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(bigquery_connection, "_DEFAULT_REQUEST_TIMEOUT_SEC", 1)

    def test_lost_list_tables_reply_is_retried(self) -> None:
        with _FakeBigQueryApi(lose="tables") as api:
            client = _client(api.url)
            outcome = _run_with_deadline(
                lambda: list(client.list_tables("my-project.my_dataset"))
            )
        assert outcome == {"value": []}
        assert api.calls == ["tables", "tables"]

    def test_lost_query_results_reply_is_retried(self) -> None:
        with _FakeBigQueryApi(lose="queries") as api:
            client = _client(api.url)
            outcome = _run_with_deadline(
                lambda: [tuple(row.values()) for row in client.query("select 1")]
            )
        assert outcome == {"value": [(1,)]}
        assert api.calls.count("queries") == 2

    def test_explicit_timeout_is_kept(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(bigquery_connection, "_DEFAULT_REQUEST_TIMEOUT_SEC", 30)
        with _FakeBigQueryApi(lose="projects") as api:
            client = _client(api.url)
            outcome = _run_with_deadline(
                lambda: list(client.list_projects(timeout=0.5)), seconds=10
            )
        assert outcome == {"value": []}
        assert api.calls == ["projects", "projects"]
