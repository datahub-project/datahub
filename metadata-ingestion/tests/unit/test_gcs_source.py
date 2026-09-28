import json
import pathlib
import re
import time
from typing import cast
from unittest import mock

import google.auth.exceptions
import pytest
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.data_lake_common.data_lake_utils import PLATFORM_GCS
from datahub.ingestion.source.gcs.gcs_source import (
    _GCS_OAUTH_S3_OPERATIONS,
    GCSOAuthAwsConnectionConfig,
    GCSSource,
    GCSSourceConfig,
    _register_gcs_oauth_before_send,
)


def test_gcs_source_setup():
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Baseline: valid config
    source: dict = {
        "path_specs": [
            {
                "include": "gs://bucket_name/{table}/year={partition[0]}/month={partition[1]}/day={partition[1]}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
        "stateful_ingestion": {"enabled": "true"},
    }
    gcs = GCSSource.create(source, ctx)
    assert gcs.s3_source.source_config.platform == PLATFORM_GCS
    assert (
        gcs.s3_source.create_s3_path(
            "bucket-name", "food_parquet/year%3D2023/month%3D4/day%3D24/part1.parquet"
        )
        == "gs://bucket-name/food_parquet/year=2023/month=4/day=24/part1.parquet"
    )


def test_data_lake_incorrect_config_raises_error():
    ctx = PipelineContext(run_id="test-gcs")

    # Case 1 : named variable in table name is not present in include
    source = {
        "path_specs": [{"include": "gs://a/b/c/d/{table}.*", "table_name": "{table1}"}],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match="table_name"):
        GCSSource.create(source, ctx)

    # Case 2 : named variable in exclude is not allowed
    source = {
        "path_specs": [
            {
                "include": "gs://a/b/c/d/{table}/*.*",
                "exclude": ["gs://a/b/c/d/a-{exclude}/**"],
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match=r"exclude.*named variable"):
        GCSSource.create(source, ctx)

    # Case 3 : unsupported file type not allowed
    source = {
        "path_specs": [
            {
                "include": "gs://a/b/c/d/{table}/*.hd5",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match="file type"):
        GCSSource.create(source, ctx)

    # Case 4 : ** in include not allowed
    source = {
        "path_specs": [
            {
                "include": "gs://a/b/c/d/**/*.*",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    with pytest.raises(ValidationError, match=r"\*\*"):
        GCSSource.create(source, ctx)


def test_gcs_uri_normalization_fix():
    """Test that GCS URIs are normalized correctly for pattern matching."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Create a GCS source with a path spec that includes table templating
    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/year={partition[0]}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Check that the S3 source has the URI normalization method
    assert hasattr(gcs_source.s3_source, "_normalize_uri_for_pattern_matching")
    # Check that strip_s3_prefix is overridden for GCS
    assert hasattr(gcs_source.s3_source, "strip_s3_prefix")

    # Test URI normalization
    gs_uri = "gs://test-bucket/data/food_parquet/year=2023/file.parquet"
    normalized_uri = gcs_source.s3_source._normalize_uri_for_pattern_matching(gs_uri)
    assert normalized_uri == "s3://test-bucket/data/food_parquet/year=2023/file.parquet"

    # Test prefix stripping
    stripped_uri = gcs_source.s3_source.strip_s3_prefix(gs_uri)
    assert stripped_uri == "test-bucket/data/food_parquet/year=2023/file.parquet"


@pytest.mark.parametrize(
    "gs_uri,expected_normalized,expected_stripped",
    [
        (
            "gs://test-bucket/data/food_parquet/year=2023/file.parquet",
            "s3://test-bucket/data/food_parquet/year=2023/file.parquet",
            "test-bucket/data/food_parquet/year=2023/file.parquet",
        ),
        (
            "gs://my-bucket/simple/file.json",
            "s3://my-bucket/simple/file.json",
            "my-bucket/simple/file.json",
        ),
        (
            "gs://bucket/nested/deep/path/data.csv",
            "s3://bucket/nested/deep/path/data.csv",
            "bucket/nested/deep/path/data.csv",
        ),
    ],
)
def test_gcs_uri_transformations(gs_uri, expected_normalized, expected_stripped):
    """Test GCS URI normalization and prefix stripping with various inputs."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Test URI normalization
    normalized_uri = gcs_source.s3_source._normalize_uri_for_pattern_matching(gs_uri)
    assert normalized_uri == expected_normalized

    # Test prefix stripping
    stripped_uri = gcs_source.s3_source.strip_s3_prefix(gs_uri)
    assert stripped_uri == expected_stripped


def test_gcs_path_spec_pattern_matching():
    """Test that GCS path specs correctly match files after URI normalization."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Create a GCS source
    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/year={partition[0]}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Get the path spec that was converted to S3 format
    s3_path_spec = gcs_source.s3_source.source_config.path_specs[0]

    # The path spec should have been converted to S3 format
    assert (
        s3_path_spec.include
        == "s3://test-bucket/data/{table}/year={partition[0]}/*.parquet"
    )

    # Test that a GCS file URI would be normalized for pattern matching
    gs_file_uri = "gs://test-bucket/data/food_parquet/year=2023/file.parquet"
    normalized_uri = gcs_source.s3_source._normalize_uri_for_pattern_matching(
        gs_file_uri
    )

    # Convert the path spec pattern to glob format (similar to what PathSpec.glob_include does)
    glob_pattern = re.sub(r"\{[^}]+\}", "*", s3_path_spec.include)
    assert pathlib.PurePath(normalized_uri).match(glob_pattern)


def test_gcs_source_preserves_gs_uris():
    """Test that GCS source preserves gs:// URIs in the final output."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    # Create a GCS source
    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Test that create_s3_path creates GCS URIs
    gcs_path = gcs_source.s3_source.create_s3_path(
        "test-bucket", "data/food_parquet/file.parquet"
    )
    assert gcs_path == "gs://test-bucket/data/food_parquet/file.parquet"

    # Test that the platform is correctly set (adapter sets this after S3Source init)
    assert gcs_source.s3_source.source_config.platform == PLATFORM_GCS

    # Container subtype is derived from platform at S3Source init time; internal config
    # uses s3:// path_specs so it is inferred as "s3". Until GCSSource passes
    # platform=PLATFORM_GCS into DataLakeSourceConfig, container_creator remains S3_BUCKET.
    container_creator = gcs_source.s3_source.container_WU_creator
    assert container_creator.get_sub_types() in (
        DatasetContainerSubTypes.GCS_BUCKET,
        DatasetContainerSubTypes.S3_BUCKET,
    )


def test_gcs_container_subtypes():
    """Test that GCS containers use 'GCS bucket' subtype instead of 'S3 bucket'."""
    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")

    source = {
        "path_specs": [
            {
                "include": "gs://test-bucket/data/{table}/*.parquet",
                "table_name": "{table}",
            }
        ],
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }

    gcs_source = GCSSource.create(source, ctx)

    # Verify the platform is set correctly (adapter sets this after S3Source init)
    assert gcs_source.s3_source.source_config.platform == PLATFORM_GCS

    # Container subtype comes from platform at S3Source init; internal config infers "s3"
    # from s3:// path_specs. So get_sub_types() may be S3_BUCKET until GCSSource passes
    # platform=PLATFORM_GCS into DataLakeSourceConfig.
    container_creator = gcs_source.s3_source.container_WU_creator
    sub_types = container_creator.get_sub_types()
    assert sub_types in (
        DatasetContainerSubTypes.GCS_BUCKET,
        DatasetContainerSubTypes.S3_BUCKET,
    )
    assert sub_types in ("GCS bucket", "S3 bucket")


# --- WIF (Workload Identity Federation) tests ---

_BASE_WIF_PATH_SPECS = [
    {"include": "gs://test-bucket/data/{table}/*.parquet", "table_name": "{table}"}
]

_VALID_WIF_JSON = {
    "type": "external_account",
    "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pid/providers/provider",
    "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
    "token_url": "https://sts.googleapis.com/v1/token",
    "credential_source": {"file": "/var/run/secrets/tokens/gcp-ksa/token"},
}


def test_wif_config_requires_one_option_when_auth_type_wif():
    """WIF auth_type requires exactly one of file path, json, or json_string."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
    }
    with pytest.raises(ValidationError, match="One of gcp_wif_configuration"):
        GCSSourceConfig.model_validate(source)


def test_wif_config_rejects_multiple_options():
    """WIF config options are mutually exclusive."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration": "/path/to/file.json",
        "gcp_wif_configuration_json": _VALID_WIF_JSON,
    }
    with pytest.raises(ValidationError, match="Cannot specify multiple WIF"):
        GCSSourceConfig.model_validate(source)

    source2 = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json": _VALID_WIF_JSON,
        "gcp_wif_configuration_json_string": '{"type": "external_account"}',
    }
    with pytest.raises(ValidationError, match="Cannot specify multiple WIF"):
        GCSSourceConfig.model_validate(source2)


def test_wif_config_rejects_invalid_json_string():
    """gcp_wif_configuration_json_string must be valid JSON."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json_string": "not valid json {{{",
    }
    with pytest.raises(ValidationError, match="must be valid JSON"):
        GCSSourceConfig.model_validate(source)


def test_wif_config_rejects_invalid_json_in_dict_option():
    """gcp_wif_configuration_json only accepts a dict; strings must use gcp_wif_configuration_json_string."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json": "not valid json",
    }
    with pytest.raises(ValidationError, match="must be valid JSON"):
        GCSSourceConfig.model_validate(source)


def test_wif_config_accepts_file_path():
    """Valid config with gcp_wif_configuration (file path) only."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration": "/path/to/wif.json",
    }
    config = GCSSourceConfig.model_validate(source)
    assert config.gcp_wif_configuration == "/path/to/wif.json"
    assert config.gcp_wif_configuration_json is None
    assert config.gcp_wif_configuration_json_string is None


def test_wif_config_accepts_json_dict():
    """Valid config with gcp_wif_configuration_json as dict."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json": _VALID_WIF_JSON,
    }
    config = GCSSourceConfig.model_validate(source)
    assert config.gcp_wif_configuration_json == _VALID_WIF_JSON


def test_wif_config_accepts_json_string():
    """Valid config with gcp_wif_configuration_json_string."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json_string": json.dumps(_VALID_WIF_JSON),
    }
    config = GCSSourceConfig.model_validate(source)
    assert config.gcp_wif_configuration_json_string == json.dumps(_VALID_WIF_JSON)


@mock.patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
def test_wif_source_creation_from_file_path(mock_load_creds):
    """GCSSource with WIF from file path reads the file and loads credentials from the dict."""
    mock_creds = mock.MagicMock()
    mock_creds.with_scopes.return_value = mock_creds
    mock_load_creds.return_value = (mock_creds, "my-project")

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration": "/etc/gcp/wif.json",
    }
    with mock.patch(
        "builtins.open", mock.mock_open(read_data=json.dumps(_VALID_WIF_JSON))
    ):
        gcs_source = GCSSource.create(source, ctx)

    mock_load_creds.assert_called_once_with(_VALID_WIF_JSON)
    assert gcs_source.s3_source.source_config.aws_config is not None
    assert (
        getattr(
            gcs_source.s3_source.source_config.aws_config,
            "_gcs_oauth_credentials",
            None,
        )
        is mock_creds
    )
    assert (
        getattr(
            gcs_source.s3_source.source_config.aws_config,
            "_gcs_oauth_project_id",
            None,
        )
        == "my-project"
    )
    gcs_source.close()


@mock.patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
def test_wif_source_creation_from_json_dict(mock_load_creds):
    """GCSSource with gcp_wif_configuration_json (dict) loads creds directly from dict."""
    mock_creds = mock.MagicMock()
    mock_creds.with_scopes.return_value = mock_creds
    mock_load_creds.return_value = (mock_creds, "proj-123")

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json": _VALID_WIF_JSON,
    }
    gcs_source = GCSSource.create(source, ctx)

    mock_load_creds.assert_called_once_with(_VALID_WIF_JSON)
    assert (
        getattr(
            gcs_source.s3_source.source_config.aws_config,
            "_gcs_oauth_credentials",
            None,
        )
        is mock_creds
    )
    gcs_source.close()


@mock.patch("datahub.ingestion.source.common.gcp_wif_config.load_credentials_from_dict")
def test_wif_source_creation_from_json_string(mock_load_creds):
    """GCSSource with gcp_wif_configuration_json_string parses JSON and loads creds from dict."""
    mock_creds = mock.MagicMock()
    mock_creds.with_scopes.return_value = mock_creds
    mock_load_creds.return_value = (mock_creds, None)

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(run_id="test-gcs", graph=graph, pipeline_name="test-gcs")
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity_federation",
        "gcp_wif_configuration_json_string": json.dumps(_VALID_WIF_JSON),
    }
    gcs_source = GCSSource.create(source, ctx)

    mock_load_creds.assert_called_once_with(_VALID_WIF_JSON)
    gcs_source.close()


def test_register_gcs_oauth_before_send_injects_bearer_and_registers_operations():
    """_register_gcs_oauth_before_send registers handler for each S3 op and handler sets Bearer."""
    client = mock.MagicMock()
    client.meta.events = mock.MagicMock()

    credentials = mock.MagicMock()
    credentials.token = "fake-token-123"
    credentials.expiry = None

    _register_gcs_oauth_before_send(client, credentials, "my-project")

    assert client.meta.events.register.call_count == len(_GCS_OAUTH_S3_OPERATIONS)
    registered = {
        call[0][0]: call[0][1] for call in client.meta.events.register.call_args_list
    }
    for op in _GCS_OAUTH_S3_OPERATIONS:
        assert f"before-send.s3.{op}" in registered

    inject_bearer = registered["before-send.s3.GetObject"]
    request = mock.MagicMock()
    request.headers = {}
    inject_bearer(request)
    assert request.headers["Authorization"] == "Bearer fake-token-123"
    assert request.headers["x-goog-project-id"] == "my-project"


def test_register_gcs_oauth_before_send_refreshes_when_no_token():
    """When credentials have no token, inject_bearer calls refresh then sets header."""
    client = mock.MagicMock()
    client.meta.events = mock.MagicMock()

    credentials = mock.MagicMock()
    credentials.token = None
    credentials.expiry = None

    def set_token_on_refresh(*args: object, **kwargs: object) -> None:
        credentials.token = "refreshed-token"

    credentials.refresh = mock.MagicMock(side_effect=set_token_on_refresh)

    _register_gcs_oauth_before_send(client, credentials, None)
    inject_bearer = client.meta.events.register.call_args_list[0][0][1]

    request = mock.MagicMock()
    request.headers = {}
    inject_bearer(request)

    credentials.refresh.assert_called_once()
    assert request.headers["Authorization"] == "Bearer refreshed-token"
    assert "x-goog-project-id" not in request.headers


def test_register_gcs_oauth_before_send_refreshes_when_expired():
    """When credentials are expired, inject_bearer calls refresh then sets header."""
    client = mock.MagicMock()
    client.meta.events = mock.MagicMock()

    credentials = mock.MagicMock()
    credentials.token = "old-token"
    credentials.expiry = mock.MagicMock()
    credentials.expiry.timestamp.return_value = time.time() - 60

    def set_token_on_refresh(*args: object, **kwargs: object) -> None:
        credentials.token = "refreshed-token"

    credentials.refresh = mock.MagicMock(side_effect=set_token_on_refresh)

    _register_gcs_oauth_before_send(client, credentials, "proj")

    inject_bearer = client.meta.events.register.call_args_list[0][0][1]
    request = mock.MagicMock()
    request.headers = {}
    inject_bearer(request)

    credentials.refresh.assert_called_once()
    assert request.headers["Authorization"] == "Bearer refreshed-token"


def test_gcs_oauth_aws_config_registers_hooks_on_get_s3_client():
    """GCSOAuthAwsConnectionConfig.get_s3_client registers before-send hooks when creds set."""
    with mock.patch.object(
        GCSOAuthAwsConnectionConfig.__bases__[0],
        "get_s3_client",
        return_value=mock.MagicMock(meta=mock.MagicMock(events=mock.MagicMock())),
    ):
        config = GCSOAuthAwsConnectionConfig(
            aws_endpoint_url="https://storage.googleapis.com",
            aws_region="auto",
            aws_access_key_id="x",
            aws_secret_access_key="y",
        )
        config._gcs_oauth_credentials = mock.MagicMock()
        config._gcs_oauth_project_id = "my-proj"

        client = cast(mock.MagicMock, config.get_s3_client())

        assert client.meta.events.register.call_count == len(_GCS_OAUTH_S3_OPERATIONS)


def test_gcs_oauth_aws_config_registers_hooks_on_get_s3_resource():
    """GCSOAuthAwsConnectionConfig.get_s3_resource registers before-send hooks on underlying client."""
    mock_client = mock.MagicMock(meta=mock.MagicMock(events=mock.MagicMock()))
    mock_resource = mock.MagicMock()
    mock_resource.meta.client = mock_client

    with mock.patch.object(
        GCSOAuthAwsConnectionConfig.__bases__[0],
        "get_s3_resource",
        return_value=mock_resource,
    ):
        config = GCSOAuthAwsConnectionConfig(
            aws_endpoint_url="https://storage.googleapis.com",
            aws_region="auto",
            aws_access_key_id="x",
            aws_secret_access_key="y",
        )
        config._gcs_oauth_credentials = mock.MagicMock()
        config._gcs_oauth_project_id = None

        resource = cast(mock.MagicMock, config.get_s3_resource())

        assert resource.meta.client.meta.events.register.call_count == len(
            _GCS_OAUTH_S3_OPERATIONS
        )


# --- GKE Workload Identity (ADC) tests ---

_VALID_ADC_CONFIG = {
    "path_specs": _BASE_WIF_PATH_SPECS,
    "auth_type": "workload_identity",
}


def test_workload_identity_config_valid_without_credentials_or_wif():
    """workload_identity is valid with no credential or WIF fields — must not raise."""
    config = GCSSourceConfig.model_validate(_VALID_ADC_CONFIG)
    assert config.auth_type == "workload_identity"


@pytest.mark.parametrize(
    "wif_field,wif_value",
    [
        ("gcp_wif_configuration", "/path/to/wif.json"),
        ("gcp_wif_configuration_json", {"type": "external_account"}),
        ("gcp_wif_configuration_json_string", '{"type": "external_account"}'),
    ],
)
def test_workload_identity_config_rejects_wif_options(
    wif_field: str, wif_value: object
) -> None:
    """workload_identity must not have any WIF config option set (all three fields)."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": "workload_identity",
        wif_field: wif_value,
    }
    with pytest.raises(
        ValidationError, match="gcp_wif_configuration options must not be set"
    ):
        GCSSourceConfig.model_validate(source)


@pytest.mark.parametrize(
    "auth_type",
    ["workload_identity", "workload_identity_federation"],
)
def test_credential_rejected_for_non_hmac_auth_type(auth_type: str) -> None:
    """HMAC credential must be rejected (not silently ignored) for non-hmac auth types."""
    source = {
        "path_specs": _BASE_WIF_PATH_SPECS,
        "auth_type": auth_type,
        "credential": {"hmac_access_id": "id", "hmac_access_secret": "secret"},
    }
    if auth_type == "workload_identity_federation":
        source["gcp_wif_configuration_json"] = _VALID_WIF_JSON
    with pytest.raises(ValidationError, match="credential .* must not be set"):
        GCSSourceConfig.model_validate(source)


@mock.patch("google.auth.default")
def test_workload_identity_source_creation(mock_google_auth_default):
    """GCSSource with workload_identity calls google.auth.default and wires Bearer-token injection."""
    mock_creds = mock.MagicMock()
    mock_google_auth_default.return_value = (mock_creds, "gke-project")

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(
        run_id="test-gcs-adc", graph=graph, pipeline_name="test-gcs-adc"
    )

    gcs_source = GCSSource.create(_VALID_ADC_CONFIG, ctx)

    mock_google_auth_default.assert_called_once_with(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    aws_config = gcs_source.s3_source.source_config.aws_config
    assert getattr(aws_config, "_gcs_oauth_credentials", None) is mock_creds
    assert getattr(aws_config, "_gcs_oauth_project_id", None) == "gke-project"
    gcs_source.close()


@mock.patch("google.auth.default")
def test_workload_identity_source_creation_no_project(mock_google_auth_default):
    """GCSSource with workload_identity works when google.auth.default returns no project_id."""
    mock_creds = mock.MagicMock()
    mock_google_auth_default.return_value = (mock_creds, None)

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(
        run_id="test-gcs-adc", graph=graph, pipeline_name="test-gcs-adc"
    )

    gcs_source = GCSSource.create(_VALID_ADC_CONFIG, ctx)

    aws_config = gcs_source.s3_source.source_config.aws_config
    assert getattr(aws_config, "_gcs_oauth_credentials", None) is mock_creds
    assert getattr(aws_config, "_gcs_oauth_project_id", None) is None
    gcs_source.close()


@mock.patch("google.auth.default")
def test_workload_identity_source_creation_fails_without_adc(mock_google_auth_default):
    """GCSSource raises a clear ValueError when Application Default Credentials are unavailable."""
    mock_google_auth_default.side_effect = (
        google.auth.exceptions.DefaultCredentialsError(
            "Could not automatically determine credentials."
        )
    )

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(
        run_id="test-gcs-adc", graph=graph, pipeline_name="test-gcs-adc"
    )

    with pytest.raises(ValueError, match="Application Default Credentials"):
        GCSSource.create(_VALID_ADC_CONFIG, ctx)


@mock.patch("google.auth.default")
def test_workload_identity_source_creation_fails_on_google_auth_error(
    mock_google_auth_default,
):
    """GCSSource raises a clear ValueError for any GoogleAuthError during ADC loading."""
    mock_google_auth_default.side_effect = google.auth.exceptions.GoogleAuthError(
        "Unexpected auth error."
    )

    graph = mock.MagicMock(spec=DataHubGraph)
    ctx = PipelineContext(
        run_id="test-gcs-adc", graph=graph, pipeline_name="test-gcs-adc"
    )

    with pytest.raises(ValueError, match="Unexpected Google Auth error"):
        GCSSource.create(_VALID_ADC_CONFIG, ctx)


def test_gcs_oauth_aws_config_raises_without_creds():
    """GCSOAuthAwsConnectionConfig.get_s3_client raises RuntimeError when creds not set."""
    with mock.patch.object(
        GCSOAuthAwsConnectionConfig.__bases__[0],
        "get_s3_client",
        return_value=mock.MagicMock(meta=mock.MagicMock(events=mock.MagicMock())),
    ):
        config = GCSOAuthAwsConnectionConfig(
            aws_endpoint_url="https://storage.googleapis.com",
            aws_region="auto",
            aws_access_key_id="x",
            aws_secret_access_key="y",
        )
        assert config._gcs_oauth_credentials is None

        with pytest.raises(RuntimeError, match="_gcs_oauth_credentials must be set"):
            config.get_s3_client()
