import datetime
import logging
import pathlib
import time
import traceback
from typing import Any, Dict, Iterator, List, Optional, Tuple
from unittest import mock

import boto3
import google.auth.exceptions
import pytest
import requests
from botocore.exceptions import ClientError
from click.testing import CliRunner
from moto import mock_aws

from datahub.cli.recipe_cli import recipe as recipe_cli
from datahub.ingestion.agent.filter_check import FilterCheckResult, check_filters
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodResult,
    list_probe_methods,
    run_probe_method,
)
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig

HMAC: Dict[str, object] = {"hmac_access_id": "id", "hmac_access_secret": "secret"}


def _recipe(*includes: str, **spec_extra: object) -> Dict[str, object]:
    return {
        "credential": HMAC,
        "path_specs": [{"include": inc, **spec_extra} for inc in includes],
    }


def _verdicts(
    recipe: Dict[str, object], kind: str, names: List[str]
) -> Tuple[FilterCheckResult, Dict[str, Tuple[bool, Optional[str]]]]:
    result = check_filters(
        source_type="gcs", config_dict=recipe, kind=kind, parent_path=[], names=names
    )
    return result, {r.name: (r.included, r.excluded_by) for r in result.results}


def test_tables_are_judged_by_path_specs_with_gs_targets() -> None:
    result, verdicts = _verdicts(
        _recipe(
            "gs://my-bucket/data/{table}/*.parquet",
            tables_filter_pattern={"deny": ["^users$"]},
        ),
        "Table",
        [
            "gs://my-bucket/data/events",
            "gs://my-bucket/data/users",
            "gs://my-bucket/data/_staging",
        ],
    )
    assert (result.filtering, result.pattern_field) == ("by_rule", "path_specs")
    assert verdicts == {
        "gs://my-bucket/data/events": (True, None),
        "gs://my-bucket/data/users": (False, "path_specs[0].tables_filter_pattern"),
        "gs://my-bucket/data/_staging": (
            False,
            "path_specs[0].include_hidden_folders",
        ),
    }
    assert {r.target for r in result.results} == {
        "gs://my-bucket/data/events",
        "gs://my-bucket/data/users",
        "gs://my-bucket/data/_staging",
    }


def test_simple_spec_files_use_the_ingestion_rule() -> None:
    _, verdicts = _verdicts(
        _recipe("gs://my-bucket/raw/*.csv", exclude=["**/tmp_*.csv"]),
        "Table",
        [
            "gs://my-bucket/raw/a.csv",
            "gs://my-bucket/raw/tmp_b.csv",
            "gs://my-bucket/raw/notes.txt",
        ],
    )
    assert verdicts == {
        "gs://my-bucket/raw/a.csv": (True, None),
        "gs://my-bucket/raw/tmp_b.csv": (False, "path_specs[0].exclude"),
        "gs://my-bucket/raw/notes.txt": (False, "path_specs[0].include"),
    }


def test_gs_excludes_are_rewritten_like_the_include() -> None:
    _, verdicts = _verdicts(
        _recipe("gs://my-bucket/raw/*.csv", exclude=["gs://my-bucket/raw/skip_*.csv"]),
        "Table",
        ["gs://my-bucket/raw/skip_1.csv"],
    )
    assert verdicts["gs://my-bucket/raw/skip_1.csv"] == (
        False,
        "path_specs[0].exclude",
    )


def test_autodetected_partition_suffix_is_judged_like_ingestion() -> None:
    # An include ending in {table} gets "/**" appended by PathSpec, which
    # equivalent_s3_path_specs strips and re-adds; the verdict must survive it.
    _, verdicts = _verdicts(
        _recipe("gs://my-bucket/data/{table}"),
        "Table",
        ["gs://my-bucket/data/events"],
    )
    assert verdicts["gs://my-bucket/data/events"] == (True, None)


def test_any_including_spec_wins() -> None:
    recipe: Dict[str, object] = {
        "credential": HMAC,
        "path_specs": [
            {
                "include": "gs://my-bucket/data/{table}/*.parquet",
                "tables_filter_pattern": {"deny": [".*"]},
            },
            {"include": "gs://my-bucket/data/{table}/*.parquet"},
        ],
    }
    _, verdicts = _verdicts(recipe, "Table", ["gs://my-bucket/data/events"])
    assert verdicts["gs://my-bucket/data/events"] == (True, None)


@pytest.mark.parametrize("kind", ["Table", "Folder"])
def test_a_non_gs_name_is_a_caller_error(kind: str) -> None:
    with pytest.raises(ValueError, match="gs://"):
        _verdicts(_recipe("gs://my-bucket/data/*.csv"), kind, ["s3://my-bucket/a.csv"])


def test_a_bucket_name_with_a_slash_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="bucket name"):
        _verdicts(
            _recipe("gs://my-bucket/data/*.csv"), "GCS bucket", ["gs://my-bucket"]
        )


def test_buckets_and_folders_are_rule_filtered_too() -> None:
    recipe = _recipe("gs://my-bucket/media/*/", emit_folders_only=True)
    result, buckets = _verdicts(recipe, "GCS bucket", ["my-bucket", "other"])
    assert result.filtering == "by_rule"
    assert buckets == {"my-bucket": (True, None), "other": (False, "path_specs")}
    _, folders = _verdicts(
        recipe,
        "Folder",
        ["gs://my-bucket/media/photos", "gs://my-bucket/media/.cache"],
    )
    assert folders == {
        "gs://my-bucket/media/photos": (True, None),
        "gs://my-bucket/media/.cache": (
            False,
            "path_specs[0].include_hidden_folders",
        ),
    }


def test_a_bucket_uri_is_not_a_folder() -> None:
    with pytest.raises(ValueError, match="gs://my-bucket.*GCS bucket") as raised:
        _verdicts(
            _recipe("gs://my-bucket/data/{table}/*.parquet"),
            "Folder",
            ["gs://my-bucket/"],
        )
    assert "s3://" not in str(raised.value)


def test_a_folders_only_spec_includes_the_folders_above_its_leaves() -> None:
    result, folders = _verdicts(
        _recipe("gs://my-bucket/media/*/*/", emit_folders_only=True),
        "Folder",
        ["gs://my-bucket/media", "gs://my-bucket/media/photos"],
    )
    assert folders == {
        "gs://my-bucket/media": (True, None),
        "gs://my-bucket/media/photos": (True, None),
    }
    assert result.warnings


def test_a_folder_above_datasets_is_a_container() -> None:
    result, folders = _verdicts(
        _recipe("gs://my-bucket/data/{table}/*.parquet"),
        "Folder",
        ["gs://my-bucket/data", "gs://my-bucket/other"],
    )
    assert folders == {
        "gs://my-bucket/data": (True, None),
        "gs://my-bucket/other": (False, "path_specs"),
    }
    assert result.warnings


KEYS = [
    "data/events/year=2024/part-0.parquet",
    "data/users/year=2024/part-0.parquet",
    "data/_staging/part-0.parquet",
    "raw/a.csv",
    "raw/notes.txt",
]


@pytest.fixture
def seeded_bucket() -> Iterator[None]:
    with (
        mock_aws(),
        mock.patch("datahub.ingestion.source.gcs.gcs_source.GCS_ENDPOINT_URL", None),
    ):
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="id",
            aws_secret_access_key="secret",
        )
        client.create_bucket(Bucket="my-bucket")
        for key in KEYS:
            client.put_object(Bucket="my-bucket", Key=key, Body=b"x")
        yield


def _run(recipe: Dict[str, object], command: str, **kwargs: str) -> ProbeMethodResult:
    return run_probe_method("gcs", recipe, command, dict(kwargs))


def _names(result: ProbeMethodResult) -> List[str]:
    assert isinstance(result.result, list)
    return [d["name"] if isinstance(d, dict) else d for d in result.result]


def test_methods_advertise_the_gcs_kinds() -> None:
    kinds = {s.command: s.kind for s in list_probe_methods("gcs")}
    assert kinds == {
        "buckets": "GCS bucket",
        "folders": None,
        "objects": None,
        "datasets": "Table",
        "path_spec_folders": "Folder",
    }


def test_buckets_lists_the_project(seeded_bucket: None) -> None:
    result = _run(_recipe("gs://my-bucket/raw/*.csv"), "buckets")
    assert result.kind == "GCS bucket"
    assert result.result == ["my-bucket"]


def test_datasets_lists_table_folders_including_denied_ones(
    seeded_bucket: None,
) -> None:
    recipe = _recipe(
        "gs://my-bucket/data/{table}/*/*.parquet",
        tables_filter_pattern={"deny": ["^users$"]},
    )
    result = _run(recipe, "datasets")
    assert result.kind == "Table"
    assert isinstance(result.result, list)
    assert {(d["name"], d["display_name"]) for d in result.result} == {
        ("gs://my-bucket/data/events", "events"),
        ("gs://my-bucket/data/users", "users"),
        ("gs://my-bucket/data/_staging", "_staging"),
    }


def test_simple_spec_datasets_are_the_objects_ingestion_lists(
    seeded_bucket: None,
) -> None:
    result = _run(_recipe("gs://my-bucket/raw/*.csv"), "datasets")
    assert set(_names(result)) == {
        "gs://my-bucket/raw/a.csv",
        "gs://my-bucket/raw/notes.txt",
    }


def test_a_listing_is_bounded_and_reports_truncation(seeded_bucket: None) -> None:
    result = _run(
        _recipe("gs://my-bucket/raw/*.csv"), "objects", bucket="my-bucket", limit="2"
    )
    assert len(_names(result)) == 2 and result.truncated
    assert all(n.startswith("gs://my-bucket/") for n in _names(result))


def test_folders_lists_one_level_as_gs_uris(seeded_bucket: None) -> None:
    result = _run(
        _recipe("gs://my-bucket/raw/*.csv"),
        "folders",
        bucket="my-bucket",
        prefix="data",
    )
    assert set(_names(result)) == {
        "gs://my-bucket/data/events",
        "gs://my-bucket/data/users",
        "gs://my-bucket/data/_staging",
    }


def test_path_spec_folders_walks_a_folders_only_spec(seeded_bucket: None) -> None:
    result = _run(
        _recipe("gs://my-bucket/data/*/", emit_folders_only=True), "path_spec_folders"
    )
    assert result.kind == "Folder"
    assert set(_names(result)) == {
        "gs://my-bucket/data/events",
        "gs://my-bucket/data/users",
        "gs://my-bucket/data/_staging",
    }


def test_datasets_on_a_folders_only_spec_is_empty_with_a_reason(
    seeded_bucket: None,
) -> None:
    result = _run(_recipe("gs://my-bucket/data/*/", emit_folders_only=True), "datasets")
    assert result.result == []
    assert any("path_spec_folders" in w for w in result.warnings)


def test_an_unknown_bucket_is_a_caller_error(seeded_bucket: None) -> None:
    with pytest.raises(ValueError, match="bucket"):
        _run(_recipe("gs://my-bucket/raw/*.csv"), "objects", bucket="no-such-bucket")


def test_only_list_operations_reach_storage(seeded_bucket: None) -> None:
    """Metadata only, enforced by the code rather than the credential."""
    recipe = _recipe("gs://my-bucket/data/{table}/*/*.parquet")
    seen: List[str] = []
    real_client = boto3.session.Session.client

    def recording_client(self: boto3.session.Session, *args: Any, **kwargs: Any) -> Any:
        client = real_client(self, *args, **kwargs)
        client.meta.events.register(
            "before-call.s3", lambda model, **_: seen.append(model.name)
        )
        return client

    with mock.patch.object(boto3.session.Session, "client", recording_client):
        for command, kwargs in [
            ("buckets", {}),
            ("folders", {"bucket": "my-bucket"}),
            ("objects", {"bucket": "my-bucket"}),
            ("datasets", {}),
        ]:
            _run(recipe, command, **kwargs)
    assert seen and set(seen) <= {"ListBuckets", "ListObjectsV2"}


def _client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": "principal some-account-detail"},
            "ResponseMetadata": {
                "RequestId": "",
                "HostId": "",
                "HTTPStatusCode": status,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        },
        "ListBuckets",
    )


def test_access_denied_on_the_whole_listing_is_a_failure(seeded_bucket: None) -> None:
    with mock.patch(
        "datahub.ingestion.source.data_lake_common.object_store_probe.list_buckets",
        side_effect=_client_error("AccessDenied", 403),
    ):
        result = _run(_recipe("gs://my-bucket/raw/*.csv"), "buckets")
    assert result.result == [] and result.failures


def test_bad_signature_is_a_connection_error_without_the_error_text(
    seeded_bucket: None,
) -> None:
    with (
        mock.patch(
            "datahub.ingestion.source.data_lake_common.object_store_probe.list_buckets",
            side_effect=_client_error("SignatureDoesNotMatch", 403),
        ),
        pytest.raises(ProbeConnectionError) as raised,
    ):
        _run(_recipe("gs://my-bucket/raw/*.csv"), "buckets")
    assert "some-account-detail" not in str(raised.value)


@pytest.fixture
def isolated_secret_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The masking registry is process-global; keep a test's secret out of
    later tests, as tests/unit/cli/test_recipe_probe_cli.py does -- cleared in
    place, before and after, so a failing assertion cannot leak it."""
    import datahub.cli.recipe_cli as rc
    from datahub.masking.secret_registry import SecretRegistry

    SecretRegistry.get_instance().clear()
    monkeypatch.setattr(rc, "_stdin_secrets", {}, raising=False)
    yield
    SecretRegistry.get_instance().clear()


def test_an_error_echoing_the_hmac_secret_is_masked_by_the_cli(
    seeded_bucket: None, isolated_secret_registry: None, tmp_path: pathlib.Path
) -> None:
    # An error the probe does not classify is reported by class name only;
    # its text, and the recipe's secret with it, must not reach the output.
    secret = "GOOG1EXAMPLEhmacSecretValue0123456789abcd"
    recipe_file = tmp_path / "gcs.yml"
    recipe_file.write_text(
        "source:\n"
        "  type: gcs\n"
        "  config:\n"
        "    credential:\n"
        "      hmac_access_id: my-access-id\n"
        f"      hmac_access_secret: {secret}\n"
        "    path_specs:\n"
        "      - include: gs://my-bucket/raw/*.csv\n"
    )
    with mock.patch(
        "datahub.ingestion.source.data_lake_common.object_store_probe.list_buckets",
        side_effect=RuntimeError(f"request signed with {secret} was refused"),
    ):
        result = CliRunner().invoke(
            recipe_cli, ["probe", "run", "buckets", "--recipe", str(recipe_file)]
        )
    assert result.exit_code == 3
    assert "RuntimeError" in result.output
    assert "was refused" not in result.output
    assert secret not in result.output


class _RefreshFailingCredentials:
    """OAuth credentials whose first refresh fails the way google-auth does,
    with the subject-token path and the STS response body in the text."""

    token: Optional[str] = None
    expiry: Optional[object] = None

    def __init__(self, error: Exception) -> None:
        self._error = error

    def refresh(self, request: object) -> None:
        raise self._error


_LEAKY_TEXT = ("/var/run/secrets/subject-token.json", "sa-name@my-project.iam")


@pytest.mark.parametrize(
    "error",
    [
        google.auth.exceptions.RefreshError(
            f"File '{_LEAKY_TEXT[0]}' was not found.",
            f'{{"error_description":"principal {_LEAKY_TEXT[1]}"}}',
        ),
        requests.exceptions.ConnectionError(
            f"token endpoint for {_LEAKY_TEXT[1]} via {_LEAKY_TEXT[0]}"
        ),
    ],
    ids=["refresh_error", "transport_error"],
)
@pytest.mark.parametrize(
    "command,recipe_include",
    [
        ("buckets", "gs://my-bucket/raw/*.csv"),
        # the per-prefix listing inside _table_folders
        ("datasets", "gs://my-bucket/data/{table}/*/*.parquet"),
    ],
)
def test_a_failed_token_refresh_is_a_scrubbed_connection_error(
    seeded_bucket: None,
    caplog: pytest.LogCaptureFixture,
    error: Exception,
    command: str,
    recipe_include: str,
) -> None:
    recipe: Dict[str, object] = {
        "auth_type": "workload_identity",
        "path_specs": [{"include": recipe_include}],
    }
    caplog.set_level(logging.DEBUG)
    with (
        mock.patch(
            "google.auth.default",
            return_value=(_RefreshFailingCredentials(error), "my-project"),
        ),
        pytest.raises(ProbeConnectionError) as raised,
    ):
        _run(recipe, command)
    message = str(raised.value)
    assert "workload_identity" in message
    for leaky in _LEAKY_TEXT:
        assert leaky not in message
        assert leaky not in caplog.text


class _ExpiringCredentials:
    """OAuth credentials holding a token that is still valid, but only for a
    minute; refreshing it fails with google-auth's leaky text."""

    token: Optional[str] = "still-valid"

    def __init__(self, now: float) -> None:
        self.expiry = datetime.datetime.fromtimestamp(now + 60)

    def refresh(self, request: object) -> None:
        raise google.auth.exceptions.RefreshError(
            f"File '{_LEAKY_TEXT[0]}' was not found.",
            f'{{"error_description":"principal {_LEAKY_TEXT[1]}"}}',
        )


def test_a_token_about_to_expire_is_refreshed_before_the_listing(
    seeded_bucket: None, caplog: pytest.LogCaptureFixture
) -> None:
    # Two minutes pass between the probe's refresh check and botocore's
    # before-send hook. A token expiring in between would be refreshed inside
    # the hook, where botocore logs the failure with google-auth's text; the
    # probe refreshes ahead of expiry so that refresh happens outside it.
    now = time.time()
    reads = iter([now])
    # gcs_source's own clock only: the first read is the probe's check, every
    # later one is the hook's.
    fake_time = mock.Mock(time=lambda: next(reads, now + 120))
    recipe: Dict[str, object] = {
        "auth_type": "workload_identity",
        "path_specs": [{"include": "gs://my-bucket/raw/*.csv"}],
    }
    caplog.set_level(logging.DEBUG)
    with (
        mock.patch(
            "google.auth.default",
            return_value=(_ExpiringCredentials(now), "my-project"),
        ),
        mock.patch("datahub.ingestion.source.gcs.gcs_source.time", fake_time),
        pytest.raises(ProbeConnectionError),
    ):
        _run(recipe, "buckets")
    for leaky in _LEAKY_TEXT:
        assert leaky not in caplog.text


def test_a_credential_load_failure_does_not_chain_the_original_error() -> None:
    from datahub.ingestion.source.gcs.gcs_probe import GCSMetadataProbe
    from datahub.ingestion.source.gcs.gcs_source import GCSSourceConfig

    config = GCSSourceConfig.model_validate(_recipe("gs://my-bucket/raw/*.csv"))
    with (
        mock.patch(
            "datahub.ingestion.source.gcs.gcs_probe.build_gcs_aws_connection_config",
            side_effect=ValueError(f"could not read {_LEAKY_TEXT[0]}"),
        ),
        pytest.raises(ProbeConnectionError) as raised,
    ):
        GCSMetadataProbe.for_config(config)
    # A caller that prints the traceback sees the cause chain too.
    rendered = "".join(traceback.format_exception(raised.value))
    assert _LEAKY_TEXT[0] not in rendered


def test_prefix_budget_stops_and_warns(
    seeded_bucket: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datahub.ingestion.source.data_lake_common import object_store_probe

    monkeypatch.setattr(object_store_probe, "MAX_RESOLVED_PREFIXES", 1)
    result = _run(_recipe("gs://my-bucket/*/{table}/*.parquet"), "datasets")
    assert any("narrow" in w for w in result.warnings)


def test_the_provider_closes_its_clients() -> None:
    from datahub.ingestion.source.gcs.gcs_probe import GCSMetadataProbe
    from datahub.ingestion.source.gcs.gcs_source import GCSSourceConfig

    probe = GCSMetadataProbe.for_config(
        GCSSourceConfig.model_validate(_recipe("gs://my-bucket/raw/*.csv"))
    )
    # pydantic refuses to set attributes on a model instance, so patch the class.
    with mock.patch.object(AwsConnectionConfig, "close_cached_s3_clients") as close:
        with probe:
            pass
    close.assert_called_once()


def test_for_config_reports_the_specs_ingestion_matches_with() -> None:
    from datahub.ingestion.source.gcs.gcs_probe import GCSMetadataProbe
    from datahub.ingestion.source.gcs.gcs_source import (
        GCSSourceConfig,
        equivalent_s3_path_specs,
    )

    config = GCSSourceConfig.model_validate(_recipe("gs://my-bucket/data/{table}"))
    probe = GCSMetadataProbe.for_config(config)
    assert [s.include for s in probe._path_specs] == [
        s.include for s in equivalent_s3_path_specs(config.path_specs)
    ]


@mock.patch("google.auth.default")
def test_unloadable_credentials_are_reported_without_their_details(
    mock_default: mock.MagicMock,
) -> None:
    mock_default.side_effect = google.auth.exceptions.DefaultCredentialsError(
        "could not parse /secrets/key.json: private_key=abc123"
    )
    recipe: Dict[str, object] = {
        "auth_type": "workload_identity",
        "path_specs": [{"include": "gs://my-bucket/raw/*.csv"}],
    }
    with pytest.raises(ProbeConnectionError) as raised:
        _run(recipe, "buckets")
    message = str(raised.value)
    assert "workload_identity" in message
    assert "abc123" not in message and "key.json" not in message
