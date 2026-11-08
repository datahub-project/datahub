"""End-to-end workunit generation of S3Source over local files and moto-backed
buckets: file grouping, content-type driven inference, tagging, profiling
wiring and the failure modes a listing can hit."""

import sys
from pathlib import Path
from typing import Any, Iterable, Optional, Type
from unittest.mock import create_autospec, patch

import pytest
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SchemaMetadataClass,
)
from tests.unit.conftest import aspects_by_urn, log_messages, single_aspect
from tests.unit.s3.conftest import (
    AWS_CONFIG,
    CSV_BODY,
    dataset_urns,
    make_s3_source,
    make_table_data,
)

# Every test runs inside mock_aws so a stray listing can never reach real AWS.
pytestmark = pytest.mark.usefixtures("s3")

GCS_ENDPOINT_AWS_CONFIG = {
    **AWS_CONFIG,
    "aws_endpoint_url": "https://storage.googleapis.com",
}


# --------------------------------------------------------------------------- #
# Local files
# --------------------------------------------------------------------------- #


def test_local_files_under_one_table_folder_become_one_dataset(
    tmp_path: Path,
) -> None:
    """All files under one {table} folder aggregate into a single dataset whose
    file count and size cover every file, with the schema read from one."""
    table_dir = tmp_path / "data" / "events"
    table_dir.mkdir(parents=True)
    (table_dir / "part-1.csv").write_text(CSV_BODY)
    (table_dir / "part-2.csv").write_text(CSV_BODY + "5,6\n")

    source = make_s3_source(
        {"include": f"{tmp_path}/data/{{table}}/*.csv", "table_name": "{table}"}
    )
    aspects = aspects_by_urn(source.get_workunits_internal())

    urns = dataset_urns(aspects)
    assert len(urns) == 1
    assert urns[0].endswith("data/events,PROD)")
    props = single_aspect(aspects[urns[0]], DatasetPropertiesClass)
    assert props.customProperties["number_of_files"] == "2"
    assert props.customProperties["size_in_bytes"] == str(
        len(CSV_BODY) + len(CSV_BODY) + 4
    )
    schema = single_aspect(aspects[urns[0]], SchemaMetadataClass)
    assert sorted(f.fieldPath for f in schema.fields) == ["a", "b"]


def test_local_single_file_path_is_scanned_as_one_dataset(tmp_path: Path) -> None:
    csv = tmp_path / "only.csv"
    csv.write_text(CSV_BODY)

    source = make_s3_source({"include": str(csv)})
    aspects = aspects_by_urn(source.get_workunits_internal())

    urns = dataset_urns(aspects)
    assert len(urns) == 1
    assert urns[0].endswith("only.csv,PROD)")


# --------------------------------------------------------------------------- #
# Simple (non-templated) S3 paths
# --------------------------------------------------------------------------- #


def test_simple_s3_path_emits_one_dataset_per_file_with_file_stats(
    s3_client: S3Client,
) -> None:
    """Without a {table} marker every matching object is its own dataset, files
    not matching the include glob are ignored, and per-file stats are recorded
    because simple paths never sample."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    s3_client.put_object(Bucket="bkt", Key="data/b.csv", Body=CSV_BODY)
    s3_client.put_object(Bucket="bkt", Key="data/ignored.txt", Body="x")

    source = make_s3_source({"include": "s3://bkt/data/*.csv"}, aws_config=AWS_CONFIG)
    aspects = aspects_by_urn(source.get_workunits_internal())

    urns = dataset_urns(aspects)
    assert [u.split(",")[1] for u in urns] == ["bkt/data/a.csv", "bkt/data/b.csv"]
    props = single_aspect(aspects[urns[0]], DatasetPropertiesClass)
    assert props.customProperties["number_of_files"] == "1"
    assert props.customProperties["size_in_bytes"] == str(len(CSV_BODY))


def test_s3_content_type_overrides_extension_for_inference(
    s3_client: S3Client,
) -> None:
    """With use_s3_content_type the object's Content-Type decides the parser,
    so a .dat object served as text/csv still yields a schema."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(
        Bucket="bkt", Key="data/export.dat", Body=CSV_BODY, ContentType="text/csv"
    )

    source = make_s3_source(
        {"include": "s3://bkt/data/*.*"},
        aws_config=AWS_CONFIG,
        use_s3_content_type=True,
    )
    aspects = aspects_by_urn(source.get_workunits_internal())

    urns = dataset_urns(aspects)
    assert len(urns) == 1
    schema = single_aspect(aspects[urns[0]], SchemaMetadataClass)
    assert sorted(f.fieldPath for f in schema.fields) == ["a", "b"]


@pytest.mark.parametrize(
    "content_type, extension, inferrer_type",
    [
        ("application/vnd.apache.parquet", ".csv", parquet.ParquetInferrer),
        ("text/tab-separated-values", ".csv", csv_tsv.TsvInferrer),
        ("application/json", ".csv", json.JsonInferrer),
        ("application/avro", ".csv", avro.AvroInferrer),
        (None, ".jsonl", json.JsonInferrer),
        (None, ".tsv", csv_tsv.TsvInferrer),
        (None, ".avro", avro.AvroInferrer),
        (None, ".xml", type(None)),
    ],
)
def test_inferrer_selection_prefers_content_type(
    content_type: Optional[str], extension: str, inferrer_type: Type[Any]
) -> None:
    """A known Content-Type wins over the file extension; without one the
    extension decides, and an unsupported extension has no inferrer."""
    source = make_s3_source({"include": "s3://bkt/data/*.csv"}, aws_config=AWS_CONFIG)

    assert isinstance(source._get_inferrer(extension, content_type), inferrer_type)


def test_unreadable_file_yields_no_schema_but_a_warning(s3_client: S3Client) -> None:
    """A parse failure inside the inferrer is reported, not raised, and the
    dataset is still emitted with an empty schema."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/bad.parquet", Body=b"not parquet")

    source = make_s3_source(
        {"include": "s3://bkt/data/*.parquet"},
        aws_config=AWS_CONFIG,
        sort_schema_fields=True,
    )
    aspects = aspects_by_urn(source.get_workunits_internal())

    urns = dataset_urns(aspects)
    assert len(urns) == 1
    schema = single_aspect(aspects[urns[0]], SchemaMetadataClass)
    assert schema.fields == []
    assert "Could not infer schema for file" in log_messages(source.report.warnings)


# --------------------------------------------------------------------------- #
# ingest_table options
# --------------------------------------------------------------------------- #


def test_platform_instance_and_lowercase_urns_apply_to_datasets(
    s3_client: S3Client,
) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="Data/Upper.csv", Body=CSV_BODY)
    path_spec = PathSpec(include="s3://bkt/Data/*.csv")
    source = make_s3_source(
        {"include": path_spec.include},
        aws_config=AWS_CONFIG,
        platform_instance="lake",
        convert_urns_to_lowercase=True,
    )

    aspects = aspects_by_urn(
        source.ingest_table(make_table_data("s3://bkt/Data/Upper.csv"), path_spec)
    )

    urns = dataset_urns(aspects)
    assert urns == [
        "urn:li:dataset:(urn:li:dataPlatform:s3,lake.bkt/data/upper.csv,PROD)"
    ]
    instance = single_aspect(aspects[urns[0]], DataPlatformInstanceClass)
    assert (
        instance.instance == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:s3,lake)"
    )


def test_schema_inference_disabled_emits_no_schema(s3_client: S3Client) -> None:
    """With enable_schema_inference off, no SchemaMetadata is emitted and the
    dataset does not claim a file its schema was inferred from."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    path_spec = PathSpec(include="s3://bkt/data/*.csv")
    source = make_s3_source(
        {"include": path_spec.include},
        aws_config=AWS_CONFIG,
        enable_schema_inference=False,
    )

    aspects = aspects_by_urn(
        source.ingest_table(make_table_data("s3://bkt/data/a.csv"), path_spec)
    )

    urn = dataset_urns(aspects)[0]
    assert not any(isinstance(a, SchemaMetadataClass) for a in aspects[urn])
    props = single_aspect(aspects[urn], DatasetPropertiesClass)
    assert "schema_inferred_from" not in props.customProperties


def test_empty_file_is_not_opened_for_schema_inference(s3_client: S3Client) -> None:
    """A zero-byte object is never opened for inference (parsers raise on empty
    input), so no schema and no inference warning are produced."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/empty.csv", Body=b"")
    path_spec = PathSpec(include="s3://bkt/data/*.csv")
    source = make_s3_source({"include": path_spec.include}, aws_config=AWS_CONFIG)
    get_fields = create_autospec(source.get_fields)
    setattr(source, "get_fields", get_fields)  # noqa: B010

    aspects = aspects_by_urn(
        source.ingest_table(
            make_table_data("s3://bkt/data/empty.csv", size=0), path_spec
        )
    )

    get_fields.assert_not_called()
    urn = dataset_urns(aspects)[0]
    assert not any(isinstance(a, SchemaMetadataClass) for a in aspects[urn])


def test_bucket_tags_are_attached_as_global_tags(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_bucket_tagging(
        Bucket="bkt", Tagging={"TagSet": [{"Key": "env", "Value": "prod"}]}
    )
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    path_spec = PathSpec(include="s3://bkt/data/*.csv")
    source = make_s3_source(
        {"include": path_spec.include},
        aws_config=AWS_CONFIG,
        use_s3_bucket_tags=True,
    )

    aspects = aspects_by_urn(
        source.ingest_table(make_table_data("s3://bkt/data/a.csv"), path_spec)
    )

    tags = single_aspect(aspects[dataset_urns(aspects)[0]], GlobalTagsClass)
    assert [t.tag for t in tags.tags] == ["urn:li:tag:env:prod"]
    assert source.report.tables_tagged == 1


# --------------------------------------------------------------------------- #
# Profiling wiring
# --------------------------------------------------------------------------- #


def test_profiling_enabled_runs_profiler_per_table_and_summarises(
    s3_client: S3Client,
) -> None:
    """With profiling on, each table is handed to the profiler once and the run
    ends with a profiling-summary telemetry event; the per-table profiler itself
    is covered by the data_lake_common profiler tests."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    source = make_s3_source(
        {"include": "s3://bkt/data/*.csv"},
        aws_config=AWS_CONFIG,
        profiling={"enabled": True},
        profile_patterns={"allow": ["bkt.*"]},
    )

    def fake_profile(*_args: Any, **_kwargs: Any) -> Iterable[MetadataWorkUnit]:
        source.profiling_times_taken.append(0.25)
        return iter([])

    get_table_profile = create_autospec(
        source.profiler.get_table_profile, side_effect=fake_profile
    )
    setattr(source.profiler, "get_table_profile", get_table_profile)  # noqa: B010

    with patch(
        "datahub.ingestion.source.s3.source.telemetry.telemetry_instance.ping",
        autospec=True,
    ) as ping:
        aspects = aspects_by_urn(source.get_workunits_internal())

    assert len(dataset_urns(aspects)) == 1
    get_table_profile.assert_called_once()
    summary = next(
        c.args[1]
        for c in ping.call_args_list
        if c.args[0] == "data_lake_profiling_summary"
    )
    assert summary["platform"] == "s3"
    assert "table_time_taken_p50" in summary
    # The profile allow/deny pattern is handed to the profiler config.
    assert source.source_config.profiling._allow_deny_patterns.allowed("bkt/data")


def test_missing_profiling_dependencies_fail_fast() -> None:
    """Enabling profiling without the optional profiling extras fails at source
    construction with an actionable error, not mid-run."""
    profiler_module = "datahub.ingestion.source.data_lake_common.profiling.profiler"
    with (
        patch.dict(sys.modules, {profiler_module: None}),
        pytest.raises(RuntimeError, match="Profiling dependencies"),
    ):
        make_s3_source(
            {"include": "s3://bkt/data/*.csv"},
            aws_config=AWS_CONFIG,
            profiling={"enabled": True},
        )


# --------------------------------------------------------------------------- #
# Object store adapters
# --------------------------------------------------------------------------- #


def test_gcs_endpoint_switches_adapter_but_keeps_s3_style_paths() -> None:
    """GCS reached through its S3-compatible endpoint uses the GCS adapter, but
    the dataset paths the source builds stay s3:// so URNs do not change."""
    source = make_s3_source(
        {"include": "s3://bkt/data/*.csv"}, aws_config=GCS_ENDPOINT_AWS_CONFIG
    )

    assert source.object_store_adapter.platform == "gcs"
    assert source.create_s3_path("bkt", "data/a.csv") == "s3://bkt/data/a.csv"


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "Bug: for an aws_endpoint_url on storage.googleapis.com, S3Source swaps in "
        "the GCS adapter and registers a create_s3_path customization on it, but "
        "never calls apply_customizations, so the registration is a no-op; the "
        "GCS adapter's get_external_url only accepts gs:// URIs while the source "
        "keeps s3:// paths, so every dataset gets no external URL."
    ),
)
def test_gcs_endpoint_datasets_link_to_the_gcs_console() -> None:
    source = make_s3_source(
        {"include": "s3://bkt/data/*.csv"}, aws_config=GCS_ENDPOINT_AWS_CONFIG
    )

    url = source.get_external_url(make_table_data("s3://bkt/data/a.csv"))

    assert url is not None
    assert url.startswith("https://console.cloud.google.com/storage/browser/")


def test_localstack_endpoint_defaults_region_for_console_links() -> None:
    """A LocalStack endpoint with no aws_region still produces console links
    with a region, defaulting to us-east-1."""
    source = make_s3_source(
        {"include": "s3://bkt/data/*.csv"},
        aws_config={
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_endpoint_url": "http://localstack:4566",
        },
    )

    url = source.get_external_url(make_table_data("s3://bkt/data/a.csv"))
    assert url is not None
    assert "region=us-east-1" in url


# --------------------------------------------------------------------------- #
# Templated paths
# --------------------------------------------------------------------------- #


def test_min_max_traversal_scans_both_oldest_and_newest_partition(
    s3_client: S3Client,
) -> None:
    """MIN_MAX scans only the oldest and newest partitions (the middle one is
    skipped) and infers the schema from the newest file."""
    s3_client.create_bucket(Bucket="bkt")
    for key in [
        "data/events/year=2022/a.csv",
        "data/events/year=2023/b.csv",
        "data/events/year=2024/c.csv",
    ]:
        s3_client.put_object(Bucket="bkt", Key=key, Body=CSV_BODY)

    source = make_s3_source(
        {
            "include": "s3://bkt/data/{table}/year=*/*.csv",
            "table_name": "{table}",
            "traversal_method": "MIN_MAX",
        },
        aws_config=AWS_CONFIG,
    )
    aspects = aspects_by_urn(source.get_workunits_internal())

    urns = dataset_urns(aspects)
    assert len(urns) == 1
    props = single_aspect(aspects[urns[0]], DatasetPropertiesClass)
    assert props.customProperties["number_of_partitions"] == "2"
    assert props.customProperties["schema_inferred_from"] == (
        "s3://bkt/data/events/year=2024/c.csv"
    )


def test_table_folder_without_matching_files_emits_nothing(
    s3_client: S3Client,
) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/events/readme.txt", Body="x")

    source = make_s3_source(
        {"include": "s3://bkt/data/{table}/*.csv", "table_name": "{table}"},
        aws_config=AWS_CONFIG,
    )

    assert dataset_urns(aspects_by_urn(source.get_workunits_internal())) == []


@pytest.mark.xfail(
    strict=True,
    raises=ClientError,
    reason=(
        "Bug: _process_templated_path detects a missing bucket with "
        "isinstance(e, s3.meta.client.exceptions.NoSuchBucket), but boto exception "
        "classes are generated per session, and the listing runs on the cached "
        "get_s3_client() while `s3` is a get_s3_resource() from a fresh session, so "
        "the check never matches and NoSuchBucket is re-raised instead of warned."
    ),
)
def test_missing_bucket_in_templated_path_is_warned_not_raised() -> None:
    source = make_s3_source(
        {"include": "s3://missing/data/{table}/*.csv", "table_name": "{table}"},
        aws_config=AWS_CONFIG,
    )

    assert list(source.get_workunits_internal()) == []
    assert "Missing bucket" in log_messages(source.report.warnings)
