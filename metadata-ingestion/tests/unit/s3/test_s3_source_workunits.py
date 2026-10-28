"""End-to-end workunit generation of S3Source over local files and moto-backed
buckets: file grouping, content-type driven inference, tagging, profiling
wiring and the failure modes a listing can hit."""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type, cast
from unittest.mock import MagicMock, patch

import pytest
from boto3.session import Session
from moto import mock_aws

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.datalake_profiler_config import (
    DataLakeProfilerConfig,
)
from datahub.ingestion.source.s3.source import (
    S3Source,
    TableData,
    partitioned_folder_comparator,
)
from datahub.ingestion.source.schema_inference import avro, csv_tsv, json, parquet
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SchemaMetadataClass,
)

AWS_CONFIG: Dict[str, str] = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "aws_region": "us-east-1",
}
CSV_BODY = "a,b\n1,2\n3,4\n"


@pytest.fixture(autouse=True)
def s3_client() -> Iterable[Any]:
    with mock_aws():
        session = Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )
        yield session.client("s3")


def _source(path_spec: Dict[str, Any], **config: Any) -> S3Source:
    return S3Source.create(
        config_dict={"path_spec": path_spec, **config},
        ctx=PipelineContext(run_id="test-s3"),
    )


def _aspects_by_urn(workunits: Iterable[MetadataWorkUnit]) -> Dict[str, List[Any]]:
    out: Dict[str, List[Any]] = {}
    for wu in workunits:
        md = wu.metadata
        if isinstance(md, MetadataChangeProposalWrapper) and md.aspect is not None:
            out.setdefault(str(md.entityUrn), []).append(md.aspect)
    return out


def _aspect(aspects: List[Any], aspect_type: Type[Any]) -> Any:
    return next(a for a in aspects if isinstance(a, aspect_type))


def _dataset_urns(aspects: Dict[str, List[Any]]) -> List[str]:
    return sorted(u for u in aspects if u.startswith("urn:li:dataset:"))


# --------------------------------------------------------------------------- #
# Local files
# --------------------------------------------------------------------------- #


def test_local_files_under_one_table_folder_become_one_dataset(
    tmp_path: Path,
) -> None:
    table_dir = tmp_path / "data" / "events"
    table_dir.mkdir(parents=True)
    (table_dir / "part-1.csv").write_text(CSV_BODY)
    (table_dir / "part-2.csv").write_text(CSV_BODY + "5,6\n")

    source = _source(
        {"include": f"{tmp_path}/data/{{table}}/*.csv", "table_name": "{table}"}
    )
    aspects = _aspects_by_urn(source.get_workunits_internal())

    urns = _dataset_urns(aspects)
    assert len(urns) == 1
    assert urns[0].endswith("data/events,PROD)")
    props = _aspect(aspects[urns[0]], DatasetPropertiesClass)
    assert props.customProperties["number_of_files"] == "2"
    assert props.customProperties["size_in_bytes"] == str(
        len(CSV_BODY) + len(CSV_BODY) + 4
    )
    schema = _aspect(aspects[urns[0]], SchemaMetadataClass)
    assert sorted(f.fieldPath for f in schema.fields) == ["a", "b"]


def test_local_single_file_path_is_scanned_as_one_dataset(tmp_path: Path) -> None:
    csv = tmp_path / "only.csv"
    csv.write_text(CSV_BODY)

    source = _source({"include": str(csv)})
    aspects = _aspects_by_urn(source.get_workunits_internal())

    urns = _dataset_urns(aspects)
    assert len(urns) == 1
    assert urns[0].endswith("only.csv,PROD)")


def test_partitioned_folder_comparator_treats_equal_numbers_as_equal() -> None:
    assert partitioned_folder_comparator("year=2024", "year=2024") == 0
    assert partitioned_folder_comparator("007", "7") == 0


# --------------------------------------------------------------------------- #
# Simple (non-templated) S3 paths
# --------------------------------------------------------------------------- #


def test_simple_s3_path_emits_one_dataset_per_file_with_file_stats(
    s3_client: Any,
) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    s3_client.put_object(Bucket="bkt", Key="data/b.csv", Body=CSV_BODY)
    s3_client.put_object(Bucket="bkt", Key="data/ignored.txt", Body="x")

    source = _source({"include": "s3://bkt/data/*.csv"}, aws_config=AWS_CONFIG)
    aspects = _aspects_by_urn(source.get_workunits_internal())

    urns = _dataset_urns(aspects)
    assert [u.split(",")[1] for u in urns] == ["bkt/data/a.csv", "bkt/data/b.csv"]
    props = _aspect(aspects[urns[0]], DatasetPropertiesClass)
    # Simple paths never sample, so per-file stats are recorded.
    assert props.customProperties["number_of_files"] == "1"
    assert props.customProperties["size_in_bytes"] == str(len(CSV_BODY))


def test_s3_content_type_overrides_extension_for_inference(s3_client: Any) -> None:
    """With use_s3_content_type the object's Content-Type decides the parser,
    so a .dat object served as text/csv still yields a schema."""
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(
        Bucket="bkt", Key="data/export.dat", Body=CSV_BODY, ContentType="text/csv"
    )

    source = _source(
        {"include": "s3://bkt/data/*.*"},
        aws_config=AWS_CONFIG,
        use_s3_content_type=True,
    )
    aspects = _aspects_by_urn(source.get_workunits_internal())

    urns = _dataset_urns(aspects)
    assert len(urns) == 1
    schema = _aspect(aspects[urns[0]], SchemaMetadataClass)
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
    source = _source({"include": "s3://bkt/data/*.csv"}, aws_config=AWS_CONFIG)

    assert isinstance(source._get_inferrer(extension, content_type), inferrer_type)


def test_unreadable_file_yields_no_schema_but_a_warning(s3_client: Any) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/bad.parquet", Body=b"not parquet")

    source = _source(
        {"include": "s3://bkt/data/*.parquet"},
        aws_config=AWS_CONFIG,
        sort_schema_fields=True,
    )
    aspects = _aspects_by_urn(source.get_workunits_internal())

    urns = _dataset_urns(aspects)
    assert len(urns) == 1
    # A parse failure inside the inferrer is reported, not raised, and the
    # dataset is still emitted with an empty schema.
    schema = _aspect(aspects[urns[0]], SchemaMetadataClass)
    assert schema.fields == []
    assert source.report.warnings.total_elements >= 1


# --------------------------------------------------------------------------- #
# ingest_table options
# --------------------------------------------------------------------------- #


def _table_data(full_path: str, size: int = len(CSV_BODY)) -> TableData:
    return TableData(
        display_name="t",
        is_s3=True,
        full_path=full_path,
        timestamp=datetime(2020, 1, 1, tzinfo=timezone.utc),
        table_path=full_path,
        size_in_bytes=size,
        number_of_files=1,
    )


def test_platform_instance_and_lowercase_urns_apply_to_datasets(
    s3_client: Any,
) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="Data/Upper.csv", Body=CSV_BODY)
    path_spec = PathSpec(include="s3://bkt/Data/*.csv")
    source = _source(
        {"include": path_spec.include},
        aws_config=AWS_CONFIG,
        platform_instance="lake",
        convert_urns_to_lowercase=True,
    )

    aspects = _aspects_by_urn(
        source.ingest_table(_table_data("s3://bkt/Data/Upper.csv"), path_spec)
    )

    urns = _dataset_urns(aspects)
    assert urns == [
        "urn:li:dataset:(urn:li:dataPlatform:s3,lake.bkt/data/upper.csv,PROD)"
    ]
    instance = _aspect(aspects[urns[0]], DataPlatformInstanceClass)
    assert (
        instance.instance == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:s3,lake)"
    )


def test_schema_inference_disabled_emits_no_schema(s3_client: Any) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    path_spec = PathSpec(include="s3://bkt/data/*.csv")
    source = _source(
        {"include": path_spec.include},
        aws_config=AWS_CONFIG,
        enable_schema_inference=False,
    )

    aspects = _aspects_by_urn(
        source.ingest_table(_table_data("s3://bkt/data/a.csv"), path_spec)
    )

    urn = _dataset_urns(aspects)[0]
    assert not any(isinstance(a, SchemaMetadataClass) for a in aspects[urn])
    props = _aspect(aspects[urn], DatasetPropertiesClass)
    assert "schema_inferred_from" not in props.customProperties


def test_empty_file_is_not_opened_for_schema_inference(s3_client: Any) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/empty.csv", Body=b"")
    path_spec = PathSpec(include="s3://bkt/data/*.csv")
    source = _source({"include": path_spec.include}, aws_config=AWS_CONFIG)
    get_fields = MagicMock()
    source.get_fields = get_fields  # type: ignore[method-assign]

    aspects = _aspects_by_urn(
        source.ingest_table(_table_data("s3://bkt/data/empty.csv", size=0), path_spec)
    )

    get_fields.assert_not_called()
    urn = _dataset_urns(aspects)[0]
    assert not any(isinstance(a, SchemaMetadataClass) for a in aspects[urn])


def test_bucket_tags_are_attached_as_global_tags(s3_client: Any) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_bucket_tagging(
        Bucket="bkt", Tagging={"TagSet": [{"Key": "env", "Value": "prod"}]}
    )
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    path_spec = PathSpec(include="s3://bkt/data/*.csv")
    source = _source(
        {"include": path_spec.include},
        aws_config=AWS_CONFIG,
        use_s3_bucket_tags=True,
    )

    aspects = _aspects_by_urn(
        source.ingest_table(_table_data("s3://bkt/data/a.csv"), path_spec)
    )

    tags = _aspect(aspects[_dataset_urns(aspects)[0]], GlobalTagsClass)
    assert [t.tag for t in tags.tags] == ["urn:li:tag:env:prod"]
    assert source.report.tables_tagged == 1


# --------------------------------------------------------------------------- #
# Profiling wiring
# --------------------------------------------------------------------------- #


def test_profiling_enabled_runs_profiler_per_table_and_summarises(
    s3_client: Any,
) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/a.csv", Body=CSV_BODY)
    source = _source(
        {"include": "s3://bkt/data/*.csv"},
        aws_config=AWS_CONFIG,
        profiling={"enabled": True},
        profile_patterns={"allow": ["bkt.*"]},
    )

    # The per-table profiler is exercised elsewhere; here only the wiring matters.
    def fake_profile(*_args: Any, **_kwargs: Any) -> Iterable[MetadataWorkUnit]:
        source.profiling_times_taken.append(0.25)
        return iter([])

    get_table_profile = MagicMock(side_effect=fake_profile)
    source.profiler.get_table_profile = get_table_profile  # type: ignore[method-assign]

    with patch(
        "datahub.ingestion.source.s3.source.telemetry.telemetry_instance.ping"
    ) as ping:
        aspects = _aspects_by_urn(source.get_workunits_internal())

    assert len(_dataset_urns(aspects)) == 1
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
    profiler_module = "datahub.ingestion.source.data_lake_common.profiling.profiler"
    with (
        patch.dict(sys.modules, {profiler_module: None}),
        pytest.raises(RuntimeError, match="Profiling dependencies"),
    ):
        _source(
            {"include": "s3://bkt/data/*.csv"},
            aws_config=AWS_CONFIG,
            profiling={"enabled": True},
        )


def test_table_level_only_profiling_disables_field_metrics() -> None:
    config = DataLakeProfilerConfig(enabled=True, profile_table_level_only=True)

    assert not config.include_field_null_count
    assert not config.include_field_min_value
    assert not config.include_field_sample_values


# --------------------------------------------------------------------------- #
# Object store adapters
# --------------------------------------------------------------------------- #


def test_gcs_endpoint_switches_adapter_but_keeps_s3_style_paths() -> None:
    source = _source(
        {"include": "s3://bkt/data/*.csv"},
        aws_config={
            **AWS_CONFIG,
            "aws_endpoint_url": "https://storage.googleapis.com",
        },
    )

    assert source.object_store_adapter.platform == "gcs"
    assert source.object_store_adapter.create_s3_path("bkt", "data/a.csv") == (
        "s3://bkt/data/a.csv"
    )


def test_localstack_endpoint_defaults_region_for_console_links() -> None:
    source = _source(
        {"include": "s3://bkt/data/*.csv"},
        aws_config={
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_endpoint_url": "http://localstack:4566",
        },
    )

    url = source.get_external_url(_table_data("s3://bkt/data/a.csv"))
    assert url is not None
    assert "region=us-east-1" in url


# --------------------------------------------------------------------------- #
# Templated paths
# --------------------------------------------------------------------------- #


def test_min_max_traversal_scans_both_oldest_and_newest_partition(
    s3_client: Any,
) -> None:
    s3_client.create_bucket(Bucket="bkt")
    for key in [
        "data/events/year=2022/a.csv",
        "data/events/year=2023/b.csv",
        "data/events/year=2024/c.csv",
    ]:
        s3_client.put_object(Bucket="bkt", Key=key, Body=CSV_BODY)

    source = _source(
        {
            "include": "s3://bkt/data/{table}/year=*/*.csv",
            "table_name": "{table}",
            "traversal_method": "MIN_MAX",
        },
        aws_config=AWS_CONFIG,
    )
    aspects = _aspects_by_urn(source.get_workunits_internal())

    urns = _dataset_urns(aspects)
    assert len(urns) == 1
    props = _aspect(aspects[urns[0]], DatasetPropertiesClass)
    # Oldest and newest partitions are scanned, the middle one is skipped, and
    # the schema is read from the newest file.
    assert props.customProperties["number_of_partitions"] == "2"
    assert props.customProperties["schema_inferred_from"] == (
        "s3://bkt/data/events/year=2024/c.csv"
    )


def test_table_folder_without_matching_files_emits_nothing(s3_client: Any) -> None:
    s3_client.create_bucket(Bucket="bkt")
    s3_client.put_object(Bucket="bkt", Key="data/events/readme.txt", Body="x")

    source = _source(
        {"include": "s3://bkt/data/{table}/*.csv", "table_name": "{table}"},
        aws_config=AWS_CONFIG,
    )

    assert _dataset_urns(_aspects_by_urn(source.get_workunits_internal())) == []


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("flag", ["use_s3_object_tags", "use_s3_content_type"])
def test_s3_only_flags_are_rejected_for_local_platform(flag: str) -> None:
    with pytest.raises(ValueError, match="platform is not s3"):
        DataLakeSourceConfig.parse_obj(
            {"path_specs": [{"include": "/local/path/*.csv"}], flag: True}
        )


def test_explicit_platform_is_not_second_guessed_from_uris() -> None:
    config = DataLakeSourceConfig.parse_obj(
        {"platform": "gcs", "path_specs": [{"include": "s3://bkt/data/*.csv"}]}
    )

    assert config.platform == "gcs"
    assert cast(List[PathSpec], config.path_specs)[0].include.startswith("s3://")
