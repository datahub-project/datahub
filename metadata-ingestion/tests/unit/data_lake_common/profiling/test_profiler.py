import io
from pathlib import Path
from types import SimpleNamespace

import boto3
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.data_lake_common.profiling.profiler import FileProfiler
from datahub.ingestion.source.s3.datalake_profiler_config import DataLakeProfilerConfig
from datahub.ingestion.source.s3.report import DataLakeSourceReport
from datahub.metadata.schema_classes import DatasetProfileClass

# 100 distinct values, each appearing twice: pct_unique=0.5, landing in the
# MANY/VERY_MANY cardinality bucket so min/max/mean/stdev get surfaced.
HIGH_CARDINALITY_IDS = list(range(1, 101)) * 2


def make_profiler(**config_kwargs: object) -> FileProfiler:
    return FileProfiler(
        aws_config=None,
        verify_ssl=None,
        report=DataLakeSourceReport(),
        profiling_times_taken=[],
        profiling_config=DataLakeProfilerConfig(enabled=True, **config_kwargs),
    )


def make_table_data(path: str) -> SimpleNamespace:
    return SimpleNamespace(
        display_name="test_table",
        full_path=path,
        table_path=path,
        partitions=None,
    )


def get_profile(work_unit: MetadataWorkUnit) -> DatasetProfileClass:
    assert isinstance(work_unit.metadata, MetadataChangeProposalWrapper)
    profile = work_unit.metadata.aspect
    assert isinstance(profile, DatasetProfileClass)
    return profile


def parquet_bytes() -> bytes:
    buf = io.BytesIO()
    pq.write_table(
        pa.table({"id": pa.array(HIGH_CARDINALITY_IDS, type=pa.int64())}), buf
    )
    return buf.getvalue()


def make_s3_profiler() -> FileProfiler:
    return FileProfiler(
        aws_config=AwsConnectionConfig(
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_region="us-east-1",
        ),
        verify_ssl=None,
        report=DataLakeSourceReport(),
        profiling_times_taken=[],
        profiling_config=DataLakeProfilerConfig(enabled=True),
    )


def test_profiles_local_parquet_file(tmp_path: Path) -> None:
    path = tmp_path / "test.parquet"
    table = pa.table(
        {
            "id": pa.array(HIGH_CARDINALITY_IDS, type=pa.int64()),
            "category": pa.array(["a", "b", "c"] * 66 + ["a", "a"]),
        }
    )
    pq.write_table(table, str(path))

    profiler = make_profiler()
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    assert len(work_units) == 1
    profile = get_profile(work_units[0])
    assert profile.rowCount == 200
    assert profile.columnCount == 2

    field_profiles = profile.fieldProfiles or []
    id_profile = next(f for f in field_profiles if f.fieldPath == "id")
    assert id_profile.min == "1"
    assert id_profile.max == "100"
    assert id_profile.uniqueCount == 100
    assert id_profile.nullCount == 0
    assert id_profile.quantiles
    assert id_profile.histogram is not None

    category_profile = next(f for f in field_profiles if f.fieldPath == "category")
    assert category_profile.distinctValueFrequencies
    assert {vf.value for vf in category_profile.distinctValueFrequencies} == {
        "a",
        "b",
        "c",
    }


def test_profiles_local_csv_file(tmp_path: Path) -> None:
    path = tmp_path / "test.csv"
    rows = "\n".join(f"{i},name{i}" for i in HIGH_CARDINALITY_IDS)
    path.write_text(f"id,name\n{rows}\n")

    profiler = make_profiler()
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    profile = get_profile(work_units[0])
    assert profile.rowCount == 200
    field_profiles = profile.fieldProfiles or []
    id_profile = next(f for f in field_profiles if f.fieldPath == "id")
    assert id_profile.min == "1"
    assert id_profile.max == "100"


def test_profiles_local_avro_file(tmp_path: Path) -> None:
    path = tmp_path / "test.avro"
    schema = {
        "type": "record",
        "name": "Test",
        "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}],
    }
    records = [{"id": i, "name": f"name{i}"} for i in HIGH_CARDINALITY_IDS]
    with open(path, "wb") as f:
        fastavro.writer(f, schema, records)

    profiler = make_profiler()
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    profile = get_profile(work_units[0])
    assert profile.rowCount == 200
    field_profiles = profile.fieldProfiles or []
    id_profile = next(f for f in field_profiles if f.fieldPath == "id")
    assert id_profile.min == "1"
    assert id_profile.max == "100"


def test_profile_table_level_only_skips_field_profiles(tmp_path: Path) -> None:
    path = tmp_path / "test.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2], type=pa.int64())}), str(path))

    profiler = make_profiler(profile_table_level_only=True)
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    profile = get_profile(work_units[0])
    assert profile.rowCount == 2
    assert not profile.fieldProfiles


def test_unreadable_file_reports_warning_and_yields_nothing(tmp_path: Path) -> None:
    path = tmp_path / "test.unsupported"
    path.write_text("not a real data file")

    profiler = make_profiler()
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    assert work_units == []
    assert profiler.report.warnings.total_elements > 0


def test_max_number_of_fields_to_profile_drops_extra_columns(tmp_path: Path) -> None:
    path = tmp_path / "test.parquet"
    pq.write_table(
        pa.table(
            {
                "a": pa.array([1], type=pa.int64()),
                "b": pa.array([2], type=pa.int64()),
                "c": pa.array([3], type=pa.int64()),
            }
        ),
        str(path),
    )

    profiler = make_profiler(max_number_of_fields_to_profile=2)
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    profile = get_profile(work_units[0])
    field_profiles = profile.fieldProfiles or []
    assert len(field_profiles) == 2
    assert profiler.report.number_of_files_filtered == 1


def test_profiles_local_tsv_and_json_files(tmp_path: Path) -> None:
    tsv = tmp_path / "t.tsv"
    tsv.write_text("id\tname\n1\ta\n2\tb\n")
    json_file = tmp_path / "t.json"
    json_file.write_text('{"id": 1, "name": "a"}\n{"id": 2, "name": "b"}\n')

    profiler = make_profiler()
    for path in (tsv, json_file):
        work_units = list(
            profiler.get_table_profile(
                make_table_data(str(path)), "urn:li:dataset:test"
            )
        )
        assert get_profile(work_units[0]).rowCount == 2


def test_corrupt_file_with_valid_extension_reports_warning(tmp_path: Path) -> None:
    # Valid extension but unreadable bytes exercises the read-exception path
    # (distinct from an unsupported extension).
    path = tmp_path / "broken.parquet"
    path.write_bytes(b"not a parquet file")

    profiler = make_profiler()
    work_units = list(
        profiler.get_table_profile(make_table_data(str(path)), "urn:li:dataset:test")
    )

    assert work_units == []
    assert profiler.report.warnings.total_elements > 0


@mock_aws
def test_profiles_single_s3_parquet_file() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="data/demo.parquet", Body=parquet_bytes())

    profiler = make_s3_profiler()
    table_data = SimpleNamespace(
        display_name="demo",
        full_path="s3://test-bucket/data/demo.parquet",
        table_path="s3://test-bucket/data/demo.parquet",
        partitions=None,
    )
    work_units = list(profiler.get_table_profile(table_data, "urn:li:dataset:test"))

    profile = get_profile(work_units[0])
    assert profile.rowCount == 200
    assert profiler.report.warnings.total_elements == 0


@mock_aws
def test_profiles_partitioned_s3_table_lists_all_files() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    for part in ("year=2023", "year=2024"):
        s3.put_object(
            Bucket="test-bucket",
            Key=f"data/{part}/part.parquet",
            Body=parquet_bytes(),
        )

    profiler = make_s3_profiler()
    table_data = SimpleNamespace(
        display_name="demo",
        full_path="s3://test-bucket/data/year=2023/part.parquet",
        table_path="s3://test-bucket/data",
        partitions=["year=2023", "year=2024"],  # truthy -> enumerate the prefix
    )
    work_units = list(profiler.get_table_profile(table_data, "urn:li:dataset:test"))

    profile = get_profile(work_units[0])
    # Both partition files (200 rows each) are streamed into one profile.
    assert profile.rowCount == 400
