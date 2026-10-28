import io
import zipfile
from datetime import datetime
from pathlib import Path

import pytest
from boto3.session import Session
from moto import mock_aws
from mypy_boto3_s3 import S3Client

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.source import S3Source, TableData

BUCKET = "zip-unit-test-bucket"


@pytest.fixture()
def s3_session():
    with mock_aws():
        yield Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )


@pytest.fixture()
def s3_client(s3_session):
    client = s3_session.client("s3")
    client.create_bucket(Bucket=BUCKET)
    return client


def _make_s3_source() -> S3Source:
    return S3Source.create(
        config_dict={
            "platform": "s3",
            "path_specs": [{"include": f"s3://{BUCKET}/{{table}}/*.zip"}],
            "aws_config": {
                "aws_region": "us-east-1",
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
            },
        },
        ctx=PipelineContext(run_id="test-zip"),
    )


def _make_local_source() -> S3Source:
    return S3Source.create(
        config_dict={"path_specs": [{"include": "/tmp/{table}.*"}]},
        ctx=PipelineContext(run_id="test-zip"),
    )


def _make_zip(entries: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


class TestGetFieldsZip:
    def _table_data(self, path: str) -> TableData:
        return TableData(
            display_name="test_table",
            is_s3=False,
            full_path=path,
            timestamp=datetime.now(),
            table_path=path,
            size_in_bytes=1024,
            number_of_files=1,
        )

    def _write_zip(self, path: Path, inner_name: str, content: bytes) -> None:
        path.write_bytes(_make_zip({inner_name: content}))

    def test_csv_zip_infers_schema(self, tmp_path):
        zip_path = tmp_path / "employees.csv.zip"
        self._write_zip(zip_path, "employees.csv", b"name,age,city\nAlice,30,NYC\n")

        source = _make_local_source()
        fields = source.get_fields(
            self._table_data(str(zip_path)),
            PathSpec(include=str(tmp_path / "*.csv.zip"), enable_compression=True),
        )

        field_names = {f.fieldPath for f in fields}
        assert {"name", "age", "city"}.issubset(field_names)

    def test_json_zip_infers_schema(self, tmp_path):
        zip_path = tmp_path / "products.json.zip"
        self._write_zip(
            zip_path, "products.json", b'[{"product": "widget", "price": 9.99}]'
        )

        source = _make_local_source()
        fields = source.get_fields(
            self._table_data(str(zip_path)),
            PathSpec(include=str(tmp_path / "*.json.zip"), enable_compression=True),
        )

        field_names = {f.fieldPath for f in fields}
        assert {"product", "price"}.issubset(field_names)

    def test_compression_disabled_skips_zip(self, tmp_path):
        zip_path = tmp_path / "data.csv.zip"
        self._write_zip(zip_path, "data.csv", b"name,age\nAlice,30\n")

        source = _make_local_source()
        fields = source.get_fields(
            self._table_data(str(zip_path)),
            PathSpec(include=str(tmp_path / "*.zip"), enable_compression=False),
        )

        assert fields == []
        assert source.report.warnings

    def test_archive_content_type_does_not_override_member_format(self, tmp_path):
        zip_path = tmp_path / "data.zip"
        self._write_zip(zip_path, "data.csv", b"name,age\nAlice,30\n")
        table_data = self._table_data(str(zip_path))
        table_data.content_type = "application/zip"

        source = _make_local_source()
        fields = source.get_fields(
            table_data, PathSpec(include=str(tmp_path / "*.zip"))
        )

        assert {f.fieldPath for f in fields} == {"name", "age"}

    def test_member_suffix_wins_over_archive_name(self, tmp_path):
        zip_path = tmp_path / "data.csv.zip"
        self._write_zip(zip_path, "data.json", b'{"name": "Alice", "age": 30}\n')

        source = _make_local_source()
        fields = source.get_fields(
            self._table_data(str(zip_path)),
            PathSpec(include=str(tmp_path / "*.zip")),
        )

        assert {f.fieldPath for f in fields} == {"name", "age"}


def test_s3_zip_member_is_read_over_range_requests(s3_client: S3Client) -> None:
    # A key with '#' must reach S3 intact.
    key = "orders/part#1/orders.csv.zip"
    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=_make_zip(
            {"__MACOSX/._orders.csv": b"\x00", "orders.csv": b"id,total\n1,9.5\n"}
        ),
    )
    full_path = f"s3://{BUCKET}/{key}"

    source = _make_s3_source()
    fields = source.get_fields(
        TableData(
            display_name="orders",
            is_s3=True,
            full_path=full_path,
            timestamp=datetime.now(),
            table_path=full_path,
            size_in_bytes=1024,
            number_of_files=1,
        ),
        PathSpec(include=f"s3://{BUCKET}/{{table}}/*/*.zip"),
    )

    assert {f.fieldPath for f in fields} == {"id", "total"}
    assert not source.report.warnings


def test_s3_zip_read_failure_is_raised(s3_client: S3Client) -> None:
    # Like any other file, a storage failure must raise so ingest_table drops
    # the schema rather than emitting one with no fields.
    full_path = f"s3://{BUCKET}/orders/missing.csv.zip"

    source = _make_s3_source()
    with pytest.raises(OSError):
        source.get_fields(
            TableData(
                display_name="orders",
                is_s3=True,
                full_path=full_path,
                timestamp=datetime.now(),
                table_path=full_path,
                size_in_bytes=1024,
                number_of_files=1,
            ),
            PathSpec(include=f"s3://{BUCKET}/{{table}}/*.zip"),
        )
