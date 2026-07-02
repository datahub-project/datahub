import io
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from boto3.session import Session
from moto import mock_aws
from mypy_boto3_s3 import S3Client

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.source import S3Source, SeekableS3File, TableData

BUCKET = "zip-unit-test-bucket"
KEY = "data/content.bin"


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


def _upload(s3_client: S3Client, content: bytes) -> None:
    s3_client.put_object(Bucket=BUCKET, Key=KEY, Body=content)


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


class TestSeekableS3File:
    def test_initial_state(self, s3_client):
        content = b"abcde"
        _upload(s3_client, content)
        f = SeekableS3File(s3_client, BUCKET, KEY)
        assert f._size == len(content)
        assert f.tell() == 0
        assert f.seekable()
        assert f.readable()

    def test_sequential_reads(self, s3_client):
        _upload(s3_client, b"Hello, World!")
        f = SeekableS3File(s3_client, BUCKET, KEY)
        assert f.read(5) == b"Hello"
        assert f.tell() == 5
        assert f.read(2) == b", "
        assert f.tell() == 7

    def test_read_all(self, s3_client):
        content = b"complete content"
        _upload(s3_client, content)
        f = SeekableS3File(s3_client, BUCKET, KEY)
        assert f.read() == content
        assert f.tell() == len(content)

    def test_read_past_eof_returns_empty(self, s3_client):
        _upload(s3_client, b"short")
        f = SeekableS3File(s3_client, BUCKET, KEY)
        f.read()
        assert f.read(10) == b""
        assert f.read() == b""

    def test_seek_set(self, s3_client):
        _upload(s3_client, b"0123456789")
        f = SeekableS3File(s3_client, BUCKET, KEY)
        f.seek(4)
        assert f.tell() == 4
        assert f.read(3) == b"456"

    def test_seek_from_current(self, s3_client):
        _upload(s3_client, b"0123456789")
        f = SeekableS3File(s3_client, BUCKET, KEY)
        f.read(3)
        f.seek(2, 1)
        assert f.tell() == 5
        assert f.read(2) == b"56"

    def test_seek_from_end(self, s3_client):
        _upload(s3_client, b"0123456789")
        f = SeekableS3File(s3_client, BUCKET, KEY)
        f.seek(-3, 2)
        assert f.tell() == 7
        assert f.read() == b"789"

    def test_seek_clamps_to_zero(self, s3_client):
        _upload(s3_client, b"abcde")
        f = SeekableS3File(s3_client, BUCKET, KEY)
        f.seek(-100)
        assert f.tell() == 0

    def test_zipfile_round_trip(self, s3_client):
        csv_content = b"name,age\nAlice,30\n"
        _upload(s3_client, _make_zip({"data.csv": csv_content}))
        f = SeekableS3File(s3_client, BUCKET, KEY)
        with zipfile.ZipFile(f) as zf:
            assert zf.namelist() == ["data.csv"]
            assert zf.read("data.csv") == csv_content


class TestOpenZipEntry:
    def test_single_csv_entry(self, tmp_path):
        csv_bytes = b"name,age\nAlice,30\n"
        zip_path = tmp_path / "data.csv.zip"
        zip_path.write_bytes(_make_zip({"data.csv": csv_bytes}))

        source = _make_local_source()
        file, ext = source._open_zip_entry(
            str(zip_path), None, PathSpec(include="s3://bucket/*.zip")
        )

        assert file is not None
        assert ext == ".csv"
        assert file.read() == csv_bytes

    def test_single_json_entry(self, tmp_path):
        zip_path = tmp_path / "data.json.zip"
        zip_path.write_bytes(_make_zip({"data.json": b'[{"name": "Alice"}]'}))

        source = _make_local_source()
        file, ext = source._open_zip_entry(
            str(zip_path), None, PathSpec(include="s3://bucket/*.zip")
        )

        assert file is not None
        assert ext == ".json"

    def test_multi_file_zip_warns_and_uses_first_supported(self, tmp_path, caplog):
        zip_path = tmp_path / "multi.zip"
        zip_path.write_bytes(
            _make_zip(
                {
                    "README.txt": b"readme",
                    "a.csv": b"x,y\n1,2\n",
                    "b.csv": b"x,y\n3,4\n",
                }
            )
        )

        source = _make_local_source()
        with caplog.at_level("WARNING"):
            file, ext = source._open_zip_entry(
                str(zip_path), None, PathSpec(include="s3://bucket/*.zip")
            )

        assert file is not None
        assert ext == ".csv"
        assert any("3 files" in r.message for r in caplog.records)

    def test_no_supported_extension_returns_none(self, tmp_path):
        zip_path = tmp_path / "data.zip"
        zip_path.write_bytes(_make_zip({"README.txt": b"nothing here"}))

        source = _make_local_source()
        file, ext = source._open_zip_entry(
            str(zip_path), None, PathSpec(include="s3://bucket/*.zip")
        )

        assert file is None
        assert source.report.warnings

    def test_bad_zip_returns_none(self, tmp_path):
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"this is not a zip file")

        source = _make_local_source()
        file, ext = source._open_zip_entry(
            str(bad_zip), None, PathSpec(include="s3://bucket/*.zip")
        )

        assert file is None
        assert source.report.warnings


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
        with patch.object(source.report, "report_warning") as mock_warn:
            fields = source.get_fields(
                self._table_data(str(zip_path)),
                PathSpec(include=str(tmp_path / "*.zip"), enable_compression=False),
            )

        assert fields == []
        mock_warn.assert_called_once()
