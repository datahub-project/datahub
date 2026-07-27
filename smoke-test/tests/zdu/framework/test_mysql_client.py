"""Unit tests for MySQLClient — uses mocked cursors; never touches a real DB."""

from __future__ import annotations

import json
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

from tests.zdu.framework.mysql_client import EbeanAspectV2Row, MySQLClient


@pytest.fixture
def cursor() -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


@pytest.fixture
def client(cursor: MagicMock) -> Iterator[MySQLClient]:
    """Yield a MySQLClient whose connections always return ``cursor``."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with patch("tests.zdu.framework.mysql_client.pymysql.connect", return_value=conn):
        yield MySQLClient(host="h", port=1, user="u", password="p", database="d")


class TestGetSchemaVersion:
    def test_returns_int_when_present(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {
            "systemmetadata": json.dumps({"schemaVersion": 3})
        }
        assert client.get_schema_version("urn:li:dataset:foo", "embed") == 3

    def test_returns_none_when_field_absent(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {"systemmetadata": "{}"}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_when_metadata_is_null(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {"systemmetadata": None}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_when_no_row(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = None
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_on_malformed_json(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {"systemmetadata": "{not json"}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None

    def test_returns_none_when_systemmetadata_is_json_null(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {"systemmetadata": "null"}
        assert client.get_schema_version("urn:li:dataset:foo", "embed") is None


class TestGetAspectRaw:
    def test_returns_dataclass(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {
            "urn": "urn:li:dataset:foo",
            "aspect": "embed",
            "version": 0,
            "metadata": "{}",
            "systemmetadata": "{}",
            "createdon": "2026-04-01 00:00:00",
            "createdby": "urn:li:corpuser:datahub",
        }
        row = client.get_aspect_raw("urn:li:dataset:foo", "embed")
        assert isinstance(row, EbeanAspectV2Row)
        assert row.urn == "urn:li:dataset:foo"

    def test_returns_none_when_missing(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = None
        assert client.get_aspect_raw("urn:li:dataset:foo", "embed") is None


class TestCountAspectsBySchemaVersion:
    def test_groups_by_schema_version_with_pymysql_string_keys(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        # Mock pymysql's actual behavior: JSON_EXTRACT yields strings.
        # Numeric JSON -> "2", JSON null -> "null", missing key -> SQL NULL (None).
        cursor.fetchall.return_value = [
            {"v": None, "n": 5},  # key absent → SQL NULL
            {"v": "null", "n": 2},  # JSON null
            {"v": "2", "n": 7},
            {"v": "4", "n": 3},
        ]
        # Both None and "null" merge to Python None → 5 + 2 = 7
        assert client.count_aspects_by_schema_version("embed") == {
            None: 7,
            2: 7,
            4: 3,
        }

    def test_returns_empty_when_no_rows(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchall.return_value = []
        assert client.count_aspects_by_schema_version("embed") == {}

    def test_unparseable_string_falls_through_to_none(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchall.return_value = [{"v": "not-an-int", "n": 1}]
        assert client.count_aspects_by_schema_version("embed") == {None: 1}


class TestGetUpgradeResult:
    def test_returns_parsed_json(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchone.return_value = {"metadata": json.dumps({"indicesState": {}})}
        assert client.get_upgrade_result("system-update-blocking") == {
            "indicesState": {}
        }

    def test_returns_none_when_missing(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = None
        assert client.get_upgrade_result("system-update-blocking") is None

    def test_returns_none_on_malformed_json(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {"metadata": "{not json"}
        assert client.get_upgrade_result("system-update-blocking") is None

    def test_returns_none_when_metadata_is_non_dict_json(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchone.return_value = {"metadata": "[]"}
        assert client.get_upgrade_result("system-update-blocking") is None


class TestFromEnvCredentials:
    """Verify MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE env vars are honored."""

    def test_mysql_credentials_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from tests.zdu.framework.config import ZDUTestConfig

        monkeypatch.setenv("MYSQL_USER", "alice")
        monkeypatch.setenv("MYSQL_PASSWORD", "secret")
        monkeypatch.setenv("MYSQL_DATABASE", "mydb")
        c = ZDUTestConfig.from_env()
        assert c.mysql_user == "alice"
        assert c.mysql_password == "secret"
        assert c.mysql_database == "mydb"


class TestFindUpgradeResultWithField:
    def test_returns_first_match(self, client: MySQLClient, cursor: MagicMock) -> None:
        cursor.fetchall.return_value = [
            {
                "urn": "urn:li:dataHubUpgrade:other",
                "metadata": json.dumps({"state": "SUCCEEDED"}),
            },
            {
                "urn": "urn:li:dataHubUpgrade:system-update-blocking",
                "metadata": json.dumps(
                    {"indicesState": {"datasetindex_v2": {"status": "COMPLETED"}}}
                ),
            },
        ]
        upgrade_id, payload = client.find_upgrade_result_with_field("indicesState")
        assert upgrade_id == "system-update-blocking"
        assert payload is not None
        assert "indicesState" in payload

    def test_returns_none_when_no_match(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchall.return_value = [
            {
                "urn": "urn:li:dataHubUpgrade:other",
                "metadata": json.dumps({"state": "SUCCEEDED"}),
            },
        ]
        assert client.find_upgrade_result_with_field("indicesState") == (None, None)

    def test_skips_malformed_metadata(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchall.return_value = [
            {"urn": "urn:li:dataHubUpgrade:bad", "metadata": "{not json"},
            {
                "urn": "urn:li:dataHubUpgrade:good",
                "metadata": json.dumps({"indicesState": {}}),
            },
        ]
        upgrade_id, _ = client.find_upgrade_result_with_field("indicesState")
        assert upgrade_id == "good"

    def test_skips_non_dict_payload(
        self, client: MySQLClient, cursor: MagicMock
    ) -> None:
        cursor.fetchall.return_value = [
            {"urn": "urn:li:dataHubUpgrade:bad", "metadata": "[]"},
            {
                "urn": "urn:li:dataHubUpgrade:good",
                "metadata": json.dumps({"indicesState": {}}),
            },
        ]
        upgrade_id, _ = client.find_upgrade_result_with_field("indicesState")
        assert upgrade_id == "good"


class TestUpsertAspectRaw:
    def test_inserts_row_with_default_systemmetadata(self) -> None:
        _client = MySQLClient(
            host="127.0.0.1", port=3306, user="u", password="p", database="d"
        )
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            _client.upsert_aspect_raw(
                urn="urn:li:dashboard:(test,zdu-io-pool-0)",
                aspect="embed",
                metadata='{"renderUrl":"http://x"}',
            )
        # Verify the SQL statement structure and the bound parameters.
        cursor.execute.assert_called_once()
        sql, params = cursor.execute.call_args.args
        assert "INSERT INTO metadata_aspect_v2" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert params[0] == "urn:li:dashboard:(test,zdu-io-pool-0)"
        assert params[1] == "embed"
        assert params[2] == 0  # version
        assert params[3] == '{"renderUrl":"http://x"}'
        assert params[4] == "{}"  # default systemmetadata
        # createdby fallback
        assert params[6] == "urn:li:corpuser:datahub"

    def test_explicit_systemmetadata_passed_through(self) -> None:
        _client = MySQLClient(
            host="127.0.0.1", port=3306, user="u", password="p", database="d"
        )
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            _client.upsert_aspect_raw(
                urn="urn:li:x",
                aspect="embed",
                metadata="{}",
                systemmetadata='{"schemaVersion":4}',
                createdby="urn:li:corpuser:zdu-test",
            )
        params = cursor.execute.call_args.args[1]
        assert params[4] == '{"schemaVersion":4}'
        assert params[6] == "urn:li:corpuser:zdu-test"

    def test_commits_after_execute(self) -> None:
        _client = MySQLClient(
            host="127.0.0.1", port=3306, user="u", password="p", database="d"
        )
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            _client.upsert_aspect_raw(
                urn="urn:li:x",
                aspect="embed",
                metadata="{}",
            )
        # Connection commit must fire so the row is durable.
        conn.commit.assert_called_once()


class TestDumpZduAspectsCsv:
    def test_returns_csv_with_header_and_rows(self, client: MySQLClient) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {
                "urn": "urn:li:dashboard:(test,zdu-tc-1)",
                "aspect": "embed",
                "version": 0,
                "metadata": '{"x":1}',
                "systemmetadata": '{"schemaVersion":4}',
                "createdon": "2026-01-01 00:00:00",
                "createdby": "urn:li:corpuser:datahub",
            },
        ]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            csv_text = client.dump_zdu_aspects_csv()
        lines = csv_text.splitlines()
        assert (
            lines[0] == "urn,aspect,version,metadata,systemmetadata,createdon,createdby"
        )
        assert "zdu-tc-1" in lines[1]
        assert "embed" in lines[1]
        # Verify the SQL filters by urn LIKE 'zdu-...' patterns.
        sql = cursor.execute.call_args.args[0]
        params = cursor.execute.call_args.args[1]
        assert "urn LIKE" in sql
        assert any("zdu-" in p for p in params)

    def test_returns_only_header_when_no_rows(self, client: MySQLClient) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        with patch.object(MySQLClient, "_conn") as conn_ctx:
            conn_ctx.return_value.__enter__.return_value = conn
            csv_text = client.dump_zdu_aspects_csv()
        assert (
            csv_text.strip()
            == "urn,aspect,version,metadata,systemmetadata,createdon,createdby"
        )
