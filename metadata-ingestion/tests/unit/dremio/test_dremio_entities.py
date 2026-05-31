import pytest

from datahub.ingestion.source.dremio.dremio_entities import DremioCatalog, DremioQuery

VALID_TS = "2024-01-15 10:30:00.000"


class TestDremioQueryClassification:
    """Tests for DremioQuery._get_query_type() and _get_query_subtype()."""

    def _make_query(self, sql: str) -> DremioQuery:
        return DremioQuery(
            job_id="test-job",
            username="user",
            submitted_ts=VALID_TS,
            query=sql,
            queried_datasets="[myspace.mytable]",
        )

    @pytest.mark.parametrize(
        "sql, expected_type",
        [
            ("SELECT col FROM table1", "SELECT"),
            ("WITH cte AS (SELECT 1) SELECT * FROM cte", "SELECT"),
            ("INSERT INTO t SELECT * FROM s", "DML"),
            ("DELETE FROM t WHERE id = 1", "DML"),
            ("UPDATE t SET x = 1 WHERE id = 1", "DML"),
            # NOTE: CREATE* is classified as DML because the type check is first-word only
            # and "CREATE" appears in QUERY_TYPES["DML"] — "CREATE VIEW" in DDL is never reached.
            ("CREATE VIEW v AS SELECT 1", "DML"),
            ("DROP TABLE t", "DDL"),
            ("ALTER TABLE t ADD COLUMN x INT", "DDL"),
        ],
    )
    def test_query_type_classification(self, sql, expected_type):
        q = self._make_query(sql)
        assert q.query_type == expected_type

    def test_subtype_is_first_operator(self):
        q = self._make_query("SELECT * FROM t")
        assert q.query_subtype == "SELECT"

    def test_subtype_for_create_view(self):
        # DML list is iterated before DDL, so "CREATE" (DML) matches before "CREATE VIEW" (DDL).
        q = self._make_query("CREATE VIEW v AS SELECT 1")
        assert q.query_subtype == "CREATE"

    def test_subtype_for_with_cte(self):
        q = self._make_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert q.query_subtype == "WITH"

    def test_queried_datasets_parsed_from_brackets(self):
        q = DremioQuery(
            job_id="j1",
            username="u",
            submitted_ts=VALID_TS,
            query="SELECT 1",
            queried_datasets="[space.schema.table1,space.schema.table2]",
        )
        assert "space.schema.table1" in q.queried_datasets
        assert "space.schema.table2" in q.queried_datasets

    def test_queried_datasets_single_entry(self):
        q = DremioQuery(
            job_id="j1",
            username="u",
            submitted_ts=VALID_TS,
            query="SELECT 1",
            queried_datasets="[myspace.table]",
        )
        assert "myspace.table" in q.queried_datasets


class TestDremioCatalogIsValidQuery:
    """Tests for DremioCatalog.is_valid_query() required-fields check."""

    VALID_QUERY = {
        "job_id": "abc123",
        "user_name": "jdoe",
        "submitted_ts": "2024-01-15 10:00:00.000",
        "query": "SELECT 1",
        "queried_datasets": "[schema.table]",
    }

    def test_valid_query_passes(self):
        catalog = DremioCatalog.__new__(DremioCatalog)
        assert catalog.is_valid_query(self.VALID_QUERY) is True

    @pytest.mark.parametrize(
        "missing_field",
        ["job_id", "user_name", "submitted_ts", "query", "queried_datasets"],
    )
    def test_missing_required_field_fails(self, missing_field):
        catalog = DremioCatalog.__new__(DremioCatalog)
        query = {k: v for k, v in self.VALID_QUERY.items() if k != missing_field}
        assert catalog.is_valid_query(query) is False

    def test_empty_string_field_fails(self):
        catalog = DremioCatalog.__new__(DremioCatalog)
        query = {**self.VALID_QUERY, "query": ""}
        assert catalog.is_valid_query(query) is False
