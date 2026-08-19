"""Boundary-recall and precision gates for the tokenizer splitter (spec targets: 11/11
recall, 10/10 no-false-split). Recall is measured as lineage-table recovery; precision
as 'single statements stay one chunk'. Generic synthetic SQL only."""

from datahub.sql_parsing.split_statements import split_statements
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result


def _tables(query: str) -> set:
    r = create_lineage_sql_parsed_result(
        query=query,
        default_db="db",
        default_schema="dbo",
        platform="mssql",
        platform_instance=None,
        env="PROD",
        graph=None,
    )
    return {u.split(",")[1].split(".")[-1] for u in r.in_tables}


def _recall(sql: str) -> set:
    got: set = set()
    for stmt in split_statements(sql, dialect="tsql"):
        if stmt.strip():
            got |= _tables(stmt)
    return {t for t in got if not t.startswith("#")}


RECALL_CASES = [
    ("SELECT a FROM foo\nSELECT b FROM bar", {"foo", "bar"}),
    ("INSERT INTO tgt SELECT * FROM bar\nSELECT c FROM baz", {"bar", "baz"}),
    (
        "SELECT a FROM foo UNION ALL SELECT b FROM bar\nSELECT z FROM qux",
        {"foo", "bar", "qux"},
    ),
    (
        "WITH c AS (SELECT n FROM seed) SELECT * FROM c\nSELECT 2 FROM other",
        {"seed", "other"},
    ),
    ("CREATE TABLE t AS SELECT * FROM src\nSELECT 1 FROM more", {"src", "more"}),
    (
        "DECLARE @a DATE\nSET @a=GETDATE()\nSELECT * FROM t1\nSELECT * FROM t2",
        {"t1", "t2"},
    ),
    (
        "BEGIN\nINSERT INTO tgt SELECT * FROM src1\nUPDATE tgt SET x=1 FROM src2\nEND",
        {"src1", "src2"},
    ),
    (
        "BEGIN TRY\nSELECT * FROM ta\nEND TRY\nBEGIN CATCH\nSELECT * FROM tb\nEND CATCH",
        {"ta", "tb"},
    ),
    ("SELECT * FROM a; SELECT * FROM b;", {"a", "b"}),
    (
        "MERGE INTO tgt USING src ON tgt.id=src.id WHEN MATCHED THEN UPDATE SET v=1",
        {"src"},
    ),
    (
        "SELECT x INTO #tmp FROM raw1\nSELECT y FROM #tmp JOIN raw2 ON 1=1",
        {"raw1", "raw2"},
    ),
    (
        "MERGE INTO tgt USING src ON tgt.id=src.id WHEN MATCHED THEN UPDATE SET v=1\nSELECT * FROM after",
        {"src", "after"},
    ),
]

PRECISION_CASES = [
    "SELECT a FROM (SELECT b FROM inner_t) x",
    "SELECT a, (SELECT MAX(id) FROM inner_t) m FROM outer_t",
    "SELECT a FROM t WHERE id IN (SELECT id FROM s)",
    "INSERT INTO tgt (a,b) SELECT a,b FROM src",
    "CREATE TABLE t AS SELECT * FROM src",
    "WITH c AS (SELECT n FROM seed) SELECT * FROM c",
    "WITH a AS (SELECT 1 FROM s1), b AS (SELECT 2 FROM s2) SELECT * FROM a JOIN b ON 1=1",
    "SELECT a FROM foo UNION ALL SELECT b FROM bar",
    "SELECT CASE WHEN x=1 THEN (SELECT m FROM y) ELSE 0 END AS c FROM t",
    "MERGE INTO tgt USING src ON tgt.id=src.id WHEN MATCHED THEN UPDATE SET v=1",
]


def test_recall_all_cases_recovered() -> None:
    misses = [
        (sql, truth, _recall(sql))
        for sql, truth in RECALL_CASES
        if not truth.issubset(_recall(sql))
    ]
    assert not misses, f"recall misses: {misses}"


def test_precision_no_false_splits() -> None:
    for sql in PRECISION_CASES:
        chunks = [s for s in split_statements(sql, dialect="tsql") if s.strip()]
        assert len(chunks) == 1, f"false split: {sql} -> {chunks}"
        # Guard against a vacuous pass: _tables() returns set() on parse failure,
        # so set() == set() would "pass" without verifying the SQL was preserved.
        # Every PRECISION_CASES statement references at least one source table.
        assert len(_tables(sql)) > 0, f"precision case has no parseable tables: {sql}"
        assert _tables(chunks[0]) == _tables(sql), f"mangled: {sql}"
