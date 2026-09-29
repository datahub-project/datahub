import re
from typing import List

import pytest

from datahub.sql_parsing.split_statements import split_statements


def test_split_statements_complex() -> None:
    # Purposely leaving the preceding whitespace on every line to test that we ignore it.
    test_sql = """
        CREATE TABLE Users (Id INT);
        -- Comment here
        INSERT INTO Users VALUES (1);
        BEGIN
            UPDATE Users SET Id = 2;
            /* Multi-line
               comment */
            DELETE FROM /* inline DELETE comment */ Users;
        END
        GO
        SELECT * FROM Users
    """

    # T-SQL syntax (BEGIN ... END, GO batch separator), so parse as tsql.
    statements = [
        statement.strip() for statement in split_statements(test_sql, dialect="tsql")
    ]
    # Comment-only chunks are never emitted on their own; a leading comment attaches to
    # the statement that follows it (here the standalone `-- Comment here` and the
    # `/* Multi-line comment */` ride along with the next INSERT/DELETE).
    # GO is a batch separator and is dropped; all real statements remain isolated.
    assert statements == [
        "CREATE TABLE Users (Id INT)",
        "-- Comment here\n        INSERT INTO Users VALUES (1)",
        "BEGIN",
        "UPDATE Users SET Id = 2",
        "/* Multi-line\n               comment */\n            DELETE FROM /* inline DELETE comment */ Users",
        "END",
        "SELECT * FROM Users",
    ]


def test_split_statements_cte() -> None:
    # SQL example from https://stackoverflow.com/a/11562724
    test_sql = """\
WITH T AS
(   SELECT  InvoiceNumber,
            DocTotal,
            SUM(Sale + VAT) OVER(PARTITION BY InvoiceNumber) AS NewDocTotal
    FROM    PEDI_InvoiceDetail
)
-- comment
/* multi-line
comment */
UPDATE  T
SET     DocTotal = NewDocTotal"""
    statements = [statement.strip() for statement in split_statements(test_sql)]
    assert statements == [
        test_sql,
    ]


def test_split_statement_drop() -> None:
    test_sql = """\
DROP TABLE #temp1
select 'foo' into #temp1
drop table #temp1
    """
    statements = [statement.strip() for statement in split_statements(test_sql)]
    assert statements == [
        "DROP TABLE #temp1",
        "select 'foo' into #temp1",
        "drop table #temp1",
    ]


def test_single_statement_with_case() -> None:
    test_sql = """\
SELECT
    a,
    b as B,
    CASE
        WHEN a = 1 THEN 'one'
        WHEN a = 2 THEN 'two'
        ELSE 'other'
    END AS c,
    ROW_NUMBER()
    OVER (
        PARTITION BY a
        ORDER BY b ASC
    ) AS d
INTO #temp1
FROM foo
LEFT JOIN
    bar
    ON
        foo.a = bar.a
        AND foo.b < bar.b
        AND bar.f = 'product'
WHERE foo.a > 2
"""

    statements = [statement.strip() for statement in split_statements(test_sql)]
    assert statements == [
        test_sql.strip(),
    ]


def test_split_strict_keywords() -> None:
    test_sql = """\
CREATE TABLE prev (a INT)
IF OBJECT_ID('#foo') IS NOT NULL
    DROP TABLE #foo
SELECT 1 as a INTO #foo
TRUNCATE TABLE #foo
SELECT 1 as a INTO #foo
    """
    statements = [statement.strip() for statement in split_statements(test_sql)]
    # The tokenizer splitter keeps `IF` with its condition rather than peeling the bare
    # keyword into its own chunk; every real DML statement is still isolated, so lineage
    # is unchanged (equivalent-or-better).
    assert statements == [
        "CREATE TABLE prev (a INT)",
        "IF OBJECT_ID('#foo') IS NOT NULL",
        "DROP TABLE #foo",
        "SELECT 1 as a INTO #foo",
        "TRUNCATE TABLE #foo",
        "SELECT 1 as a INTO #foo",
    ]


def test_split_statement_with_try_catch():
    test_sql = """\
BEGIN TRY
    -- Generate divide-by-zero error.
    SELECT 1 / 0;
END TRY

BEGIN CATCH
    -- Execute error retrieval routine.
    SELECT ERROR_MESSAGE() AS ErrorMessage;
END CATCH;
        """
    statements = [statement.strip() for statement in split_statements(test_sql)]
    # Control-flow keywords are peeled individually (BEGIN/TRY/END/CATCH as separate
    # chunks). Each leading comment attaches to its following SELECT. The two real
    # SELECTs are isolated, so lineage is unchanged (equivalent-or-better).
    expected = [
        "BEGIN",
        "TRY",
        "-- Generate divide-by-zero error.\n    SELECT 1 / 0",
        "END",
        "TRY",
        "BEGIN",
        "CATCH",
        "-- Execute error retrieval routine.\n    SELECT ERROR_MESSAGE() AS ErrorMessage",
        "END",
        "CATCH",
    ]
    assert statements == expected


def test_split_statement_with_empty_query():
    test_sql = ""
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected: List[str] = []
    assert statements == expected


def test_split_statement_with_empty_string_in_query():
    test_sql = """\
SELECT
    a,
    b as B
FROM myTable
WHERE
    a = ''"""
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected = [test_sql]
    assert statements == expected


def test_split_statement_with_quotes_in_sting_in_query():
    test_sql = """\
SELECT
    a,
    b as B
FROM myTable
WHERE
    a = 'hi, my name''s tim.'"""
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected = [test_sql]
    assert statements == expected


def test_split_statement_with_merge_query():
    test_sql = """\
MERGE INTO myTable AS t
USING myTable2 AS s
ON t.a = s.a
WHEN MATCHED THEN
    UPDATE SET t.b = s.b
WHEN NOT MATCHED THEN
    INSERT (a, b) VALUES (s.a, s.b)"""
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected = [test_sql]
    assert statements == expected


def test_split_statement_with_end_keyword_in_string():
    test_sql = """
    SELECT
        [Id],
        'End Date' as category
    INTO myprodtable
    FROM myrawtable
    """
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected = [test_sql.strip()]
    assert statements == expected


def test_split_statement_with_end_keyword_in_string_with_escape():
    test_sql = """
    SELECT
        [Id],
        '''Escaped Part'' End Date' as category
    INTO myprodtable
    FROM myrawtable
    """
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected = [test_sql.strip()]
    assert statements == expected


def test_split_statement_with_end_keyword_in_bracketed_identifier():
    test_sql = """
    SELECT
        [Id],
        [End Date]
    INTO myprodtable
    FROM myrawtable
    """
    statements = [statement.strip() for statement in split_statements(test_sql)]
    expected = [test_sql.strip()]
    assert statements == expected


def test_split_statement_with_end_keyword_in_bracketed_identifier_with_escapes():
    test_sql = """
    SELECT
        [Id],
        [[Escaped Part]] End Date]
    INTO myprodtable
    FROM myrawtable
    """
    # `[...]` with an escaped `]]` is MSSQL delimited-identifier syntax; the tokenizer
    # only treats it as a single identifier under the tsql dialect (under the permissive
    # default, the escaped `]]` is read as bracket nesting). Callers pass the real dialect.
    statements = [
        statement.strip() for statement in split_statements(test_sql, dialect="tsql")
    ]
    expected = [test_sql.strip()]
    assert statements == expected


def test_split_select_ending_with_parenthesis():
    """
    Regression test: SELECT statements ending with a closing parenthesis
    (e.g., from function calls like GETDATE()) should be properly split
    from subsequent CREATE INDEX statements.

    Previously, the splitter mistook trailing ')' as a CTE indicator
    and incorrectly kept both statements together.
    """
    select_stmt = (
        "SELECT * INTO #temp FROM source WHERE date > DATEADD(YEAR, -1, GETDATE())"
    )
    create_stmt = "CREATE INDEX idx ON #temp (col1)"

    test_sql = f"{select_stmt}\n\n{create_stmt}"

    statements = [s.strip() for s in split_statements(test_sql)]

    assert len(statements) == 2
    assert statements[0] == select_stmt
    assert statements[1] == create_stmt


def test_cte_with_select_not_split():
    """
    Verify CTEs (WITH ... AS) followed by SELECT are kept together as one statement.
    This ensures the fix for parenthesis splitting doesn't break CTE handling.
    """
    test_sql = """\
    WITH temp_cte AS (
        SELECT * FROM source WHERE date > GETDATE()
    )
    SELECT * FROM temp_cte"""

    statements = [s.strip() for s in split_statements(test_sql)]

    assert len(statements) == 1
    assert statements[0] == test_sql.strip()


def test_split_mssql_insert_with_closing_paren_in_where():
    """
    INSERT with closing paren in WHERE clause should split correctly from the
    following DELETE statement. A trailing ')' in a DML WHERE clause must not
    be treated as a CTE-continuation marker.
    """
    test_sql = """\
INSERT INTO TimeSeries.dbo.european_priips_kid_information
    (kid_synthetic_risk_indicator)
SELECT
    src.kid_summary_risk_indicator
FROM Staging.dbo.source src
WHERE (src.record_action IS NULL OR (src.record_action <> 'DELETE'))
DELETE dst
FROM CurrentData.dbo.european_priips_kid_information dst
WHERE dst.expired = 1"""

    statements = [statement.strip() for statement in split_statements(test_sql)]

    # Should split into 2 statements, not 1 incorrectly merged statement
    assert len(statements) == 2, (
        f"Expected 2 statements, got {len(statements)}. "
        "INSERT with closing paren in WHERE must not continue to next statement."
    )

    # First statement should be the complete INSERT
    assert statements[0].startswith("INSERT INTO TimeSeries")
    assert statements[0].endswith("'DELETE'))")

    # Second statement should be the DELETE
    assert statements[1].startswith("DELETE dst")
    assert statements[1].endswith("expired = 1")

    # Verify INSERT statement length is correct (not including DELETE)
    # The INSERT should NOT include "DELETE dst FROM..." text
    assert "DELETE dst" not in statements[0], (
        "INSERT statement should not include next DELETE statement"
    )


def test_split_mssql_update_with_closing_paren_in_where():
    """
    Test that UPDATE with closing paren in WHERE clause splits correctly.
    """
    test_sql = """\
UPDATE TimeSeries.dbo.european_priips_kid_information
SET kid_synthetic_risk_indicator = src.kid_summary_risk_indicator
FROM Staging.dbo.source src
WHERE (dst.share_class_id IS NULL OR dst.publication_date IS NULL)
INSERT INTO CurrentData.dbo.output (id) SELECT id FROM input"""

    statements = [statement.strip() for statement in split_statements(test_sql)]

    assert len(statements) == 2
    assert statements[0].startswith("UPDATE TimeSeries")
    assert statements[0].endswith("publication_date IS NULL)")
    assert statements[1].startswith("INSERT INTO CurrentData")


def test_split_mssql_delete_with_closing_paren_in_where():
    """
    Test that DELETE with closing paren in WHERE clause splits correctly.
    """
    test_sql = """\
DELETE dst
FROM CurrentData.dbo.european_priips_kid_information dst
WHERE (dst.expired = 1 OR (dst.status = 'DELETED'))
UPDATE TimeSeries.dbo.target SET value = 1"""

    statements = [statement.strip() for statement in split_statements(test_sql)]

    assert len(statements) == 2
    assert statements[0].startswith("DELETE dst")
    assert statements[0].endswith("'DELETED'))")
    assert statements[1].startswith("UPDATE TimeSeries")


def test_split_cte_still_works_correctly():
    """
    Legitimate CTEs (WITH ... AS (...) SELECT) must stay as one statement;
    the closing paren of the CTE body correctly continues into the main SELECT.
    """
    test_sql = """\
WITH summary AS (
    SELECT id, name
    FROM source
    WHERE (active = 1)
)
SELECT * FROM summary"""

    statements = [statement.strip() for statement in split_statements(test_sql)]

    # CTE should remain as single statement
    assert len(statements) == 1
    assert "WITH summary AS" in statements[0]
    assert "SELECT * FROM summary" in statements[0]


def test_split_oracle_plsql_function():
    """
    Test Oracle PL/SQL function with WHILE loop, IF statements, and SELECT INTO.

    Oracle PL/SQL control flow keywords (WHILE, LOOP, END LOOP, ELSIF, RETURN, etc.)
    should be split out, allowing the SELECT INTO statements inside to be extracted
    for lineage analysis.
    """
    test_sql = """\
BEGIN
v_found:='N';

SELECT min(line_num) INTO v_line FROM order_lines WHERE order_id=1;

WHILE v_found='N' LOOP
  SELECT tax_rate
  INTO v_tax 
  FROM order_lines 
  WHERE order_id=1 and line_num=v_line;

  IF v_tax is null THEN
     v_line:=v_line -1;
  ELSE 
     v_found:='Y';
  END IF;

END LOOP;

result_out:=v_tax;
RETURN result_out;
END;"""

    statements = [
        statement.strip() for statement in split_statements(test_sql, dialect="oracle")
    ]

    # Verify SELECT statements are properly extracted — this is the lineage-critical part:
    # both SELECT INTO statements must be isolated and complete (not merged into control flow).
    select_statements = [s for s in statements if s.upper().startswith("SELECT")]
    assert len(select_statements) == 2, (
        f"Expected 2 SELECT statements, got {len(select_statements)}"
    )

    # First SELECT INTO should be complete
    assert "SELECT min(line_num) INTO v_line FROM order_lines" in select_statements[0]
    assert "WHERE order_id=1" in select_statements[0]

    # Second SELECT INTO should be complete (not merged with WHILE)
    assert "SELECT tax_rate" in select_statements[1]
    assert "INTO v_tax" in select_statements[1]
    assert "FROM order_lines" in select_statements[1]
    assert "WHERE order_id=1 and line_num=v_line" in select_statements[1]

    # Control flow is broken out so the SELECTs stay isolated. The tokenizer splitter keeps
    # a leading keyword with its clause (e.g. "WHILE v_found='N'") rather than peeling the
    # bare keyword, so assert the body was fragmented and no SELECT chunk absorbed the
    # surrounding WHILE/LOOP/RETURN scaffolding (equivalent-or-better for lineage).
    assert len(statements) > len(select_statements)
    for sel in select_statements:
        assert "WHILE" not in sel.upper()
        assert "LOOP" not in sel.upper()
        assert "RETURN" not in sel.upper()


def test_split_insert_select_with_case_expression():
    """
    Regression test: INSERT...SELECT containing a CASE expression should not be
    split at the END keyword closing the CASE block, regardless of case.
    The END keyword comparison must be case-insensitive.
    """
    test_sql = """\
delete from raw.eventhub.events_datamill_all_regions
where date = '2026-03-31';

insert into raw.eventhub.events_datamill_all_regions
select
    date_part as date,
    case
        when person is not null
        then initiator_id
    end as user_id,
    user_or_anonymous_id
from raw.eventhub.events_datamill_eu
where date_part = '2026-03-31'"""

    statements = [statement.strip() for statement in split_statements(test_sql)]
    assert len(statements) == 2
    assert statements[0].startswith("delete from")
    assert statements[1].startswith("insert into")
    assert "end as user_id" in statements[1]
    assert "from raw.eventhub.events_datamill_eu" in statements[1]


def test_split_snowflake_materialize_native_table():
    """
    Regression test for customer bug: Snowflake SQLExecuteQueryOperator task with
    a CASE expression inside an INSERT...SELECT was causing DataHub to hang due to
    incorrect statement splitting at the END keyword closing the CASE block.
    """
    test_sql = """\
create table if not exists my_schema.my_table cluster by (date, event) (
    date date,
    event varchar
);

delete from my_schema.my_table
where date = '2026-03-31'
    and dc_id = 'eu';

insert into my_schema.my_table
select
    date_part as date,
    event as event,
    case
        when person is not null
        then initiator_id
    end as user_id,
    user_or_anonymous_id as user_or_anonymous_id,
    is_valid as is_valid,
    'eu' as dc_id
from my_schema.my_source_table
where date_part = '2026-03-31'
    and greatest(skip_validation, is_valid)
order by date, event"""

    statements = [statement.strip() for statement in split_statements(test_sql)]
    assert len(statements) == 3
    assert statements[0].startswith("create table if not exists")
    assert statements[1].startswith("delete from")
    assert statements[2].startswith("insert into")
    assert "end as user_id" in statements[2]
    assert "from my_schema.my_source_table" in statements[2]


def test_splits_no_semicolon_tsql_batch() -> None:
    # A T-SQL batch without semicolons is split via keyword boundaries.
    batch = (
        "DECLARE @p as DATE\n"
        "SET @p = DATEADD(yy, -1, GETDATE())\n"
        "SELECT a INTO #x FROM t1 WHERE d >= @p\n"
        "SELECT b INTO #y FROM #x JOIN t2 ON 1 = 1"
    )
    stmts = [s.strip() for s in split_statements(batch, dialect="tsql")]
    assert stmts == [
        "DECLARE @p as DATE",
        "SET @p = DATEADD(yy, -1, GETDATE())",
        "SELECT a INTO #x FROM t1 WHERE d >= @p",
        "SELECT b INTO #y FROM #x JOIN t2 ON 1 = 1",
    ]


def test_keyword_boundaries_keep_subquery_and_update_set_intact() -> None:
    # A SET with a sub-query must not split inside the parentheses.
    stmts = [
        s.strip()
        for s in split_statements(
            "SET @p = (SELECT max(x) FROM t)\nSELECT a INTO #x FROM t1"
        )
    ]
    assert stmts == ["SET @p = (SELECT max(x) FROM t)", "SELECT a INTO #x FROM t1"]
    # UPDATE ... SET is a clause, not a SET statement -> must not split.
    assert len(list(split_statements("UPDATE t SET col = 1 WHERE id = 2"))) == 1


def test_keyword_boundaries_do_not_split_insert_or_merge_select() -> None:
    # INSERT INTO ... SELECT and MERGE INTO ... USING (SELECT ...) are valid SQL
    # (incl. T-SQL); the INTO there must NOT promote the SELECT to a new statement.
    insert_select = (
        "INSERT INTO db.dbo.tgt (c) SELECT s.c FROM db.dbo.src s WHERE (s.a IS NULL)\n"
        "DELETE d FROM db.dbo.tgt d WHERE d.x = 1"
    )
    stmts = [s.strip() for s in split_statements(insert_select)]
    assert len(stmts) == 2
    assert stmts[0].startswith("INSERT INTO") and "SELECT s.c" in stmts[0]
    assert stmts[1].startswith("DELETE")

    merge = (
        "MERGE INTO db.dbo.t USING (SELECT x FROM s) src ON t.id = src.x "
        "WHEN MATCHED THEN UPDATE SET v = 1"
    )
    assert len(list(split_statements(merge))) == 1


def test_keyword_boundaries_do_not_split_declare_cursor() -> None:
    # DECLARE ... CURSOR FOR SELECT is a single statement (valid T-SQL and Oracle).
    # Only the DECLARE @var / SET @var variable forms are boundaries, so this must
    # not be split.
    cursor = "DECLARE c CURSOR FOR SELECT * FROM t"
    assert len(list(split_statements(cursor))) == 1


def test_leading_comment_block_attaches_to_next_statement() -> None:
    # A comment header preceding the first statement must not be emitted as its own
    # (comment-only) statement -- downstream SQL parsers raise on comment-only input.
    # The comments attach to the following statement instead.
    batch = (
        "-- Change Log:\n"
        "-- Date Modified By Description\n"
        "-- 01/01/2021 abc Updating something\n"
        "DECLARE @p as DATE\n"
        "SET @p = DATEADD(yy, -1, GETDATE())"
    )
    stmts = list(split_statements(batch))
    assert len(stmts) == 2
    assert stmts[0].startswith("-- Change Log:")
    assert stmts[0].rstrip().endswith("DECLARE @p as DATE")
    assert stmts[1] == "SET @p = DATEADD(yy, -1, GETDATE())"


def test_comment_only_input_yields_no_statements() -> None:
    # Input that is nothing but comments is not a statement -- emitting it would feed
    # comment-only text to the SQL parser, which raises. It must yield nothing.
    comment_only = "-- just a header\n/* and a block comment */\n"
    assert list(split_statements(comment_only)) == []


def test_no_emitted_statement_is_comment_only() -> None:
    # Invariant across a batch with comments interleaved and trailing: every emitted
    # chunk contains real SQL; none is comment-only (which downstream parsers reject).
    batch = "-- header\nSET @a = 1\n/* recompute */\nSET @b = 2\n-- trailing note"
    stmts = list(split_statements(batch))
    assert stmts, "expected at least one statement"
    for stmt in stmts:
        stripped = re.sub(
            r"/\*.*?\*/", "", re.sub(r"--[^\n]*", "", stmt), flags=re.DOTALL
        )
        assert stripped.strip(), f"comment-only chunk emitted: {stmt!r}"


def test_new_semicolon_split_and_offsets() -> None:
    # ; splits; statements exclude the terminator; original text preserved.
    assert [
        s.strip() for s in split_statements("SELECT * FROM a; SELECT * FROM b;")
    ] == [
        "SELECT * FROM a",
        "SELECT * FROM b",
    ]


def test_new_paren_depth_no_split_in_subquery() -> None:
    # A semicolon-less single statement with a subquery stays one statement.
    sql = "SELECT a FROM t WHERE id IN (SELECT id FROM s)"
    assert [s.strip() for s in split_statements(sql)] == [sql]


def test_new_tokenizer_failure_falls_back_to_whole_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import datahub.sql_parsing.split_statements as m

    def boom(*a: object, **k: object) -> None:
        raise RuntimeError("tokenizer exploded")

    monkeypatch.setattr(m, "_tokenize", boom)
    assert list(split_statements("anything at all")) == ["anything at all"]


def test_new_empty_input_yields_nothing() -> None:
    assert list(split_statements("")) == []
    assert list(split_statements("   \n  ")) == []


def test_new_splits_consecutive_dml_no_semicolon() -> None:
    sql = "CREATE TABLE t (a INT)\nINSERT INTO t VALUES (1)\nDELETE FROM t WHERE a = 1"
    out = [s.strip() for s in split_statements(sql)]
    assert out[0].startswith("CREATE TABLE t")
    assert out[1].startswith("INSERT INTO t")
    assert out[2].startswith("DELETE FROM t")


def test_new_splits_bare_consecutive_selects() -> None:
    assert [
        s.strip() for s in split_statements("SELECT a FROM foo\nSELECT b FROM bar")
    ] == [
        "SELECT a FROM foo",
        "SELECT b FROM bar",
    ]


def test_new_insert_select_then_trailing_select_splits_once() -> None:
    out = [
        s.strip()
        for s in split_statements(
            "INSERT INTO tgt SELECT * FROM bar\nSELECT c FROM baz"
        )
    ]
    assert len(out) == 2
    assert out[0].startswith("INSERT INTO tgt") and "SELECT * FROM bar" in out[0]
    assert out[1] == "SELECT c FROM baz"


def test_new_does_not_split_insert_select_cte_subquery() -> None:
    # Each must stay ONE statement (precision).
    assert len(list(split_statements("INSERT INTO tgt (a,b) SELECT a,b FROM src"))) == 1
    assert (
        len(list(split_statements("WITH c AS (SELECT n FROM seed) SELECT * FROM c")))
        == 1
    )
    assert (
        len(list(split_statements("SELECT a, (SELECT MAX(id) FROM i) m FROM o"))) == 1
    )
    assert len(list(split_statements("CREATE TABLE t AS SELECT * FROM src"))) == 1


def test_new_union_all_keeps_following_select() -> None:
    # UNION ALL / UNION DISTINCT must not let the post-set-op SELECT start a new statement.
    assert (
        len(list(split_statements("SELECT a FROM foo UNION ALL SELECT b FROM bar")))
        == 1
    )
    assert (
        len(
            list(split_statements("SELECT a FROM foo UNION DISTINCT SELECT b FROM bar"))
        )
        == 1
    )


def test_new_merge_does_not_split_on_then_dml() -> None:
    sql = (
        "MERGE INTO tgt USING src ON tgt.id=src.id "
        "WHEN MATCHED THEN UPDATE SET v=1 "
        "WHEN NOT MATCHED THEN INSERT (a) VALUES (1)"
    )
    assert len(list(split_statements(sql))) == 1


def test_new_case_end_not_a_statement_boundary() -> None:
    sql = "SELECT CASE WHEN x = 1 THEN (SELECT m FROM y) ELSE 0 END AS c FROM t"
    assert len(list(split_statements(sql))) == 1


def test_new_try_catch_isolates_inner_selects() -> None:
    sql = (
        "BEGIN TRY\nSELECT * FROM ta\nEND TRY\nBEGIN CATCH\nSELECT * FROM tb\nEND CATCH"
    )
    chunks = [s.strip() for s in split_statements(sql)]
    assert any(c == "SELECT * FROM ta" for c in chunks)
    assert any(c == "SELECT * FROM tb" for c in chunks)


def test_new_begin_end_proc_isolates_dml() -> None:
    sql = (
        "BEGIN\nINSERT INTO tgt SELECT * FROM src1\nUPDATE tgt SET x = 1 FROM src2\nEND"
    )
    chunks = [s.strip() for s in split_statements(sql)]
    assert any(c.startswith("INSERT INTO tgt") for c in chunks)
    assert any(c.startswith("UPDATE tgt") for c in chunks)


def test_new_if_exists_is_ddl_not_control_flow() -> None:
    # `IF EXISTS` / `IF NOT EXISTS` is DDL syntax, not a control-flow boundary — the
    # statement must stay whole rather than peeling `IF` into its own chunk.
    assert len(list(split_statements("DROP TABLE IF EXISTS x"))) == 1
    assert len(list(split_statements("CREATE TABLE IF NOT EXISTS x (a INT)"))) == 1


def test_new_set_declare_variable_boundaries() -> None:
    sql = "DECLARE @p AS DATE\nSET @p = DATEADD(yy, -1, GETDATE())\nSELECT a FROM t WHERE d >= @p"
    out = [s.strip() for s in split_statements(sql)]
    assert out == [
        "DECLARE @p AS DATE",
        "SET @p = DATEADD(yy, -1, GETDATE())",
        "SELECT a FROM t WHERE d >= @p",
    ]


def test_new_update_set_clause_not_split() -> None:
    assert len(list(split_statements("UPDATE t SET col = 1 WHERE id = 2"))) == 1


def test_new_declare_cursor_not_split() -> None:
    assert len(list(split_statements("DECLARE c CURSOR FOR SELECT * FROM t"))) == 1


def test_new_for_update_clause_not_split() -> None:
    # FOR UPDATE / FOR SHARE are locking clauses, not a new UPDATE statement.
    assert len(list(split_statements("SELECT * FROM t WHERE x = 1 FOR UPDATE"))) == 1


def test_new_cte_feeding_dml_is_one_statement() -> None:
    # WITH <cte> AS (...) <DML> ... is a single statement (the DML is the CTE's main query).
    assert (
        len(list(split_statements("WITH t AS (SELECT a FROM s) UPDATE t SET x = 1")))
        == 1
    )
    assert (
        len(
            list(
                split_statements(
                    "WITH t AS (SELECT a FROM s) INSERT INTO tgt SELECT a FROM t"
                )
            )
        )
        == 1
    )
    assert (
        len(
            list(
                split_statements(
                    "WITH t AS (SELECT a FROM s) DELETE FROM t WHERE a = 1"
                )
            )
        )
        == 1
    )


def test_new_cte_then_separate_statement_still_splits() -> None:
    # A genuinely separate statement after a CTE-SELECT must still split (regression guard:
    # the CTE guard only protects the FIRST query right after the CTE definition's paren).
    out = [
        s.strip()
        for s in split_statements(
            "WITH t AS (SELECT a FROM s) SELECT * FROM t\nUPDATE other SET y = 2"
        )
    ]
    assert len(out) == 2
    assert out[0].startswith("WITH t AS")
    assert out[1].startswith("UPDATE other")


def test_new_go_batch_separator_splits_and_drops_go() -> None:
    # GO is a batch separator (not SQL); statements in each batch are isolated and GO is dropped.
    out = [
        s.strip()
        for s in split_statements(
            "SELECT a FROM t1\nGO\nSELECT b FROM t2", dialect="tsql"
        )
    ]
    assert out == ["SELECT a FROM t1", "SELECT b FROM t2"]


def test_new_go_not_a_batch_separator_for_non_tsql_dialects() -> None:
    # GO is a client batch separator only in T-SQL/Sybase (sqlcmd/SSMS/isql). For other
    # dialects a bare `GO` line is not special and must NOT be dropped as a separator --
    # the splitter stays generic and dialect-aware, not T-SQL-biased.
    for dialect in ("postgres", "mysql", "oracle", None):
        joined = "\n".join(
            split_statements("SELECT a FROM t1\nGO\nSELECT b FROM t2", dialect=dialect)
        )
        assert "GO" in joined.upper(), (dialect, joined)


def test_new_go_after_end_does_not_swallow_following_statement() -> None:
    # Regression: `; END GO <stmt>` must not let sqlglot swallow the trailing statement.
    out = [
        s.strip()
        for s in split_statements(
            "DELETE FROM Users;\nEND\nGO\nSELECT * FROM Users", dialect="tsql"
        )
    ]
    # the trailing SELECT must be recoverable as its own chunk (lineage on Users preserved)
    assert any(c == "SELECT * FROM Users" for c in out), out


def test_new_merge_then_bare_select_splits() -> None:
    # A complete MERGE followed by a separate top-level SELECT (no semicolon) must split;
    # the MERGE's source is always parenthesized, so it never owns a top-level SELECT.
    out = [
        s.strip()
        for s in split_statements(
            "MERGE INTO tgt USING src ON tgt.id=src.id WHEN MATCHED THEN UPDATE SET v=1\n"
            "SELECT * FROM after",
            dialect="tsql",
        )
    ]
    assert len(out) == 2
    assert out[0].startswith("MERGE INTO tgt")
    assert out[1] == "SELECT * FROM after"


def test_new_go_case_insensitive_own_line_only() -> None:
    # lowercase `go` on its own line is a separator; `GO` as part of an identifier is not.
    assert [
        s.strip()
        for s in split_statements(
            "SELECT 1 FROM a\ngo\nSELECT 2 FROM b", dialect="tsql"
        )
    ] == [
        "SELECT 1 FROM a",
        "SELECT 2 FROM b",
    ]
    # a column/identifier containing GO must not be split
    assert len(list(split_statements("SELECT govalue FROM t", dialect="tsql"))) == 1


def test_new_postgres_upsert_on_conflict_do_update_is_one_statement() -> None:
    # `ON CONFLICT ... DO UPDATE SET ...` is part of the INSERT (upsert), not a new UPDATE.
    assert (
        len(
            list(
                split_statements(
                    "INSERT INTO t (id, v) VALUES (1, 2) "
                    "ON CONFLICT (id) DO UPDATE SET v = excluded.v",
                    dialect="postgres",
                )
            )
        )
        == 1
    )


def test_new_mysql_upsert_on_duplicate_key_update_is_one_statement() -> None:
    # `ON DUPLICATE KEY UPDATE ...` is part of the INSERT (upsert), not a new UPDATE.
    assert (
        len(
            list(
                split_statements(
                    "INSERT INTO t (id, v) VALUES (1, 2) "
                    "ON DUPLICATE KEY UPDATE v = VALUES(v)",
                    dialect="mysql",
                )
            )
        )
        == 1
    )


def test_new_upsert_guard_does_not_swallow_a_real_following_update() -> None:
    # Regression: a genuine UPDATE statement after an INSERT (no DO/KEY before it) still splits.
    out = [
        s.strip()
        for s in split_statements(
            "INSERT INTO t (id) VALUES (1)\nUPDATE t SET v = 2", dialect="tsql"
        )
    ]
    assert len(out) == 2
    assert out[1].startswith("UPDATE")


def test_new_dml_keyword_used_as_identifier_not_split() -> None:
    # A reserved DML word used as a table/column name (after FROM/JOIN/INTO/comma/dot) is an
    # identifier, not a statement boundary.
    assert (
        len(
            list(
                split_statements(
                    "SELECT * FROM updates u JOIN merge m ON u.id = m.id",
                    dialect="tsql",
                )
            )
        )
        == 1
    )
    assert (
        len(list(split_statements("DELETE FROM merge WHERE id = 1", dialect="tsql")))
        == 1
    )
    assert (
        len(
            list(
                split_statements("INSERT INTO merge SELECT * FROM src", dialect="tsql")
            )
        )
        == 1
    )


def test_new_go_inside_string_literal_is_not_a_batch_separator() -> None:
    # A line containing only GO inside a single-quoted string is data, not a batch separator.
    out = [
        s.strip()
        for s in split_statements(
            "SELECT 'first line\nGO\nsecond line' AS c FROM t", dialect="tsql"
        )
    ]
    assert len(out) == 1
    assert out[0] == "SELECT 'first line\nGO\nsecond line' AS c FROM t"


def test_new_grant_revoke_privileges_not_split() -> None:
    # GRANT/REVOKE name privileges (SELECT/INSERT/UPDATE/...) that are not query keywords;
    # the statement must not split at the first privilege.
    assert (
        len(list(split_statements("GRANT SELECT ON t TO role_x", dialect="tsql"))) == 1
    )
    assert (
        len(list(split_statements("REVOKE SELECT ON t FROM role_x", dialect="tsql")))
        == 1
    )
    assert (
        len(
            list(
                split_statements(
                    "GRANT SELECT, INSERT, UPDATE ON t TO role_x", dialect="tsql"
                )
            )
        )
        == 1
    )


def test_new_keyword_as_identifier_single_statement() -> None:
    # Keywords used as column/table/alias/function names must NOT split the statement.
    for sql in [
        "SELECT start, end FROM t",  # `end` column after comma
        "SELECT end FROM t",  # `end` column after SELECT
        "SELECT a FROM t WHERE end > 0",  # `end` in WHERE
        "SELECT d AS end FROM t",  # `end` alias
        "SELECT t.end FROM t",  # `end` after dot
        "SELECT a FROM t GROUP BY end",  # `end` in GROUP BY
        "SELECT a FROM t WHERE x = end",  # `end` after operator
        "SELECT * FROM begin",  # table named `begin`
        "SELECT begin, fin FROM t",  # `begin` column
        "SELECT x AS update FROM t",  # alias named `update`
        "SELECT x AS select FROM t",  # alias named `select`
    ]:
        assert len(list(split_statements(sql, dialect="tsql"))) == 1, sql


def test_new_if_function_not_split() -> None:
    assert (
        len(list(split_statements("SELECT IF(x > 0, a, b) FROM t", dialect="mysql")))
        == 1
    )


def test_new_go_inside_comment_not_a_batch_separator() -> None:
    # A GO line inside a block or line comment is not a batch separator.
    assert (
        len(
            list(
                split_statements(
                    "SELECT 1 FROM t\n/* note:\nGO\nhere */", dialect="tsql"
                )
            )
        )
        == 1
    )
    assert len(list(split_statements("SELECT 1 FROM t -- GO\n", dialect="tsql"))) == 1


def test_new_statement_ending_keyword_then_real_statement_splits() -> None:
    # Regression guard: tokens that can END a statement (SET ... ON, DEFAULT VALUES) must
    # NOT be continuation tokens, or the following real statement is merged away (lineage loss).
    for sql in [
        "SET NOCOUNT ON\nSELECT * FROM t",
        "SET ANSI_NULLS ON\nSELECT * FROM t",
        "INSERT INTO t DEFAULT VALUES\nSELECT * FROM s",
    ]:
        assert len(list(split_statements(sql, dialect="tsql"))) == 2, sql


def test_new_keyword_named_cte_and_target_not_split() -> None:
    # A CTE or DML target named like a keyword is an identifier, not a boundary.
    for sql in [
        "WITH end AS (SELECT 1 a) SELECT * FROM end",
        "WITH update AS (SELECT 1 a) SELECT * FROM update",
        "UPDATE update SET x = 1",
        "DROP TABLE merge",
    ]:
        assert len(list(split_statements(sql, dialect="tsql"))) == 1, sql


def test_new_splitter_failure_falls_back_to_whole_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import datahub.sql_parsing.split_statements as m

    def boom(self):  # _TokenSplitter.split raising
        raise RuntimeError("splitter exploded")

    monkeypatch.setattr(m._TokenSplitter, "split", boom)
    assert list(split_statements("SELECT 1; SELECT 2", dialect="tsql")) == [
        "SELECT 1; SELECT 2"
    ]


def test_new_go_with_crlf_line_endings() -> None:
    out = [
        s.strip()
        for s in split_statements(
            "SELECT a FROM t1\r\nGO\r\nSELECT b FROM t2", dialect="tsql"
        )
    ]
    assert out == ["SELECT a FROM t1", "SELECT b FROM t2"]


def test_new_semicolon_inside_string_literal_stays_one_statement() -> None:
    assert len(list(split_statements("SELECT 'a; b' AS c FROM t", dialect="tsql"))) == 1


def test_new_unbalanced_input_does_not_raise_or_lose_sql() -> None:
    sql = "SELECT a FROM t WHERE (x = 1"  # unclosed paren
    out = list(split_statements(sql, dialect="tsql"))
    assert out == [sql]  # whole input preserved, no raise


def test_new_postgres_dollar_quoted_body_stays_one_statement() -> None:
    sql = "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; SELECT 2; $$ LANGUAGE sql"
    assert len(list(split_statements(sql, dialect="postgres"))) == 1


def test_new_semicolon_only_input_yields_nothing() -> None:
    assert list(split_statements(";;;", dialect="tsql")) == []


def test_new_go_inside_dollar_quoted_body_not_a_batch_separator() -> None:
    # A GO line inside a Postgres dollar-quoted body ($tag$...$tag$ or $$...$$) is data,
    # not a batch separator.
    tagged = (
        "CREATE FUNCTION f() RETURNS int AS $body$\nBEGIN\nGO\nRETURN 1;\nEND\n$body$ "
        "LANGUAGE plpgsql"
    )
    assert len(list(split_statements(tagged, dialect="postgres"))) == 1
    untagged = "CREATE FUNCTION g() RETURNS int AS $$\nGO\nSELECT 1\n$$ LANGUAGE sql"
    assert len(list(split_statements(untagged, dialect="postgres"))) == 1


def test_new_comment_only_input_all_dialect_styles_yields_nothing() -> None:
    # Comment-only-ness is determined by the tokenizer (no tokens => not a statement), so
    # every dialect comment style is handled -- including mysql '#', which a '--'/'/* */'
    # regex would miss.
    assert list(split_statements("-- just a comment")) == []
    assert list(split_statements("/* block only */")) == []
    assert list(split_statements("# mysql comment", dialect="mysql")) == []
    assert list(split_statements("-- a\n/* b */", dialect="mysql")) == []


def test_new_identifier_tokenized_as_all_keyword_does_not_merge() -> None:
    # A temp-table like #1All tokenizes HASH+NUMBER+ALL; the ALL keyword must not be treated
    # as a set-op continuation that swallows the following statement (under-split / lineage loss).
    out = [
        c.strip()
        for c in split_statements("DROP TABLE #1All\nDROP TABLE #2", dialect="tsql")
    ]
    assert len(out) == 2, out
    # set-op continuation still works (handled by set-op run tracking, not the continuation set)
    assert (
        len(
            list(
                split_statements(
                    "SELECT a FROM x UNION ALL SELECT b FROM y", dialect="tsql"
                )
            )
        )
        == 1
    )
    assert (
        len(
            list(
                split_statements(
                    "SELECT a FROM x UNION DISTINCT SELECT b FROM y", dialect="tsql"
                )
            )
        )
        == 1
    )
