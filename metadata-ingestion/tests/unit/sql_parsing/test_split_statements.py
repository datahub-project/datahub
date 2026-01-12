from typing import List

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

    statements = [statement.strip() for statement in split_statements(test_sql)]
    assert statements == [
        "CREATE TABLE Users (Id INT)",
        "-- Comment here",
        "INSERT INTO Users VALUES (1)",
        "BEGIN",
        "UPDATE Users SET Id = 2",
        "/* Multi-line\n               comment */",
        "DELETE FROM /* inline DELETE comment */ Users",
        "END",
        "GO",
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
    assert statements == [
        "CREATE TABLE prev (a INT)",
        "IF",
        "OBJECT_ID('#foo') IS NOT NULL",
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
    expected = [
        "BEGIN TRY",
        "-- Generate divide-by-zero error.",
        "SELECT 1 / 0",
        "END TRY",
        "BEGIN CATCH",
        "-- Execute error retrieval routine.",
        "SELECT ERROR_MESSAGE() AS ErrorMessage",
        "END CATCH",
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
    statements = [statement.strip() for statement in split_statements(test_sql)]
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
    Test Fix #4: INSERT with closing paren in WHERE clause should split correctly.

    Bug: _has_preceding_cte() was too simplistic - it assumed ANY closing paren
    meant a CTE continuation. This caused INSERT statements ending with ) in WHERE
    clauses to incorrectly continue parsing into the next statement.

    Fix: Check if current statement starts with INSERT/UPDATE/DELETE - if so, the
    closing paren is from the DML statement itself, not a CTE.
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
        "This verifies Fix #4: INSERT with closing paren in WHERE doesn't continue to next statement."
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
    Test that legitimate CTEs still work correctly after the fix.

    The fix should only affect INSERT/UPDATE/DELETE statements, not CTEs.
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
