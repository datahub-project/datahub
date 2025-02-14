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
        "SELECT 'foo' into #temp1",
        "DROP table #temp1",
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
