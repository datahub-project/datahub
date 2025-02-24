from datahub.sql_parsing.split_statements import split_statements


def test_split_statements_complex() -> None:
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
