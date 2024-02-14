from sqlglot.dialects.tsql import TSQL


class TSQLv2(TSQL):
    class Tokenizer(TSQL.Tokenizer):
        """
        SQL server allows to define alieses in 4 different ways, for example

        SELECT
            c1 as c1_alias,
            c2 as [c2_alias],
            c3 as "c3_alias",
            c4 as 'c4_alias'
        FROM
            tbl

        The fix adds support of 4th case

        SELECT
            c4 as 'c4_alias'
        FROM
            tbl
        """

        IDENTIFIERS = ["'", *TSQL.Tokenizer.IDENTIFIERS]
