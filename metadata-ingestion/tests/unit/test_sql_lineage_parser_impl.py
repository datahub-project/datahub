from datahub.utilities.sql_lineage_parser_impl import SqlLineageSQLParserImpl


def test_hash_in_sql_query_with_no_space():
    parser = SqlLineageSQLParserImpl(
        sql_query="""
/*
HERE IS A STANDARD COMMENT BLOCK
THIS WILL NOT BREAK sqllineage
*/
CREATE OR REPLACE TABLE `foo.bar.trg_tbl`AS
#This, comment will not break sqllineage
SELECT foo
-- this comment will not break sqllineage either
# this comment will not break sqllineage either
FROM `foo.bar.src_tbl`
        """
    )

    assert parser.get_tables() == ["foo.bar.src_tbl"]
