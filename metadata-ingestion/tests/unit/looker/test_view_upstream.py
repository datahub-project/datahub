import pytest

from datahub.ingestion.source.looker.view_upstream import _drop_hive_dot


@pytest.mark.parametrize(
    "urn,expected",
    [
        # Hive: hive. prefix stripped from the dataset id.
        (
            "urn:li:dataset:(urn:li:dataPlatform:hive,hive.my_database.my_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
        ),
        # Glue queried via Athena/Presto uses the same hive.<db>.<table> syntax — strip it too.
        (
            "urn:li:dataset:(urn:li:dataPlatform:glue,hive.analytics.my_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:glue,analytics.my_table,PROD)",
        ),
        # Glue URN without a hive. prefix passes through unchanged.
        (
            "urn:li:dataset:(urn:li:dataPlatform:glue,analytics.my_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:glue,analytics.my_table,PROD)",
        ),
        # Other platforms are not touched even if they contain "hive." literally.
        (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,hive.db.t,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,hive.db.t,PROD)",
        ),
        # Hive URN without a hive. prefix is a no-op.
        (
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
        ),
    ],
)
def test_drop_hive_dot(urn: str, expected: str) -> None:
    assert _drop_hive_dot(urn) == expected
