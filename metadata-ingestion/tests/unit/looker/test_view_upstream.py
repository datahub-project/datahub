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
        # Glue with a region/instance prefix — mirrors real Cisco URNs where the
        # connection string carries an AWS region segment before the hive. prefix.
        (
            "urn:li:dataset:(urn:li:dataPlatform:glue,us-east-1.hive.analytics.my_table,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:glue,us-east-1.analytics.my_table,PROD)",
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
    ids=[
        "hive_platform_strips_prefix",
        "glue_platform_strips_prefix",
        "glue_platform_with_region_strips_prefix",
        "glue_without_hive_prefix_unchanged",
        "other_platform_unchanged",
        "hive_without_hive_prefix_unchanged",
    ],
)
def test_drop_hive_dot(urn: str, expected: str) -> None:
    assert _drop_hive_dot(urn) == expected
