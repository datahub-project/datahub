from datahub.ingestion.source.sql.sql_utils import gen_schema_key


def test_guid_generators():
    expected_guid = "f5e571e4a9acce86333e6b427ba1651f"
    schema_key = gen_schema_key(
        db_name="hive",
        schema="db1",
        platform="hive",
        platform_instance="PROD",
        env="PROD",
    )

    guid = schema_key.guid()
    assert guid == expected_guid
