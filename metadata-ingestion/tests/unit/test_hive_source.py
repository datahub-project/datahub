import pytest


@pytest.mark.integration
def test_hive_configuration_get_identifier_with_database():
    from datahub.ingestion.source.hive import HiveConfig

    test_db_name = "test_database"
    test_schema_name = "test_schema"
    test_table_name = "test_table"
    config_dict = {
        "username": "test",
        "password": "test",
        "host_port": "test:80",
        "database": test_db_name,
        "scheme": "hive+https",
    }
    hive_config = HiveConfig.parse_obj(config_dict)
    expected_output = f"{test_db_name}.{test_schema_name}.{test_table_name}"
    output = hive_config.get_identifier(schema=test_schema_name, table=test_table_name)
    assert output == expected_output
