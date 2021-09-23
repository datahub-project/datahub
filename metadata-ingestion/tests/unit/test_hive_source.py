import pytest


@pytest.mark.integration
def test_hive_configuration_get_identifier_with_database():
    from datahub.ingestion.api.common import PipelineContext
    from datahub.ingestion.source.sql.hive import HiveConfig, HiveSource

    test_db_name = "test_database"
    # test_table_name = "test_table"
    config_dict = {
        "username": "test",
        "password": "test",
        "host_port": "test:80",
        "database": test_db_name,
        "scheme": "hive+https",
    }
    hive_config = HiveConfig.parse_obj(config_dict)
    expected_output = f"{test_db_name}"
    ctx = PipelineContext(run_id="test")
    hive_source = HiveSource(hive_config, ctx)
    output = HiveSource.get_schema_names(hive_source, hive_config)
    assert output == [expected_output]
