import deepdiff

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.hive import HiveConfig, HiveSource
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column


def test_hive_configuration_get_identifier_with_database():
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


def test_hive_configuration_get_avro_schema_from_native_data_type():
    # Test 3  - struct of struct
    datatype_string = "struct<type:string,provider:array<int>,abc:struct<t1:string>>"
    output = get_avro_schema_for_hive_column("service", datatype_string)
    diff = deepdiff.DeepDiff(
        (
            {
                "type": "record",
                "native_data_type": "struct<type:string,provider:array<int>,abc:struct<t1:string>>",
                "name": "__struct_fa089c000053479b8d73496a2d95af64",
                "fields": [
                    {
                        "name": "type",
                        "type": {
                            "type": "string",
                            "native_data_type": "string",
                            "_nullable": True,
                        },
                    },
                    {
                        "name": "provider",
                        "type": {
                            "type": "array",
                            "native_data_type": "array<int>",
                            "items": {
                                "type": "int",
                                "native_data_type": "int",
                                "_nullable": True,
                            },
                        },
                    },
                    {
                        "name": "abc",
                        "type": {
                            "type": "record",
                            "native_data_type": "struct<t1:string>",
                            "name": "__struct_0a5925decc1743a09f9a7f7fc7a7efe6",
                            "fields": [
                                {
                                    "name": "t1",
                                    "type": {
                                        "type": "string",
                                        "native_data_type": "string",
                                        "_nullable": True,
                                    },
                                }
                            ],
                        },
                    },
                ],
            }
        ),
        output["fields"][0]["type"],  # type: ignore
        exclude_regex_paths=[
            r"root\['name'\]",
            r"root\['fields'\]\[2\]\['type'\]\['name'\]",
        ],
    )

    assert diff == {}
