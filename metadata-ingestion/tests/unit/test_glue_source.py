import json

from botocore.stub import Stubber
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.glue import GlueSource, GlueSourceConfig, get_column_type
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    MapTypeClass,
    SchemaFieldDataType,
    StringTypeClass,
)
from tests.test_helpers import mce_helpers
from tests.unit.test_glue_source_stubs import (
    get_databases_response,
    get_dataflow_graph_response_1,
    get_dataflow_graph_response_2,
    get_jobs_response,
    get_object_body_1,
    get_object_body_2,
    get_object_response_1,
    get_object_response_2,
    get_tables_response_1,
    get_tables_response_2,
)

FROZEN_TIME = "2020-04-14 07:00:00"


def glue_source() -> GlueSource:
    return GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2", extract_transforms=True),
    )


def test_get_column_type_contains_key():

    field_type = "char"
    data_type = get_column_type(glue_source(), field_type, "a_table", "a_field")
    assert data_type.to_obj() == SchemaFieldDataType(type=StringTypeClass()).to_obj()


def test_get_column_type_contains_array():

    field_type = "array_lol"
    data_type = get_column_type(glue_source(), field_type, "a_table", "a_field")
    assert data_type.to_obj() == SchemaFieldDataType(type=ArrayTypeClass()).to_obj()


def test_get_column_type_contains_map():

    field_type = "map_hehe"
    data_type = get_column_type(glue_source(), field_type, "a_table", "a_field")
    assert data_type.to_obj() == SchemaFieldDataType(type=MapTypeClass()).to_obj()


def test_get_column_type_contains_set():

    field_type = "set_yolo"
    data_type = get_column_type(glue_source(), field_type, "a_table", "a_field")
    assert data_type.to_obj() == SchemaFieldDataType(type=ArrayTypeClass()).to_obj()


def test_get_column_type_not_contained():

    glue_source_instance = glue_source()

    field_type = "bad_column_type"
    data_type = get_column_type(glue_source_instance, field_type, "a_table", "a_field")
    assert data_type.to_obj() == SchemaFieldDataType(type=StringTypeClass()).to_obj()
    assert glue_source_instance.report.warnings["bad_column_type"] == [
        "The type 'bad_column_type' is not recognised for field 'a_field' in table 'a_table', "
        "setting as StringTypeClass."
    ]


@freeze_time(FROZEN_TIME)
def test_glue_ingest(tmp_path, pytestconfig):

    glue_source_instance = glue_source()

    with Stubber(glue_source_instance.glue_client) as glue_stubber:

        glue_stubber.add_response("get_databases", get_databases_response, {})
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_1,
            {"DatabaseName": "flights-database"},
        )
        glue_stubber.add_response(
            "get_tables",
            get_tables_response_2,
            {"DatabaseName": "test-database"},
        )
        glue_stubber.add_response("get_jobs", get_jobs_response, {})
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_1,
            {"PythonScript": get_object_body_1},
        )
        glue_stubber.add_response(
            "get_dataflow_graph",
            get_dataflow_graph_response_2,
            {"PythonScript": get_object_body_2},
        )

        with Stubber(glue_source_instance.s3_client) as s3_stubber:

            s3_stubber.add_response(
                "get_object",
                get_object_response_1,
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-1.py",
                },
            )
            s3_stubber.add_response(
                "get_object",
                get_object_response_2,
                {
                    "Bucket": "aws-glue-assets-123412341234-us-west-2",
                    "Key": "scripts/job-2.py",
                },
            )

            mce_objects = [
                wu.mce.to_obj() for wu in glue_source_instance.get_workunits()
            ]

            with open(str(tmp_path / "glue_mces.json"), "w") as f:
                json.dump(mce_objects, f, indent=2)

    # Verify the output.
    test_resources_dir = pytestconfig.rootpath / "tests/unit/glue"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "glue_mces.json",
        golden_path=test_resources_dir / "glue_mces_golden.json",
    )
