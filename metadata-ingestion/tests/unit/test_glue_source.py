import json
from typing import Dict, Tuple, Type

import pytest
from botocore.stub import Stubber
from freezegun import freeze_time

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    MapTypeClass,
    RecordTypeClass,
    StringTypeClass,
)
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column
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


column_type_test_cases: Dict[str, Tuple[str, Type]] = {
    "char": ("char", StringTypeClass),
    "array": ("array<int>", ArrayTypeClass),
    "map": ("map<string, int>", MapTypeClass),
    "struct": ("struct<a:int, b:string>", RecordTypeClass),
}


@pytest.mark.parametrize(
    "hive_column_type, expected_type",
    column_type_test_cases.values(),
    ids=column_type_test_cases.keys(),
)
def test_column_type(hive_column_type: str, expected_type: Type) -> None:
    avro_schema = get_avro_schema_for_hive_column(
        f"test_column_{hive_column_type}", hive_column_type
    )
    schema_fields = avro_schema_to_mce_fields(json.dumps(avro_schema))
    actual_schema_field_type = schema_fields[0].type
    assert type(actual_schema_field_type.type) == expected_type


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
                wu.metadata.to_obj() for wu in glue_source_instance.get_workunits()
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


def test_underlying_platform_takes_precendence():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2", underlying_platform="athena"),
    )
    assert source.get_underlying_platform() == "athena"


def test_underlying_platform_cannot_be_other_than_athena():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(
            aws_region="us-west-2", underlying_platform="data-warehouse"
        ),
    )
    assert source.get_underlying_platform() == "glue"


def test_without_underlying_platform():
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-source-test"),
        config=GlueSourceConfig(aws_region="us-west-2"),
    )
    assert source.get_underlying_platform() == "glue"
