import io
import json
import pathlib
import shutil
from unittest.mock import patch

import fastavro
import pytest
from avrogen import avrojson
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from datahub.cli.json_file import check_mce_file
from datahub.emitter import mce_builder
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    OwnershipClass,
    _Aspect,
)
from datahub.metadata.schemas import getMetadataChangeEventSchema
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.type_helpers import PytestConfig

FROZEN_TIME = "2021-07-22 18:54:06"


def test_codegen_aspect_name():
    assert issubclass(OwnershipClass, _Aspect)

    assert OwnershipClass.ASPECT_NAME == "ownership"
    assert OwnershipClass.get_aspect_name() == "ownership"


def test_cannot_instantiated_codegen_aspect():
    with pytest.raises(TypeError, match="instantiate"):
        _Aspect()


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "json_filename",
    [
        # Normal test.
        "tests/unit/serde/test_serde_large.json",
        # Ensure correct representation of chart info's input list.
        "tests/unit/serde/test_serde_chart_snapshot.json",
        # Check usage stats as well.
        "tests/unit/serde/test_serde_usage.json",
        # Profiles with the MetadataChangeProposal format.
        "tests/unit/serde/test_serde_profile.json",
    ],
)
def test_serde_to_json(
    pytestconfig: PytestConfig, tmp_path: pathlib.Path, json_filename: str
) -> None:
    golden_file = pytestconfig.rootpath / json_filename

    output_filename = "output.json"
    output_file = tmp_path / output_filename
    pipeline = Pipeline.create(
        {
            "source": {"type": "file", "config": {"filename": str(golden_file)}},
            "sink": {"type": "file", "config": {"filename": str(output_file)}},
            "run_id": "serde_test",
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{output_filename}",
        golden_path=golden_file,
    )


@pytest.mark.parametrize(
    "json_filename",
    [
        "tests/unit/serde/test_serde_large.json",
        "tests/unit/serde/test_serde_chart_snapshot.json",
        "tests/unit/serde/test_serde_extra_field.json",
    ],
)
@freeze_time(FROZEN_TIME)
def test_serde_to_avro(
    pytestconfig: PytestConfig,
    json_filename: str,
) -> None:
    # In this test, we want to read in from JSON -> MCE object.
    # Next we serialize from MCE to Avro and then deserialize back to MCE.
    # Finally, we want to compare the two MCE objects.
    with patch(
        "datahub.ingestion.api.common.PipelineContext", autospec=True
    ) as mock_pipeline_context:

        json_path = pytestconfig.rootpath / json_filename
        source = GenericFileSource(
            ctx=mock_pipeline_context, config=FileSourceConfig(path=str(json_path))
        )
        mces = list(source.iterate_mce_file(str(json_path)))

        # Serialize to Avro.
        parsed_schema = fastavro.parse_schema(
            json.loads(getMetadataChangeEventSchema())
        )
        fo = io.BytesIO()
        out_records = [mce.to_obj(tuples=True) for mce in mces]
        fastavro.writer(fo, parsed_schema, out_records)

        # Deserialized from Avro.
        fo.seek(0)
        in_records = list(fastavro.reader(fo, return_record_name=True))
        in_mces = [
            MetadataChangeEventClass.from_obj(record, tuples=True)
            for record in in_records
        ]

        # Check diff
        assert len(mces) == len(in_mces)
        for i in range(len(mces)):
            assert mces[i] == in_mces[i]


@pytest.mark.parametrize(
    "json_filename",
    [
        # Normal test.
        "tests/unit/serde/test_serde_large.json",
        # Check for backwards compatability with specifying all union types.
        "tests/unit/serde/test_serde_backwards_compat.json",
        # Usage stats.
        "tests/unit/serde/test_serde_usage.json",
        # Profiles with the MetadataChangeProposal format.
        "tests/unit/serde/test_serde_profile.json",
        # Ensure sample MCE files are valid.
        "examples/mce_files/single_mce.json",
        "examples/mce_files/mce_list.json",
        "examples/mce_files/bootstrap_mce.json",
    ],
)
@freeze_time(FROZEN_TIME)
def test_check_metadata_schema(pytestconfig: PytestConfig, json_filename: str) -> None:
    json_file_path = pytestconfig.rootpath / json_filename

    run_datahub_cmd(["check", "metadata-file", f"{json_file_path}"])


def test_check_metadata_rewrite(
    pytestconfig: PytestConfig, tmp_path: pathlib.Path
) -> None:
    json_input = (
        pytestconfig.rootpath / "tests/unit/serde/test_canonicalization_input.json"
    )
    json_output_reference = (
        pytestconfig.rootpath / "tests/unit/serde/test_canonicalization_output.json"
    )

    output_file_path = tmp_path / "output.json"
    shutil.copyfile(json_input, output_file_path)
    run_datahub_cmd(
        ["check", "metadata-file", f"{output_file_path}", "--rewrite", "--unpack-mces"]
    )

    mce_helpers.check_golden_file(
        pytestconfig, output_path=output_file_path, golden_path=json_output_reference
    )


@pytest.mark.parametrize(
    "json_filename",
    [
        # Missing fields.
        "tests/unit/serde/test_serde_missing_field.json",
    ],
)
def test_check_mce_schema_failure(
    pytestconfig: PytestConfig, json_filename: str
) -> None:
    json_file_path = pytestconfig.rootpath / json_filename

    try:
        check_mce_file(str(json_file_path))
        raise AssertionError("MCE File validated successfully when it should not have")
    except Exception as e:
        assert "is missing required field: active" in str(e)


def test_field_discriminator() -> None:
    cost_object = models.CostClass(
        costType=models.CostTypeClass.ORG_COST_TYPE,
        cost=models.CostCostClass(
            fieldDiscriminator=models.CostCostDiscriminatorClass.costCode,
            costCode="sampleCostCode",
        ),
    )

    assert cost_object.validate()


def test_type_error() -> None:
    dataflow = models.DataFlowSnapshotClass(
        urn=mce_builder.make_data_flow_urn(
            orchestrator="argo", flow_id="42", cluster="DEV"
        ),
        aspects=[
            models.DataFlowInfoClass(
                name="hello_datahub",
                description="Hello Datahub",
                externalUrl="http://example.com",
                # This is a type error - custom properties should be a Dict[str, str].
                customProperties={"x": 1},  # type: ignore
            )
        ],
    )

    with pytest.raises(avrojson.AvroTypeException):
        dataflow.to_obj()


def test_null_hiding() -> None:
    schemaField = models.SchemaFieldClass(
        fieldPath="foo",
        type=models.SchemaFieldDataTypeClass(type=models.StringTypeClass()),
        nativeDataType="VARCHAR(50)",
    )
    assert schemaField.validate()

    ser = schemaField.to_obj()
    ser_without_ordered_fields = json.loads(json.dumps(ser))

    assert ser_without_ordered_fields == {
        "fieldPath": "foo",
        "isPartOfKey": False,
        "nativeDataType": "VARCHAR(50)",
        "nullable": False,
        "recursive": False,
        "type": {"type": {"com.linkedin.pegasus2avro.schema.StringType": {}}},
    }


def test_missing_optional_simple() -> None:
    original = models.DataHubResourceFilterClass.from_obj(
        {
            "allResources": False,
            "filter": {
                "criteria": [
                    {
                        "condition": "EQUALS",
                        "field": "RESOURCE_TYPE",
                        "values": ["notebook", "dataset", "dashboard"],
                    }
                ]
            },
        }
    )

    # This one is missing the optional filters.allResources field.
    revised_obj = {
        "filter": {
            "criteria": [
                {
                    "condition": "EQUALS",
                    "field": "RESOURCE_TYPE",
                    "values": ["notebook", "dataset", "dashboard"],
                }
            ]
        },
    }
    revised = models.DataHubResourceFilterClass.from_obj(revised_obj)
    assert revised.validate()

    assert original == revised


def test_missing_optional_in_union() -> None:
    # This one doesn't contain any optional fields and should work fine.
    revised_json = json.loads(
        '{"lastUpdatedTimestamp":1662356745807,"actors":{"groups":[],"resourceOwners":false,"allUsers":true,"allGroups":false,"users":[]},"privileges":["EDIT_ENTITY_ASSERTIONS","EDIT_DATASET_COL_GLOSSARY_TERMS","EDIT_DATASET_COL_TAGS","EDIT_DATASET_COL_DESCRIPTION"],"displayName":"customtest","resources":{"filter":{"criteria":[{"field":"RESOURCE_TYPE","condition":"EQUALS","values":["notebook","dataset","dashboard"]}]},"allResources":false},"description":"","state":"ACTIVE","type":"METADATA"}'
    )
    revised = models.DataHubPolicyInfoClass.from_obj(revised_json)

    # This one is missing the optional filters.allResources field.
    original_json = json.loads(
        '{"privileges":["EDIT_ENTITY_ASSERTIONS","EDIT_DATASET_COL_GLOSSARY_TERMS","EDIT_DATASET_COL_TAGS","EDIT_DATASET_COL_DESCRIPTION"],"actors":{"resourceOwners":false,"groups":[],"allGroups":false,"allUsers":true,"users":[]},"lastUpdatedTimestamp":1662356745807,"displayName":"customtest","description":"","resources":{"filter":{"criteria":[{"field":"RESOURCE_TYPE","condition":"EQUALS","values":["notebook","dataset","dashboard"]}]}},"state":"ACTIVE","type":"METADATA"}'
    )
    original = models.DataHubPolicyInfoClass.from_obj(original_json)

    assert revised == original
