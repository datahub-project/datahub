# flake8: noqa

import json
import os
import time

from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import \
    DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import *
from datahub.metadata.schema_classes import DatasetLineageTypeClass
from freezegun import freeze_time

from ingest_api.helper.mce_convenience import (create_new_schema_mce,
                                               generate_json_output_mce, generate_json_output_mcp,
                                               make_browsepath_mce,
                                               make_dataset_description_mce,
                                               make_dataset_urn,
                                               make_institutionalmemory_mce,
                                               make_lineage_mce,
                                               make_ownership_mce,
                                               make_platform, make_profile_mcp, make_status_mce,
                                               make_user_urn)
from ingest_api.helper.models import determine_type

FROZEN_TIME = "2021-07-01 02:58:30.242"


@freeze_time(FROZEN_TIME)
def test_make_csv_dataset(
    inputs={
        "dataset_name": "my_test_dataset",
        "dataset_type": "csv",
        "dataset_owner": "34552",
        "dataset_description": "hello this is description of dataset",
        "fields": [
            {
                "fieldPath": "field1",
                "field_type": "string",
                "field_description": "col1 description",
            },
            {
                "fieldPath": "field2",
                "field_type": "num",
                "field_description": "col2 description",
            },
        ],
        "dataset_origin": "origin of dataset",
        "dataset_location": "location of dataset",
    },
    specified_time=1625108310242,
):
    """ "
    This test is meant to test if datahub.metadata.schema_classes have changed definitions.
    Once definitions have changed, the API will fail to emit the correct MCE.
    Hence, if this function is unable to generate a valid MCE to compare to a existing MCE json,
    it is time to update the code.
    Also, not all MCE can be compared. I did not include the option to specific specific timestamps
    for all functions, because I think it is unnecessary. Hence, comparing to golden_mce.json is impossible
    for some aspects. But, if the test_function run ok, i believe it is safe to assume the code is well.
    """
    dataset_urn = make_dataset_urn(
        platform=inputs["dataset_type"], name=inputs["dataset_name"]
    )
    owner_urn = make_user_urn(inputs["dataset_owner"])
    output_mce = create_new_schema_mce(
        platformName=make_platform(inputs["dataset_type"]),
        actor=owner_urn,
        fields=inputs["fields"],
        system_time=specified_time,
    )
    ownership_mce = make_ownership_mce(actor=owner_urn, dataset_urn=dataset_urn)
    memory_mce = make_institutionalmemory_mce(
        dataset_urn=dataset_urn,
        input_url=["www.yahoo.com", "www.google.com"],
        input_description=["yahoo", "google"],
        actor=owner_urn,
    )
    lineage_mce = make_lineage_mce(
        upstream_urns=[
            "urn:li:dataset:(urn:li:dataPlatform:csv,my_upstream_dataset,PROD)"
        ],
        downstream_urn=dataset_urn,
        actor=owner_urn,
        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
    )
    description_mce = make_dataset_description_mce(
        dataset_name=dataset_urn,
        description=inputs["dataset_description"],
        customProperties={
            "dataset_origin": inputs["dataset_origin"],
            "dataset_location": inputs["dataset_location"],
        },
    )
    path_mce = make_browsepath_mce(path=["/csv/my_test_dataset"])
    output_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "my_test_dataset_1625108310242.json",
    )
    golden_file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "golden_schema_mce.json"
    )
    dataset_snapshot = DatasetSnapshot(
        urn=dataset_urn,
        aspects=[],
    )
    dataset_snapshot.aspects = [output_mce, description_mce, path_mce]
    metadata_record = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

    generate_json_output_mce(metadata_record, os.path.dirname(os.path.realpath(__file__)))
    with open(output_path, "r") as f:
        generated_dict = json.dumps(json.load(f), sort_keys=True)
    with open(golden_file_path, "r") as f:
        golden_mce = json.dumps(json.load(f), sort_keys=True)
    print(f"generated : {generated_dict}")
    print("-------------")
    print(f"golden {golden_mce}")
    assert generated_dict == golden_mce
    os.remove(output_path)


@freeze_time(FROZEN_TIME)
def test_delete(
    inputs={"dataset_name": "my_test_dataset", "dataset_type": "csv"}
) -> None:
    dataset_urn = make_dataset_urn(
        platform=inputs["dataset_type"], name=inputs["dataset_name"]
    )
    delete_mce = make_status_mce(dataset_urn, desired_status=True)

    golden_file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "golden_delete_mce.json"
    )
    output_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f"{inputs['dataset_name']}_{int(time.time()*1000)}.json",
    )
    generate_json_output_mce(
        delete_mce, file_loc=os.path.dirname(os.path.realpath(__file__))
    )
    with open(output_path, "r") as f:
        generated_dict = json.dumps(json.load(f), sort_keys=True)
    with open(golden_file_path, "r") as f:
        golden_mce = json.dumps(json.load(f), sort_keys=True)
    assert generated_dict == golden_mce
    os.remove(output_path)

@freeze_time(FROZEN_TIME)
def test_create_profile(
    inputs = {"dataset_name": "my_test_dataset3", "timestamp":12345, \
        "sample_values": {"field1":["f1s1", "f1s2"], "field2":["f2s1", "f2s2"]}, \
        "dataset_type": "csv"
        }
) -> None:
    dataset_urn = make_dataset_urn(
        platform=inputs["dataset_type"], name=inputs["dataset_name"]
    )
    profile_mcpw = make_profile_mcp(inputs["timestamp"], inputs["sample_values"], dataset_urn)
    golden_file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "golden_profile.json"
    )
    output_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f"{inputs['dataset_name']}_{int(time.time()*1000)}.json",
    )
    generate_json_output_mcp(
        profile_mcpw, file_loc=os.path.dirname(os.path.realpath(__file__))
    )
    with open(output_path, "r") as f:
        generated_dict = json.dumps(json.load(f), sort_keys=True)
    with open(golden_file_path, "r") as f:
        golden_mce = json.dumps(json.load(f), sort_keys=True)
    assert generated_dict == golden_mce
    os.remove(output_path)


def test_type_string():
    assert determine_type("text/csv") == "csv"
    assert determine_type({"dataset_type": "application/octet-stream"}) == "csv"
    assert determine_type("garbage") != "csv"
