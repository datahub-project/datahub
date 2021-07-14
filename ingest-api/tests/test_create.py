import sys
import json
import os
from datahub.metadata.schema_classes import DatasetLineageTypeClass

from ingest_api.helper.mce_convenience import make_dataset_description_mce, make_recover_mce, make_schema_mce, make_dataset_urn, \
        generate_json_output, make_ownership_mce, make_platform, make_user_urn,\
        make_delete_mce, make_browsepath_mce, make_institutionalmemory_mce, make_lineage_mce
from ingest_api.helper.models import determine_type

def test_make_csv_dataset(inputs = {"dataset_name":"my_test_dataset",
        "dataset_type":"csv",
        "dataset_owner": "34552",
        "dataset_description": "hello this is description of dataset",
        "fields": 
                [{"fieldPath":"field1",
                "field_type":"string", 
                "field_description":"col1 description"},
                {"fieldPath":"field2",
                "field_type":"num", 
                "field_description":"col2 description"}], 
        "dataset_origin":"origin of dataset", 
        "dataset_location":"location of dataset"},
        specified_time = 1625108310242):
        """"
        This test is meant to test if datahub.metadata.schema_classes have changed definitions.
        Once definitions have changed, the API will fail to emit the correct MCE.
        Hence, if this function is unable to generate a valid MCE to compare to a existing MCE json,
        it is time to update the code.
        Also, not all MCE can be compared. I did not include the option to specific specific timestamps
        for all functions, because I think it is unnecessary. Hence, comparing to golden_mce.json is impossible 
        for some aspects. But, if the test_function run ok, i believe it is safe to assume the code is well.
        """
        dataset_urn = make_dataset_urn(platform = inputs["dataset_type"], name=inputs["dataset_name"])
        owner_urn = make_user_urn(inputs["dataset_owner"])
        output_mce = make_schema_mce(
                dataset_urn = dataset_urn,
                platformName = make_platform(inputs["dataset_type"]),
                actor = owner_urn,
                fields = inputs["fields"],
                system_time=specified_time)
        ownership_mce = make_ownership_mce(actor = owner_urn, dataset_urn=dataset_urn)
        memory_mce = make_institutionalmemory_mce(dataset_urn= dataset_urn,
                                        input_url=["www.yahoo.com", "www.google.com"],
                                        input_description=["yahoo", "google"],
                                        actor = owner_urn)
        lineage_mce = make_lineage_mce(upstream_urns=["urn:li:dataset:(urn:li:dataPlatform:csv,my_upstream_dataset,PROD)"],
                                        downstream_urn=dataset_urn,
                                        actor = owner_urn,
                                        lineage_type=DatasetLineageTypeClass.TRANSFORMED                
                                        )
        description_mce = make_dataset_description_mce(dataset_name = dataset_urn,
                        description = inputs["dataset_description"], 
                        customProperties={"dataset_origin":inputs["dataset_origin"],"dataset_location":inputs["dataset_location"]})
        path_mce = make_browsepath_mce(dataset_urn = dataset_urn, path = ["/csv/my_test_dataset"])
        output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_create_output.json")
        golden_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "golden_schema_mce.json")  
        
        generate_json_output([output_mce, description_mce, path_mce], file_loc=output_path)        
        with open(output_path, "r") as f:
                generated_dict= json.dumps(json.load(f), sort_keys=True)
        with open(golden_file_path, "r") as f:
                golden_mce= json.dumps(json.load(f), sort_keys=True)
        assert generated_dict == golden_mce

def test_delete_undo(inputs = {"dataset_name":"my_test_dataset",
                                "dataset_type":"csv"}
                                ) -> None:
        dataset_urn = make_dataset_urn(platform = inputs["dataset_type"], name=inputs["dataset_name"])
        delete_mce = make_delete_mce(dataset_urn)
        undo_delete_mce = make_recover_mce(dataset_urn)
        output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_delete_mce_output.json")
        golden_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "golden_delete_undo_mce.json")
        generate_json_output([delete_mce, undo_delete_mce], file_loc=output_path)
        with open(output_path, "r") as f:
                generated_dict= json.dumps(json.load(f), sort_keys=True)
        with open(golden_file_path, "r") as f:
                golden_mce= json.dumps(json.load(f), sort_keys=True)
        assert generated_dict == golden_mce

def test_type_string():
        assert determine_type('text/csv') =='csv'
        assert determine_type({"dataset_type":'application/octet-stream'}) == 'csv'
        assert determine_type("garbage") != 'csv'