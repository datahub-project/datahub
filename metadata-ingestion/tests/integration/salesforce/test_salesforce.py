import json
import pathlib
from unittest import mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

FROZEN_TIME = "2022-05-12 11:00:00"

test_resources_dir = None


def _read_response(file_name: str) -> dict:
    response_json_path = f"{test_resources_dir}/mock_files/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


def side_effect_call_salesforce(type, url):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    if url.endswith("/services/data/"):
        return MockResponse(_read_response("versions_response.json"), 200)
    if url.endswith("FROM EntityDefinition WHERE IsCustomizable = true"):
        return MockResponse(_read_response("entity_definition_soql_response.json"), 200)
    elif url.endswith("FROM EntityParticle WHERE EntityDefinitionId='Account'"):
        return MockResponse(_read_response("account_fields_soql_response.json"), 200)
    elif url.endswith("FROM CustomField WHERE EntityDefinitionId='Account'"):
        return MockResponse(
            _read_response("account_custom_fields_soql_response.json"), 200
        )
    elif url.endswith("FROM CustomObject where DeveloperName='Property'"):
        return MockResponse(
            _read_response("property_custom_object_soql_response.json"), 200
        )
    elif url.endswith(
        "FROM EntityParticle WHERE EntityDefinitionId='01I5i000000Y6fp'"
    ):  # DurableId of Property__c
        return MockResponse(_read_response("property_fields_soql_response.json"), 200)
    elif url.endswith("FROM CustomField WHERE EntityDefinitionId='01I5i000000Y6fp'"):
        return MockResponse(
            _read_response("property_custom_fields_soql_response.json"), 200
        )
    elif url.endswith("/recordCount?sObjects=Property__c"):
        return MockResponse(_read_response("record_count_property_response.json"), 200)
    return MockResponse({}, 404)


@freeze_time(FROZEN_TIME)
def test_salesforce_ingest(pytestconfig, tmp_path):
    global test_resources_dir
    test_resources_dir = pathlib.Path(
        pytestconfig.rootpath / "tests/integration/salesforce"
    )

    with mock.patch("simple_salesforce.Salesforce") as mock_sdk:
        mock_sf = mock.Mock()
        mocked_call = mock.Mock()
        mocked_call.side_effect = side_effect_call_salesforce
        mock_sf._call_salesforce = mocked_call
        mock_sdk.return_value = mock_sf

        pipeline = Pipeline.create(
            {
                "run_id": "salesforce-test",
                "source": {
                    "type": "salesforce",
                    "config": {
                        "auth": "DIRECT_ACCESS_TOKEN",
                        "instance_url": "https://mydomain.my.salesforce.com/",
                        "access_token": "access_token`",
                        "ingest_tags": True,
                        "object_pattern": {
                            "allow": [
                                "^Account$",
                                "^Property__c$",
                            ],
                        },
                        "domain": {"sales": {"allow": {"^Property__c$"}}},
                        "profiling": {"enabled": True},
                        "profile_pattern": {
                            "allow": [
                                "^Property__c$",
                            ]
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/salesforce_mces.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/salesforce_mces.json",
            golden_path=test_resources_dir / "salesforce_mces_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )
