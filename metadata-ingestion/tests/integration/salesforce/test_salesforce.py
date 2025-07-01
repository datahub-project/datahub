import json
import pathlib
from unittest import mock
from unittest.mock import Mock

from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.salesforce import SalesforceConfig, SalesforceSource
from datahub.testing import mce_helpers

FROZEN_TIME = "2022-05-12 11:00:00"

test_resources_dir = pathlib.Path(__file__).parent


def _read_response(file_name: str) -> dict:
    response_json_path = f"{test_resources_dir}/mock_files/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def side_effect_call_salesforce(type, url):
    if url.endswith("/services/data/"):
        return MockResponse(_read_response("versions_response.json"), 200)
    if url.endswith("/describe/"):
        # extract object name from url
        object_name = url.split("/")[-2]
        return MockResponse(
            {
                "name": object_name,
                "fields": [
                    {"name": "SLA__c", "calculatedFormula": "IF(TIER=='GOLD', 60, 300)"}
                ],
            },
            200,
        )
    elif url.endswith("FROM EntityDefinition WHERE IsCustomizable = true"):
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


@mock.patch("datahub.ingestion.source.salesforce.Salesforce")
def test_latest_version(mock_sdk):
    mock_sf = mock.Mock()
    mocked_call = mock.Mock()
    mocked_call.side_effect = side_effect_call_salesforce
    mock_sf._call_salesforce = mocked_call
    mock_sdk.return_value = mock_sf

    config = SalesforceConfig.parse_obj(
        {
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
        }
    )
    source = SalesforceSource(config=config, ctx=Mock())
    list(source.get_workunits())  # to ensure salesforce client is initialised
    calls = mock_sf._call_salesforce.mock_calls
    assert len(calls) > 1, (
        "We didn't specify version but source didn't call SF API to get the latest one"
    )

    assert calls[0].args[-1].endswith("/services/data/"), (
        "Source didn't call proper SF API endpoint to get all versions"
    )
    assert mock_sf.sf_version == "54.0", (
        "API version was not correctly set (see versions_responses.json)"
    )


@mock.patch("datahub.ingestion.source.salesforce.Salesforce")
def test_custom_version(mock_sdk):
    mock_sf = mock.Mock()
    mocked_call = mock.Mock()
    mocked_call.side_effect = side_effect_call_salesforce
    mock_sf._call_salesforce = mocked_call
    mock_sdk.return_value = mock_sf

    config = SalesforceConfig.parse_obj(
        {
            "auth": "DIRECT_ACCESS_TOKEN",
            "api_version": "46.0",
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
        }
    )
    source = SalesforceSource(config=config, ctx=Mock())
    list(source.get_workunits())  # to ensure salesforce client is initialised

    calls = mock_sf._call_salesforce.mock_calls

    assert not calls[0].args[-1].endswith("/services/data/"), (
        "Source called API to get all versions even though we specified proper version"
    )

    assert mock_sdk.call_args.kwargs["version"] == "46.0", (
        "API client object was not correctly initialized with the custom version"
    )


@freeze_time(FROZEN_TIME)
def test_salesforce_ingest(pytestconfig, tmp_path):
    with mock.patch("datahub.ingestion.source.salesforce.Salesforce") as mock_sdk:
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
                        "use_referenced_entities_as_upstreams": False,
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


@freeze_time(FROZEN_TIME)
def test_salesforce_ingest_with_lineage(pytestconfig, tmp_path):
    with mock.patch("datahub.ingestion.source.salesforce.Salesforce") as mock_sdk:
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
                        "use_referenced_entities_as_upstreams": True,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/salesforce_mces_with_lineage.json",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=f"{tmp_path}/salesforce_mces_with_lineage.json",
            golden_path=test_resources_dir / "salesforce_mces_with_lineage_golden.json",
            ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
        )
