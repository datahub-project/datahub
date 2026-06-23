from functools import partial
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlsplit

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sac.sac import SACSource, SACSourceConfig
from datahub.testing import mce_helpers

MOCK_TENANT_URL = "http://tenant"
MOCK_TOKEN_URL = "http://tenant.authentication/oauth/token"
MOCK_CLIENT_ID = "foo"
MOCK_CLIENT_SECRET = "bar"
MOCK_ACCESS_TOKEN = "foobaraccesstoken"


@pytest.mark.integration
def test_sac(
    pytestconfig,
    tmp_path,
    requests_mock,
    mock_time,
):
    requests_mock.post(
        MOCK_TOKEN_URL,
        json=match_token_url,
    )

    test_resources_dir = pytestconfig.rootpath / "tests/integration/sac"

    # The connector queries the OData "Resources" data endpoints directly (without reading the
    # $metadata document), so only the data endpoints are mocked here.
    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/Resources",
        json=match_resources,
    )

    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/Resources('LXTH4JCE36EOYLU41PIINLYPU9XRYM26')/resourceModels",
        json=partial(match_resource, resource_id="LXTH4JCE36EOYLU41PIINLYPU9XRYM26"),
    )

    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/Resources('EOYLU41PIILXTH4JCE36NLYPU9XRYM26')/resourceModels",
        json=partial(match_resource, resource_id="EOYLU41PIILXTH4JCE36NLYPU9XRYM26"),
    )

    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/dataimport/models",
        json=match_models,
    )

    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/dataimport/models/DXGWZKANLK73U3VEL8Q577BA2F/metadata",
        json=match_model_metadata,
    )

    pipeline = Pipeline.create(
        {
            "run_id": "sac-integration-test",
            "source": {
                "type": "sac",
                "config": {
                    "tenant_url": MOCK_TENANT_URL,
                    "token_url": MOCK_TOKEN_URL,
                    "client_id": MOCK_CLIENT_ID,
                    "client_secret": MOCK_CLIENT_SECRET,
                },
            },
            "sink": {
                "type": "file",
                "config": {"filename": f"{tmp_path}/sac_mces.json"},
            },
        },
    )

    pipeline.run()
    pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/sac_mces.json",
        golden_path=test_resources_dir / "sac_mces_golden.json",
        ignore_paths=mce_helpers.IGNORE_PATH_TIMESTAMPS,
    )


@pytest.mark.integration
def test_query_odata_entities_follows_pagination(requests_mock):
    # The Resources endpoint can return results across multiple server-driven pages linked by
    # "__next"; the connector must follow them and concatenate the results.
    requests_mock.post(MOCK_TOKEN_URL, json=match_token_url)

    page_1 = {
        "d": {
            "results": [{"resourceId": "A"}],
            "__next": f"{MOCK_TENANT_URL}/api/v1/Resources?$skiptoken=PAGE2",
        }
    }
    page_2 = {"d": {"results": [{"resourceId": "B"}]}}

    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/Resources",
        [{"json": page_1}, {"json": page_2}],
    )

    config = SACSourceConfig(
        tenant_url=MOCK_TENANT_URL,
        token_url=MOCK_TOKEN_URL,
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
    )
    source = SACSource(config, PipelineContext(run_id="sac-pagination-test"))

    results = list(source._query_odata_entities("Resources", select="resourceId"))

    assert [entity["resourceId"] for entity in results] == ["A", "B"]


def match_token_url(request, context):
    form = parse_qs(request.text, strict_parsing=True)

    assert "grant_type" in form
    assert len(form["grant_type"]) == 1
    assert form["grant_type"][0] == "client_credentials"

    assert "client_id" in form
    assert len(form["client_id"]) == 1
    assert form["client_id"][0] == MOCK_CLIENT_ID

    assert "client_secret" in form
    assert len(form["client_secret"]) == 1
    assert form["client_secret"][0] == MOCK_CLIENT_SECRET

    json = {
        "access_token": MOCK_ACCESS_TOKEN,
        "expires_in": 3599,
    }

    return json


def check_authorization(headers: Dict[str, str]) -> None:
    assert "Authorization" in headers
    assert headers["Authorization"] == f"Bearer {MOCK_ACCESS_TOKEN}"

    assert "x-sap-sac-custom-auth" in headers
    assert headers["x-sap-sac-custom-auth"] == "true"


def query_params(request: Any) -> Dict[str, List[str]]:
    # parse from request.url because requests_mock lowercases the values exposed via request.qs
    return parse_qs(urlsplit(request.url).query)


def match_resources(request, context):
    check_authorization(request.headers)

    params = query_params(request)
    assert params["$format"] == ["json"]
    assert "resourceId" in params["$select"][0]
    # the access-control predicates must be sent, otherwise private/sample content could be ingested
    assert "isTemplate eq 0" in params["$filter"][0]
    assert "isSample eq 0" in params["$filter"][0]
    assert "isPublic eq 1" in params["$filter"][0]

    json = {
        "d": {
            "results": [
                {
                    "__metadata": {
                        "type": "sap.fpa.services.search.internal.ResourcesType",
                        "uri": "/api/v1/Resources('LXTH4JCE36EOYLU41PIINLYPU9XRYM26')",
                    },
                    "name": "Name of the story",
                    "description": "Description of the story",
                    "resourceId": "LXTH4JCE36EOYLU41PIINLYPU9XRYM26",
                    "resourceType": "STORY",
                    "resourceSubtype": "",
                    "storyId": "STORY:t.4:LXTH4JCE36EOYLU41PIINLYPU9XRYM26",
                    "createdTime": "/Date(1667544309783)/",
                    "createdBy": "JOHN_DOE",
                    "modifiedBy": "JOHN_DOE",
                    "modifiedTime": "/Date(1673067981272)/",
                    "isMobile": 0,
                    "openURL": "/sap/fpa/ui/tenants/3c44c/bo/story/LXTH4JCE36EOYLU41PIINLYPU9XRYM26",
                    "ancestorPath": '["Public","Folder 1","Folder 2"]',
                },
                {
                    "__metadata": {
                        "type": "sap.fpa.services.search.internal.ResourcesType",
                        "uri": "/api/v1/Resources('EOYLU41PIILXTH4JCE36NLYPU9XRYM26')",
                    },
                    "name": "Name of the application",
                    "description": "Description of the application",
                    "resourceId": "EOYLU41PIILXTH4JCE36NLYPU9XRYM26",
                    "resourceType": "STORY",
                    "resourceSubtype": "APPLICATION",
                    "storyId": "STORY:t.4:EOYLU41PIILXTH4JCE36NLYPU9XRYM26",
                    "createdTime": "/Date(1673279404272)/",
                    "createdBy": "SYSTEM",
                    "modifiedBy": "$DELETED_USER$",
                    "modifiedTime": "/Date(1673279414272)/",
                    "isMobile": 0,
                    "openURL": "/sap/fpa/ui/tenants/3c44c/bo/story/EOYLU41PIILXTH4JCE36NLYPU9XRYM26",
                    "ancestorPath": '["Public","Folder 1","Folder 2"]',
                },
            ],
        },
    }

    return json


def match_resource(request, context, resource_id):
    check_authorization(request.headers)

    params = query_params(request)
    assert params["$format"] == ["json"]
    assert "modelId" in params["$select"][0]

    json = {
        "d": {
            "results": [
                {
                    "__metadata": {
                        "type": "sap.fpa.services.search.internal.ModelsType",
                        "uri": f"/api/v1/Models(resourceId='{resource_id}',modelId='t.4.ANL8Q577BA2F73KU3VELDXGWZK%3AANL8Q577BA2F73KU3VELDXGWZK')",
                    },
                    "modelId": "t.4.ANL8Q577BA2F73KU3VELDXGWZK:ANL8Q577BA2F73KU3VELDXGWZK",
                    "name": "Name of the first model (BW)",
                    "description": "Description of the first model which has a connection to a BW query",
                    "externalId": "query:[][][QUERY_TECHNICAL_NAME]",
                    "connectionId": "BW",
                    "systemType": "BW",
                },
                {
                    "__metadata": {
                        "type": "sap.fpa.services.search.internal.ModelsType",
                        "uri": f"/api/v1/Models(resourceId='{resource_id}',modelId='t.4.K73U3VELDXGWZKANL8Q577BA2F%3AK73U3VELDXGWZKANL8Q577BA2F')",
                    },
                    "modelId": "t.4.K73U3VELDXGWZKANL8Q577BA2F:K73U3VELDXGWZKANL8Q577BA2F",
                    "name": "Name of the second model (HANA)",
                    "description": "Description of the second model which has a connection to a HANA view",
                    "externalId": "view:[SCHEMA][NAMESPACE.SCHEMA][VIEW]",
                    "connectionId": "HANA",
                    "systemType": "HANA",
                },
                {
                    "__metadata": {
                        "type": "sap.fpa.services.search.internal.ModelsType",
                        "uri": f"/api/v1/Models(resourceId='{resource_id}',modelId='t.4.DXGWZKANLK73U3VEL8Q577BA2F%3ADXGWZKANLK73U3VEL8Q577BA2F')",
                    },
                    "modelId": "t.4.DXGWZKANLK73U3VEL8Q577BA2F:DXGWZKANLK73U3VEL8Q577BA2F",
                    "name": "Name of the third model (Import)",
                    "description": "Description of the third model which was imported",
                    "externalId": "",
                    "connectionId": "",
                    "systemType": None,
                },
            ],
        },
    }

    return json


def match_models(request, context):
    check_authorization(request.headers)

    json = {
        "models": [
            {
                "modelID": "DXGWZKANLK73U3VEL8Q577BA2F",
                "modelName": "Name of the third model (Import)",
                "modelDescription": "Description of the third model which was imported",
                "modelURL": f"{MOCK_TENANT_URL}/api/v1/dataimport/models/DXGWZKANLK73U3VEL8Q577BA2F",
            },
        ],
    }

    return json


def match_model_metadata(request, context):
    check_authorization(request.headers)

    json = {
        "factData": {
            "keys": [
                "Account",
                "FIELD1",
                "FIELD2",
                "FIELD3",
                "Version",
            ],
            "columns": [
                {
                    "columnName": "Account",
                    "columnDataType": "string",
                    "maxLength": 256,
                    "isKey": True,
                    "propertyType": "PROPERTY",
                    "descriptionName": "Account",
                },
                {
                    "columnName": "FIELD1",
                    "columnDataType": "string",
                    "maxLength": 256,
                    "isKey": True,
                    "propertyType": "PROPERTY",
                    "descriptionName": "FIELD1",
                },
                {
                    "columnName": "FIELD2",
                    "columnDataType": "string",
                    "maxLength": 256,
                    "isKey": True,
                    "propertyType": "PROPERTY",
                    "descriptionName": "FIELD2",
                },
                {
                    "columnName": "FIELD3",
                    "columnDataType": "string",
                    "maxLength": 256,
                    "isKey": True,
                    "propertyType": "DATE",
                    "descriptionName": "FIELD3",
                },
                {
                    "columnName": "Version",
                    "columnDataType": "string",
                    "maxLength": 300,
                    "isKey": True,
                    "propertyType": "PROPERTY",
                    "descriptionName": "Version",
                },
                {
                    "columnName": "SignedData",
                    "columnDataType": "decimal",
                    "maxLength": 32,
                    "precision": 31,
                    "scale": 7,
                    "isKey": False,
                    "propertyType": "PROPERTY",
                    "descriptionName": "SignedData",
                },
            ],
        },
    }

    return json
