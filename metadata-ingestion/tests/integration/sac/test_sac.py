import json
from functools import partial
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlsplit

import pytest
from requests.exceptions import RetryError
from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.sac.sac import SACSource, SACSourceConfig
from datahub.ingestion.source.sac.sac_common import ResourceModel
from datahub.testing import mce_helpers

MOCK_TENANT_URL = "http://tenant"
MOCK_TOKEN_URL = "http://tenant.authentication/oauth/token"
MOCK_CLIENT_ID = "foo"
MOCK_CLIENT_SECRET = "bar"
MOCK_ACCESS_TOKEN = "foobaraccesstoken"
MOCK_ACQUIRED_MODEL_ID = "ACQUIREDMODEL123"


def _des_metadata_url(model_id: str) -> str:
    return f"{MOCK_TENANT_URL}/api/v1/dataexport/providers/sac/{model_id}/$metadata"


def _acquired_model_dataset_urn() -> str:
    return (
        "urn:li:dataset:(urn:li:dataPlatform:sac,"
        f"t.4.{MOCK_ACQUIRED_MODEL_ID}:{MOCK_ACQUIRED_MODEL_ID},PROD)"
    )


def _register_pipeline_mocks(requests_mock, test_resources_dir: Path) -> None:
    requests_mock.post(MOCK_TOKEN_URL, json=match_token_url)

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


@pytest.mark.integration
def test_sac(
    pytestconfig,
    tmp_path,
    requests_mock,
    mock_time,
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sac"

    _register_pipeline_mocks(requests_mock, test_resources_dir)

    # Acquired-model schema is opt-in; enable it so the golden covers the DES path.
    des_metadata = (
        test_resources_dir / "fixtures/acquired_model_des_metadata.xml"
    ).read_text()
    requests_mock.get(_des_metadata_url(MOCK_ACQUIRED_MODEL_ID), text=des_metadata)

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
                    "ingest_acquired_data_model_schema_metadata": True,
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

    source = pipeline.source
    assert isinstance(source, SACSource)
    assert source.report.acquired_model_schema_resolved == 1
    # Models 1 (BW) and 2 (HANA) are live and skipped up front (no DES request).
    assert source.report.acquired_model_schema_skipped_known_live == 2
    assert source.report.acquired_model_schema_skipped_live_412 == 0
    assert source.report.acquired_model_schema_failed == 0

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


def _make_source(requests_mock, run_id: str) -> SACSource:
    requests_mock.post(MOCK_TOKEN_URL, json=match_token_url)
    config = SACSourceConfig(
        tenant_url=MOCK_TENANT_URL,
        token_url=MOCK_TOKEN_URL,
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
    )
    return SACSource(config, PipelineContext(run_id=run_id))


def _acquired_model(model_id: str = "ACQUIRED_MODEL") -> ResourceModel:
    return ResourceModel(
        namespace="t.S",
        model_id=model_id,
        name="Acquired model",
        description=None,
        system_type=None,
        connection_id=None,
        external_id=None,
        is_import=False,
    )


def test_acquired_model_schema_transport_error_degrades_gracefully(requests_mock):
    # A per-model Data Export Service failure (here the retry adapter exhausting on
    # repeated 5xx, surfaced as RetryError) must not abort the run: the schema is
    # skipped, the failure is counted, and no exception propagates.
    source = _make_source(requests_mock, "sac-des-error-test")
    model = _acquired_model("BROKEN_PROVIDER")
    requests_mock.get(
        _des_metadata_url(model.model_id),
        exc=RetryError("too many 500 error responses"),
    )

    assert source._get_data_export_schema(model) is None
    assert source.report.acquired_model_schema_failed == 1


def test_acquired_model_schema_skips_live_model_on_412(requests_mock):
    # The Data Export Service returns 412 for Live Data Models (e.g. DWC), whose schema lives
    # in the source system. That is an expected skip, not a failure.
    source = _make_source(requests_mock, "sac-des-412-test")
    model = _acquired_model("LIVE_DWC_MODEL")
    requests_mock.get(_des_metadata_url(model.model_id), status_code=412)

    assert source._get_data_export_schema(model) is None
    assert source.report.acquired_model_schema_skipped_live_412 == 1
    assert source.report.acquired_model_schema_failed == 0


def test_acquired_model_schema_non_ok_http_error_degrades(requests_mock):
    # A non-412 HTTP error (e.g. a per-model 403 grant miss) is counted as a failure and the
    # model is emitted without a schema.
    source = _make_source(requests_mock, "sac-des-403-test")
    model = _acquired_model()
    requests_mock.get(_des_metadata_url(model.model_id), status_code=403)

    assert source._get_data_export_schema(model) is None
    assert source.report.acquired_model_schema_failed == 1


def test_acquired_model_schema_malformed_metadata_degrades(requests_mock):
    source = _make_source(requests_mock, "sac-des-malformed-test")
    model = _acquired_model()
    requests_mock.get(_des_metadata_url(model.model_id), text="<edmx:Edmx> not closed")

    assert source._get_data_export_schema(model) is None
    assert source.report.acquired_model_schema_failed == 1


def test_acquired_model_schema_empty_fact_data_degrades(requests_mock):
    # FactData with no properties: emitted without a schema, counted as failed.
    source = _make_source(requests_mock, "sac-des-empty-test")
    model = _acquired_model()
    empty_fact_data = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">'
        "<edmx:DataServices>"
        '<Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="sac">'
        '<EntityType Name="FactData"/>'
        "</Schema></edmx:DataServices></edmx:Edmx>"
    )
    requests_mock.get(_des_metadata_url(model.model_id), text=empty_fact_data)

    assert source._get_data_export_schema(model) is None
    assert source.report.acquired_model_schema_failed == 1


def test_acquired_model_schema_des_failure_does_not_abort_pipeline(
    pytestconfig, tmp_path, requests_mock, mock_time
):
    # A per-model DES failure must not abort the run: the model is still emitted (with
    # datasetProperties, no schemaMetadata) and the failure is counted.
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sac"
    _register_pipeline_mocks(requests_mock, test_resources_dir)
    requests_mock.get(_des_metadata_url(MOCK_ACQUIRED_MODEL_ID), status_code=403)

    output_path = f"{tmp_path}/sac_mces.json"
    pipeline = Pipeline.create(
        {
            "run_id": "sac-des-pipeline-failure-test",
            "source": {
                "type": "sac",
                "config": {
                    "tenant_url": MOCK_TENANT_URL,
                    "token_url": MOCK_TOKEN_URL,
                    "client_id": MOCK_CLIENT_ID,
                    "client_secret": MOCK_CLIENT_SECRET,
                    "ingest_acquired_data_model_schema_metadata": True,
                },
            },
            "sink": {"type": "file", "config": {"filename": output_path}},
        },
    )

    pipeline.run()
    pipeline.raise_from_status()

    source = pipeline.source
    assert isinstance(source, SACSource)
    assert source.report.acquired_model_schema_failed == 1
    assert source.report.acquired_model_schema_resolved == 0

    acquired_urn = _acquired_model_dataset_urn()
    aspects_by_type = {
        record["aspectName"]
        for record in json.loads(Path(output_path).read_text())
        if record.get("entityUrn") == acquired_urn and "aspectName" in record
    }
    # The model is still emitted, just without a DES-derived schema.
    assert "datasetProperties" in aspects_by_type
    assert "schemaMetadata" not in aspects_by_type


def _register_test_connection_mocks(requests_mock: Mocker) -> None:
    requests_mock.post(MOCK_TOKEN_URL, json=match_token_url)
    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/Resources", json={"d": {"results": []}}
    )
    requests_mock.get(f"{MOCK_TENANT_URL}/api/v1/dataimport/models", json={})


def _test_connection_config() -> Dict[str, str]:
    return {
        "tenant_url": MOCK_TENANT_URL,
        "token_url": MOCK_TOKEN_URL,
        "client_id": MOCK_CLIENT_ID,
        "client_secret": MOCK_CLIENT_SECRET,
    }


def test_connection_probes_data_export_service(requests_mock):
    # With acquired-model schema ingestion enabled, test_connection probes the Data Export
    # Service; a missing "Data Export Service" OAuth grant (403) surfaces as a SCHEMA_METADATA
    # capability failure, not a basic-connectivity failure (the tenant can still connect).
    _register_test_connection_mocks(requests_mock)
    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/dataexport/administration/Namespaces(NamespaceID='sac')/Providers",
        status_code=403,
    )

    report = SACSource.test_connection(
        {
            **_test_connection_config(),
            "ingest_acquired_data_model_schema_metadata": True,
        }
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    schema_capability = report.capability_report[SourceCapability.SCHEMA_METADATA]
    assert not schema_capability.capable
    assert "403" in (schema_capability.failure_reason or "")


def test_connection_succeeds_when_data_export_service_is_reachable(requests_mock):
    _register_test_connection_mocks(requests_mock)
    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/dataexport/administration/Namespaces(NamespaceID='sac')/Providers",
        json={"value": []},
    )

    report = SACSource.test_connection(
        {
            **_test_connection_config(),
            "ingest_acquired_data_model_schema_metadata": True,
        }
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    assert any(
        "dataexport/administration" in req.url for req in requests_mock.request_history
    )


def test_connection_skips_data_export_service_when_disabled(requests_mock):
    # With acquired-model schema ingestion explicitly disabled, the Data Export Service is
    # not probed and its access is not required for a successful connection test.
    _register_test_connection_mocks(requests_mock)

    report = SACSource.test_connection(
        {
            **_test_connection_config(),
            "ingest_acquired_data_model_schema_metadata": False,
        }
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is None
    assert not any("dataexport" in req.url for req in requests_mock.request_history)


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
                {
                    "__metadata": {
                        "type": "sap.fpa.services.search.internal.ModelsType",
                        "uri": f"/api/v1/Models(resourceId='{resource_id}',modelId='t.4.{MOCK_ACQUIRED_MODEL_ID}%3A{MOCK_ACQUIRED_MODEL_ID}')",
                    },
                    "modelId": f"t.4.{MOCK_ACQUIRED_MODEL_ID}:{MOCK_ACQUIRED_MODEL_ID}",
                    "name": "Name of the fourth model (Acquired)",
                    "description": "Description of an acquired SAC-stored model whose schema comes from the Data Export Service",
                    # Acquired: no externalId/connectionId/systemType, so it takes the DES path.
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
