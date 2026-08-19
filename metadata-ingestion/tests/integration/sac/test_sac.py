from functools import partial
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlsplit

import pytest
from requests.exceptions import RetryError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sac.sac import (
    ConnectionMappingConfig,
    SACSource,
    SACSourceConfig,
)
from datahub.ingestion.source.sac.sac_common import ResourceModel
from datahub.metadata.schema_classes import SubTypesClass, UpstreamLineageClass
from datahub.testing import mce_helpers

DWC_MODEL_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:sac,"
    "t.3.C1ekdhlvx11ts0000000000000:C1ekdhlvx11ts0000000000000,PROD)"
)

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


def test_acquired_model_schema_transport_error_degrades_gracefully(requests_mock):
    # A per-model Data Export Service failure (here the retry adapter exhausting on
    # repeated 5xx, surfaced as RetryError) must not abort the run: the schema is
    # skipped, the failure is counted, and no exception propagates.
    requests_mock.post(MOCK_TOKEN_URL, json=match_token_url)

    config = SACSourceConfig(
        tenant_url=MOCK_TENANT_URL,
        token_url=MOCK_TOKEN_URL,
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
    )
    source = SACSource(config, PipelineContext(run_id="sac-des-error-test"))

    model = ResourceModel(
        namespace="t.S",
        model_id="BROKEN_PROVIDER",
        name="Broken model",
        description=None,
        system_type=None,
        connection_id=None,
        external_id=None,
        is_import=False,
    )
    requests_mock.get(
        f"{MOCK_TENANT_URL}/api/v1/dataexport/providers/sac/{model.model_id}/$metadata",
        exc=RetryError("too many 500 error responses"),
    )

    assert source._get_data_export_schema(model) is None
    assert source.report.acquired_model_schema_failed == 1


def _dwc_source(
    requests_mock: Any,
    connection_mapping: Dict[str, ConnectionMappingConfig],
    resolve_datasphere_lineage: bool = True,
) -> SACSource:
    # SACSource.__init__ eagerly fetches an OAuth token, so the token endpoint is mocked.
    requests_mock.post(MOCK_TOKEN_URL, json=match_token_url)
    config = SACSourceConfig(
        tenant_url=MOCK_TENANT_URL,
        token_url=MOCK_TOKEN_URL,
        client_id=MOCK_CLIENT_ID,
        client_secret=MOCK_CLIENT_SECRET,
        connection_mapping=connection_mapping,
        resolve_datasphere_lineage=resolve_datasphere_lineage,
    )
    return SACSource(config, PipelineContext(run_id="sac-dwc-test"))


def _dwc_model(name: str) -> ResourceModel:
    # DWC live models carry an empty externalId; only the name links to the Datasphere object.
    return ResourceModel(
        namespace="t.3.C1ekdhlvx11ts0000000000000",
        model_id="C1ekdhlvx11ts0000000000000",
        name=name,
        description=name,
        system_type="DWC",
        connection_id="DWCPROD",
        external_id="",
        is_import=False,
    )


def test_resolve_datasphere_upstream_builds_urn_from_configured_space(requests_mock):
    source = _dwc_source(
        requests_mock,
        {"DWCPROD": ConnectionMappingConfig(datasphere_space="BDAP_SAC")},
    )

    urn = source._resolve_datasphere_upstream(_dwc_model("Fax_Mart"))

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,bdap_sac.fax_mart,PROD)"
    )


def test_resolve_datasphere_upstream_honors_platform_instance_and_env(requests_mock):
    source = _dwc_source(
        requests_mock,
        {
            "DWCPROD": ConnectionMappingConfig(
                datasphere_space="bdap_sac",
                platform_instance="prod_ds",
                env="DEV",
            )
        },
    )

    urn = source._resolve_datasphere_upstream(_dwc_model("Analytics_3658_Results"))

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,prod_ds.bdap_sac.analytics_3658_results,DEV)"
    )


def test_resolve_datasphere_upstream_preserves_case_when_lowercase_disabled(
    requests_mock,
):
    # Mirrors the airbyte/sigma per-connection convert_urns_to_lowercase override:
    # when the upstream connector was run without lower-casing, casing is preserved.
    source = _dwc_source(
        requests_mock,
        {
            "DWCPROD": ConnectionMappingConfig(
                datasphere_space="BDAP_SAC",
                convert_urns_to_lowercase=False,
            )
        },
    )

    urn = source._resolve_datasphere_upstream(_dwc_model("Fax_Mart"))

    assert (
        urn
        == "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,BDAP_SAC.Fax_Mart,PROD)"
    )


def test_resolve_datasphere_upstream_no_space_is_skipped(requests_mock):
    source = _dwc_source(
        requests_mock,
        {"DWCPROD": ConnectionMappingConfig(platform_instance="prod_ds")},
    )

    assert source._resolve_datasphere_upstream(_dwc_model("Fax_Mart")) is None
    assert source.report.dwc_lineage_skipped_no_space == 1


def test_get_model_workunits_emits_datasphere_upstream_and_subtype(requests_mock):
    # Drives a DWC model through the full workunit emission (not just the resolver) so the
    # UpstreamLineage MCP and the SAC_LIVE_DATA_MODEL subtype gate are exercised end-to-end.
    source = _dwc_source(
        requests_mock,
        {"DWCPROD": ConnectionMappingConfig(datasphere_space="BDAP_SAC")},
    )

    workunits = list(source.get_model_workunits(DWC_MODEL_URN, _dwc_model("Fax_Mart")))

    upstream = _first_aspect(workunits, UpstreamLineageClass)
    assert upstream is not None
    assert [u.dataset for u in upstream.upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,bdap_sac.fax_mart,PROD)"
    ]

    subtype = _first_aspect(workunits, SubTypesClass)
    assert subtype is not None
    assert subtype.typeNames == [DatasetSubTypes.SAC_LIVE_DATA_MODEL]
    assert source.report.dwc_lineage_resolved == 1


def test_get_model_workunits_skips_dwc_lineage_when_disabled(requests_mock):
    # With the flag off a DWC model must not emit upstream lineage and, crucially, must not
    # fall through to the generic "Unknown system type" warning (DWC is a known type).
    source = _dwc_source(
        requests_mock,
        {"DWCPROD": ConnectionMappingConfig(datasphere_space="BDAP_SAC")},
        resolve_datasphere_lineage=False,
    )

    workunits = list(source.get_model_workunits(DWC_MODEL_URN, _dwc_model("Fax_Mart")))

    assert _first_aspect(workunits, UpstreamLineageClass) is None
    subtype = _first_aspect(workunits, SubTypesClass)
    assert subtype is not None
    assert subtype.typeNames == [DatasetSubTypes.SAC_LIVE_DATA_MODEL]
    assert list(source.report.warnings) == []


def _first_aspect(workunits: List[Any], aspect_type: Any) -> Any:
    for workunit in workunits:
        aspect = workunit.get_aspect_of_type(aspect_type)
        if aspect is not None:
            return aspect
    return None


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
