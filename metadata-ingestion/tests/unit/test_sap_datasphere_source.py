# tests/unit/test_sap_datasphere_source.py
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, cast

import pytest
import requests
import requests_mock as rm
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sap_datasphere import source as source_module
from datahub.ingestion.source.sap_datasphere.client import SapDatasphereClient
from datahub.ingestion.source.sap_datasphere.config import SapDatasphereConfig
from datahub.ingestion.source.sap_datasphere.lineage import (
    ColumnLineageContext,
    ColumnLineagePair,
    UpstreamColRef,
)
from datahub.ingestion.source.sap_datasphere.platform_mapping import ResolvedPlatform
from datahub.ingestion.source.sap_datasphere.report import (
    ApiCallStats,
    SapDatasphereReport,
)
from datahub.ingestion.source.sap_datasphere.source import SapDatasphereSource, _chunked
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    EditableDatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.utilities.lossy_collections import LossyList
from tests.unit.sap_datasphere_test_helpers import (
    aspect_as,
    aspect_of,
    entity_urn_of,
)


@pytest.fixture(autouse=True)
def _default_csn_endpoint_404(requests_mock):
    """include_view_definitions defaults to True, so the connector now fetches
    each asset's CSN from the per-object-type /dwaas-core/ endpoint even when
    lineage is off. Tests that don't care about CSN don't mock that endpoint;
    register a low-priority 404 fallback so those fetches degrade gracefully
    (fetch_object_definition catches RequestException and returns None) instead
    of raising NoMockAddress. Tests that register a specific CSN URL override
    this — requests_mock checks matchers in reverse registration order, and this
    autouse fixture registers before each test body runs.
    """
    requests_mock.get(
        re.compile(
            r"https://[^/]+/dwaas-core/api/v1/spaces/[^/]+/(views|analyticmodels)/"
        ),
        status_code=404,
    )


def test_get_workunits_emits_container_for_space(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-run")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={"value": []},
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # Must have at least: containerProperties + dataPlatformInstance for S1
    aspect_types = [aspect_of(wu).__class__.__name__ for wu in workunits]
    assert "ContainerPropertiesClass" in aspect_types
    assert "DataPlatformInstanceClass" in aspect_types


def test_schema_emitted_for_exposed_asset(requests_mock):
    fixture_xml = (
        Path(__file__).parent / "fixtures" / "sap_datasphere_dimension_day.xml"
    ).read_text()

    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-schema")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "label": "Day",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/DIM_DAY/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/DIM_DAY/$metadata",
        text=fixture_xml,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    aspect_types = {aspect_of(wu).__class__.__name__ for wu in workunits}
    assert "SchemaMetadataClass" in aspect_types

    schema_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SchemaMetadataClass"
    )
    fields = aspect_as(schema_wu, SchemaMetadataClass).fields
    assert len(fields) == 7
    field_names = {f.fieldPath for f in fields}
    assert "DATE_SQL" in field_names


def test_expose_for_consumption_only_skips_null_url(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "expose_for_consumption_only": True,
        }
    )
    ctx = PipelineContext(run_id="test-expose")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "UNEXPOSED",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    aspect_types = {aspect_of(wu).__class__.__name__ for wu in workunits}
    assert "DatasetPropertiesClass" not in aspect_types
    assert source.report.assets_filtered == 1


def test_schema_failure_appends_to_lossy_list_and_warns(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-schema-failure")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "BROKEN",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/BROKEN/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/BROKEN/$metadata",
        status_code=500,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    assert isinstance(source.report.assets_schema_failed, LossyList)
    assert "BROKEN" in list(source.report.assets_schema_failed)
    # report.warning() should have been called (visible in report.warnings)
    warning_messages = [w.message for w in source.report.warnings]
    assert any("BROKEN" in m for m in warning_messages), (
        f"Expected a warning mentioning BROKEN, got: {warning_messages}"
    )


def test_config_rejects_when_no_credentials_present():
    with pytest.raises(ValidationError) as excinfo:
        SapDatasphereConfig.model_validate(
            {"base_url": "https://myco.eu10.hcs.cloud.sap"}
        )
    assert (
        "credential" in str(excinfo.value).lower()
        or "token" in str(excinfo.value).lower()
    )


def test_config_accepts_raw_token():
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    assert cfg.token is not None
    assert cfg.token.get_secret_value() == "tok"


def test_config_accepts_refresh_token_with_client_id():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rt",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        }
    )
    assert cfg.refresh_token is not None
    assert cfg.refresh_token.get_secret_value() == "rt"


def test_config_accepts_client_credentials():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "cs",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        }
    )
    assert cfg.client_id == "cid"


def test_all_config_fields_have_descriptions():
    missing = []
    for field_name, field_info in SapDatasphereConfig.model_fields.items():
        if field_info.description is None and not field_name.startswith("_"):
            missing.append(field_name)
    # Fields inherited from mixins (env, platform_instance, stateful_ingestion) are allowed to have no description
    inherited_ok = {"env", "platform_instance", "stateful_ingestion"}
    missing_local = [f for f in missing if f not in inherited_ok]
    assert not missing_local, f"Fields missing descriptions: {missing_local}"


def test_assets_endpoint_failure_continues_to_next_space(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-asset-failure")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={
            "value": [
                {"name": "GOOD", "label": "Good Space"},
                {"name": "BAD", "label": "Bad Space"},
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('GOOD')/assets",
        json={"value": []},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('BAD')/assets",
        status_code=500,
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())  # should NOT raise

    container_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "ContainerPropertiesClass"
    }
    assert len(container_urns) == 2  # Both space containers should still emit

    warning_messages = [w.message for w in source.report.warnings]
    assert any("BAD" in m for m in warning_messages), (
        f"Expected a warning mentioning BAD space, got: {warning_messages}"
    )


def test_spaces_endpoint_failure_emits_warning(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-spaces-failure")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        status_code=503,
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())  # should NOT raise

    # The point is: even with the spaces endpoint failing, we exit gracefully
    # without crashing — and emit a warning. We don't need to assert what
    # workunits are produced (with SDK V2 + no spaces, there may be none).
    warning_messages = [w.message for w in source.report.warnings]
    assert any("space" in m.lower() for m in warning_messages)


def test_paginate_concatenates_pages(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )

    # Two-page response: 2 items first (full page), 1 item second (partial → stop).
    # Use page_size=2 so the first page is recognised as full and pagination continues.
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        [
            {"json": {"value": [{"name": "S1"}, {"name": "S2"}]}},
            {"json": {"value": [{"name": "S3"}]}},
        ],
    )

    client = SapDatasphereClient(cfg)
    url = f"{cfg.base_url}/api/v1/datasphere/consumption/catalog/spaces"
    items = list(client._paginate(url, page_size=2))
    names = [s["name"] for s in items]
    assert names == ["S1", "S2", "S3"]


def test_client_raises_value_error_on_missing_access_token(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rt",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )

    # OAuth endpoint returns 200 but no access_token field
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"error": "invalid_grant", "error_description": "refresh token expired"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )

    # Token is fetched lazily — the ValueError surfaces on first request, not __init__
    client = SapDatasphereClient(cfg)
    with pytest.raises(ValueError) as excinfo:
        list(client.list_spaces())

    assert "access_token" in str(excinfo.value).lower()
    # The error should mention the OAuth server's response so the user can diagnose
    assert (
        "invalid_grant" in str(excinfo.value)
        or "refresh token expired" in str(excinfo.value)
        or "response" in str(excinfo.value).lower()
    )


def test_client_does_not_fetch_token_in_init():
    """Constructing the client with a raw token should not make a network call."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "raw_tok"}
    )
    # Should not raise even though we haven't mocked any HTTP calls
    client = SapDatasphereClient(cfg)
    # For raw-token configs, the Authorization header may be set eagerly (no network call needed).
    # The key requirement: no network exception is raised during construction.
    assert client is not None


def test_dataset_picks_up_entity_label_and_custom_props_from_edmx(requests_mock):
    edmx_xml = """<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="ns">
      <EntityType Name="REVENUE">
        <Property Name="ID" Type="Edm.Int32"/>
      </EntityType>
      <Annotations Target="ns.REVENUE">
        <Annotation Term="Common.Label" String="Monthly Revenue"/>
        <Annotation Term="Analytics.dimensionType" EnumMember="Analytics.DimensionType/Time"/>
        <Annotation Term="Analytics.Measure"/>
      </Annotations>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-entity-label")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "REVENUE",
                    "label": "Revenue",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/REVENUE/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/REVENUE/$metadata", text=edmx_xml
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    props_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "DatasetPropertiesClass"
    )
    props = aspect_as(props_wu, DatasetPropertiesClass)
    # Custom properties should include the parsed annotations
    assert props.customProperties.get("sap_dimension_type") == "Time"
    assert props.customProperties.get("sap_is_measure") == "true"

    # Description is stored in EditableDatasetPropertiesClass when running outside
    # the ingestion pipeline (SDK default attribution); check both aspects.
    description_candidates = []
    for wu in workunits:
        aspect = aspect_of(wu)
        if isinstance(aspect, (DatasetPropertiesClass, EditableDatasetPropertiesClass)):
            if aspect.description:
                description_candidates.append(aspect.description)
    assert any("Monthly Revenue" in d for d in description_candidates), (
        f"Expected 'Monthly Revenue' in a description aspect, got: {description_candidates}"
    )


def test_client_fetches_refresh_token_lazily(requests_mock):
    """Refresh-token client should NOT call XSUAA in __init__; only on first request."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "refresh_token": "rt",
            "client_id": "cid",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )

    # No mock for XSUAA yet — construction must NOT call it
    client = SapDatasphereClient(cfg)
    # If __init__ tried to call XSUAA, requests_mock would have raised NoMockAddress
    # (or the call would appear in request_history).
    xsuaa_calls_during_init = [
        h for h in requests_mock.request_history if "authentication" in h.url
    ]
    assert len(xsuaa_calls_during_init) == 0, (
        f"Token fetched during __init__: {[h.url for h in xsuaa_calls_during_init]}"
    )

    # Now arm the mocks and make a real request
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        json={"access_token": "fresh_token"},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )

    list(client.list_spaces())

    # NOW the token should have been fetched (exactly once)
    token_calls = [
        h for h in requests_mock.request_history if "authentication" in h.url
    ]
    assert len(token_calls) >= 1, "Expected at least 1 token fetch after first request"


def test_emits_dataplatforminstance_for_dataset(requests_mock):
    """SDK V2 should automatically emit dataPlatformInstance — verify it lands."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-dpi")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "T1",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    aspect_types = [aspect_of(wu).__class__.__name__ for wu in workunits]
    # Both container and dataset should emit DataPlatformInstanceClass
    assert aspect_types.count("DataPlatformInstanceClass") >= 2, (
        f"Expected ≥2 DataPlatformInstanceClass aspects (one per entity), got: {aspect_types}"
    )
    # Subtype aspect for the dataset should be present
    assert "SubTypesClass" in aspect_types
    # Container subtype "Space" should appear
    subtypes_lists = [
        aspect_as(wu, SubTypesClass).typeNames
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SubTypesClass"
    ]
    flat_subtypes = [t for sub in subtypes_lists for t in sub]
    assert "Space" in flat_subtypes
    assert "View" in flat_subtypes  # T1 is not analytical → subtype "View"


def test_asset_pattern_deny_filters_emitted_datasets(requests_mock):
    """L2: asset_pattern.deny must prevent dataset emission for matching assets
    and bump the `assets_filtered` counter without emitting the dataset URN.

    Symmetric to ``test_space_pattern_actually_filters_emitted_containers``.
    """
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "asset_pattern": {"deny": ["^TMP_.*"]},
        }
    )
    ctx = PipelineContext(run_id="test-asset-filter")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "KEEP_ME",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                },
                {
                    "name": "TMP_DROP",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                },
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if (entity_urn_of(wu) or "").startswith("urn:li:dataset:")
    }
    # KEEP_ME emits a dataset, TMP_DROP does not. URNs are lowercased by default.
    assert any("keep_me" in u for u in dataset_urns), (
        f"Expected KEEP_ME dataset URN; got: {dataset_urns}"
    )
    assert not any("tmp_drop" in u for u in dataset_urns), (
        f"TMP_DROP should be filtered out; got: {dataset_urns}"
    )
    assert source.report.assets_scanned == 2
    assert source.report.assets_filtered == 1


def test_space_pattern_actually_filters_emitted_containers(requests_mock):
    """Verify space_pattern.deny prevents container emission for matching spaces."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "space_pattern": {"deny": ["^BLOCKED.*"]},
        }
    )
    ctx = PipelineContext(run_id="test-space-filter")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={
            "value": [
                {"name": "ALLOWED", "label": "Allowed Space"},
                {"name": "BLOCKED_HR", "label": "Blocked Space"},
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('ALLOWED')/assets",
        json={"value": []},
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # Container URNs are opaque hashes; use customProperties['space'] to identify
    emitted_spaces = {
        aspect_as(wu, ContainerPropertiesClass).customProperties["space"]
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "ContainerPropertiesClass"
    }
    # ALLOWED should be emitted, BLOCKED_HR should not. The container key's
    # `space` segment is lowercased (convert_urns_to_lowercase default True).
    assert "allowed" in emitted_spaces
    assert "blocked_hr" not in emitted_spaces
    assert source.report.spaces_scanned == 2
    assert source.report.spaces_filtered == 1
    # The `assets` endpoint for BLOCKED_HR must NEVER be called
    requested_urls = [h.url for h in requests_mock.request_history]
    assert not any("'BLOCKED_HR'" in u for u in requested_urls)


def test_dataset_urn_includes_space_and_asset_and_platform_instance(requests_mock):
    """Dataset URNs must encode platform, name=<space>.<asset>, and platform_instance.

    Managed assets are emitted under the ``sap-datasphere`` platform with the
    top-level ``platform_instance`` — any ``_managed`` entry's platform/instance
    fields are ignored.
    """
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "platform_instance": "tenant_a",
            "env": "PROD",
        }
    )
    ctx = PipelineContext(run_id="test-urn")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "FINANCE", "label": "Finance"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('FINANCE')/assets",
        json={
            "value": [
                {
                    "name": "GL_BALANCE",
                    "spaceName": "FINANCE",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/FINANCE/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) >= 1
    sample_urn = next(iter(dataset_urns))
    assert "urn:li:dataPlatform:sap-datasphere" in sample_urn
    assert "finance.gl_balance" in sample_urn
    assert "tenant_a" in sample_urn
    assert ",PROD)" in sample_urn


def test_malformed_edmx_emits_warning_and_appends_to_failed_list(requests_mock):
    """When EDMX XML is malformed, the source should warn AND track in assets_schema_failed."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-malformed-edmx")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "BAD",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/BAD/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    # Server returns 200 but the body is not valid XML
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/BAD/$metadata",
        text="<not-valid-edmx>oops",
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    assert "BAD" in list(source.report.assets_schema_failed), (
        "Asset name should be tracked in assets_schema_failed even on parse error"
    )
    warning_messages = [w.message for w in source.report.warnings]
    assert any("BAD" in m for m in warning_messages), (
        f"Expected a warning mentioning BAD asset, got: {warning_messages}"
    )


def test_edmx_404_skips_quietly_without_warning(requests_mock):
    """A 404 from the EDMX endpoint is benign (asset not OData-exposed). The
    source must NOT emit an 'EDMX schema fetch failed' warning and must NOT
    track the asset in assets_schema_failed."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-edmx-404")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "GONE",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/GONE/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/GONE/$metadata",
        status_code=404,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    assert "GONE" not in list(source.report.assets_schema_failed), (
        "A benign 404 must not be tracked as a schema failure"
    )
    titles = [w.title or "" for w in source.report.warnings]
    assert "EDMX schema fetch failed" not in titles, (
        f"A benign 404 must not emit an EDMX-schema-fetch warning; got: {titles}"
    )


def test_edmx_403_not_double_warned_by_parse_schema(requests_mock):
    """A 403 is already warned by the client (fetch_edmx). _parse_schema must
    NOT emit a second 'EDMX schema fetch failed' warning for it, though the
    asset is still tracked in assets_schema_failed."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-edmx-403")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DENIED",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/DENIED/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/DENIED/$metadata",
        status_code=403,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    # The asset is still tracked as a schema failure.
    assert "DENIED" in list(source.report.assets_schema_failed)
    titles = [w.title or "" for w in source.report.warnings]
    # The client already warned with the forbidden title; _parse_schema must
    # not add the generic fetch-failed warning on top.
    assert "EDMX schema fetch failed" not in titles, (
        f"A 403 must not be double-warned by _parse_schema; got: {titles}"
    )
    assert any("forbidden" in t.lower() for t in titles), (
        f"Expected the client's 403/forbidden warning to be present; got: {titles}"
    )


def test_edmx_network_error_emits_fetch_failed_warning(requests_mock):
    """A genuine network error (RequestException) must still surface the
    'EDMX schema fetch failed' warning and track the asset."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-edmx-error")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "FLAKY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/FLAKY/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/FLAKY/$metadata",
        exc=requests.exceptions.ConnectionError,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    assert "FLAKY" in list(source.report.assets_schema_failed)
    titles = [w.title or "" for w in source.report.warnings]
    assert "EDMX schema fetch failed" in titles, (
        f"A genuine fetch error must emit the EDMX-schema-fetch warning; got: {titles}"
    )


def test_edmx_with_no_entity_type_emits_warning(requests_mock):
    """When EDMX has no EntityType element, the source should warn and skip the schema."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-no-entitytype")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "EMPTY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/EMPTY/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    # Server returns 200 with valid XML but no EntityType
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/EMPTY/$metadata",
        text='<?xml version="1.0"?>'
        '<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" '
        'xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">'
        '<edmx:DataServices><Schema Namespace="ns"/></edmx:DataServices>'
        "</edmx:Edmx>",
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    assert "EMPTY" in list(source.report.assets_schema_failed)
    warning_messages = [w.message for w in source.report.warnings]
    assert any("EMPTY" in m for m in warning_messages)


def test_subtype_is_analytical_model_when_supportsanalyticalqueries(requests_mock):
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-subtype")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "AM",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": True,
                    "hasParameters": False,
                },
                {
                    "name": "VIEW1",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                },
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # Map dataset URN → list of subtypes
    subtypes_per_urn: Dict[str, List[str]] = {}
    for wu in workunits:
        if aspect_of(wu).__class__.__name__ == "SubTypesClass":
            subtypes_per_urn.setdefault(entity_urn_of(wu), []).extend(
                aspect_as(wu, SubTypesClass).typeNames
            )

    am_urn = next(u for u in subtypes_per_urn if "s1.am" in u)
    view_urn = next(u for u in subtypes_per_urn if "s1.view1" in u)
    assert "Analytic Model" in subtypes_per_urn[am_urn]
    assert "View" in subtypes_per_urn[view_urn]


def test_business_layer_failure_degrades_to_no_lineage_not_dropped(
    requests_mock, monkeypatch
):
    """If _apply_business_layer raises (e.g. malformed dataEntity.key breaking
    URN construction), the analytic-model dataset must still be emitted (with
    its pre-business-layer lineage), NOT dropped by the per-asset isolation
    handler. A degradation report.warning must be recorded."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-bl-degrade")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "AM",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": True,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)

    def _boom(*args, **kwargs):
        raise ValueError("simulated star-schema lineage assembly failure")

    monkeypatch.setattr(source, "_apply_business_layer", _boom)

    workunits = list(source.get_workunits())

    # The analytic-model dataset must still be emitted (a subtype workunit for
    # it is sufficient evidence the dataset was not dropped).
    am_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SubTypesClass"
        and "s1.am" in (entity_urn_of(wu) or "")
    }
    assert am_urns, "Analytic-model dataset was dropped instead of degrading"

    titles = [w.title or "" for w in source.report.warnings]
    assert any("business layer" in (t or "").lower() for t in titles), (
        f"Expected a business-layer degradation warning; got: {titles}"
    )
    # It must NOT have been routed through the drop-the-whole-asset handler.
    assert not any(t == "Failed to emit Datasphere asset" for t in titles), (
        f"Asset was dropped by per-asset isolation; got titles: {titles}"
    )


def test_managed_asset_urn_uses_sap_datasphere_platform_by_default(requests_mock):
    """A local/managed asset (no @remote.source) should emit under the
    sap-datasphere platform — managed assets belong to the Datasphere tenant's
    identity, not the underlying HANA storage."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-managed-urn")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) >= 1
    for urn in dataset_urns:
        assert "urn:li:dataPlatform:sap-datasphere" in urn, (
            f"Expected sap-datasphere platform URN, got: {urn}"
        )
    sample = next(iter(dataset_urns))
    assert "s1.dim_day" in sample


def test_managed_asset_respects_platform_instance_override(requests_mock):
    """Managed assets inherit the top-level ``platform_instance``.

    Any ``_managed`` entry's platform/platform_instance fields are ignored —
    the synthetic ``_managed`` key only honors ``enabled``.
    """
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "platform_instance": "prod_datasphere_tenant",
        }
    )
    ctx = PipelineContext(run_id="test-managed-instance")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert any("prod_datasphere_tenant" in urn for urn in dataset_urns), (
        f"Expected platform_instance prod_datasphere_tenant in URNs, got: {dataset_urns}"
    )


def test_managed_disabled_skips_assets(requests_mock):
    """Setting enabled=False on _managed should skip all managed datasets."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "connection_to_platform_map": {
                "_managed": {"platform": "hana", "enabled": False},
            },
        }
    )
    ctx = PipelineContext(run_id="test-managed-disabled")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert dataset_urns == set(), (
        f"Expected no datasets when _managed.enabled=False, got: {dataset_urns}"
    )
    assert len(list(source.report.assets_skipped_disabled)) >= 1


def test_space_container_data_platform_instance_aspect_uses_sap_datasphere(
    requests_mock,
):
    """Containers (Spaces) are Datasphere-only abstractions and stay under the
    sap-datasphere platform even after the storage-platform refactor."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
        }
    )
    ctx = PipelineContext(run_id="test-space-platform")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={"value": []},
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # Find the container's dataPlatformInstance aspect.
    dpi_urns = []
    for wu in workunits:
        if (
            aspect_of(wu).__class__.__name__ == "DataPlatformInstanceClass"
            and "urn:li:container:" in entity_urn_of(wu)
        ):
            platform_urn = aspect_as(wu, DataPlatformInstanceClass).platform
            dpi_urns.append(platform_urn)

    assert any("urn:li:dataPlatform:sap-datasphere" in p for p in dpi_urns), (
        f"Expected container's dataPlatformInstance.platform to reference "
        f"sap-datasphere, got: {dpi_urns}"
    )


def test_table_lineage_emitted_from_csn_query_from_ref(requests_mock):
    """When include_lineage=True, a view's CSN query.SELECT.from.ref becomes an
    UpstreamLineage MCP under the resolved platform."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_lineage": True,
        }
    )
    ctx = PipelineContext(run_id="test-csn-lineage")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "VIEW_X",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/VIEW_X",
        json={
            "definitions": {
                "VIEW_X": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["TABLE_Y"], "as": "Y"},
                            "columns": [{"ref": ["COL1"]}],
                        }
                    },
                }
            }
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # Find an UpstreamLineage aspect for VIEW_X
    lineage_wus = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "UpstreamLineageClass"
    ]
    assert len(lineage_wus) >= 1, (
        f"Expected at least one UpstreamLineageClass aspect; got aspects: "
        f"{[aspect_of(wu).__class__.__name__ for wu in workunits]}"
    )
    upstreams = aspect_as(lineage_wus[0], UpstreamLineageClass).upstreams
    upstream_urns = [u.dataset for u in upstreams]
    assert any("sap-datasphere" in u and "s1.table_y" in u for u in upstream_urns), (
        f"Expected upstream URN pointing at sap-datasphere:S1.TABLE_Y, got: {upstream_urns}"
    )


def test_lineage_not_fetched_when_include_lineage_false(requests_mock):
    """When both include_lineage and include_view_definitions are False, the CSN
    endpoint is never called."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_view_definitions": False,
        }
    )
    ctx = PipelineContext(run_id="test-no-csn")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "VIEW_X",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    csn_calls = [
        h
        for h in requests_mock.request_history
        if "/dwaas-core/api/v1/spaces/" in h.url
        and ("/views/" in h.url or "/analyticmodels/" in h.url)
    ]
    assert len(csn_calls) == 0, (
        f"CSN should NOT be fetched when include_lineage=False; "
        f"got {len(csn_calls)} per-object-type calls: {[h.url for h in csn_calls]}"
    )


def test_unknown_connection_typeid_counts_as_assets_skipped_unknown_typeid(
    requests_mock,
):
    """Assets routed to an unknown-typeId connection should NOT be lumped into
    assets_filtered. They should land in the new assets_skipped_unknown_typeid
    counter for distinguishability."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_lineage": True,
        }
    )
    ctx = PipelineContext(run_id="test-unknown-typeid")

    # A connection with a typeId we don't have a built-in default for (BIGQUERY).
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": "GBQ_PROD", "typeId": "BIGQUERY"}],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "FED_TABLE",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    # CSN routes this asset to the unknown GBQ_PROD connection
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/FED_TABLE",
        json={
            "definitions": {
                "FED_TABLE": {
                    "kind": "entity",
                    "@remote.source": "GBQ_PROD",
                }
            }
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert dataset_urns == set(), (
        f"Expected no dataset URNs for unknown-typeId asset; got: {dataset_urns}"
    )
    # The new counter should be 1; assets_filtered should NOT increment for this.
    assert source.report.assets_skipped_unknown_typeid is not None
    skipped = list(source.report.assets_skipped_unknown_typeid)
    assert any("FED_TABLE" in s or "GBQ_PROD" in s for s in skipped), (
        f"Expected FED_TABLE in assets_skipped_unknown_typeid, got: {skipped}"
    )


def test_keyerror_in_asset_record_does_not_crash_ingestion(requests_mock):
    """A malformed asset record (missing 'name') should not abort the whole space.
    Other assets in the same space should still be ingested."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-keyerror")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    # First asset is malformed (no 'name' key); second is valid.
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    # NOTE: missing "name" key — _emit_asset guards this with a
                    # specific "malformed asset record" warning (mirroring the
                    # malformed-space guard) rather than raising KeyError.
                    "label": "Malformed",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                },
                {
                    "name": "GOOD_ASSET",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                },
            ]
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())  # must NOT raise

    # GOOD_ASSET should still be emitted despite the prior malformed record
    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert any("good_asset" in urn for urn in dataset_urns), (
        f"Expected GOOD_ASSET to still ingest after malformed record; got: {dataset_urns}"
    )
    # A SPECIFIC malformed-record warning should be emitted (not the generic
    # isolation-wrapper catch-all), mirroring the malformed-space-record guard.
    warning_titles = [w.title or "" for w in source.report.warnings]
    assert any("malformed Datasphere asset record" in t for t in warning_titles), (
        f"Expected a specific malformed-asset-record warning; got: {warning_titles}"
    )


def test_keyerror_in_space_record_does_not_crash_ingestion(requests_mock):
    """A malformed space record (missing 'name') should not abort the whole run.
    Other spaces should still be processed."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-space-keyerror")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/GOOD_SPACE/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={
            "value": [
                {"label": "Malformed Space"},  # missing 'name'
                {"name": "GOOD_SPACE", "label": "Good Space"},
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('GOOD_SPACE')/assets",
        json={"value": []},
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())  # must NOT raise

    container_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if "urn:li:container:" in entity_urn_of(wu)
    }
    # GOOD_SPACE container should still be emitted
    assert len(container_urns) >= 1, (
        f"Expected GOOD_SPACE container despite malformed sibling; got: {container_urns}"
    )


def test_lowercase_urn_lowercases_emitted_dataset_name(requests_mock):
    """The dataset name portion of the URN is lowercased by default
    (convert_urns_to_lowercase defaults True), including for federated assets
    routed through an external connection such as Snowflake."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_lineage": True,
            "connection_to_platform_map": {
                "SNOWFLAKE_PROD": {
                    "platform": "snowflake",
                    "platform_instance": "acct_xyz",
                    "lowercase_urn": True,
                },
            },
        }
    )
    ctx = PipelineContext(run_id="test-lowercase")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": "SNOWFLAKE_PROD", "typeId": "SNOWFLAKE"}],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    # CSN routes the view through SNOWFLAKE_PROD via @remote.source
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/DIM_DAY",
        json={
            "definitions": {
                "DIM_DAY": {
                    "kind": "entity",
                    "@remote.source": "SNOWFLAKE_PROD",
                    "@DataWarehouse.external.schema": "PUBLIC",
                }
            }
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) >= 1
    # The dataset name portion of the URN should be all-lowercase ("s1.dim_day")
    for urn in dataset_urns:
        # URN shape: urn:li:dataset:(urn:li:dataPlatform:snowflake,<instance>.<name>,<env>)
        assert "s1.dim_day" in urn, (
            f"Expected lowercased dataset name 's1.dim_day' in URN, got: {urn}"
        )
        assert "DIM_DAY" not in urn, (
            f"Expected uppercase 'DIM_DAY' NOT to appear; got: {urn}"
        )


def test_federated_asset_routes_to_remote_source_platform(requests_mock):
    """When include_lineage=True and CSN has @remote.source pointing at a
    configured external connection, the dataset URN should be under the FEDERATED
    storage platform (e.g., Snowflake), NOT under sap-datasphere."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_lineage": True,
            "platform_type_defaults": {
                "SNOWFLAKE": {
                    "platform": "snowflake",
                    "lowercase_urn": True,
                },
            },
        }
    )
    ctx = PipelineContext(run_id="test-federated")

    # Tenant has a Snowflake connection
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": "SNOWFLAKE_PROD", "typeId": "SNOWFLAKE"}],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "FED_CUSTOMERS",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    # CSN for this asset has @remote.source pointing at SNOWFLAKE_PROD
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/FED_CUSTOMERS",
        json={
            "definitions": {
                "FED_CUSTOMERS": {
                    "kind": "entity",
                    "@remote.source": "SNOWFLAKE_PROD",
                    "@DataWarehouse.external.schema": "PUBLIC",
                }
            }
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) >= 1, (
        f"Expected at least one dataset URN; got none. Workunits: "
        f"{[type(aspect_of(wu)).__name__ for wu in workunits]}"
    )
    # The federated dataset should be under snowflake platform, lowercased
    for urn in dataset_urns:
        assert "urn:li:dataPlatform:snowflake" in urn, (
            f"Expected snowflake platform URN for federated asset; got: {urn}"
        )
        assert "urn:li:dataPlatform:sap-datasphere" not in urn, (
            f"Federated asset should NOT be under sap-datasphere; got: {urn}"
        )
        assert "urn:li:dataPlatform:hana" not in urn, (
            f"Federated asset should NOT be under hana; got: {urn}"
        )
        assert "s1.fed_customers" in urn, (
            f"Expected lowercased 's1.fed_customers' in URN, got: {urn}"
        )


def test_federated_remote_table_still_emits_on_storage_platform(requests_mock):
    """Regression test for Tasks 1-3: routing managed assets to sap-datasphere
    must NOT break federated routing.

    A Datasphere view that federates via @remote.source to a Snowflake
    connection (configured explicitly in ``connection_to_platform_map``) MUST
    emit its URN under the Snowflake platform, NOT under sap-datasphere.
    """
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_lineage": True,
            "platform_instance": "datasphere_tenant",
            "connection_to_platform_map": {
                "SNOWFLAKE_PROD": {
                    "platform": "snowflake",
                    "platform_instance": "snow_acct",
                },
            },
        }
    )
    ctx = PipelineContext(run_id="test-federated-storage-platform")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[{"name": "SNOWFLAKE_PROD", "typeId": "SNOWFLAKE"}],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "FED_SNOWFLAKE_CUST",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/FED_SNOWFLAKE_CUST",
        json={
            "definitions": {
                "FED_SNOWFLAKE_CUST": {
                    "kind": "entity",
                    "@remote.source": "SNOWFLAKE_PROD",
                    "@DataWarehouse.external.schema": "PUBLIC",
                }
            }
        },
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if "urn:li:dataset:" in (entity_urn_of(wu) or "")
    }
    assert len(dataset_urns) >= 1, (
        f"Expected at least one dataset URN; got none. Workunits: "
        f"{[type(aspect_of(wu)).__name__ for wu in workunits]}"
    )
    fed_urns = [u for u in dataset_urns if "fed_snowflake_cust" in u]
    assert fed_urns, f"Expected dataset URN for FED_SNOWFLAKE_CUST; got: {dataset_urns}"
    for urn in fed_urns:
        assert "urn:li:dataPlatform:snowflake" in urn, (
            f"Federated asset must be emitted on snowflake; got: {urn}"
        )
        assert "urn:li:dataPlatform:sap-datasphere" not in urn, (
            f"Federated asset must NOT be on sap-datasphere; got: {urn}"
        )
        assert "snow_acct" in urn, (
            f"Federated asset must use the federated connection's "
            f"platform_instance 'snow_acct'; got: {urn}"
        )


_EDMX_WITH_UNKNOWN_TYPE = """<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="ns">
      <EntityType Name="WEIRD">
        <Key><PropertyRef Name="K"/></Key>
        <Property Name="K" Type="Edm.String" Nullable="false"/>
        <Property Name="BLOB_COL" Type="Edm.Stream"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_unknown_edm_type_surfaces_to_report(requests_mock):
    """When EDMX parsing encounters an Edm.* type that the connector doesn't
    understand, the dataset is still emitted (with NullType for that column) and
    the report records the asset + a structured warning so operators can identify
    which assets need type-map extensions."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-unknown-edm-type")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "WEIRD",
                    "label": "Weird Asset",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/WEIRD/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/WEIRD/$metadata",
        text=_EDMX_WITH_UNKNOWN_TYPE,
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    # Dataset is still emitted despite the unknown EDMX type.
    assert any("weird" in u for u in dataset_urns), (
        f"Expected WEIRD dataset to be emitted; got: {dataset_urns}"
    )

    warning_titles = [w.title or "" for w in source.report.warnings]
    assert any("Unknown EDMX field type" in t for t in warning_titles), (
        f"Expected an 'Unknown EDMX field type(s)' warning; got: {warning_titles}"
    )
    assert "S1.WEIRD" in list(source.report.assets_with_unknown_edm_types)


def test_lineage_extraction_failure_emits_dataset_without_upstream(
    requests_mock, monkeypatch
):
    """When ``extract_upstream_refs`` raises, the dataset must still be emitted
    (without upstreamLineage) and the report must surface a structured warning."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_lineage": True,
        }
    )
    ctx = PipelineContext(run_id="test-lineage-failure")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/dwaas-core/api/v1/spaces/S1/views/DIM_DAY",
        json={
            "definitions": {
                "DIM_DAY": {
                    "kind": "entity",
                    "query": {"SELECT": {"from": {"ref": ["UPSTREAM_TABLE"]}}},
                }
            }
        },
    )

    source = SapDatasphereSource(ctx, cfg)

    def _boom(_csn_def):
        raise RuntimeError("simulated CSN walk failure")

    monkeypatch.setattr(source._lineage_extractor, "extract_upstream_refs", _boom)

    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert any("dim_day" in u for u in dataset_urns), (
        f"Dataset should still be emitted even when lineage extraction fails; "
        f"got: {dataset_urns}"
    )

    # Ensure no UpstreamLineage workunit was emitted for the asset.
    upstream_lineage_workunits = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "UpstreamLineageClass"
    ]
    assert upstream_lineage_workunits == [], (
        "No UpstreamLineageClass should be emitted when the extractor raises"
    )

    warning_titles = [w.title or "" for w in source.report.warnings]
    assert any("Failed to extract CSN lineage" in t for t in warning_titles), (
        f"Expected lineage failure warning; got: {warning_titles}"
    )


# --- FIX 1: max_workers_assets ---------------------------------------------


def _make_multi_asset_mocks(requests_mock: rm.Mocker, n_assets: int) -> None:
    """Wire up requests_mock for a single space ``S1`` with ``n_assets`` distinct
    assets, each with no metadata URL (so no EDMX fetch needed)."""
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": f"ASSET_{i:03d}",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
                for i in range(n_assets)
            ]
        },
    )


def test_max_workers_assets_serial_path_unchanged(requests_mock):
    """With ``max_workers_assets=1`` the serial loop should emit the same URNs as
    the existing (pre-threading) tests do."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "max_workers_assets": 1,
        }
    )
    ctx = PipelineContext(run_id="test-serial")
    _make_multi_asset_mocks(requests_mock, n_assets=5)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) == 5
    for i in range(5):
        assert any(f"s1.asset_{i:03d}" in u for u in dataset_urns), (
            f"Expected ASSET_{i:03d} URN; got: {dataset_urns}"
        )


def test_max_workers_assets_parallel_path_emits_same_workunits(requests_mock):
    """With ``max_workers_assets=4`` the set of emitted URNs must match the
    serial path; ordering may differ."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "max_workers_assets": 4,
        }
    )
    ctx = PipelineContext(run_id="test-parallel")
    _make_multi_asset_mocks(requests_mock, n_assets=12)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) == 12
    for i in range(12):
        assert any(f"s1.asset_{i:03d}" in u for u in dataset_urns), (
            f"Expected ASSET_{i:03d} URN; got: {dataset_urns}"
        )


# --- asset_batch_size chunking ---------------------------------------------


def test_chunked_helper_exact_and_remainder_sizes():
    """``_chunked`` yields successive lists of up to ``size`` items, lazily —
    covering both exact multiples and a trailing partial (remainder) chunk."""

    # `_chunked` is element-type agnostic; integers exercise the chunk boundaries
    # most readably even though the source signature annotates Iterable[Dict].
    def chunk(items: Iterable[object], size: int) -> List[List[object]]:
        return cast(
            List[List[object]], list(_chunked(cast(Iterable[Dict], items), size))
        )

    # Remainder cases: final chunk is shorter than `size` (the classic off-by-one).
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunk([1, 2, 3, 4, 5, 6, 7], 3) == [[1, 2, 3], [4, 5, 6], [7]]
    # Exact multiple: no trailing partial chunk.
    assert chunk([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]
    assert chunk([1], 5) == [[1]]
    # Empty input -> no chunks at all.
    assert chunk([], 3) == []
    # Works on an exhaustible iterator (generator), not just a list.
    assert chunk((x for x in range(5)), 2) == [[0, 1], [2, 3], [4]]


def test_asset_batching_emits_all_assets(requests_mock):
    """With ``asset_batch_size`` smaller than the number of assets in a space,
    every asset must still be emitted exactly once (chunking must not drop or
    duplicate)."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "max_workers_assets": 4,
            "asset_batch_size": 2,
        }
    )
    ctx = PipelineContext(run_id="test-batch-all")
    _make_multi_asset_mocks(requests_mock, n_assets=5)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert len(dataset_urns) == 5
    for i in range(5):
        assert any(f"s1.asset_{i:03d}" in u for u in dataset_urns), (
            f"Expected ASSET_{i:03d} URN; got: {dataset_urns}"
        )


def test_asset_batching_processes_in_chunks(requests_mock, monkeypatch):
    """``ThreadedIteratorExecutor.process`` must be invoked ``ceil(n/batch)``
    times — proving the asset stream is fed to the executor in bounded chunks
    rather than all at once (5 assets, batch_size=2 -> 3 invocations)."""

    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "max_workers_assets": 4,
            "asset_batch_size": 2,
        }
    )
    ctx = PipelineContext(run_id="test-batch-chunks")
    _make_multi_asset_mocks(requests_mock, n_assets=5)

    real_process = source_module.ThreadedIteratorExecutor.process
    call_args_list = []

    def _spy_process(*, worker_func, args_list, max_workers):
        # Materialize args_list so we can both count the chunk and forward it.
        chunk = list(args_list)
        call_args_list.append(chunk)
        yield from real_process(
            worker_func=worker_func,
            args_list=iter(chunk),
            max_workers=max_workers,
        )

    monkeypatch.setattr(
        source_module.ThreadedIteratorExecutor, "process", staticmethod(_spy_process)
    )

    source = SapDatasphereSource(ctx, cfg)
    list(source.get_workunits())

    # ceil(5 / 2) == 3 invocations, with chunk sizes 2, 2, 1.
    assert len(call_args_list) == 3, (
        f"Expected 3 executor invocations for 5 assets at batch_size=2; "
        f"got {len(call_args_list)} (sizes {[len(c) for c in call_args_list]})"
    )
    assert sorted(len(c) for c in call_args_list) == [1, 2, 2]


# --- FIX 3: TestableSource.test_connection ---------------------------------


def test_test_connection_succeeds_with_valid_credentials(requests_mock):
    """When credentials work and spaces endpoint returns 200, both
    ``basic_connectivity`` and the CONTAINERS capability should be capable."""

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )

    report = SapDatasphereSource.test_connection(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    containers_cap = report.capability_report.get(SourceCapability.CONTAINERS)
    assert containers_cap is not None and containers_cap.capable is True


def test_test_connection_fails_on_auth_error(requests_mock):
    """When XSUAA returns 401 to the token endpoint, ``basic_connectivity``
    must surface ``capable=False`` with an auth-related ``failure_reason``."""
    requests_mock.post(
        "https://myco.authentication.eu10.hana.ondemand.com/oauth/token",
        status_code=401,
        json={"error": "unauthorized"},
    )

    report = SapDatasphereSource.test_connection(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "client_id": "cid",
            "client_secret": "csec",
            "xsuaa_url": "https://myco.authentication.eu10.hana.ondemand.com",
        }
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert report.basic_connectivity.failure_reason is not None
    assert "auth" in report.basic_connectivity.failure_reason.lower()


# --- FIX 4: scaling-ceiling warning ----------------------------------------


def test_scale_warning_emitted_above_threshold(requests_mock):
    """Crossing the 50K-emitted-datasets threshold with stateful_ingestion
    enabled should fire a single ``Approaching stateful-ingestion scaling
    ceiling`` warning."""
    # Construct with stateful disabled (so we don't need a real state provider),
    # then flip the enabled flag on the loaded config object so the warning
    # condition is satisfied.
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "stateful_ingestion": {"enabled": False},
        }
    )
    ctx = PipelineContext(run_id="test-scale-warning")
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )

    source = SapDatasphereSource(ctx, cfg)
    # Flip the flag now that the source is initialized — _check_scale_warning
    # reads it dynamically.
    assert source.config.stateful_ingestion is not None
    source.config.stateful_ingestion.enabled = True

    # Pre-threshold — no warning yet.
    source._datasets_emitted = 49999
    source._check_scale_warning()
    titles_before = [w.title or "" for w in source.report.warnings]
    assert not any("scaling ceiling" in t for t in titles_before), (
        f"Premature warning: {titles_before}"
    )

    # Cross the threshold — warning fires exactly once.
    source._datasets_emitted = 50000
    source._check_scale_warning()
    source._datasets_emitted = 50001
    source._check_scale_warning()  # idempotent — should NOT emit a second time.

    titles_after = [w.title or "" for w in source.report.warnings]
    scale_titles = [t for t in titles_after if "scaling ceiling" in t]
    assert len(scale_titles) == 1, (
        f"Expected exactly one scaling-ceiling warning, got: {scale_titles}"
    )


def test_scale_warning_not_emitted_when_stateful_ingestion_disabled(requests_mock):
    """If stateful_ingestion isn't enabled the warning must not fire — operators
    using non-stateful ingestion don't hit the checkpoint payload ceiling."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-scale-no-stateful")
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )

    source = SapDatasphereSource(ctx, cfg)
    source._datasets_emitted = 100_000
    source._check_scale_warning()

    titles = [w.title or "" for w in source.report.warnings]
    assert not any("scaling ceiling" in t for t in titles), (
        f"Warning should not fire without stateful_ingestion enabled: {titles}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# M4: column_pattern filter
# ─────────────────────────────────────────────────────────────────────────────


def test_column_pattern_filters_columns_and_increments_report(requests_mock):
    """A column matching column_pattern.deny must NOT appear in SchemaFieldClass
    output and report.columns_filtered must increment by the dropped count."""
    fixture_xml = (
        Path(__file__).parent / "fixtures" / "sap_datasphere_dimension_day.xml"
    ).read_text()

    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "column_pattern": {"deny": ["^DATE_SQL$"]},
        }
    )
    ctx = PipelineContext(run_id="test-column-pattern")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/DIM_DAY/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/DIM_DAY/$metadata",
        text=fixture_xml,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    schema_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SchemaMetadataClass"
    )
    field_names = {
        f.fieldPath for f in aspect_as(schema_wu, SchemaMetadataClass).fields
    }
    assert "DATE_SQL" not in field_names, (
        f"DATE_SQL should be filtered by column_pattern deny; got: {field_names}"
    )
    assert source.report.columns_filtered == 1, (
        f"Expected exactly 1 filtered column; got: {source.report.columns_filtered}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# M5: built-in defaults warning for S3/GCS without platform_instance
# ─────────────────────────────────────────────────────────────────────────────


def test_s3_builtin_default_without_instance_warns_once(requests_mock):
    """When multiple assets route through the built-in S3 default with no
    platform_instance, the warning must fire exactly once per platform."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
        }
    )
    ctx = PipelineContext(run_id="test-builtin-warning")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": f"ASSET_{i}",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
                for i in range(3)
            ]
        },
    )
    # All three assets route through an S3 connection that triggers the S3 built-in default
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[
            {"name": "MY_S3_CONN", "typeId": "S3"},
        ],
    )

    source = SapDatasphereSource(ctx, cfg)
    # Force each asset's connection to MY_S3_CONN by injecting CSN-like routing
    # Since we don't fetch CSN here (include_lineage=False), all assets go
    # to the _managed (hana) path. To exercise the S3 builtin path, simulate the
    # warning helper directly with a ResolvedPlatform.

    resolved_s3 = ResolvedPlatform(
        platform="s3",
        platform_instance=None,
        env="PROD",
    )
    for _ in range(5):
        source._maybe_warn_builtin_defaults_missing_instance(resolved_s3)

    s3_warnings = [
        w for w in source.report.warnings if "S3 default mapping" in (w.title or "")
    ]
    assert len(s3_warnings) == 1, (
        f"Expected exactly one S3 built-in defaults warning; got {len(s3_warnings)}: "
        f"{[w.title for w in source.report.warnings]}"
    )


def test_s3_builtin_default_with_instance_does_not_warn():
    """If platform_instance is set on the S3 default, no warning is emitted."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-builtin-warning-suppressed")
    source = SapDatasphereSource(ctx, cfg)

    resolved = ResolvedPlatform(
        platform="s3",
        platform_instance="my_s3_account",
        env="PROD",
    )
    source._maybe_warn_builtin_defaults_missing_instance(resolved)
    titles = [w.title or "" for w in source.report.warnings]
    assert not any("S3" in t for t in titles), (
        f"Expected no S3 warning when platform_instance is set; got: {titles}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2-tier containers (Space → Object): no synthetic folder layer
# ─────────────────────────────────────────────────────────────────────────────


def _mixed_case_space_and_assets(requests_mock: rm.Mocker) -> None:
    """Wire up a single mixed-case space with a View and an Analytical Model.

    Used by the 2-tier / lowercase tests below. Asset names use SAP's dotted
    technical-name convention with mixed case so we can assert URN lowercasing
    independently of display/property case preservation.
    """
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "MySpace", "label": "My Space"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('MySpace')/assets",
        json={
            "value": [
                {
                    "name": "SAP.TIME.VIEW_DIMENSION_DAY",
                    "label": "Day Dimension",
                    "spaceName": "MySpace",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                },
                {
                    "name": "SAP.TIME.M_TIME_DIMENSION",
                    "label": "Time Model",
                    "spaceName": "MySpace",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": True,
                    "hasParameters": False,
                },
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/MySpace/connections",
        json=[],
    )


def test_datasets_parent_directly_to_space_no_folders(requests_mock):
    """Option B: datasets are parented directly to the Space container; no
    folder containers are emitted."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-2tier")
    _mixed_case_space_and_assets(requests_mock)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # The Space container (subtype "Space") must be emitted.
    space_container_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SubTypesClass"
        and "urn:li:container:" in (entity_urn_of(wu) or "")
        and "Space" in aspect_as(wu, SubTypesClass).typeNames
    }
    assert len(space_container_urns) == 1, (
        f"Expected exactly 1 Space container; got: {space_container_urns}"
    )

    # NO container may carry the "Folder" subtype.
    folder_subtype_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SubTypesClass"
        and "Folder" in aspect_as(wu, SubTypesClass).typeNames
    }
    assert not folder_subtype_urns, (
        f"Expected no Folder-subtype containers; got: {folder_subtype_urns}"
    )

    # Each dataset's container aspect must point at the Space container.
    dataset_parents = {
        entity_urn_of(wu): aspect_as(wu, ContainerClass).container
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "ContainerClass"
        and "urn:li:dataset:" in (entity_urn_of(wu) or "")
    }
    assert dataset_parents, "Expected ContainerClass aspects on datasets"
    space_urn = next(iter(space_container_urns))
    for dataset_urn, parent in dataset_parents.items():
        assert parent == space_urn, (
            f"Dataset {dataset_urn} parented to {parent}; "
            f"expected the Space container {space_urn}"
        )


def test_urns_lowercased_by_default_display_preserves_case(requests_mock):
    """convert_urns_to_lowercase defaults True. The dataset URN is lowercased,
    but display_name and customProperties keep the original SAP case."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    assert cfg.convert_urns_to_lowercase is True
    ctx = PipelineContext(run_id="test-lower-default")
    _mixed_case_space_and_assets(requests_mock)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if (entity_urn_of(wu) or "").startswith("urn:li:dataset:")
    }
    # The View dataset URN name segment must be fully lowercase.
    assert any("sap.time.view_dimension_day" in urn for urn in dataset_urns), (
        f"Expected a lowercased dataset URN name segment; got: {dataset_urns}"
    )
    # And NOT present in original mixed case anywhere in a URN.
    assert not any("SAP.TIME.VIEW_DIMENSION_DAY" in urn for urn in dataset_urns), (
        f"URN name segments must be lowercased; got: {dataset_urns}"
    )

    # datasetProperties.name (display) must retain original case.
    display_names = {
        aspect_as(wu, DatasetPropertiesClass).name
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "DatasetPropertiesClass"
    }
    assert "Day Dimension" in display_names, (
        f"Expected display name to retain original case; got: {display_names}"
    )

    # customProperties sap_datasphere_asset must retain original SAP case.
    custom_props = [
        aspect_as(wu, DatasetPropertiesClass).customProperties
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "DatasetPropertiesClass"
    ]
    asset_props = {
        cp.get("sap_datasphere_asset") for cp in custom_props if cp is not None
    }
    assert "SAP.TIME.VIEW_DIMENSION_DAY" in asset_props, (
        f"Expected customProperties to retain original SAP case; got: {asset_props}"
    )


def test_convert_urns_to_lowercase_false_keeps_case(requests_mock):
    """When explicitly set False, URNs keep original case."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "convert_urns_to_lowercase": False,
        }
    )
    assert cfg.convert_urns_to_lowercase is False
    ctx = PipelineContext(run_id="test-lower-false")
    _mixed_case_space_and_assets(requests_mock)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if (entity_urn_of(wu) or "").startswith("urn:li:dataset:")
    }
    assert any("SAP.TIME.VIEW_DIMENSION_DAY" in urn for urn in dataset_urns), (
        f"Expected original-case dataset URN when lowercasing disabled; got: {dataset_urns}"
    )


def test_report_has_column_lineage_unresolved_counter():
    """The report should expose a LossyList for column-lineage refs we couldn't resolve."""
    report = SapDatasphereReport()
    assert isinstance(report.column_lineage_unresolved, LossyList)
    report.column_lineage_unresolved.append("S1.VIEW.col1 -> MISSING_TABLE.x")
    assert len(report.column_lineage_unresolved) == 1


def test_include_lineage_emits_fine_grained_column_lineage(requests_mock):
    """When include_lineage=True and the asset's CSN has structured columns,
    the emitted UpstreamLineageClass aspect MUST include `fineGrainedLineages`
    with one entry per resolvable downstream column."""
    tenant = "https://t.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "MID_VIEW",
                    "label": "MID_VIEW",
                    "metadataUrl": f"{tenant}/edmx/S1/MID_VIEW/$metadata",
                    "supportsAnalyticalQueries": False,
                }
            ]
        },
    )
    requests_mock.get(
        f"{tenant}/edmx/S1/MID_VIEW/$metadata",
        text="""<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="S1">
      <EntityType Name="MID_VIEW">
        <Key><PropertyRef Name="ID"/></Key>
        <Property Name="ID" Type="Edm.Int64" Nullable="false"/>
        <Property Name="total" Type="Edm.Decimal" Nullable="true"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>""",
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/views/MID_VIEW",
        json={
            "definitions": {
                "MID_VIEW": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_TABLE"]},
                            "columns": [
                                {"ref": ["ID"]},
                                {
                                    "func": "SUM",
                                    "args": [{"ref": ["AMOUNT"]}],
                                    "as": "total",
                                },
                            ],
                        }
                    },
                }
            }
        },
    )

    config = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        include_lineage=True,
        env="PROD",
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    workunits = list(source.get_workunits())

    upstream_aspects = [
        a for wu in workunits if isinstance((a := aspect_of(wu)), UpstreamLineageClass)
    ]
    assert len(upstream_aspects) == 1, (
        f"Expected one UpstreamLineageClass aspect, got {len(upstream_aspects)}"
    )
    aspect = upstream_aspects[0]
    assert aspect.fineGrainedLineages, "Missing fineGrainedLineages on UpstreamLineage"
    assert len(aspect.fineGrainedLineages) == 2, (
        f"Expected 2 column pairs (ID, total), got {len(aspect.fineGrainedLineages)}"
    )
    # The "total" pair carries the AGGREGATE transformOperation
    total_pair = [
        fg
        for fg in aspect.fineGrainedLineages
        if any("total" in d for d in (fg.downstreams or []))
    ][0]
    assert total_pair.transformOperation == "AGGREGATE"
    assert total_pair.upstreams is not None
    assert total_pair.downstreams is not None
    assert len(total_pair.upstreams) == 1
    assert "urn:li:dataPlatform:sap-datasphere" in total_pair.upstreams[0]
    assert "urn:li:dataPlatform:sap-datasphere" in total_pair.downstreams[0]
    assert "urn:li:schemaField:" in total_pair.upstreams[0]
    assert ",AMOUNT)" in total_pair.upstreams[0]
    assert ",total)" in total_pair.downstreams[0]


def test_lineage_fine_capability_decorator_present():
    """The source declares LINEAGE_FINE capability so DataHub UI shows the column-lineage badge."""

    # get_capabilities is attached dynamically by the @capability decorator,
    # so it isn't visible to static type checkers on the class. Use a non-literal
    # attribute name so mypy doesn't resolve it to a (missing) static attribute.
    get_caps_attr = "get_capabilities"
    caps = getattr(SapDatasphereSource, get_caps_attr)()
    assert caps, "Source must have capabilities declared via @capability"
    capability_names = {c.capability for c in caps}
    assert SourceCapability.LINEAGE_FINE in capability_names, (
        "LINEAGE_FINE capability missing from SapDatasphereSource"
    )


def test_unresolved_column_refs_populate_report_counter(requests_mock):
    """When the CSN walker can't resolve a column ref (e.g. 3-segment ref),
    the source surfaces it via `report.column_lineage_unresolved` so operators
    can debug silent drops rather than being blind to them."""
    tenant = "https://t.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "MID_VIEW",
                    "label": "MID_VIEW",
                    "metadataUrl": f"{tenant}/edmx/S1/MID_VIEW/$metadata",
                    "supportsAnalyticalQueries": False,
                }
            ]
        },
    )
    requests_mock.get(
        f"{tenant}/edmx/S1/MID_VIEW/$metadata",
        text="""<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="S1">
      <EntityType Name="MID_VIEW">
        <Key><PropertyRef Name="ID"/></Key>
        <Property Name="ID" Type="Edm.Int64" Nullable="false"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>""",
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    # CSN with a 3-segment ref — unresolvable by the walker.
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/views/MID_VIEW",
        json={
            "definitions": {
                "MID_VIEW": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_TABLE"]},
                            "columns": [
                                {
                                    "ref": ["DB", "SCHEMA", "TABLE"],
                                    "as": "bad",
                                }
                            ],
                        }
                    },
                }
            }
        },
    )

    config = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        include_lineage=True,
        env="PROD",
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    list(source.get_workunits())

    assert len(source.report.column_lineage_unresolved) >= 1, (
        f"Expected report.column_lineage_unresolved to record the 3-segment ref; "
        f"got: {list(source.report.column_lineage_unresolved)}"
    )
    # Each entry must include the full downstream dataset URN so the same
    # entry from different runs/spaces/connectors is globally addressable.
    entries = list(source.report.column_lineage_unresolved)
    assert all("urn:li:dataset:" in entry for entry in entries), (
        f"Expected each entry to contain the downstream dataset URN; got: {entries}"
    )


def test_malformed_csn_populates_column_lineage_unresolved_counter(requests_mock):
    """When the per-object-type endpoint returns CSN with a structurally broken
    ``query`` field (string instead of dict), the dataset still emits but the
    ``<malformed>`` marker reaches ``report.column_lineage_unresolved`` so
    operators can distinguish a corrupt CSN from a legitimate base table."""
    tenant = "https://t.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "BROKEN_VIEW",
                    "label": "BROKEN_VIEW",
                    "metadataUrl": f"{tenant}/edmx/S1/BROKEN_VIEW/$metadata",
                    "supportsAnalyticalQueries": False,
                }
            ]
        },
    )
    requests_mock.get(
        f"{tenant}/edmx/S1/BROKEN_VIEW/$metadata",
        text="""<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="S1">
      <EntityType Name="BROKEN_VIEW">
        <Key><PropertyRef Name="ID"/></Key>
        <Property Name="ID" Type="Edm.Int64" Nullable="false"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>""",
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    # CSN with a structurally broken ``query`` field (string, not dict).
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/views/BROKEN_VIEW",
        json={
            "definitions": {
                "BROKEN_VIEW": {
                    "kind": "entity",
                    "query": "this-is-not-a-dict",
                }
            }
        },
    )

    config = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        include_lineage=True,
        env="PROD",
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    workunits = list(source.get_workunits())

    # The dataset still emits despite the malformed CSN.
    dataset_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if entity_urn_of(wu) and "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert any("broken_view" in u for u in dataset_urns), (
        f"Dataset should still emit even when CSN is malformed; got URNs: {dataset_urns}"
    )

    # And the malformed marker reaches the report counter.
    unresolved_entries = list(source.report.column_lineage_unresolved)
    assert any("<malformed>" in entry for entry in unresolved_entries), (
        f"Expected <malformed> marker in column_lineage_unresolved. "
        f"Got: {unresolved_entries}"
    )


def test_lineage_aspect_assembly_failure_emits_dataset_without_lineage(
    requests_mock, monkeypatch
):
    """If non-walker steps in lineage aspect assembly raise (URN construction,
    _build_upstream_lineage, etc.), the dataset must still emit with a warning —
    a single bad asset must not crash the per-asset emit path."""
    tenant = "https://t.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "MID_VIEW",
                    "label": "MID_VIEW",
                    "metadataUrl": None,
                    "supportsAnalyticalQueries": False,
                }
            ]
        },
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/views/MID_VIEW",
        json={
            "definitions": {
                "MID_VIEW": {
                    "kind": "entity",
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["BASE_TABLE"]},
                            "columns": [{"ref": ["ID"]}],
                        }
                    },
                }
            }
        },
    )

    config = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        include_lineage=True,
        env="PROD",
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)

    # Make _build_upstream_lineage raise — simulating a non-walker failure
    # (e.g. URN-construction blowing up on a weird platform value).
    def _boom(*args, **kwargs):
        raise RuntimeError("simulated assembly failure")

    monkeypatch.setattr(source, "_build_upstream_lineage", _boom)

    workunits = list(source.get_workunits())

    # Dataset still emitted.
    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert any("mid_view" in u for u in dataset_urns), (
        f"Dataset should still be emitted even when assembly fails; got: {dataset_urns}"
    )

    # No UpstreamLineage workunit was emitted.
    upstream_lineage_workunits = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "UpstreamLineageClass"
    ]
    assert upstream_lineage_workunits == [], (
        "No UpstreamLineageClass should be emitted when assembly raises"
    )

    # Warning surfaced via the report.
    warning_titles = [w.title or "" for w in source.report.warnings]
    assert any("Failed to build lineage aspect" in t for t in warning_titles), (
        f"Expected assembly failure warning; got: {warning_titles}"
    )


def test_csn_fetch_500_emits_dataset_without_lineage_and_warns(requests_mock):
    """When the per-object-type CSN endpoint returns 500 with
    include_lineage=True, the dataset is still emitted (without UpstreamLineage),
    the failure is recorded in report.assets_csn_fetch_failed (as documented in
    the config docstring), and a structured warning is surfaced."""
    tenant = "https://t.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "MID_VIEW",
                    "label": "MID_VIEW",
                    "metadataUrl": None,
                    "supportsAnalyticalQueries": False,
                }
            ]
        },
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    # The CSN endpoint blows up with a 500.
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/views/MID_VIEW",
        status_code=500,
        text="internal server error",
    )

    config = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        include_lineage=True,
        env="PROD",
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    workunits = list(source.get_workunits())

    # Dataset still emitted.
    dataset_urns = {
        entity_urn_of(wu) for wu in workunits if "urn:li:dataset:" in entity_urn_of(wu)
    }
    assert any("mid_view" in u for u in dataset_urns), (
        f"Dataset should still be emitted when CSN fetch fails; got: {dataset_urns}"
    )

    # No UpstreamLineage emitted (no CSN means no lineage to extract).
    upstream_lineage_workunits = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "UpstreamLineageClass"
    ]
    assert upstream_lineage_workunits == [], (
        "No UpstreamLineageClass should be emitted when CSN fetch returns 500"
    )

    # report.assets_csn_fetch_failed populated (as documented in config.py:239).
    assert "S1.MID_VIEW" in list(source.report.assets_csn_fetch_failed), (
        f"Expected S1.MID_VIEW in assets_csn_fetch_failed; "
        f"got: {list(source.report.assets_csn_fetch_failed)}"
    )

    # Structured warning surfaced.
    warning_titles = [w.title or "" for w in source.report.warnings]
    assert any("Failed to fetch object definition" in t for t in warning_titles), (
        f"Expected object-definition fetch warning; got: {warning_titles}"
    )


def test_build_fine_grained_lineages_reports_wildcard_missing_qname_and_unresolved(
    requests_mock: rm.Mocker,
) -> None:
    """Direct unit test for ``_build_fine_grained_lineages``: feed a hand-built
    ``ColumnLineageContext`` with mixed pairs and assert which entries land in
    the report counter."""

    # Minimal config + source setup (no HTTP requests needed for this test).
    config = SapDatasphereConfig(
        base_url="https://test.eu10.hcs.cloud.sap",
        token="t",
        env="PROD",
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)

    upstream_urn_by_name = {
        "BASE": "urn:li:dataset:(urn:li:dataPlatform:hana,SCHEMA.BASE,PROD)",
    }

    column_lineage = ColumnLineageContext(
        pairs=(
            # Normal pair — should produce FineGrainedLineage entry
            ColumnLineagePair(
                downstream_col="id",
                upstream_refs=(UpstreamColRef("BASE", "ID"),),
                transform_op="IDENTITY",
            ),
            # Wildcard upstream — should report, no entry
            ColumnLineagePair(
                downstream_col="all_data",
                upstream_refs=(UpstreamColRef("BASE", "*"),),
                transform_op="IDENTITY",
            ),
            # Missing upstream qname — should report, no entry
            ColumnLineagePair(
                downstream_col="orphan",
                upstream_refs=(UpstreamColRef("UNKNOWN_TABLE", "x"),),
                transform_op="IDENTITY",
            ),
            # Walker-unresolved — should report, no entry
            ColumnLineagePair(
                downstream_col="weird",
                upstream_refs=(),
                unresolved_refs=("<some walker diagnostic>",),
            ),
        ),
        downstream_dataset_urn=(
            "urn:li:dataset:(urn:li:dataPlatform:hana,SCHEMA.VIEW,PROD)"
        ),
    )

    fine_grained = source._build_fine_grained_lineages(
        column_lineage, upstream_urn_by_name
    )

    # Only the normal pair survives
    assert len(fine_grained) == 1
    assert fine_grained[0].downstreams is not None
    assert fine_grained[0].downstreams[0].endswith("id)")

    # Three diagnostic entries in the report
    entries = list(source.report.column_lineage_unresolved)
    assert len(entries) == 3, f"Expected 3 entries; got {len(entries)}: {entries}"
    assert any("all_data" in e and "wildcard" in e.lower() for e in entries), entries
    assert any("orphan" in e and "missing upstream" in e.lower() for e in entries), (
        entries
    )
    assert any("weird" in e and "some walker diagnostic" in e for e in entries), entries


def test_include_lineage_does_not_emit_kba_warning():
    """After PR-3 migrated lineage extraction off the internal /deepsea/
    endpoint and onto the supported /dwaas-core/ per-object-type API, the
    runtime KBA #3517441 disclosure warning must NOT fire — lineage is now a
    fully supported feature."""
    config = SapDatasphereConfig(
        base_url="https://test.eu10.hcs.cloud.sap",
        token="t",
        env="PROD",
        include_lineage=True,
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    assert not any(
        "3517441" in str(w.context or "") or "3517441" in str(w.message or "")
        for w in source.report.warnings
    ), (
        f"include_lineage=True should not emit the KBA warning after PR-3; "
        f"got warnings: {[(w.title, w.context) for w in source.report.warnings]}"
    )


def test_include_local_tables_emits_dataset_stubs(requests_mock):
    """End-to-end: include_local_tables=True discovers Local Tables via the
    supported /dwaas-core/ endpoint and emits Dataset stubs under a 'Local
    Tables' folder, with managed-asset URNs under the sap-datasphere platform."""
    tenant = "https://test.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={"value": []},  # No exposed views in this test
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/localtables",
        json=[
            {"technicalName": "SAP.TIME.M_TIME_DIMENSION"},
            {"technicalName": "BASE_FACTS"},
        ],
    )
    # Per-table CSN fetch — column metadata for the schema= aspect on stubs.
    for lt_name in ("SAP.TIME.M_TIME_DIMENSION", "BASE_FACTS"):
        requests_mock.get(
            f"{tenant}/dwaas-core/api/v1/spaces/S1/localtables/{lt_name}",
            json={
                "definitions": {
                    lt_name: {
                        "kind": "entity",
                        "elements": {
                            "COL_A": {"type": "cds.String", "length": 10},
                            "COL_B": {"type": "cds.hana.TINYINT"},
                        },
                    }
                }
            },
        )
    config = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        env="PROD",
        platform_instance="test_tenant",
        include_local_tables=True,
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    workunits = list(source.get_workunits())

    dataset_urns = {
        entity_urn_of(wu)
        for wu in workunits
        if (entity_urn_of(wu) or "").startswith("urn:li:dataset:")
    }
    assert any(
        "m_time_dimension" in u and "sap-datasphere" in u for u in dataset_urns
    ), (
        f"Expected sap-datasphere-platform Dataset URN for M_TIME_DIMENSION; got: {dataset_urns}"
    )
    assert any("base_facts" in u and "sap-datasphere" in u for u in dataset_urns), (
        f"Expected sap-datasphere-platform Dataset URN for BASE_FACTS; got: {dataset_urns}"
    )
    assert source.report.local_tables_emitted == 2

    # Local Tables now carry schemaMetadata from the per-table CSN fetch — this
    # is what lets DataHub render column-level lineage edges between Views and
    # the Local Tables they reference (see Task B follow-up).
    schema_aspects = [
        wu.metadata
        for wu in workunits
        if (entity_urn_of(wu) or "").startswith("urn:li:dataset:")
        and getattr(getattr(wu.metadata, "aspect", None), "ASPECT_NAME", None)
        == "schemaMetadata"
    ]
    assert len(schema_aspects) >= 2, (
        f"Expected ≥2 schemaMetadata aspects on local-table stubs; got {len(schema_aspects)}"
    )


def test_include_local_tables_off_by_default(requests_mock):
    """Default behavior is unchanged — no Local Tables endpoint hit."""
    tenant = "https://test.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": []},
    )
    config = SapDatasphereConfig(base_url=tenant, token="t", env="PROD")
    source = SapDatasphereSource(PipelineContext(run_id="t"), config)
    list(source.get_workunits())
    # If the localtables endpoint were called, requests_mock would raise
    # NoMockAddress — silent pass means the endpoint wasn't hit.
    assert source.report.local_tables_emitted == 0


# ─────────────────────────────────────────────────────────────────────────────
# Tag emission for SAP CDS semantic annotations
# ─────────────────────────────────────────────────────────────────────────────


_EDMX_FOR_TAGS = """<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"
            xmlns="http://docs.oasis-open.org/odata/ns/edm" Version="4.0">
  <edmx:DataServices>
    <Schema Namespace="ns">
      <EntityType Name="REVENUE">
        <Key><PropertyRef Name="ID"/></Key>
        <Property Name="ID" Type="Edm.Int32" Nullable="false"/>
        <Property Name="YEAR_VAL" Type="Edm.Int32"/>
        <Property Name="AMOUNT_USD" Type="Edm.Decimal" Precision="18" Scale="2"/>
        <Property Name="UNIT_CODE" Type="Edm.String"/>
      </EntityType>
      <Annotations Target="ns.REVENUE">
        <Annotation Term="Common.Label" String="Revenue"/>
        <Annotation Term="Analytics.Measure"/>
        <Annotation Term="Analytics.dimensionType" EnumMember="Analytics.DimensionType/Time"/>
      </Annotations>
      <Annotations Target="ns.REVENUE/YEAR_VAL">
        <Annotation Term="Common.IsCalendarYear"/>
      </Annotations>
      <Annotations Target="ns.REVENUE/AMOUNT_USD">
        <Annotation Term="Common.IsCurrency"/>
      </Annotations>
      <Annotations Target="ns.REVENUE/UNIT_CODE">
        <Annotation Term="Common.IsUnit"/>
      </Annotations>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _wire_revenue_asset(requests_mock: rm.Mocker) -> None:
    """Mock endpoints for a single REVENUE asset with the EDMX above."""
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "REVENUE",
                    "label": "Revenue",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/REVENUE/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/REVENUE/$metadata",
        text=_EDMX_FOR_TAGS,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )


def test_emit_sap_semantics_as_tags_default_true_emits_field_tags(requests_mock):
    """Field-level CDS semantic annotations surface as DataHub tags on
    ``SchemaField.globalTags``, in addition to the existing custom-property
    description suffix."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-field-tags")
    _wire_revenue_asset(requests_mock)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    schema_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SchemaMetadataClass"
    )
    fields_by_name = {
        f.fieldPath: f for f in aspect_as(schema_wu, SchemaMetadataClass).fields
    }

    year = fields_by_name["YEAR_VAL"]
    assert year.globalTags is not None, "YEAR_VAL should have a globalTags aspect"
    assert {t.tag for t in year.globalTags.tags} == {"urn:li:tag:sap:calendar:year"}

    amount = fields_by_name["AMOUNT_USD"]
    assert amount.globalTags is not None
    assert {t.tag for t in amount.globalTags.tags} == {
        "urn:li:tag:sap:semantic:currency"
    }

    unit = fields_by_name["UNIT_CODE"]
    assert unit.globalTags is not None
    assert {t.tag for t in unit.globalTags.tags} == {"urn:li:tag:sap:semantic:unit"}

    # Plain ID field has no semantic annotation → no globalTags.
    assert fields_by_name["ID"].globalTags is None

    # And the description-suffix custom props are still present (additive).
    assert year.description is not None and "sap_calendar_type=year" in year.description


def test_emit_sap_semantics_as_tags_false_suppresses_tags(requests_mock):
    """When ``emit_sap_semantics_as_tags=False``, schema fields have no
    ``globalTags`` and no dataset-level globalTags aspect is emitted."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "emit_sap_semantics_as_tags": False,
        }
    )
    ctx = PipelineContext(run_id="test-tags-off")
    _wire_revenue_asset(requests_mock)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    schema_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SchemaMetadataClass"
    )
    for f in aspect_as(schema_wu, SchemaMetadataClass).fields:
        assert f.globalTags is None, (
            f"Field {f.fieldPath} should have no globalTags when emission is off; "
            f"got {f.globalTags}"
        )

    # No dataset-level globalTags MCPs either.
    assert not [
        wu for wu in workunits if aspect_of(wu).__class__.__name__ == "GlobalTagsClass"
    ], "No GlobalTags aspects should be emitted when emission is off"

    # And no standalone TagProperties MCPs.
    assert not [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "TagPropertiesClass"
    ], "No TagProperties MCPs should be emitted when emission is off"

    # The existing sap_* custom-property description suffix MUST still be present
    # — the flag only suppresses tags, not the additive custom-property behaviour.
    year = next(
        f
        for f in aspect_as(schema_wu, SchemaMetadataClass).fields
        if f.fieldPath == "YEAR_VAL"
    )
    assert year.description is not None and "sap_calendar_type=year" in year.description


def test_predefined_tag_entities_emitted_once_per_run(requests_mock):
    """Standalone ``TagPropertiesClass`` MCPs are emitted once per run, not
    once per asset. With 10 predefined tag URNs we expect exactly 10
    TagProperties MCPs even when N assets share the same tag URNs."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-tag-entities-once")

    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": f"A{i}",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": "https://myco.eu10.hcs.cloud.sap/edmx/REVENUE/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
                for i in range(3)
            ]
        },
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/edmx/REVENUE/$metadata",
        text=_EDMX_FOR_TAGS,
    )
    requests_mock.get(
        "https://myco.eu10.hcs.cloud.sap/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    tag_property_urns = [
        entity_urn_of(wu)
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "TagPropertiesClass"
    ]
    # The set covers the 10 predefined SAP tag URNs (Dimension, Measure, the 6
    # calendar tags, currency, unit). Each is emitted exactly once.
    assert len(tag_property_urns) == len(set(tag_property_urns)), (
        f"Expected distinct TagProperties URNs; got duplicates: "
        f"{[u for u in tag_property_urns if tag_property_urns.count(u) > 1]}"
    )
    expected_urns = {
        "urn:li:tag:Dimension",
        "urn:li:tag:Measure",
        "urn:li:tag:sap:semantic:currency",
        "urn:li:tag:sap:semantic:unit",
        "urn:li:tag:sap:calendar:year",
        "urn:li:tag:sap:calendar:month",
        "urn:li:tag:sap:calendar:quarter",
        "urn:li:tag:sap:calendar:week",
        "urn:li:tag:sap:calendar:date",
        "urn:li:tag:sap:calendar:yearmonth",
    }
    assert set(tag_property_urns) == expected_urns, (
        f"Expected exactly the 10 predefined SAP tag URNs; got: {tag_property_urns}"
    )


def test_analytics_dimension_type_emits_entity_level_tag(requests_mock):
    """An entity with ``@Analytics.DimensionType='Time'`` gets a
    ``sap:dimension_type:Time`` tag at the Dataset level, not the field level.
    Likewise, the entity-level ``@Analytics.Measure`` annotation surfaces as a
    flat ``urn:li:tag:Measure`` on the Dataset."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    ctx = PipelineContext(run_id="test-entity-tag")
    _wire_revenue_asset(requests_mock)

    source = SapDatasphereSource(ctx, cfg)
    workunits = list(source.get_workunits())

    # Find the Dataset-level GlobalTagsClass aspect for the REVENUE dataset.
    dataset_tag_aspects = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "GlobalTagsClass"
        and "urn:li:dataset:" in (entity_urn_of(wu) or "")
        and "revenue" in (entity_urn_of(wu) or "")
    ]
    assert len(dataset_tag_aspects) == 1, (
        f"Expected exactly one dataset-level GlobalTagsClass for REVENUE; "
        f"got {len(dataset_tag_aspects)}"
    )
    tag_urns = {t.tag for t in aspect_as(dataset_tag_aspects[0], GlobalTagsClass).tags}
    assert tag_urns == {
        "urn:li:tag:Measure",
        "urn:li:tag:sap:dimension_type:Time",
    }, f"Unexpected dataset-level tags for REVENUE: {tag_urns}"


def _setup_view_with_csn_query(
    requests_mock: rm.Mocker, *, analytic: bool = False, sql: Optional[str] = None
) -> str:
    """Wire a single Space with one View (or Analytic Model) whose CSN carries a
    `query` dict, plus a Local Table with only `elements` (no query).

    When ``sql`` is provided the View's CSN also carries the
    ``@DataWarehouse.sqlEditor.query`` annotation, marking it an SQL view.
    """
    tenant = "https://myco.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "VIEW_X",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": None,
                    "supportsAnalyticalQueries": analytic,
                    "hasParameters": False,
                }
            ]
        },
    )
    object_type = "analyticmodels" if analytic else "views"
    view_def = {
        "kind": "entity",
        "query": {
            "SELECT": {
                "from": {"ref": ["TABLE_Y"], "as": "Y"},
                "columns": [{"ref": ["COL1"]}],
                "distinct": False,
            }
        },
    }
    if sql is not None:
        view_def["@DataWarehouse.sqlEditor.query"] = sql
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/{object_type}/VIEW_X",
        json={"definitions": {"VIEW_X": view_def}},
    )
    return tenant


def _view_properties_aspects(
    workunits: Iterable[MetadataWorkUnit],
) -> List[ViewPropertiesClass]:
    return [
        aspect_as(wu, ViewPropertiesClass)
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "ViewPropertiesClass"
    ]


def test_view_definition_emitted_as_csn_viewproperties(requests_mock):
    """Views emit a viewProperties aspect carrying the CSN query as viewLogic
    with viewLanguage 'CSN' and materialized False."""
    _setup_view_with_csn_query(requests_mock)
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            # include_view_definitions defaults to True
        }
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-view-def"), cfg)
    workunits = list(source.get_workunits())

    vps = _view_properties_aspects(workunits)
    assert len(vps) == 1, (
        f"Expected exactly one ViewPropertiesClass aspect; got {len(vps)}: "
        f"{[aspect_of(wu).__class__.__name__ for wu in workunits]}"
    )
    vp = vps[0]
    assert vp.viewLanguage == "CSN"
    assert vp.materialized is False
    assert vp.viewLogic and "SELECT" in vp.viewLogic, (
        f"viewLogic should be the serialized CSN query containing 'SELECT'; "
        f"got: {vp.viewLogic!r}"
    )


def test_sql_view_definition_emitted_as_sql():
    """A view whose CSN carries @DataWarehouse.sqlEditor.query (SQL view) emits
    viewProperties with viewLanguage 'SQL' and the raw SQL as viewLogic —
    NOT the CSN JSON."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-sql-view"), cfg)
    raw_sql = 'SELECT "A" FROM "X"'
    csn_def = {
        "kind": "entity",
        "@DataWarehouse.sqlEditor.query": raw_sql,
        # SQL views ALSO carry a parsed query CQN; the raw SQL must win.
        "query": {"SELECT": {"from": {"ref": ["X"]}, "columns": [{"ref": ["A"]}]}},
    }
    vp = source._build_view_properties(csn_def)
    assert vp is not None
    assert vp.viewLanguage == "SQL"
    assert vp.viewLogic == raw_sql
    assert vp.materialized is False


def test_sql_view_definition_emitted_as_sql_end_to_end(requests_mock):
    """End-to-end: a View asset whose fetched CSN carries the sqlEditor
    annotation emits viewProperties with viewLanguage 'SQL'."""
    raw_sql = 'SELECT "YEAR", "MONTH" FROM "SAP.TIME.M_TIME_DIMENSION"'
    _setup_view_with_csn_query(requests_mock, sql=raw_sql)
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-sql-e2e"), cfg)
    workunits = list(source.get_workunits())

    vps = _view_properties_aspects(workunits)
    assert len(vps) == 1
    assert vps[0].viewLanguage == "SQL"
    assert vps[0].viewLogic == raw_sql
    assert vps[0].materialized is False


def test_graphical_view_still_emitted_as_csn():
    """A view with only `query` (no sqlEditor annotation) still emits CSN."""
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-graphical-view"), cfg)
    query = {"SELECT": {"from": {"ref": ["X"]}, "columns": [{"ref": ["A"]}]}}
    csn_def = {"kind": "entity", "query": query}
    vp = source._build_view_properties(csn_def)
    assert vp is not None
    assert vp.viewLanguage == "CSN"
    assert vp.viewLogic == json.dumps(query, indent=2, sort_keys=False)
    assert vp.materialized is False


def test_analytic_model_emits_view_definition(requests_mock):
    """Analytic Models (supportsAnalyticalQueries) also emit viewProperties."""
    _setup_view_with_csn_query(requests_mock, analytic=True)
    cfg = SapDatasphereConfig.model_validate(
        {"base_url": "https://myco.eu10.hcs.cloud.sap", "token": "tok"}
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-am-def"), cfg)
    workunits = list(source.get_workunits())

    vps = _view_properties_aspects(workunits)
    assert len(vps) == 1
    assert vps[0].viewLanguage == "CSN"


def test_local_table_has_no_view_definition(requests_mock):
    """Local Tables are base tables — they must NOT get a viewProperties aspect."""
    tenant = "https://test.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "S1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={"value": []},  # no exposed views
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/localtables",
        json=[{"technicalName": "BASE_FACTS"}],
    )
    # Local Table CSN: only `elements`, no `query`.
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/localtables/BASE_FACTS",
        json={
            "definitions": {
                "BASE_FACTS": {
                    "kind": "entity",
                    "elements": {"COL_A": {"type": "cds.String", "length": 10}},
                }
            }
        },
    )
    cfg = SapDatasphereConfig(
        base_url=tenant,
        token="t",
        env="PROD",
        include_local_tables=True,
        # include_view_definitions defaults True; must still not touch local tables
    )
    source = SapDatasphereSource(PipelineContext(run_id="t"), cfg)
    workunits = list(source.get_workunits())

    vps = _view_properties_aspects(workunits)
    assert len(vps) == 0, (
        f"Local Tables must not emit viewProperties; got {len(vps)} aspects"
    )


def test_view_definitions_disabled_emits_no_viewproperties(requests_mock):
    """include_view_definitions False → no viewProperties aspect emitted."""
    _setup_view_with_csn_query(requests_mock)
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "include_view_definitions": False,
        }
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-no-view-def"), cfg)
    workunits = list(source.get_workunits())

    vps = _view_properties_aspects(workunits)
    assert len(vps) == 0, (
        f"Expected no ViewPropertiesClass aspects when disabled; got {len(vps)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Analytic-model business layer (businessLayerDefinitions): star-schema lineage
# (fact + dimensions), measure/dimension field tags, and variables — exercised
# through the catalog discovery path (the only discovery path). The catalog
# `_emit_asset` fetches each analytic model's CSN (routed via
# supportsAnalyticalQueries) and runs it through `_apply_business_layer`.
# ─────────────────────────────────────────────────────────────────────────────

_AM_EDMX = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="AM" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="AM">
        <Key><PropertyRef Name="BusinessPartner"/></Key>
        <Property Name="BW_ORDERVALUE" Type="Edm.Decimal" Precision="17" Scale="2"/>
        <Property Name="BusinessPartner" Type="Edm.String" MaxLength="10"/>
      </EntityType>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _wire_business_layer_analytic_model(requests_mock: rm.Mocker) -> str:
    """Wire S1 with a single analytic model AM, discovered via the consumption
    catalog. Its CSN carries a businessLayerDefinitions block (fact + dimension
    sources, measures, attributes, variables). The fact source lives in a
    DIFFERENT space (FINANCE_DATA), so its upstream URN must be built from the
    qualified key directly, not re-prefixed with S1. Schema fields come from the
    EDMX `$metadata` document (the catalog path's schema source)."""
    tenant = "https://myco.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "AM",
                    "label": "AM",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": f"{tenant}/edmx/AM/$metadata",
                    "supportsAnalyticalQueries": True,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(f"{tenant}/edmx/AM/$metadata", text=_AM_EDMX)
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/analyticmodels/AM",
        json={
            "definitions": {
                "AM": {
                    "kind": "entity",
                    "@EndUserText.label": "AM",
                    "@ObjectModel.modelingPattern": {"#": "ANALYTICAL_CUBE"},
                    "elements": {
                        "BW_ORDERVALUE": {"type": "cds.Decimal"},
                        "BusinessPartner": {"type": "cds.String"},
                    },
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["FINANCE_DATA.SALES_ALL_GE"]},
                            "columns": [
                                {"ref": ["BW_ORDERVALUE"]},
                                {"ref": ["BusinessPartner"]},
                            ],
                        }
                    },
                }
            },
            "businessLayerDefinitions": {
                "AM": {
                    "sourceModel": {
                        "factSources": {
                            "F": {"dataEntity": {"key": "FINANCE_DATA.SALES_ALL_GE"}}
                        },
                        "dimensionSources": {
                            "_BP": {
                                "dataEntity": {
                                    "key": "FINANCE_DATA.BusinessPartner_DIM_GE"
                                }
                            }
                        },
                    },
                    "measures": {"BW_ORDERVALUE": {"isAuxiliary": False}},
                    "attributes": {"BusinessPartner": {}},
                    "variables": {"P_DATE": {}},
                }
            },
        },
    )
    return tenant


def test_analytic_model_emits_fact_and_dimension_lineage(requests_mock):
    tenant = _wire_business_layer_analytic_model(requests_mock)
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": tenant,
            "token": "tok",
            "include_lineage": True,
        }
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-bl-lineage"), cfg)
    workunits = list(source.get_workunits())

    lineage_wus = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "UpstreamLineageClass"
    ]
    assert len(lineage_wus) >= 1, "Expected an UpstreamLineage aspect"
    upstream_urns = [
        u.dataset for u in aspect_as(lineage_wus[0], UpstreamLineageClass).upstreams
    ]
    # Fact + dimension, both built from the cross-space qualified key
    # (finance_data, NOT s1), lowercase.
    assert any("finance_data.sales_all_ge" in u for u in upstream_urns), (
        f"Expected fact upstream finance_data.sales_all_ge; got {upstream_urns}"
    )
    assert any("finance_data.businesspartner_dim_ge" in u for u in upstream_urns), (
        f"Expected dimension upstream finance_data.businesspartner_dim_ge; got {upstream_urns}"
    )
    # The fact must NOT be double-prefixed with the connector's own space.
    assert not any("s1.finance_data" in u for u in upstream_urns), (
        f"Fact must not be double-prefixed with S1; got {upstream_urns}"
    )


def test_analytic_model_tags_measures_and_dimensions(requests_mock):
    tenant = _wire_business_layer_analytic_model(requests_mock)
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": tenant,
            "token": "tok",
            "include_lineage": True,
        }
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-bl-tags"), cfg)
    workunits = list(source.get_workunits())

    schema_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SchemaMetadataClass"
    )
    fields_by_name = {
        f.fieldPath: f for f in aspect_as(schema_wu, SchemaMetadataClass).fields
    }
    measure = fields_by_name["BW_ORDERVALUE"]
    dim = fields_by_name["BusinessPartner"]
    assert measure.globalTags is not None
    assert "urn:li:tag:Measure" in {t.tag for t in measure.globalTags.tags}, (
        f"BW_ORDERVALUE should carry the Measure tag; got "
        f"{[t.tag for t in measure.globalTags.tags]}"
    )
    assert dim.globalTags is not None
    assert "urn:li:tag:Dimension" in {t.tag for t in dim.globalTags.tags}, (
        f"BusinessPartner should carry the Dimension tag; got "
        f"{[t.tag for t in dim.globalTags.tags]}"
    )


def test_analytic_model_variables_custom_property(requests_mock):
    tenant = _wire_business_layer_analytic_model(requests_mock)
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": tenant,
            "token": "tok",
            "include_lineage": True,
        }
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-bl-vars"), cfg)
    workunits = list(source.get_workunits())

    props_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "DatasetPropertiesClass"
        and "am" in (entity_urn_of(wu) or "")
    )
    assert (
        aspect_as(props_wu, DatasetPropertiesClass).customProperties.get(
            "sap_variables"
        )
        == "P_DATE"
    )


def test_plain_view_unaffected(requests_mock):
    """A regular view (no businessLayerDefinitions) emits exactly as before — no
    sap_variables custom property, and lineage still derives from the query
    FROM ref (prefixed with the connector's own space)."""
    tenant = "https://myco.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "V_DAY",
                    "label": "Day Dimension",
                    "spaceName": "S1",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )
    requests_mock.get(
        f"{tenant}/dwaas-core/api/v1/spaces/S1/views/V_DAY",
        json={
            "definitions": {
                "V_DAY": {
                    "kind": "entity",
                    "@EndUserText.label": "Day Dimension",
                    "elements": {
                        "CALDAY": {"type": "cds.String", "length": 8},
                        "AMOUNT": {"type": "cds.Decimal"},
                    },
                    "query": {
                        "SELECT": {
                            "from": {"ref": ["M_TIME_DIMENSION"], "as": "T"},
                            "columns": [{"ref": ["CALDAY"]}, {"ref": ["AMOUNT"]}],
                        }
                    },
                }
            }
        },
    )
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": tenant,
            "token": "tok",
            "include_lineage": True,
        }
    )
    source = SapDatasphereSource(PipelineContext(run_id="test-bl-plain"), cfg)
    workunits = list(source.get_workunits())

    props_wus = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "DatasetPropertiesClass"
    ]
    for wu in props_wus:
        assert (
            "sap_variables"
            not in aspect_as(wu, DatasetPropertiesClass).customProperties
        ), "Plain views must not get a sap_variables custom property"

    lineage_wus = [
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "UpstreamLineageClass"
    ]
    assert len(lineage_wus) >= 1
    upstream_urns = [
        u.dataset for u in aspect_as(lineage_wus[0], UpstreamLineageClass).upstreams
    ]
    assert any("s1.m_time_dimension" in u for u in upstream_urns), (
        f"Plain view lineage should still come from query FROM; got {upstream_urns}"
    )


def test_catalog_mode_unchanged(requests_mock):
    """The catalog discovery path discovers views via the consumption catalog
    and sources schema from EDMX."""
    fixture_xml = (
        Path(__file__).parent / "fixtures" / "sap_datasphere_dimension_day.xml"
    ).read_text()
    tenant = "https://myco.eu10.hcs.cloud.sap"
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces",
        json={"value": [{"name": "S1", "label": "Space 1"}]},
    )
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/consumption/catalog/spaces('S1')/assets",
        json={
            "value": [
                {
                    "name": "DIM_DAY",
                    "label": "Day",
                    "spaceName": "S1",
                    "assetRelationalMetadataUrl": f"{tenant}/edmx/DIM_DAY/$metadata",
                    "supportsAnalyticalQueries": False,
                    "hasParameters": False,
                }
            ]
        },
    )
    requests_mock.get(f"{tenant}/edmx/DIM_DAY/$metadata", text=fixture_xml)
    requests_mock.get(
        f"{tenant}/api/v1/datasphere/spaces/S1/connections",
        json=[],
    )

    cfg = SapDatasphereConfig.model_validate({"base_url": tenant, "token": "tok"})
    source = SapDatasphereSource(PipelineContext(run_id="test-catalog"), cfg)
    workunits = list(source.get_workunits())

    # Catalog path discovers via the consumption catalog assets endpoint.
    catalog_asset_calls = [
        h
        for h in requests_mock.request_history
        if "/consumption/catalog/spaces('S1')/assets" in h.url
    ]
    assert len(catalog_asset_calls) >= 1, "Catalog mode must hit the catalog assets API"

    # EDMX-sourced schema still emitted.
    schema_wu = next(
        wu
        for wu in workunits
        if aspect_of(wu).__class__.__name__ == "SchemaMetadataClass"
    )
    assert "DATE_SQL" in {
        f.fieldPath for f in aspect_as(schema_wu, SchemaMetadataClass).fields
    }


def test_qualified_upstream_urn_uses_key_directly_lowercased():
    """A fully-qualified <space>.<object> key builds a sap-datasphere URN using
    the key as the URN name directly (lowercased by default), NOT re-prefixed
    with the connector's current space."""
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "platform_instance": "demo_trial",
        }
    )
    src = SapDatasphereSource(PipelineContext(run_id="test-qualified-urn"), cfg)
    urn = src._qualified_upstream_urn("FINANCE_DATA.SALES_ALL_GE")
    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,"
        "demo_trial.finance_data.sales_all_ge,PROD)"
    )


def test_qualified_upstream_urn_respects_lowercase_false():
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "platform_instance": "demo_trial",
            "convert_urns_to_lowercase": False,
        }
    )
    src = SapDatasphereSource(PipelineContext(run_id="test-qualified-urn-case"), cfg)
    urn = src._qualified_upstream_urn("FINANCE_DATA.SALES_ALL_GE")
    assert "FINANCE_DATA.SALES_ALL_GE" in urn  # original case preserved


# ---------------------------------------------------------------------------
# Analytic-model business-layer fine-grained-lineage keep/drop logic
# ---------------------------------------------------------------------------


def _bl_source(run_id: str) -> SapDatasphereSource:
    """A minimal source for direct ``_apply_business_layer`` unit tests:
    no platform_instance, env=PROD, default lowercasing. With these settings
    ``_qualified_upstream_urn("finance.sales")`` ->
    ``urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.sales,PROD)``.
    """
    cfg = SapDatasphereConfig.model_validate(
        {
            "base_url": "https://myco.eu10.hcs.cloud.sap",
            "token": "tok",
            "env": "PROD",
        }
    )
    return SapDatasphereSource(PipelineContext(run_id=run_id), cfg)


def _business_layer_csn(model_name: str, *, fact_key: str, dim_key: str) -> dict:
    """A CSN ``businessLayerDefinitions`` block with one fact + one dimension
    source, shaped the way ``parse_business_layer`` consumes it."""
    return {
        "businessLayerDefinitions": {
            model_name: {
                "sourceModel": {
                    "factSources": {"F": {"dataEntity": {"key": fact_key}}},
                    "dimensionSources": {"D": {"dataEntity": {"key": dim_key}}},
                },
                "measures": {},
                "attributes": {},
                "variables": {},
            }
        }
    }


def test_business_layer_keeps_fgl_when_upstreams_match():
    """When every query-derived fine-grained-lineage upstream's parent dataset
    URN is in the business-layer upstream-URN set, the FGL is RETAINED on the
    returned (business-layer-authoritative) UpstreamLineage."""

    src = _bl_source("bl-keep")

    # Business-layer fact + dimension keys -> these become the authoritative
    # table-level upstreams (and define the allowed FGL-parent URN set).
    fact_urn = "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.sales,PROD)"
    dim_urn = "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.region,PROD)"
    model_urn = "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.am,PROD)"

    # Query FGL whose upstream schemaField parents are exactly the BL fact/dim.
    fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        upstreams=[
            f"urn:li:schemaField:({fact_urn},total)",
            f"urn:li:schemaField:({dim_urn},region)",
        ],
        downstreams=[f"urn:li:schemaField:({model_urn},total)"],
    )
    query_upstreams = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=(
                    "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,"
                    "finance.am.finance.sales,PROD)"  # double-prefixed query FROM
                ),
                type=DatasetLineageTypeClass.VIEW,
            )
        ],
        fineGrainedLineages=[fgl],
    )

    csn = _business_layer_csn("am", fact_key="finance.sales", dim_key="finance.region")
    result = src._apply_business_layer(
        csn,
        "am",
        schema_fields=[],
        custom_properties={},
        query_upstreams=query_upstreams,
    )

    assert result is not None
    # Table-level upstreams are now the business-layer fact + dimension (the
    # double-prefixed query-FROM upstream is gone).
    upstream_datasets = {u.dataset for u in result.upstreams}
    assert upstream_datasets == {fact_urn, dim_urn}
    # FGL is RETAINED because both upstream parents are in the BL set.
    assert result.fineGrainedLineages, "fineGrainedLineages should be kept"
    assert len(result.fineGrainedLineages) == 1
    assert result.fineGrainedLineages[0] is fgl


def test_business_layer_drops_fgl_when_upstreams_mismatch():
    """When a query-derived FGL upstream's parent dataset URN is NOT in the
    business-layer upstream set (e.g. the double-prefixed fact URN), the FGL is
    DROPPED and only the business-layer table-level upstreams remain."""

    src = _bl_source("bl-drop")

    fact_urn = "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.sales,PROD)"
    dim_urn = "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.region,PROD)"
    model_urn = "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,finance.am,PROD)"
    # The query-FROM walker produced a DOUBLE-PREFIXED fact URN; its FGL dangles
    # off this wrong URN, which is NOT in the business-layer set.
    double_prefixed_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,"
        "finance.am.finance.sales,PROD)"
    )

    fgl = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        upstreams=[f"urn:li:schemaField:({double_prefixed_urn},total)"],
        downstreams=[f"urn:li:schemaField:({model_urn},total)"],
    )
    query_upstreams = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=double_prefixed_urn,
                type=DatasetLineageTypeClass.VIEW,
            )
        ],
        fineGrainedLineages=[fgl],
    )

    csn = _business_layer_csn("am", fact_key="finance.sales", dim_key="finance.region")
    result = src._apply_business_layer(
        csn,
        "am",
        schema_fields=[],
        custom_properties={},
        query_upstreams=query_upstreams,
    )

    assert result is not None
    upstream_datasets = {u.dataset for u in result.upstreams}
    assert upstream_datasets == {fact_urn, dim_urn}
    # FGL DROPPED: its upstream parent is the double-prefixed fact, not in the
    # business-layer set -> set to None (not an empty list).
    assert result.fineGrainedLineages is None


def test_schema_field_parent_extracts_dataset_urn():
    """``_schema_field_parent`` returns the inner dataset URN of a schemaField
    URN, and returns non-schemaField input unchanged (the current contract)."""
    src = _bl_source("schema-field-parent")

    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:sap-datasphere,inst.space.obj,PROD)"
    )
    schema_field_urn = f"urn:li:schemaField:({dataset_urn},COL)"
    assert src._schema_field_parent(schema_field_urn) == dataset_urn

    # Edge case: a non-schemaField URN is returned unchanged.
    assert src._schema_field_parent(dataset_urn) == dataset_urn
    assert src._schema_field_parent("not-a-urn") == "not-a-urn"


def test_report_api_call_aggregates_per_endpoint():
    """report_api_call must accumulate count/total/avg/max per operation."""
    report = SapDatasphereReport()
    report.report_api_call("csn_fetch", 0.05)
    report.report_api_call("csn_fetch", 0.05)
    report.report_api_call("csn_fetch", 0.20)

    stats = report.api_timings["csn_fetch"]
    assert stats.count == 3
    assert stats.total_seconds == pytest.approx(0.30)
    assert stats.max_seconds == pytest.approx(0.20)
    assert stats.avg_seconds == pytest.approx(0.10)


def test_report_api_call_avg_seconds_zero_when_no_calls():
    """avg_seconds must not divide by zero for an unused stats bucket."""

    assert ApiCallStats().avg_seconds == 0.0


def test_report_slowest_api_calls_keeps_largest_and_caps_at_ten():
    """The slowest-N tracker keeps only the 10 largest individual calls and drops
    the smallest once full."""
    report = SapDatasphereReport()
    # Push 15 calls with strictly increasing durations.
    for i in range(1, 16):
        report.report_api_call("csn_fetch", i * 0.01, url=f"http://x/{i}")

    # Capped at 10.
    assert len(report.slowest_api_calls) == 10
    assert len(report._slowest_api_calls_heap) == 10

    # The smallest retained call corresponds to the 6th-fastest (0.06s); the four
    # fastest (0.01..0.05) were dropped.
    retained_seconds = sorted(s for s, _op, _u in report._slowest_api_calls_heap)
    assert retained_seconds[0] == pytest.approx(0.06)
    assert retained_seconds[-1] == pytest.approx(0.15)

    # Human-readable list is sorted slowest-first.
    assert report.slowest_api_calls[0].startswith("csn_fetch: 150ms")


def test_report_api_timings_excluded_from_as_obj_underscore_fields():
    """The underscore-prefixed heap field must not leak into the serialized report;
    the public api_timings / slowest_api_calls must render."""
    report = SapDatasphereReport()
    report.report_api_call("edmx_fetch", 0.03, url="http://x/$metadata")
    obj = report.as_obj()
    assert "_slowest_api_calls_heap" not in obj
    assert "_SLOWEST_N" not in obj
    assert "api_timings" in obj
    assert "slowest_api_calls" in obj
