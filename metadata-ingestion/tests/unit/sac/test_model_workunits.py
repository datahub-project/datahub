from typing import Any, Dict, List, Optional

from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sac.sac import (
    ConnectionMappingConfig,
    SACSource,
    SACSourceConfig,
)
from datahub.ingestion.source.sac.sac_common import ResourceModel
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DatasetKeyClass,
    UpstreamLineageClass,
)

_TENANT_URL = "http://tenant"
_TOKEN_URL = "http://tenant.authentication/oauth/token"
_ACQUIRED_MODEL_ID = "ACQUIREDMODEL123"


def _make_source(
    requests_mock: Mocker,
    platform_instance: Optional[str] = None,
    connection_mapping: Optional[Dict[str, ConnectionMappingConfig]] = None,
) -> SACSource:
    requests_mock.post(_TOKEN_URL, json={"access_token": "token", "expires_in": 3599})
    config = SACSourceConfig(
        tenant_url=_TENANT_URL,
        token_url=_TOKEN_URL,
        client_id="foo",
        client_secret="bar",
        platform_instance=platform_instance,
        connection_mapping=connection_mapping or {},
    )
    return SACSource(config, PipelineContext(run_id="sac-model-browse-path-unit"))


def _acquired_model_dataset_urn() -> str:
    return (
        "urn:li:dataset:(urn:li:dataPlatform:sac,"
        f"t.4.{_ACQUIRED_MODEL_ID}:{_ACQUIRED_MODEL_ID},PROD)"
    )


def _acquired_model() -> ResourceModel:
    return ResourceModel(
        namespace="t.S",
        model_id="ACQUIRED_MODEL",
        name="Acquired model",
        description=None,
        system_type=None,
        connection_id=None,
        external_id=None,
        is_import=False,
    )


def _aspects(workunits: List[MetadataWorkUnit], aspect_type: Any) -> List[Any]:
    return [
        aspect
        for workunit in workunits
        if (aspect := workunit.get_aspect_of_type(aspect_type)) is not None
    ]


def test_get_model_workunits_emits_single_canonical_models_browse_path(requests_mock):
    source = _make_source(requests_mock)

    workunits = list(
        source.get_model_workunits(_acquired_model_dataset_urn(), _acquired_model())
    )

    # Collect every browse aspect (not just the first) so a second, conflicting one
    # added later would fail the test rather than slip through.
    browse_v2 = _aspects(workunits, BrowsePathsV2Class)
    assert len(browse_v2) == 1
    assert [entry.id for entry in browse_v2[0].path] == ["Models"]

    browse_v1 = _aspects(workunits, BrowsePathsClass)
    assert len(browse_v1) == 1
    assert browse_v1[0].paths == ["/sac/Models"]


def test_get_model_workunits_emits_platform_instance_as_urn(requests_mock):
    source = _make_source(requests_mock, platform_instance="myinstance")

    workunits = list(
        source.get_model_workunits(_acquired_model_dataset_urn(), _acquired_model())
    )

    dpis = _aspects(workunits, DataPlatformInstanceClass)
    assert len(dpis) == 1
    assert (
        dpis[0].instance
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:sac,myinstance)"
    )


def _s4hana_live_model(
    external_id: str = "query:[][][2CZMY_QUERY_VIEW]",
) -> ResourceModel:
    return ResourceModel(
        namespace="t.S",
        model_id="LIVE_MODEL",
        name="S/4HANA live model",
        description=None,
        system_type="S/4 HANA",
        connection_id="MY_S4_CONNECTION",
        external_id=external_id,
        is_import=False,
    )


def test_get_model_workunits_resolves_s4hana_query_lineage(requests_mock):
    source = _make_source(requests_mock)

    workunits = list(
        source.get_model_workunits(_acquired_model_dataset_urn(), _s4hana_live_model())
    )

    expected_upstream = "urn:li:dataset:(urn:li:dataPlatform:s4hana,MY_S4_CONNECTION.zmy_query_view,PROD)"
    lineage = _aspects(workunits, UpstreamLineageClass)
    assert len(lineage) == 1
    assert [u.dataset for u in lineage[0].upstreams] == [expected_upstream]
    # The upstream query dataset is created even if its source system is never ingested.
    upstream_keys = _aspects(workunits, DatasetKeyClass)
    assert [(k.platform, k.name) for k in upstream_keys] == [
        ("urn:li:dataPlatform:s4hana", "MY_S4_CONNECTION.zmy_query_view")
    ]
    assert not source.report.warnings


def test_get_model_workunits_s4hana_uses_connection_mapping(requests_mock):
    source = _make_source(
        requests_mock,
        connection_mapping={
            "MY_S4_CONNECTION": ConnectionMappingConfig(
                platform="my_platform",
                platform_instance="prod_s4",
                convert_urns_to_lowercase=False,
            )
        },
    )

    workunits = list(
        source.get_model_workunits(_acquired_model_dataset_urn(), _s4hana_live_model())
    )

    lineage = _aspects(workunits, UpstreamLineageClass)
    assert [u.dataset for u in lineage[0].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:my_platform,prod_s4.ZMY_QUERY_VIEW,PROD)"
    ]


def test_get_model_workunits_s4hana_embedded_bw_query_resolves_as_bw(requests_mock):
    # A query without the `2C` prefix is a real BW query in the embedded BW, not a
    # CDS view, so it keeps BW's platform, naming and casing.
    source = _make_source(requests_mock)

    workunits = list(
        source.get_model_workunits(
            _acquired_model_dataset_urn(),
            _s4hana_live_model(external_id="query:[][][ZQ_MY_BW_QUERY]"),
        )
    )

    lineage = _aspects(workunits, UpstreamLineageClass)
    assert [u.dataset for u in lineage[0].upstreams] == [
        "urn:li:dataset:(urn:li:dataPlatform:bw,MY_S4_CONNECTION.QUERY/ZQ_MY_BW_QUERY,PROD)"
    ]


def test_get_model_workunits_malformed_hana_view_id_warns(requests_mock):
    source = _make_source(requests_mock)
    model = ResourceModel(
        namespace="t.S",
        model_id="LIVE_MODEL",
        name="HANA live model",
        description=None,
        system_type="HANA",
        connection_id="MY_HANA_CONNECTION",
        external_id="view:[MY_SCHEMA]",
        is_import=False,
    )

    workunits = list(source.get_model_workunits(_acquired_model_dataset_urn(), model))

    assert not _aspects(workunits, UpstreamLineageClass)
    assert len(source.report.warnings) == 1
