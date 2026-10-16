from typing import Any, List, Optional

from requests_mock import Mocker

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sac.sac import SACSource, SACSourceConfig
from datahub.ingestion.source.sac.sac_common import ResourceModel
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
)

_TENANT_URL = "http://tenant"
_TOKEN_URL = "http://tenant.authentication/oauth/token"
_ACQUIRED_MODEL_ID = "ACQUIREDMODEL123"


def _make_source(
    requests_mock: Mocker, platform_instance: Optional[str] = None
) -> SACSource:
    requests_mock.post(_TOKEN_URL, json={"access_token": "token", "expires_in": 3599})
    config = SACSourceConfig(
        tenant_url=_TENANT_URL,
        token_url=_TOKEN_URL,
        client_id="foo",
        client_secret="bar",
        platform_instance=platform_instance,
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
