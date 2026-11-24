"""Shared builders for the Looker (dashboard source) unit tests.

The only thing these ever mock is the ``looker_sdk`` client handed to
``looker_sdk.init40``; the DataHub ``LookerAPI`` wrapper, the source and its
registries run for real on top of it.
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerAPIConfig,
)
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource
from datahub.sdk.entity import Entity

LOOKER_BASE_URL = "https://looker.example.com"


class EmittedWorkUnits:
    """Flattens a source's output (workunits and SDK entities) and indexes the
    aspects by entity URN."""

    def __init__(self, items: Iterable[Union[MetadataWorkUnit, Entity]]):
        self.workunits: List[MetadataWorkUnit] = []
        for item in items:
            if isinstance(item, Entity):
                self.workunits.extend(item.as_workunits())
            else:
                self.workunits.append(item)

    def aspects(self, cls: type) -> Dict[str, List[Any]]:
        found: Dict[str, List[Any]] = {}
        for wu in self.workunits:
            aspect = wu.get_aspect_of_type(cls)
            if aspect is not None:
                found.setdefault(wu.get_urn(), []).append(aspect)
        return found

    def urns(self, cls: type) -> List[str]:
        return sorted(self.aspects(cls))


@pytest.fixture
def looker_api_config() -> LookerAPIConfig:
    return LookerAPIConfig(
        base_url=LOOKER_BASE_URL,
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


@pytest.fixture
def looker_api_factory(
    looker_api_config: LookerAPIConfig,
) -> Callable[[mock.MagicMock], LookerAPI]:
    """Build a real ``LookerAPI`` wrapper around a fake ``looker_sdk`` client."""

    def build(client: mock.MagicMock) -> LookerAPI:
        with mock.patch("looker_sdk.init40", return_value=client):
            return LookerAPI(config=looker_api_config)

    return build


@pytest.fixture
def looker_config_factory() -> Callable[..., LookerDashboardSourceConfig]:
    """Build a ``LookerDashboardSourceConfig``; keyword arguments override or
    extend the raw recipe dict."""

    def build(**overrides: Any) -> LookerDashboardSourceConfig:
        raw: Dict[str, Any] = {
            "base_url": LOOKER_BASE_URL,
            "client_id": "id",
            "client_secret": "secret",
        }
        raw.update(overrides)
        return LookerDashboardSourceConfig.model_validate(raw)

    return build


@pytest.fixture
def looker_source_factory(
    looker_config_factory: Callable[..., LookerDashboardSourceConfig],
) -> Callable[..., LookerDashboardSource]:
    """Build a ``LookerDashboardSource`` over a fake ``looker_sdk`` client.

    Defaults to a single thread, no usage extraction and owner extraction so
    tests are deterministic; keyword arguments are config overrides. Enabling
    ``stateful_ingestion`` attaches a specced graph so checkpointing works.
    """

    def build(client: mock.MagicMock, **overrides: Any) -> LookerDashboardSource:
        raw: Dict[str, Any] = {
            "max_threads": 1,
            "extract_usage_history": False,
            "extract_owners": True,
        }
        raw.update(overrides)
        config = looker_config_factory(**raw)
        graph: Optional[DataHubGraph] = None
        pipeline_name: Optional[str] = None
        if config.stateful_ingestion and config.stateful_ingestion.enabled:
            graph = mock.MagicMock(spec=DataHubGraph)
            pipeline_name = "p"
        ctx = PipelineContext("t", pipeline_name=pipeline_name, graph=graph)
        with mock.patch("looker_sdk.init40", return_value=client):
            return LookerDashboardSource(config, ctx)

    return build
