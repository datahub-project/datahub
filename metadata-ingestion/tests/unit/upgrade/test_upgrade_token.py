"""The version check must honor OAuth (auth=) client configs, not just static
tokens — server config is fetched through DataHubGraph, which resolves either."""

import asyncio
from typing import Dict, Optional

import pytest
from packaging.version import Version

from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.upgrade import upgrade
from datahub.utilities.server_config_util import RestServiceConfig

_RAW_CONFIG = {
    "versions": {"acryldata/datahub": {"version": "v1.2.0"}},
    "datahub": {"serverType": "prod"},
}


def test_version_check_fetches_config_via_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: Dict[str, DatahubClientConfig] = {}

    class FakeGraph:
        def __init__(self, config: DatahubClientConfig) -> None:
            captured["config"] = config
            self.server_config = RestServiceConfig(raw_config=_RAW_CONFIG)

    auth_config = DatahubClientConfig(
        server="http://gms:8080",
        auth={"type": "static", "config": {"token": "provider-tok"}},
    )
    monkeypatch.setattr(upgrade, "load_client_config", lambda: auth_config)
    monkeypatch.setattr(upgrade, "DataHubGraph", FakeGraph)

    result = asyncio.run(upgrade.get_server_version_stats())

    server_version: Optional[Version] = result[1]
    assert server_version == Version("1.2.0")
    # The auth provider config must reach the graph untouched, with the
    # best-effort bounds applied.
    assert captured["config"].auth is not None
    assert captured["config"].retry_max_times == 0
    assert captured["config"].timeout_sec == 3
