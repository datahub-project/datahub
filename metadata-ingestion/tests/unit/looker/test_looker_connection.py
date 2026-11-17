"""Resolving a LookML ``connection:`` name to a platform definition, from the
recipe's ``connection_to_platform_map`` or, failing that, the Looker API."""

import pathlib
from typing import Optional
from unittest import mock

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import DBConnection

from datahub.ingestion.source.looker.looker_connection import (
    get_connection_def_based_on_connection_string,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker.lookml_config import (
    LookMLSourceConfig,
    LookMLSourceReport,
)


def _config(tmp_path: pathlib.Path, with_map: bool) -> LookMLSourceConfig:
    raw = {
        "base_folder": str(tmp_path),
        "project_name": "p",
        "api": {
            "client_id": "id",
            "client_secret": "secret",
            "base_url": "https://looker.example.com",
        },
    }
    if with_map:
        raw["connection_to_platform_map"] = {"warehouse": "snowflake"}
    return LookMLSourceConfig.model_validate(raw)


def _client(connection: Optional[DBConnection]) -> mock.MagicMock:
    client = mock.MagicMock(spec=LookerAPI)
    if connection is None:
        client.connection.side_effect = SDKError("forbidden")
    else:
        client.connection.return_value = connection
    return client


def test_configured_mapping_wins_without_api_call(tmp_path: pathlib.Path) -> None:
    client = _client(DBConnection(name="warehouse", dialect_name="postgres"))
    definition = get_connection_def_based_on_connection_string(
        connection="warehouse",
        source_config=_config(tmp_path, with_map=True),
        looker_client=client,
        reporter=LookMLSourceReport(),
    )
    assert definition is not None
    assert definition.platform == "snowflake"
    client.connection.assert_not_called()


def test_api_connection_is_cached_in_config(tmp_path: pathlib.Path) -> None:
    """A connection fetched from the API is written back into the config map so
    every later view on the same connection skips the API call."""
    client = _client(
        DBConnection(name="warehouse", dialect_name="postgres", database="db")
    )
    config = _config(tmp_path, with_map=False)
    definition = get_connection_def_based_on_connection_string(
        connection="warehouse",
        source_config=config,
        looker_client=client,
        reporter=LookMLSourceReport(),
    )
    assert definition is not None
    assert definition.platform == "postgres"
    assert config.connection_to_platform_map == {"warehouse": definition}

    get_connection_def_based_on_connection_string(
        connection="warehouse",
        source_config=config,
        looker_client=client,
        reporter=LookMLSourceReport(),
    )
    client.connection.assert_called_once()


def test_api_error_yields_no_definition(tmp_path: pathlib.Path) -> None:
    assert (
        get_connection_def_based_on_connection_string(
            connection="warehouse",
            source_config=_config(tmp_path, with_map=False),
            looker_client=_client(None),
            reporter=LookMLSourceReport(),
        )
        is None
    )


def test_connection_without_dialect_is_reported(tmp_path: pathlib.Path) -> None:
    reporter = LookMLSourceReport()
    definition = get_connection_def_based_on_connection_string(
        connection="warehouse",
        source_config=_config(tmp_path, with_map=False),
        looker_client=_client(DBConnection(name="warehouse", dialect_name=None)),
        reporter=reporter,
    )
    assert definition is None
    # The warning's message carries the connection key; its wording is not pinned.
    assert any("warehouse" in (w.message or "") for w in reporter.warnings)


def test_no_api_and_no_mapping_yields_none(tmp_path: pathlib.Path) -> None:
    assert (
        get_connection_def_based_on_connection_string(
            connection="warehouse",
            source_config=_config(tmp_path, with_map=False),
            looker_client=None,
            reporter=LookMLSourceReport(),
        )
        is None
    )
