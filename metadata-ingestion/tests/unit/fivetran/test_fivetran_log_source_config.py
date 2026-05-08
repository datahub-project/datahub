"""Tests for the `log_source` mode toggle and its credential-block
validation."""

from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fivetran.config import FivetranSourceConfig
from datahub.ingestion.source.fivetran.fivetran import FivetranSource
from datahub.ingestion.source.fivetran.fivetran_log_db_reader import FivetranLogDbReader
from datahub.ingestion.source.fivetran.fivetran_log_rest_reader import (
    FivetranLogRestReader,
)


def _base_config(**overrides):
    cfg = {
        "fivetran_log_config": {
            "destination_platform": "snowflake",
            "snowflake_destination_config": {
                "account_id": "x",
                "username": "u",
                "password": "p",
                "warehouse": "w",
                "database": "d",
                "log_schema": "s",
            },
        },
    }
    cfg.update(overrides)
    return cfg


class TestLogSourceField:
    def test_infers_log_database_when_only_log_config_present(self):
        # Only `fivetran_log_config` provided — no api_config. The connector
        # has nowhere to talk to the REST API, so DB mode is the only option.
        cfg = FivetranSourceConfig.model_validate(_base_config())
        assert cfg.log_source == "log_database"

    def test_infers_rest_api_when_only_api_config_present(self):
        # Pure REST mode — no DB log config supplied. Inference picks
        # `rest_api` because there's no other choice.
        cfg = FivetranSourceConfig.model_validate(
            {"api_config": {"api_key": "k", "api_secret": "s"}}
        )
        assert cfg.log_source == "rest_api"

    def test_infers_log_database_when_both_blocks_present(self):
        # Both blocks → DB-primary by default. The DB log carries the
        # canonical lineage and DPI events; REST only fills in destination
        # routing and Google Sheets details. Users wanting REST-primary in
        # a hybrid setup must opt in via `log_source: rest_api`.
        cfg = FivetranSourceConfig.model_validate(
            _base_config(api_config={"api_key": "k", "api_secret": "s"})
        )
        assert cfg.log_source == "log_database"

    def test_explicit_log_database_honoured(self):
        cfg = FivetranSourceConfig.model_validate(
            _base_config(log_source="log_database")
        )
        assert cfg.log_source == "log_database"

    def test_explicit_rest_api_honoured(self):
        cfg = FivetranSourceConfig.model_validate(
            _base_config(
                log_source="rest_api",
                api_config={"api_key": "k", "api_secret": "s"},
            )
        )
        assert cfg.log_source == "rest_api"

    def test_rest_api_mode_requires_api_config(self):
        with pytest.raises(ValueError, match="api_config"):
            FivetranSourceConfig.model_validate(_base_config(log_source="rest_api"))

    def test_no_credentials_rejected(self):
        # No fivetran_log_config and no api_config — nothing to read.
        with pytest.raises(ValueError, match="must be provided"):
            FivetranSourceConfig.model_validate({})

    def test_invalid_value_rejected(self):
        with pytest.raises(ValueError):
            FivetranSourceConfig.model_validate(_base_config(log_source="kafka_topic"))


def test_log_database_mode_constructs_FivetranLogDbReader():
    cfg_dict = _base_config()
    with patch(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
    ):
        src = FivetranSource.create(cfg_dict, ctx=PipelineContext(run_id="x"))
    assert isinstance(src.log_reader, FivetranLogDbReader)


def test_rest_api_mode_constructs_FivetranLogRestReader():
    # _base_config includes a fivetran_log_config — that's the hybrid mode
    # (REST + DB log for run history), which instantiates a Snowflake engine.
    # Patch create_engine so the test doesn't try to actually connect.
    cfg_dict = _base_config(
        log_source="rest_api",
        api_config={"api_key": "k", "api_secret": "s"},
    )
    with patch(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
    ):
        src = FivetranSource.create(cfg_dict, ctx=PipelineContext(run_id="x"))
    assert isinstance(src.log_reader, FivetranLogRestReader)


def test_rest_api_mode_pure_no_db_log():
    # With log_source: rest_api and NO fivetran_log_config, no Snowflake
    # engine should be instantiated.
    cfg_dict = {
        "log_source": "rest_api",
        "api_config": {"api_key": "k", "api_secret": "s"},
    }
    src = FivetranSource.create(cfg_dict, ctx=PipelineContext(run_id="x"))
    assert isinstance(src.log_reader, FivetranLogRestReader)
    assert src.log_reader._db_log_reader is None


def test_rest_api_mode_hybrid_wires_db_log_reader():
    # With both log_source: rest_api AND fivetran_log_config, the REST reader
    # gets a DB log reader for sync history fetching.
    cfg_dict = _base_config(
        log_source="rest_api",
        api_config={"api_key": "k", "api_secret": "s"},
    )
    with patch(
        "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
    ):
        src = FivetranSource.create(cfg_dict, ctx=PipelineContext(run_id="x"))
    assert isinstance(src.log_reader, FivetranLogRestReader)
    assert src.log_reader._db_log_reader is not None
    assert isinstance(src.log_reader._db_log_reader, FivetranLogDbReader)


def test_rest_api_mode_shares_single_api_client_with_source():
    # The source's destination-discovery path and the REST reader's
    # connector-listing path must share one `FivetranAPIClient` instance.
    # Two clients would fragment `_destination_cache` and waste an HTTP
    # session per ingest. Pin the single-instance invariant.
    cfg_dict = {
        "log_source": "rest_api",
        "api_config": {"api_key": "k", "api_secret": "s"},
    }
    src = FivetranSource.create(cfg_dict, ctx=PipelineContext(run_id="x"))
    assert isinstance(src.log_reader, FivetranLogRestReader)
    assert src.api_client is not None
    assert src.log_reader.api_client is src.api_client


class TestFivetranLogConfigOptionalInRestMode:
    def test_rest_api_mode_does_not_require_fivetran_log_config(self):
        # In rest_api mode, fivetran_log_config should be optional —
        # the REST reader doesn't use it.
        cfg = FivetranSourceConfig.model_validate(
            {
                "log_source": "rest_api",
                "api_config": {"api_key": "k", "api_secret": "s"},
            }
        )
        assert cfg.log_source == "rest_api"
        assert cfg.fivetran_log_config is None

    def test_explicit_log_database_still_requires_fivetran_log_config(self):
        # When the user pins `log_source: log_database` explicitly but
        # supplies only `api_config`, surface the conflict as an error
        # rather than silently switching modes.
        with pytest.raises(ValueError, match="fivetran_log_config"):
            FivetranSourceConfig.model_validate(
                {
                    "log_source": "log_database",
                    "api_config": {"api_key": "k", "api_secret": "s"},
                }
            )


class TestPerConnectorLimits:
    """`max_jobs_per_connector` etc. live on `FivetranSourceConfig` (the
    parent) so they apply equally to log_database and rest_api modes.
    """

    def test_top_level_placement(self):
        cfg = FivetranSourceConfig.model_validate(
            _base_config(max_jobs_per_connector=1234)
        )
        assert cfg.max_jobs_per_connector == 1234

    def test_top_level_placement_works_in_rest_mode(self):
        # The whole point of moving the field — REST recipes (no
        # fivetran_log_config) can override the limit.
        cfg = FivetranSourceConfig.model_validate(
            {
                "log_source": "rest_api",
                "api_config": {"api_key": "k", "api_secret": "s"},
                "max_jobs_per_connector": 1234,
            }
        )
        assert cfg.max_jobs_per_connector == 1234
        assert cfg.fivetran_log_config is None


class TestMaxLimitsLegacyShim:
    """Backwards-compat shim: max_*_per_connector fields used to live on
    FivetranLogConfig. Old recipes that nest them under fivetran_log_config
    must keep working; new recipes use the top-level placement.
    """

    def test_legacy_nested_max_lifted_to_top_level(self):
        # Old recipe shape: max field nested under fivetran_log_config.
        cfg_dict = {
            "fivetran_log_config": {
                "destination_platform": "snowflake",
                "snowflake_destination_config": {
                    "account_id": "x",
                    "username": "u",
                    "password": "p",
                    "warehouse": "w",
                    "database": "d",
                    "log_schema": "s",
                },
                "max_column_lineage_per_connector": 5000,
                "max_table_lineage_per_connector": 200,
                "max_jobs_per_connector": 999,
            },
        }
        with pytest.warns(DeprecationWarning if False else Warning):
            cfg = FivetranSourceConfig.model_validate(cfg_dict)
        assert cfg.max_column_lineage_per_connector == 5000
        assert cfg.max_table_lineage_per_connector == 200
        assert cfg.max_jobs_per_connector == 999

    def test_top_level_wins_over_nested(self):
        # If the user specifies BOTH (mid-migration), top-level wins.
        cfg_dict = {
            "fivetran_log_config": {
                "destination_platform": "snowflake",
                "snowflake_destination_config": {
                    "account_id": "x",
                    "username": "u",
                    "password": "p",
                    "warehouse": "w",
                    "database": "d",
                    "log_schema": "s",
                },
                "max_column_lineage_per_connector": 100,  # legacy
            },
            "max_column_lineage_per_connector": 7777,  # new placement
        }
        with pytest.warns(Warning):
            cfg = FivetranSourceConfig.model_validate(cfg_dict)
        assert cfg.max_column_lineage_per_connector == 7777
