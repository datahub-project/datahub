import logging
import subprocess
import sys
import textwrap

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.source.profiling.config import (
    ProfilingConfig,
    ProfilingIsolationLevel,
)
from datahub.ingestion.source.sqlalchemy_profiler.query_combiner import (
    DEFAULT_MAX_DISTINCT_PER_STATEMENT,
    MAX_QUERIES_TO_COMBINE_AT_ONCE,
)


def test_profile_table_level_only():
    config = ProfilingConfig.model_validate(
        {"enabled": True, "profile_table_level_only": True}
    )
    assert config.any_field_level_metrics_enabled() is False

    config = ProfilingConfig.model_validate(
        {
            "enabled": True,
            "profile_table_level_only": True,
            "include_field_max_value": False,
        }
    )
    assert config.any_field_level_metrics_enabled() is False


def test_profile_table_level_only_fails_with_field_metric_enabled():
    with pytest.raises(
        ValueError,
        match="Cannot enable field-level metrics if profile_table_level_only is set",
    ):
        ProfilingConfig.model_validate(
            {
                "enabled": True,
                "profile_table_level_only": True,
                "include_field_max_value": True,
            }
        )


def test_profiling_method_field_removed() -> None:
    # `method` was removed together with the Great Expectations profiler.
    # SQLAlchemy is the only SQL profiler; recipes that still set `method` are
    # accepted (the field is dropped) with a deprecation warning.
    with pytest.warns(ConfigurationWarning, match="method was removed"):
        config = ProfilingConfig.model_validate({"enabled": True, "method": "ge"})
    assert not hasattr(config, "method")


def test_profiling_isolation_level_default_is_none():
    # Unset by default: nothing is set on the profiling connection, so one
    # transaction spans the whole table profile.
    config = ProfilingConfig.model_validate({"enabled": True})
    assert config.profiling_isolation_level is None


def test_profiling_isolation_level_normalizes_case_and_underscores():
    # The validator accepts case/underscore variants so the enum rejects typos
    # at config-parse time while still matching the SQL standard names with
    # spaces.
    assert (
        ProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "autocommit"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.AUTOCOMMIT
    )
    assert (
        ProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "read_committed"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.READ_COMMITTED
    )
    assert (
        ProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "read committed"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.READ_COMMITTED
    )
    assert (
        ProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "REPEATABLE_READ"}
        ).profiling_isolation_level
        is ProfilingIsolationLevel.REPEATABLE_READ
    )


def test_profiling_isolation_level_empty_string_is_none():
    # Empty/whitespace normalizes to None, i.e. leave the connection alone.
    assert (
        ProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "  "}
        ).profiling_isolation_level
        is None
    )


def test_profiling_isolation_level_rejects_unknown_value():
    # The enum prevents typos at config-parse time.
    with pytest.raises(ValueError):
        ProfilingConfig.model_validate(
            {"enabled": True, "profiling_isolation_level": "BOGUS"}
        )


def test_profiling_isolation_level_json_schema_has_default():
    # docs_config_table.py gates the "Default:" line on `"default" in json_props`.
    # Field(default=None) emits that key; default_factory does not.
    schema = ProfilingConfig.model_json_schema()
    assert "default" in schema["properties"]["profiling_isolation_level"]


def test_max_distinct_per_statement_default_matches_combiner_constant() -> None:
    # Drift guard: the config duplicates the literal rather than importing
    # the combiner, so the two must be kept in lockstep.
    config = ProfilingConfig()
    assert config.max_distinct_per_statement == DEFAULT_MAX_DISTINCT_PER_STATEMENT


def test_max_queries_to_combine_default_matches_combiner_constant() -> None:
    # Same drift guard as above: the config duplicates the literal.
    assert ProfilingConfig().max_queries_to_combine == MAX_QUERIES_TO_COMBINE_AT_ONCE


def test_query_timeout_seconds_rejects_a_value_the_server_cannot_hold() -> None:
    # Postgres takes the limit as a signed 32-bit count of milliseconds, so a
    # larger value is rejected by the SET -- which left the transaction aborted
    # and cost the table every query that followed.
    with pytest.raises(ValueError):
        ProfilingConfig(query_timeout_seconds=2147484)
    assert ProfilingConfig(query_timeout_seconds=2147483)


def test_flatten_is_off_by_default() -> None:
    # The flag ships off. Flipping the default is a separate, deliberate PR.
    assert ProfilingConfig().query_combiner_flatten_enabled is False


def test_flatten_without_query_combiner_warns_but_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Warn rather than raise: turning the combiner off is a legitimate way
    # to troubleshoot a run.
    config = ProfilingConfig.model_validate(
        {"query_combiner_enabled": False, "query_combiner_flatten_enabled": True}
    )
    assert config.query_combiner_flatten_enabled

    with caplog.at_level(logging.WARNING):
        ProfilingConfig.model_validate(
            {"query_combiner_enabled": False, "query_combiner_flatten_enabled": True}
        )
    assert any("has no effect" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "values",
    [
        {"use_sampling": True, "limit": 100, "offset": 25},
        {"use_sampling": False, "offset": 25},
    ],
)
def test_the_config_keeps_what_the_recipe_asked_for(values: dict) -> None:
    # Whether a bound can be honoured is the source's call, made per asset and
    # reported from there. The config neither decides it nor rewrites the
    # recipe, so what is read back is what was written.
    config = ProfilingConfig.model_validate(values)

    assert config.limit == values.get("limit")
    assert config.offset == values.get("offset")


@pytest.mark.parametrize(
    "values",
    [
        {"limit": 0},
        {"limit": -1},
        {"offset": -1, "limit": 10},
    ],
)
def test_out_of_range_limit_and_offset_are_rejected(values: dict) -> None:
    # These are field constraints, not validators: caught at config-parse time
    # rather than as a database error mid-run -- `limit: 0` used to fall through
    # to a full-table profile silently.
    with pytest.raises(ValidationError):
        ProfilingConfig.model_validate({"use_sampling": False, **values})


def test_sampling_is_off_by_default() -> None:
    # The shared default is off: most sources do not implement sampling, so a
    # flag that is on where it does nothing only misleads.
    assert ProfilingConfig().use_sampling is False


def test_sources_that_sample_turn_sampling_on() -> None:
    # The three sources that implement sampling opt back in. This is the guard
    # that matters after flipping the shared default: a missing override -- or a
    # subclass that exists but is not wired to the source's `profiling` field --
    # means a large table is profiled in full rather than sampled.
    #
    # Dremio stays off, and tests/unit/dremio/ pins that: it applies
    # limit/offset but cannot sample, so opting in would cost it the bound.
    from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
    from datahub.ingestion.source.excel.config import ExcelSourceConfig
    from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config

    assert BigQueryV2Config.model_validate({}).profiling.use_sampling is True
    assert (
        SnowflakeV2Config.model_validate(
            {"account_id": "abc12345"}
        ).profiling.use_sampling
        is True
    )
    assert (
        ExcelSourceConfig.model_validate(
            {"path_list": ["dummy"]}
        ).profiling.use_sampling
        is True
    )


def test_profiling_and_kafka_config_import_without_sqlalchemy_or_greenlet() -> None:
    # kafka, cassandra, and excel configs import the profiling config at module
    # scope, and none of those extras ship sqlalchemy. Importing either module
    # must not pull sqlalchemy or greenlet at module scope. Running in a
    # subprocess gives a cold interpreter — what a `pip install
    # acryl-datahub[kafka]` user actually hits — and avoids mutating this
    # process's sys.modules, which previously left a stale duplicate of
    # kafka_config reachable by attribute traversal and broke order-dependent
    # tests that patched it.
    script = textwrap.dedent(
        """
        import sys


        class _BlockFinder:
            def find_spec(self, name, path, target=None):
                if name.split(".")[0] in ("sqlalchemy", "greenlet"):
                    raise ImportError(
                        f"import of {name!r} is blocked for this test"
                    )
                return None


        sys.meta_path.insert(0, _BlockFinder())

        import datahub.ingestion.source.profiling.config  # noqa: F401
        import datahub.ingestion.source.kafka.kafka_config  # noqa: F401

        for blocked in ("sqlalchemy", "greenlet"):
            assert blocked not in sys.modules, (
                f"{blocked} was imported at module scope"
            )
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"subprocess exited {result.returncode}\n"
        f"stdout: {result.stdout.decode()}\n"
        f"stderr: {result.stderr.decode()}"
    )


@pytest.mark.parametrize(
    "tags",
    [
        ["my_tag"],
        ["urn:li:tag:my_tag"],
        ["my_tag", "urn:li:tag:other"],
        None,
        [],
    ],
)
def test_tags_to_ignore_sampling_accepts_names_and_urns(tags):
    assert ProfilingConfig(tags_to_ignore_sampling=tags).tags_to_ignore_sampling == tags


@pytest.mark.parametrize("bad", ["urn:li:corpuser:alice", "urn:li:tag:"])
def test_tags_to_ignore_sampling_rejects_a_malformed_entry(bad):
    # Caught at config-parse time on purpose. The profiler resolves these tags
    # behind a handler that answers any failure with "treat the table as
    # tagged", so a typo reaching that handler would drop every column value
    # for every table and blame the DataHub server for it.
    with pytest.raises(Exception) as err:
        ProfilingConfig(tags_to_ignore_sampling=[bad])
    assert bad in str(err.value)
