"""Tests for per-message priority band configuration and weighted fair queuing."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from datahub.pgqueue.config import (
    PgQueueConnectionConfig,
    PgQueueEmitterConfig,
    PgQueueTopicDefaultsConfig,
    PgQueueTopicPartialConfig,
)
from datahub.pgqueue.priority_bands import (
    DEFAULT_BANDS_JSON,
    DEFAULT_PRIORITY,
    MAX_PRIORITY,
    MIN_PRIORITY,
    PRIORITY_BANDS_ENV_VAR,
    PriorityBand,
    PriorityBandConfig,
    resolve_priority_bands_json,
    weighted_fair_fetch,
)


class TestPriorityBand:
    def test_valid_band(self) -> None:
        band = PriorityBand(0, 3, 70)
        assert band.min_priority == 0
        assert band.max_priority == 3
        assert band.weight == 70

    def test_single_priority_band(self) -> None:
        band = PriorityBand(5, 5, 10)
        assert band.min_priority == 5

    def test_min_greater_than_max_raises(self) -> None:
        with pytest.raises(ValueError, match="min_priority"):
            PriorityBand(5, 3, 10)

    def test_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            PriorityBand(-1, 3, 10)
        with pytest.raises(ValueError, match="out of range"):
            PriorityBand(0, 10, 10)

    def test_zero_weight_raises(self) -> None:
        with pytest.raises(ValueError, match="weight must be positive"):
            PriorityBand(0, 3, 0)


class TestPriorityBandConfig:
    def test_parse_default(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        assert len(config.bands) == 3
        assert config.bands[0] == PriorityBand(0, 3, 70)
        assert config.bands[1] == PriorityBand(4, 6, 20)
        assert config.bands[2] == PriorityBand(7, 9, 10)

    def test_single_band_full_range(self) -> None:
        config = PriorityBandConfig.parse('[{"range":[0,9],"weight":1}]')
        assert len(config.bands) == 1

    def test_overlap_raises(self) -> None:
        with pytest.raises(ValueError, match="overlap"):
            PriorityBandConfig.parse(
                '[{"range":[0,5],"weight":50},{"range":[4,9],"weight":50}]'
            )

    def test_gap_raises(self) -> None:
        with pytest.raises(ValueError, match="not covered"):
            PriorityBandConfig.parse(
                '[{"range":[0,3],"weight":50},{"range":[5,9],"weight":50}]'
            )

    def test_incomplete_coverage_raises(self) -> None:
        with pytest.raises(ValueError, match="not covered"):
            PriorityBandConfig.parse('[{"range":[0,8],"weight":100}]')

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            PriorityBandConfig.parse("[]")

    def test_invalid_json_raises(self) -> None:
        with pytest.raises((ValueError, json.JSONDecodeError)):
            PriorityBandConfig.parse("not json")

    def test_batch_limits_proportional(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        limits = config.batch_limits(10)
        assert limits[0] == 7
        assert limits[1] == 2
        assert limits[2] == 1
        assert sum(limits) == 10

    def test_batch_limits_exact(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        limits = config.batch_limits(100)
        assert limits == [70, 20, 10]

    def test_batch_limits_small(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        limits = config.batch_limits(3)
        assert sum(limits) == 3
        assert limits[0] >= 1

    def test_batch_limits_zero(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        limits = config.batch_limits(0)
        assert limits == [0, 0, 0]

    def test_batch_limits_equal_weights(self) -> None:
        config = PriorityBandConfig.parse(
            '[{"range":[0,3],"weight":1},{"range":[4,6],"weight":1},{"range":[7,9],"weight":1}]'
        )
        limits = config.batch_limits(9)
        assert limits == [3, 3, 3]


class TestWeightedFairFetch:
    def test_all_bands_full(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        result = weighted_fair_fetch(
            config,
            10,
            lambda min_p, max_p, lim: [f"p{min_p}-{max_p}"] * lim,
        )
        assert len(result) == 10

    def test_empty_queue(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        result: list[str] = weighted_fair_fetch(
            config, 10, lambda min_p, max_p, lim: []
        )
        assert result == []

    def test_zero_limit(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)
        result = weighted_fair_fetch(
            config,
            0,
            lambda min_p, max_p, lim: ["should-not-be-called"],
        )
        assert result == []

    def test_redistribution(self) -> None:
        config = PriorityBandConfig.parse(DEFAULT_BANDS_JSON)

        def _fetch(min_p: int, max_p: int, lim: int) -> list:
            if min_p == 0:
                return ["high"]
            return [f"other-{i}" for i in range(min(lim, 20))]

        result = weighted_fair_fetch(config, 10, _fetch)
        assert len(result) == 10
        assert result[0] == "high"

    def test_single_band_passthrough(self) -> None:
        config = PriorityBandConfig.parse('[{"range":[0,9],"weight":100}]')
        calls: list = []

        def _fetch(min_p: int, max_p: int, lim: int) -> list:
            calls.append((min_p, max_p, lim))
            return ["a", "b"]

        result = weighted_fair_fetch(config, 5, _fetch)
        assert len(result) == 2
        assert calls == [(0, 9, 5)]


class TestPriorityConstants:
    def test_range_is_zero_to_nine(self) -> None:
        assert MIN_PRIORITY == 0
        assert MAX_PRIORITY == 9

    def test_default_is_five(self) -> None:
        assert DEFAULT_PRIORITY == 5


class TestPriorityConfigDefaults:
    """Verify Pydantic config models expose the new priority fields with correct defaults."""

    def test_topic_defaults_has_priority_bands(self) -> None:
        td = PgQueueTopicDefaultsConfig()
        assert td.priority_bands is not None
        parsed = PriorityBandConfig.parse(td.priority_bands)
        assert len(parsed.bands) == 3

    def test_topic_defaults_no_priority_levels_field(self) -> None:
        assert "priority_levels" not in PgQueueTopicDefaultsConfig.model_fields

    def test_topic_partial_priority_bands_default_none(self) -> None:
        tp = PgQueueTopicPartialConfig()
        assert tp.priority_bands is None
        assert "priority_levels" not in PgQueueTopicPartialConfig.model_fields

    def test_emitter_default_priority_is_five(self) -> None:
        cfg = PgQueueEmitterConfig(
            queue=PgQueueConnectionConfig(
                host_port="localhost:5432",
                database="db",
                username="u",
                password="p",
            )
        )
        assert cfg.default_priority == 5

    def test_emitter_priority_rejects_above_nine(self) -> None:
        with pytest.raises(ValidationError):
            PgQueueEmitterConfig(
                queue=PgQueueConnectionConfig(
                    host_port="localhost:5432",
                    database="db",
                    username="u",
                    password="p",
                ),
                default_priority=10,
            )

    def test_emitter_priority_rejects_negative(self) -> None:
        with pytest.raises(ValidationError):
            PgQueueEmitterConfig(
                queue=PgQueueConnectionConfig(
                    host_port="localhost:5432",
                    database="db",
                    username="u",
                    password="p",
                ),
                default_priority=-1,
            )

    def test_emitter_priority_boundaries_accepted(self) -> None:
        base_kwargs = dict(
            queue=PgQueueConnectionConfig(
                host_port="localhost:5432",
                database="db",
                username="u",
                password="p",
            ),
        )
        cfg0 = PgQueueEmitterConfig(**base_kwargs, default_priority=0)
        assert cfg0.default_priority == 0
        cfg9 = PgQueueEmitterConfig(**base_kwargs, default_priority=9)
        assert cfg9.default_priority == 9

    def test_topic_override_merges_priority_bands(self) -> None:
        custom_bands = '[{"range":[0,9],"weight":100}]'
        queue = PgQueueConnectionConfig(
            host_port="localhost:5432",
            database="db",
            username="u",
            password="p",
            topic_overrides={
                "SpecialTopic_v1": PgQueueTopicPartialConfig(
                    priority_bands=custom_bands,
                ),
            },
        )
        merged = queue.merged_topic_defaults_for("SpecialTopic_v1")
        assert merged.priority_bands == custom_bands

        default_merged = queue.merged_topic_defaults_for("Other_v1")
        assert default_merged.priority_bands == queue.topic_defaults.priority_bands


class TestRepositoryPriorityValidation:
    """Verify repository-level priority bounds checking (mocked, no database)."""

    def test_enqueue_rejects_priority_above_nine(self) -> None:
        from unittest.mock import MagicMock, patch

        from datahub.pgqueue.repository import PgQueueRepository

        repo = PgQueueRepository("queue", "metadata_queue")
        conn = MagicMock()
        conn.autocommit = True

        with (
            patch.object(repo, "ensure_topic", return_value=1),
            patch.object(repo, "fetch_topic_row", return_value=(1, 4, None)),
            pytest.raises(ValueError, match="out of range"),
        ):
            repo.enqueue(
                conn,
                topic_name="t",
                routing_key="k",
                partition_count=4,
                retention_max_age_seconds=0,
                max_rows_per_topic=0,
                max_total_payload_bytes=0,
                default_content_type_mime=None,
                priority=10,
                payload=b"x",
                content_type=None,
                headers=(),
            )

    def test_enqueue_rejects_negative_priority(self) -> None:
        from unittest.mock import MagicMock, patch

        from datahub.pgqueue.repository import PgQueueRepository

        repo = PgQueueRepository("queue", "metadata_queue")
        conn = MagicMock()
        conn.autocommit = True

        with (
            patch.object(repo, "ensure_topic", return_value=1),
            patch.object(repo, "fetch_topic_row", return_value=(1, 4, None)),
            pytest.raises(ValueError, match="out of range"),
        ):
            repo.enqueue(
                conn,
                topic_name="t",
                routing_key="k",
                partition_count=4,
                retention_max_age_seconds=0,
                max_rows_per_topic=0,
                max_total_payload_bytes=0,
                default_content_type_mime=None,
                priority=-1,
                payload=b"x",
                content_type=None,
                headers=(),
            )


class TestResolvePriorityBandsJson:
    """Tests for the priority bands resolution hierarchy:
    GMS server config → ENV override → hardcoded default.
    """

    def test_returns_hardcoded_default_when_no_sources(self) -> None:
        result = resolve_priority_bands_json(server_config=None, env_override=None)
        assert result == DEFAULT_BANDS_JSON

    def test_env_override_wins_over_hardcoded_default(self) -> None:
        custom = '[{"range":[0,9],"weight":100}]'
        result = resolve_priority_bands_json(server_config=None, env_override=custom)
        assert result == custom

    def test_server_config_wins_over_env(self) -> None:
        server_bands = '[{"range":[0,4],"weight":60},{"range":[5,9],"weight":40}]'
        env_bands = '[{"range":[0,9],"weight":100}]'
        server_config = {"pgQueue": {"priorityBands": server_bands}}
        result = resolve_priority_bands_json(
            server_config=server_config, env_override=env_bands
        )
        assert result == server_bands

    def test_server_config_missing_pgqueue_key_falls_to_env(self) -> None:
        env_bands = '[{"range":[0,9],"weight":100}]'
        server_config = {"datahub": {"serverType": "prod"}}
        result = resolve_priority_bands_json(
            server_config=server_config, env_override=env_bands
        )
        assert result == env_bands

    def test_server_config_pgqueue_missing_prioritybands_falls_to_env(self) -> None:
        env_bands = '[{"range":[0,9],"weight":100}]'
        server_config = {"pgQueue": {"someOtherKey": "value"}}
        result = resolve_priority_bands_json(
            server_config=server_config, env_override=env_bands
        )
        assert result == env_bands

    def test_server_config_empty_prioritybands_falls_to_env(self) -> None:
        env_bands = '[{"range":[0,9],"weight":100}]'
        server_config = {"pgQueue": {"priorityBands": ""}}
        result = resolve_priority_bands_json(
            server_config=server_config, env_override=env_bands
        )
        assert result == env_bands

    def test_server_config_non_string_prioritybands_falls_to_env(self) -> None:
        env_bands = '[{"range":[0,9],"weight":100}]'
        server_config = {"pgQueue": {"priorityBands": 123}}
        result = resolve_priority_bands_json(
            server_config=server_config, env_override=env_bands
        )
        assert result == env_bands

    def test_reads_from_os_environ_when_env_override_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        custom = '[{"range":[0,9],"weight":100}]'
        monkeypatch.setenv(PRIORITY_BANDS_ENV_VAR, custom)
        result = resolve_priority_bands_json(server_config=None, env_override=None)
        assert result == custom

    def test_empty_env_var_falls_to_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(PRIORITY_BANDS_ENV_VAR, "")
        result = resolve_priority_bands_json(server_config=None, env_override=None)
        assert result == DEFAULT_BANDS_JSON

    def test_server_config_none_pgqueue_falls_through(self) -> None:
        server_config = {"pgQueue": None}
        result = resolve_priority_bands_json(
            server_config=server_config, env_override=None
        )
        assert result == DEFAULT_BANDS_JSON
