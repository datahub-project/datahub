from __future__ import annotations

from datahub.pgqueue.config import (
    PgQueueConnectionConfig,
    PgQueueTopicPartialConfig,
)


def test_merged_topic_defaults_for_applies_override_by_topic_name() -> None:
    queue = PgQueueConnectionConfig(
        host_port="localhost:5432",
        database="d",
        username="u",
        password="p",
        topic_overrides={
            "MetadataChangeLog_Timeseries_v1": PgQueueTopicPartialConfig(
                retention_max_age_seconds=7776000,
            ),
        },
    )
    merged = queue.merged_topic_defaults_for("MetadataChangeLog_Timeseries_v1")
    assert merged.retention_max_age_seconds == 7776000


def test_merged_topic_defaults_aggressive_retention_override() -> None:
    queue = PgQueueConnectionConfig(
        host_port="localhost:5432",
        database="datahub",
        username="datahub",
        password="datahub",
        topic_overrides={
            "MetadataChangeProposal_v1": PgQueueTopicPartialConfig(
                aggressive_retention=True
            ),
        },
    )
    merged = queue.merged_topic_defaults_for("MetadataChangeProposal_v1")
    assert merged.aggressive_retention is True
    assert queue.merged_topic_defaults_for("other").aggressive_retention is False

    base_only = queue.merged_topic_defaults_for("Other_v1")
    assert (
        base_only.retention_max_age_seconds
        == queue.topic_defaults.retention_max_age_seconds
    )
