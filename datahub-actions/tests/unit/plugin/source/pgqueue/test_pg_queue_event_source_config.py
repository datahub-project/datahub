from datahub_actions.plugin.source.pgqueue.pg_queue_event_source import (
    PgQueueEventSourceConfig,
)


def _make_config() -> PgQueueEventSourceConfig:
    return PgQueueEventSourceConfig(
        queue={
            "host_port": "localhost:5432",
            "database": "datahub",
            "username": "datahub",
            "password": "datahub",
        }
    )


def test_build_consumer_config_uses_pipeline_name_as_group() -> None:
    cfg = _make_config()
    consumer_cfg = cfg.build_consumer_config("my-pipeline")
    assert consumer_cfg.consumer_group == "my-pipeline"
    assert consumer_cfg.queue.database == "datahub"
    assert consumer_cfg.topic_routes["mcl"] == "MetadataChangeLog_Versioned_v1"


def test_config_no_lock_owner_suffix() -> None:
    """lock_owner_suffix was removed — config must not accept it."""
    cfg = _make_config()
    assert not hasattr(cfg, "lock_owner_suffix")


def test_build_consumer_config_no_lock_owner_suffix() -> None:
    """Built consumer config must not contain lock_owner_suffix."""
    cfg = _make_config()
    consumer_cfg = cfg.build_consumer_config("pipeline")
    assert not hasattr(consumer_cfg, "lock_owner_suffix")
