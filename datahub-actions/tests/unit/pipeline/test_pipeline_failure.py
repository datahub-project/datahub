import logging
import time
from typing import Any, Iterator, cast
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from confluent_kafka import KafkaError, KafkaException

from datahub_actions.cli.actions import actions
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager


@pytest.fixture(autouse=True)
def _clear_registry() -> Iterator[None]:
    PipelineManager.pipeline_registry.clear()
    yield
    PipelineManager.pipeline_registry.clear()


def _failing_pipeline(name: str) -> Pipeline:
    source = MagicMock()
    source.events.side_effect = KafkaException(
        KafkaError(KafkaError._TRANSPORT, "Failed to get metadata")
    )
    return Pipeline(
        name=name,
        source=source,
        filters=[],
        transforms=[],
        action=MagicMock(),
        retry_count=0,
        failure_mode=None,
        failed_events_dir=None,
    )


def test_source_failure_is_logged_and_pipeline_stops(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = PipelineManager()
    pipeline = _failing_pipeline("broken")

    manager.start_pipeline("broken", pipeline)
    manager.pipeline_registry["broken"].thread.join(timeout=5)

    assert not manager.has_running_pipelines()
    # The source is closed rather than left half-open behind a dead thread.
    cast(MagicMock, pipeline.source).close.assert_called_once()
    assert any(
        r.levelno == logging.ERROR and "broken" in r.message for r in caplog.records
    )


def test_cli_exits_non_zero_once_no_pipeline_is_running(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "name: broken\nsource:\n  type: kafka\naction:\n  type: hello_world\n"
    )
    monkeypatch.setattr(
        "datahub_actions.cli.actions.pipeline_config_to_pipeline",
        lambda _: _failing_pipeline("broken"),
    )
    real_sleep = time.sleep
    sleeps = 0

    def short_sleep(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > 500:
            raise AssertionError("CLI kept running after its only pipeline died")
        real_sleep(0.01)

    monkeypatch.setattr("datahub_actions.cli.actions.time.sleep", short_sleep)

    result = CliRunner().invoke(actions, ["run", "-c", str(config)])

    assert isinstance(result.exception, SystemExit), result.exception
    assert result.exit_code == 1
    assert any(
        r.levelno == logging.ERROR and "No Action Pipeline is running" in r.message
        for r in caplog.records
    )
