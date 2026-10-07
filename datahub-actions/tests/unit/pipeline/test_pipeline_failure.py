import logging
import threading
import time
from typing import Any, Dict, Iterable, Iterator, List, cast
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner, Result
from confluent_kafka import KafkaError, KafkaException

from datahub_actions.cli.actions import actions
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager

_EXITING_ON_FAILURE = "No Action Pipeline is running any more"


class _StillRunning(Exception):
    """Raised from the CLI's sleep to end a CLI that would otherwise loop forever."""


@pytest.fixture(autouse=True)
def _clear_registry() -> Iterator[None]:
    PipelineManager.pipeline_registry.clear()
    yield
    PipelineManager.pipeline_registry.clear()


def _pipeline(name: str, source: MagicMock) -> Pipeline:
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


def _failing_pipeline(name: str) -> Pipeline:
    source = MagicMock()
    source.events.side_effect = KafkaException(
        KafkaError(KafkaError._TRANSPORT, "Failed to get metadata")
    )
    return _pipeline(name, source)


def _finished_pipeline(name: str) -> Pipeline:
    # A source whose iterator ends, e.g. the DataHub Cloud source after
    # kill_after_idle_timeout.
    source = MagicMock()
    source.events.return_value = iter([])
    return _pipeline(name, source)


def _blocking_pipeline(name: str, release: threading.Event) -> Pipeline:
    def events() -> Iterable[Any]:
        release.wait(timeout=10)
        return iter([])

    source = MagicMock()
    source.events.side_effect = events
    return _pipeline(name, source)


def _run_cli(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
    pipelines: Dict[str, Pipeline],
    max_sleeps: int = 500,
) -> Result:
    configs: List[str] = []
    for name in pipelines:
        config = tmp_path / f"{name}.yaml"
        config.write_text(
            f"name: {name}\nsource:\n  type: kafka\naction:\n  type: hello_world\n"
        )
        configs += ["-c", str(config)]
    monkeypatch.setattr(
        "datahub_actions.cli.actions.pipeline_config_to_pipeline",
        lambda cfg: pipelines[cfg["name"]],
    )
    real_sleep = time.sleep
    sleeps = 0

    def short_sleep(_: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps > max_sleeps:
            raise _StillRunning()
        real_sleep(0.01)

    monkeypatch.setattr("datahub_actions.cli.actions.time.sleep", short_sleep)
    return CliRunner().invoke(actions, ["run", *configs])


def _logged_exiting_on_failure(caplog: pytest.LogCaptureFixture) -> bool:
    return any(
        r.levelno == logging.ERROR and _EXITING_ON_FAILURE in r.message
        for r in caplog.records
    )


def test_source_failure_is_logged_and_pipeline_stops(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = PipelineManager()
    pipeline = _failing_pipeline("broken")

    manager.start_pipeline("broken", pipeline)
    manager.pipeline_registry["broken"].thread.join(timeout=5)

    assert not manager.has_running_pipelines()
    assert manager.has_failed_pipelines()
    # The source is closed rather than left half-open behind a dead thread.
    cast(MagicMock, pipeline.source).close.assert_called_once()
    assert any(
        r.levelno == logging.ERROR and "broken" in r.message for r in caplog.records
    )


def test_cli_exits_non_zero_once_no_pipeline_is_running_after_a_failure(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = _run_cli(tmp_path, monkeypatch, {"broken": _failing_pipeline("broken")})

    assert isinstance(result.exception, SystemExit), result.exception
    assert result.exit_code == 1
    assert _logged_exiting_on_failure(caplog)


def test_cli_exits_zero_when_every_pipeline_finished_cleanly(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = _run_cli(
        tmp_path,
        monkeypatch,
        {
            "idle_a": _finished_pipeline("idle_a"),
            "idle_b": _finished_pipeline("idle_b"),
        },
    )

    # sys.exit(0) reads as a clean run to CliRunner, so no exception is recorded;
    # a CLI still looping would have raised _StillRunning.
    assert result.exception is None, result.exception
    assert result.exit_code == 0
    assert not _logged_exiting_on_failure(caplog)


def test_cli_exits_non_zero_when_one_pipeline_failed_and_the_rest_finished(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    result = _run_cli(
        tmp_path,
        monkeypatch,
        {"broken": _failing_pipeline("broken"), "idle": _finished_pipeline("idle")},
    )

    assert isinstance(result.exception, SystemExit), result.exception
    assert result.exit_code == 1
    assert _logged_exiting_on_failure(caplog)


def test_cli_keeps_running_while_another_pipeline_is_still_running(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    release = threading.Event()
    try:
        result = _run_cli(
            tmp_path,
            monkeypatch,
            {
                "broken": _failing_pipeline("broken"),
                "alive": _blocking_pipeline("alive", release),
            },
            max_sleeps=50,
        )
    finally:
        release.set()

    assert isinstance(result.exception, _StillRunning), result.exception
    assert not _logged_exiting_on_failure(caplog)
