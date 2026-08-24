"""Characterization tests for the graceful-shutdown path.

SIGTERM handling (see `datahub_actions.cli.actions._register_shutdown_handlers`)
makes this path reachable in a container for the first time, so the behaviours
below now run on every container stop instead of effectively never. Each test
pins down current behaviour so a later change that bounds the shutdown path has
a baseline to change against; none of them assert desirable behaviour.
"""

from typing import Any, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub_actions.pipeline.pipeline import Pipeline, PipelineException
from datahub_actions.pipeline.pipeline_manager import PipelineManager, PipelineSpec


def _pipeline(name: str = "test_pipeline", tmp_path: Optional[Any] = None) -> Pipeline:
    return Pipeline(
        name=name,
        source=MagicMock(),
        filters=[],
        transforms=[],
        action=MagicMock(),
        retry_count=0,
        failure_mode=None,
        failed_events_dir=str(tmp_path) if tmp_path is not None else None,
    )


@pytest.fixture(autouse=True)
def _clear_registry() -> Any:
    # pipeline_registry is declared on the class, not the instance, so state
    # leaks between PipelineManager instances and therefore between tests.
    PipelineManager.pipeline_registry.clear()
    yield
    PipelineManager.pipeline_registry.clear()


def test_stop_closes_failed_events_fd_before_the_worker_is_joined(
    tmp_path: Any,
) -> None:
    """`stop()` closes the failed-events file while the worker thread is still live.

    `Pipeline.stop()` runs on the signal-handler thread and closes
    `_failed_events_fd` before `PipelineManager.stop_pipeline()` joins the worker.
    A failed event arriving in that window cannot be recorded, and
    `_append_failed_event_to_file` converts the closed-file error into a
    PipelineException -- destroying the event it exists to preserve.
    """
    pipeline = _pipeline(tmp_path=tmp_path)
    pipeline.stop()

    event = MagicMock()
    event.as_json.return_value = '{"eventType": "test"}'

    with pytest.raises(PipelineException, match="Failed to log failed event to file"):
        pipeline._append_failed_event_to_file(event)


def test_stop_all_abandons_remaining_pipelines_when_one_fails_to_stop(
    tmp_path: Any,
) -> None:
    """`stop_all()` re-raises on the first failure, so later pipelines never stop.

    Because the exception escapes, `handle_shutdown` also never reaches
    `sys.exit(0)`: the process exits non-zero on an unhandled exception and the
    surviving pipelines are left un-stopped, so their sources are never closed.
    """
    manager = PipelineManager()

    failing = _pipeline("failing", tmp_path)
    failing.stop = MagicMock(side_effect=Exception("stop failed"))  # type: ignore[method-assign]
    surviving = _pipeline("surviving", tmp_path)
    surviving.stop = MagicMock()  # type: ignore[method-assign]

    for name, pipeline in (("failing", failing), ("surviving", surviving)):
        manager.pipeline_registry[name] = PipelineSpec(name, pipeline, MagicMock())

    with pytest.raises(Exception, match="Caught exception while attempting to stop"):
        manager.stop_all()

    surviving.stop.assert_not_called()
    assert "surviving" in manager.pipeline_registry


def test_stop_pipeline_joins_the_worker_thread_without_a_timeout(tmp_path: Any) -> None:
    """`stop_pipeline()` joins with no timeout, so shutdown is unbounded.

    A worker that does not wind down keeps the process alive until the container
    runtime's grace period expires and SIGKILL lands -- at which point no offset
    is committed, which is the failure the SIGTERM handler set out to avoid.
    """
    manager = PipelineManager()
    pipeline = _pipeline("hanging", tmp_path)
    pipeline.stop = MagicMock()  # type: ignore[method-assign]
    pipeline._stats.mark_start()  # isolate the join from the stats path below
    thread = MagicMock()
    manager.pipeline_registry["hanging"] = PipelineSpec("hanging", pipeline, thread)

    manager.stop_pipeline("hanging")

    # No positional or keyword timeout: join() blocks forever by contract.
    thread.join.assert_called_once_with()


def test_pipeline_registry_is_shared_across_manager_instances(tmp_path: Any) -> None:
    """`pipeline_registry` is a class attribute, so instances share one dict."""
    first, second = PipelineManager(), PipelineManager()
    pipeline = _pipeline("shared", tmp_path)
    first.pipeline_registry["shared"] = PipelineSpec("shared", pipeline, MagicMock())

    assert "shared" in second.pipeline_registry


def test_stop_pipeline_survives_a_pipeline_that_never_started(tmp_path: Any) -> None:
    """A pipeline registered before its worker reached `mark_start()` must still stop.

    `start_pipeline` registers the spec before starting the thread, and `run()` sets
    `started_at` as its first statement, so a shutdown can land while stats are still
    empty. Printing the run summary is best-effort: it must not escalate into a stop
    failure, which would abandon every remaining pipeline.
    """
    manager = PipelineManager()
    pipeline = _pipeline("unstarted", tmp_path)
    pipeline.stop = MagicMock()  # type: ignore[method-assign]
    thread = MagicMock()
    thread.ident = None  # never started
    manager.pipeline_registry["unstarted"] = PipelineSpec("unstarted", pipeline, thread)

    manager.stop_pipeline("unstarted")

    pipeline.stop.assert_called_once()
    thread.join.assert_not_called()  # join() on an unstarted thread raises
    assert "unstarted" not in manager.pipeline_registry


def test_start_pipeline_registers_before_starting_the_worker(tmp_path: Any) -> None:
    """The worker must be reachable by stop_all() the instant it can be running.

    If the thread were started first, a shutdown signal arriving in the gap would not
    find the pipeline, so nothing would stop it -- and being non-daemon it would then
    block interpreter exit until the runtime gave up and sent SIGKILL.
    """
    manager = PipelineManager()
    pipeline = _pipeline("ordered", tmp_path)
    registered_when_started: List[bool] = []

    def fake_start() -> None:
        registered_when_started.append("ordered" in manager.pipeline_registry)

    with patch("datahub_actions.pipeline.pipeline_manager.Thread") as thread_cls:
        thread_cls.return_value.start.side_effect = fake_start
        manager.start_pipeline("ordered", pipeline)

    assert registered_when_started == [True], (
        "pipeline was not in the registry at the moment its worker started"
    )
