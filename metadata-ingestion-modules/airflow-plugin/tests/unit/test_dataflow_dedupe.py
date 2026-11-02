"""Unit tests for task-event DataFlow emission and its per-DAG-run dedupe."""

import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import MagicMock, patch

import datahub_airflow_plugin.airflow3.datahub_listener as listener_mod
from datahub.metadata.schema_classes import StatusClass
from datahub_airflow_plugin._dataflow_dedupe import DagRunEmissionTracker
from datahub_airflow_plugin.airflow3.datahub_listener import DataHubListener

FLOW_URN = "urn:li:dataFlow:(airflow,scrub,prod)"


class TestDagRunEmissionTracker:
    def test_first_claim_wins_and_repeat_is_skipped(self, tmp_path: Path) -> None:
        tracker = DagRunEmissionTracker(marker_dir=tmp_path)
        assert tracker.claim("scrub", "run_1")
        assert not tracker.claim("scrub", "run_1")
        assert tracker.claim("scrub", "run_2")
        assert tracker.claim("other", "run_1")

    def test_marker_dedupes_across_processes(self, tmp_path: Path) -> None:
        assert DagRunEmissionTracker(marker_dir=tmp_path).claim("scrub", "run_1")
        # A fresh tracker stands in for another task process on the same worker.
        assert not DagRunEmissionTracker(marker_dir=tmp_path).claim("scrub", "run_1")

    def test_stale_marker_is_reclaimed(self, tmp_path: Path) -> None:
        DagRunEmissionTracker(marker_dir=tmp_path).claim("scrub", "run_1")
        (marker,) = list(tmp_path.iterdir())
        old = time.time() - 120
        os.utime(marker, (old, old))
        tracker = DagRunEmissionTracker(marker_dir=tmp_path, marker_ttl_seconds=60)
        assert tracker.claim("scrub", "run_1")
        assert marker.stat().st_mtime > old

    def test_unusable_marker_dir_fails_open(self, tmp_path: Path) -> None:
        not_a_dir = tmp_path / "file"
        not_a_dir.write_text("")
        tracker = DagRunEmissionTracker(marker_dir=not_a_dir)
        assert tracker.claim("scrub", "run_1")
        # The in-process cache still dedupes when markers are unavailable.
        assert not tracker.claim("scrub", "run_1")

    def test_evicted_entry_falls_back_to_marker(self, tmp_path: Path) -> None:
        tracker = DagRunEmissionTracker(marker_dir=tmp_path, max_entries=1)
        assert tracker.claim("scrub", "run_1")
        assert tracker.claim("scrub", "run_2")
        assert not tracker.claim("scrub", "run_1")


def _dag_run(run_id: str = "run_1") -> Any:
    dag = SimpleNamespace(
        dag_id="scrub", tasks=[SimpleNamespace(task_id=t) for t in ("a", "b", "c")]
    )
    return SimpleNamespace(dag_id="scrub", run_id=run_id, dag=dag)


def _on_dag_start_stub(emitter: MagicMock) -> Any:
    return SimpleNamespace(
        config=SimpleNamespace(platform_instance=None),
        _get_emitter=lambda: emitter,
        _make_emit_callback=lambda: None,
    )


def _datajob_status_urns(emitter: MagicMock) -> List[str]:
    return [
        call.args[0].entityUrn
        for call in emitter.emit.call_args_list
        if isinstance(call.args[0].aspect, StatusClass)
        and call.args[0].entityUrn.startswith("urn:li:dataJob:")
    ]


class TestOnDagStartTaskStatuses:
    def _run(self, emit_task_statuses: bool) -> MagicMock:
        emitter = MagicMock()
        dataflow = SimpleNamespace(
            urn=FLOW_URN, tags=[], description=None, emit=MagicMock()
        )
        with patch.object(
            listener_mod.AirflowGenerator, "generate_dataflow", return_value=dataflow
        ):
            DataHubListener.on_dag_start(
                _on_dag_start_stub(emitter),  # type: ignore[arg-type]
                _dag_run(),
                emit_task_statuses=emit_task_statuses,
            )
        dataflow.emit.assert_called_once()
        return emitter

    def test_dag_run_event_marks_every_task(self) -> None:
        assert len(_datajob_status_urns(self._run(emit_task_statuses=True))) == 3

    def test_task_event_skips_per_task_statuses(self) -> None:
        assert _datajob_status_urns(self._run(emit_task_statuses=False)) == []


class TestEmitDataflowForTaskEvent:
    def _stub(self, dedupe: bool, marker_dir: Path) -> Any:
        return SimpleNamespace(
            config=SimpleNamespace(dedupe_dataflow_per_dag_run=dedupe),
            _dataflow_emission_tracker=DagRunEmissionTracker(marker_dir=marker_dir),
            on_dag_start=MagicMock(),
        )

    def test_emits_on_every_task_without_dedupe(self, tmp_path: Path) -> None:
        stub = self._stub(dedupe=False, marker_dir=tmp_path)
        for _ in range(3):
            DataHubListener._emit_dataflow_for_task_event(stub, _dag_run())  # type: ignore[arg-type]
        assert stub.on_dag_start.call_count == 3
        for call in stub.on_dag_start.call_args_list:
            assert call.kwargs == {"emit_task_statuses": False}

    def test_emits_once_per_dag_run_with_dedupe(self, tmp_path: Path) -> None:
        stub = self._stub(dedupe=True, marker_dir=tmp_path)
        for run_id in ["run_1", "run_1", "run_1", "run_2"]:
            DataHubListener._emit_dataflow_for_task_event(stub, _dag_run(run_id))  # type: ignore[arg-type]
        assert stub.on_dag_start.call_count == 2
