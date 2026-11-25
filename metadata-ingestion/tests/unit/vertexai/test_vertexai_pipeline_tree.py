import json
from dataclasses import replace
from functools import lru_cache
from typing import Dict, List, Tuple

import pytest
from google.cloud.aiplatform_v1.types import (
    PipelineJob as PipelineJobType,
    PipelineTaskDetail,
)

from datahub.ingestion.source.vertexai.vertexai_pipeline_tree import (
    PipelineStep,
    TaskRun,
    build_run_tree,
    build_step_tree,
    is_started,
)
from tests.integration.vertexai.mock_vertexai import PIPELINE_TASKS_FIXTURE_DIR

State = PipelineTaskDetail.State


@lru_cache(maxsize=None)
def _load(name: str) -> PipelineJobType:
    return PipelineJobType.from_json(
        (PIPELINE_TASKS_FIXTURE_DIR / f"{name}.json").read_text(),
        ignore_unknown_fields=True,
    )


def _run_name(name: str) -> str:
    return _load(name).name.rsplit("/", 1)[-1]


def _steps(name: str) -> Dict[str, PipelineStep]:
    return build_step_tree(_load(name).pipeline_spec)


def _details(name: str) -> List[PipelineTaskDetail]:
    return list(_load(name).job_detail.task_details)


def _detail(name: str, task_name: str) -> PipelineTaskDetail:
    return next(d for d in _details(name) if d.task_name == task_name)


def test_build_step_tree_paths() -> None:
    assert set(_steps("nested_run")) == {
        "announce",
        "exit-handler-1",
        "exit-handler-1.importer",
        "exit-handler-1.model-upload",
        "exit-handler-1.sdk-upload",
        "exit-handler-1.importer-2",
        "exit-handler-1.use-model",
        "exit-handler-1.branch-a",
        "exit-handler-1.branch-b",
        "exit-handler-1.branch-a.train",
        "exit-handler-1.branch-b.train",
    }
    loop_paths = set(_steps("loop_run"))
    assert "for-loop-2.evaluate" in loop_paths
    assert not any("worker" in path for path in loop_paths)
    assert "condition-1.emit-2" in _steps("failure_run")


def test_build_step_tree_names_and_flags() -> None:
    nested = _steps("nested_run")
    assert nested["exit-handler-1.use-model"].shown_name == "check-champion"
    assert _steps("failure_run")["condition-1"].shown_name == "never-true"

    assert _steps("loop_run")["for-loop-2"].is_loop
    assert nested["exit-handler-1.branch-a"].parent_path == "exit-handler-1"


def test_artifact_loop_is_a_loop() -> None:
    spec = json.loads((PIPELINE_TASKS_FIXTURE_DIR / "loop_run.json").read_text())[
        "pipelineSpec"
    ]
    loop_task = spec["root"]["dag"]["tasks"]["for-loop-2"]
    del loop_task["parameterIterator"]
    loop_task["artifactIterator"] = {
        "itemInput": "pipelinechannel--datasets-loop-item",
        "items": {"inputArtifact": "pipelinechannel--datasets"},
    }
    steps = build_step_tree(spec)
    assert steps["for-loop-2"].is_loop

    tree = build_run_tree(steps, _details("loop_run"), run_name=_run_name("loop_run"))
    evaluate_runs = [r for r in tree.runs if r.step.path == "for-loop-2.evaluate"]
    assert len(evaluate_runs) == 2
    assert {r.parent_run_path for r in evaluate_runs} == {"for-loop-2"}
    assert tree.unmatched == []


def test_build_step_tree_upstreams() -> None:
    nested = _steps("nested_run")
    assert nested["exit-handler-1.model-upload"].upstream_paths == (
        "exit-handler-1.importer",
    )
    assert nested["announce"].upstream_paths == ("exit-handler-1",)


def test_build_step_tree_rejects_malformed_spec() -> None:
    with pytest.raises(TypeError):
        build_step_tree({"root": {"dag": {"tasks": ["not", "a", "mapping"]}}})
    assert build_step_tree({}) == {}
    with pytest.raises(TypeError):
        build_step_tree(
            {"root": {"dag": {"tasks": {"a": {"dependentTasks": [{"not": "text"}]}}}}}
        )


def test_build_step_tree_stops_at_self_containing_component() -> None:
    spec = {
        "root": {"dag": {"tasks": {"a": {"componentRef": {"name": "comp-a"}}}}},
        "components": {
            "comp-a": {"dag": {"tasks": {"b": {"componentRef": {"name": "comp-a"}}}}}
        },
    }
    assert set(build_step_tree(spec)) == {"a", "a.b"}


def test_is_started() -> None:
    assert is_started(_detail("failure_run", "boom"))
    assert not is_started(_detail("failure_run", "condition-1"))
    assert not is_started(_detail("failure_run", "emit-3"))


def test_build_run_tree_parents() -> None:
    nested = build_run_tree(
        _steps("nested_run"), _details("nested_run"), run_name=_run_name("nested_run")
    )
    by_step = {run.step.path: run for run in nested.runs}
    assert by_step["exit-handler-1.branch-a.train"].parent_run_path == (
        "exit-handler-1.branch-a"
    )
    assert by_step["announce"].parent_run_path is None

    loop = build_run_tree(
        _steps("loop_run"), _details("loop_run"), run_name=_run_name("loop_run")
    )
    evaluate_runs = [r for r in loop.runs if r.step.path == "for-loop-2.evaluate"]
    assert sorted(r.run_path for r in evaluate_runs) == [
        "for-loop-2.for-loop-2-worker-0.evaluate",
        "for-loop-2.for-loop-2-worker-1.evaluate",
    ]
    assert {r.parent_run_path for r in evaluate_runs} == {"for-loop-2"}
    assert not any("worker" in r.step.path for r in loop.runs)
    assert loop.unmatched == []


def test_build_run_tree_started_only() -> None:
    failure = build_run_tree(
        _steps("failure_run"),
        _details("failure_run"),
        run_name=_run_name("failure_run"),
    )
    assert {r.step.path for r in failure.runs} == {"emit", "boom"}

    running = build_run_tree(
        _steps("running_run"),
        _details("running_run"),
        run_name=_run_name("running_run"),
    )
    assert "for-loop-2" not in {r.step.path for r in running.runs}


def _upstreams(runs: List[TaskRun], step_path: str) -> Tuple[str, ...]:
    return next(r for r in runs if r.step.path == step_path).upstream_run_paths


def test_build_run_tree_upstreams() -> None:
    loop = build_run_tree(
        _steps("loop_run"), _details("loop_run"), run_name=_run_name("loop_run")
    )
    assert _upstreams(loop.runs, "check-registry-model") == ("importer",)

    nested = build_run_tree(
        _steps("nested_run"), _details("nested_run"), run_name=_run_name("nested_run")
    )
    assert _upstreams(nested.runs, "announce") == ("exit-handler-1",)

    steps = _steps("failure_run")
    steps["boom"] = replace(steps["boom"], upstream_paths=("emit", "condition-1"))
    failure = build_run_tree(
        steps, _details("failure_run"), run_name=_run_name("failure_run")
    )
    assert _upstreams(failure.runs, "boom") == ("emit",)


def _started_detail(
    name: str, task_id: int, parent_task_id: int, *, like: PipelineTaskDetail
) -> PipelineTaskDetail:
    return PipelineTaskDetail(
        task_id=task_id,
        parent_task_id=parent_task_id,
        task_name=name,
        state=State.SUCCEEDED,
        start_time=like.start_time,
    )


def test_build_run_tree_reports_unmatched() -> None:
    details = _details("nested_run")
    root = next(d for d in details if d.parent_task_id == 0)
    ghost = _started_detail("ghost", 7, root.task_id, like=root)
    tree = build_run_tree(
        _steps("nested_run"), details + [ghost], run_name=_run_name("nested_run")
    )
    assert tree.unmatched == ["ghost"]
    assert "ghost" not in {r.run_path for r in tree.runs}


def test_build_run_tree_reports_orphans_as_paths() -> None:
    details = _details("nested_run")
    root = next(d for d in details if d.parent_task_id == 0)
    orphan = _started_detail("lost", 7, 99, like=root)
    child = _started_detail("kid", 8, 7, like=root)
    tree = build_run_tree(
        _steps("nested_run"),
        details + [orphan, child],
        run_name=_run_name("nested_run"),
    )
    assert tree.unmatched == ["lost", "lost.kid"]
    assert len(tree.runs) == 11


def test_build_run_tree_survives_parent_cycle() -> None:
    root = _details("nested_run")[0]
    first = _started_detail("x", 1, 2, like=root)
    second = _started_detail("y", 2, 1, like=root)
    tree = build_run_tree({}, [first, second], run_name=_run_name("nested_run"))
    assert tree.runs == []
    assert set(tree.unmatched) == {"y.x", "y"}


def test_build_run_tree_parent_is_a_started_step() -> None:
    details = [
        PipelineTaskDetail(d, state=State.NOT_TRIGGERED)
        if d.task_name == "exit-handler-1"
        else d
        for d in _details("nested_run")
    ]
    tree = build_run_tree(
        _steps("nested_run"), details, run_name=_run_name("nested_run")
    )
    by_step = {run.step.path: run for run in tree.runs}
    assert "exit-handler-1" not in by_step
    assert by_step["exit-handler-1.importer"].parent_run_path is None
    assert by_step["exit-handler-1.branch-a.train"].parent_run_path == (
        "exit-handler-1.branch-a"
    )
