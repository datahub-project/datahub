import json
from pathlib import Path
from typing import Dict, List, Sequence

from datahub.ingestion.source.boomi_data_integration.models import River
from datahub.ingestion.source.boomi_data_integration.river_parser import (
    FlattenedLogic,
    flatten_logic_steps,
)

# Anonymised captures of live API responses, shared with the integration test.
API_RESPONSES_DIR = (
    Path(__file__).parents[2]
    / "integration"
    / "boomi_data_integration"
    / "api_responses"
)


def _step(step_id: str, step_type: str = "action", **extra: object) -> Dict:
    return {"type": step_type, "name": step_id, "step_id": step_id, **extra}


def _run_once(steps: Sequence[object], is_parallel: bool = False) -> Dict:
    return {
        "type": "run_once",
        "name": "container",
        "container_id": "c",
        "is_parallel": is_parallel,
        "steps": steps,
    }


def _flatten(steps: List[Dict]) -> FlattenedLogic:
    river = River.model_validate(
        {
            "cross_id": "r-1",
            "name": "Logic",
            "type": "logic",
            "properties": {"logic_steps": steps},
        }
    )
    return flatten_logic_steps(river)


def _edges(result: FlattenedLogic) -> Dict[str, List[str]]:
    return {job.step_id: job.upstream_step_ids for job in result.jobs}


def test_sequential_container_chains_steps_in_order() -> None:
    result = _flatten([_run_once([_step("a"), _step("b"), _step("c")])])
    assert _edges(result) == {"a": [], "b": ["a"], "c": ["b"]}


def test_parallel_container_steps_share_predecessor_and_join_after() -> None:
    result = _flatten(
        [
            _run_once([_step("start")]),
            _run_once([_step("p1"), _step("p2")], is_parallel=True),
            _run_once([_step("end")]),
        ]
    )
    assert _edges(result) == {
        "start": [],
        "p1": ["start"],
        "p2": ["start"],
        "end": ["p1", "p2"],
    }


def test_nested_loop_and_condition_branches() -> None:
    condition = {
        "type": "condition",
        "name": "branch",
        "steps": [
            {"condition_name": "yes", "is_else": False, "step": _step("yes")},
            {
                "condition_name": "else",
                "is_else": True,
                "step": _run_once([_step("no1"), _step("no2")]),
            },
            {"condition_name": "empty", "is_else": False, "step": None},
        ],
    }
    loop = {
        "type": "loop",
        "name": "loop",
        "loop_over_value": "{x}",
        "loop_over_variable_name": ["x"],
        "steps": [_step("l1"), condition],
    }
    result = _flatten([_run_once([_step("first"), loop, _step("last")])])
    assert _edges(result) == {
        "first": [],
        "l1": ["first"],
        "yes": ["l1"],
        "no1": ["l1"],
        "no2": ["no1"],
        # The empty branch passes l1 through.
        "last": ["yes", "no2", "l1"],
    }


def test_disabled_and_invalid_steps_are_skipped_without_breaking_the_chain() -> None:
    result = _flatten(
        [
            _run_once(
                [
                    _step("a"),
                    _step("off", is_enabled=False),
                    {"type": "action", "name": "no id"},
                    {"name": "no type"},
                    _step("b"),
                ]
            )
        ]
    )
    assert _edges(result) == {"a": [], "b": ["a"]}
    assert result.disabled == 1
    assert result.invalid == 2


def test_empty_parallel_container_passes_predecessor_through() -> None:
    result = _flatten(
        [_step("a"), _run_once([], is_parallel=True), _step("b")],
    )
    assert _edges(result) == {"a": [], "b": ["a"]}


def test_condition_branch_that_runs_nothing_passes_predecessor_through() -> None:
    condition = {
        "type": "condition",
        "name": "branch",
        "steps": [
            {"condition_name": "yes", "step": _step("yes")},
            {"condition_name": "off", "step": _step("off", is_enabled=False)},
        ],
    }
    result = _flatten([_step("a"), condition, _step("b")])
    assert _edges(result) == {"a": [], "yes": ["a"], "b": ["yes", "a"]}


def test_malformed_child_only_drops_that_child() -> None:
    result = _flatten([_run_once([_step("a"), "not an object", _step("b")])])
    assert _edges(result) == {"a": [], "b": ["a"]}
    assert result.invalid == 1


def test_live_logic_river_definition() -> None:
    # Main container: sql step, river step, parallel container (river step and
    # sql step), disabled step; then a condition whose else branch has no step.
    river = River.model_validate(
        json.loads(
            (API_RESPONSES_DIR / "river_6ae8ef6f61a8be2df8f864ca.json").read_text()
        )
    )
    result = flatten_logic_steps(river)

    sql, run_river, parallel_river, parallel_sql, final = (
        "96542bd28b184cc9b1aa2ba3b92a3fbb",
        "1d306f8dbb0d4284bf72028a8fdccaae",
        "735f9f564b8640819f8a31cf0f2990e2",
        "97e1c551d9a9456a9932712efc968c7d",
        "d00c1958b9a649efbcb3b2d493164c15",
    )
    assert _edges(result) == {
        sql: [],
        run_river: [sql],
        parallel_river: [run_river],
        parallel_sql: [run_river],
        final: [parallel_river, parallel_sql],
    }
    assert result.disabled == 1
    assert result.invalid == 0


def test_exit_steps_are_the_last_steps_to_run() -> None:
    result = _flatten(
        [
            _step("start"),
            _run_once([_step("p1"), _step("p2")], is_parallel=True),
        ]
    )
    assert result.exit_step_ids == ["p1", "p2"]
