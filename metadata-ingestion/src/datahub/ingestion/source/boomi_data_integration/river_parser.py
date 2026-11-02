from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pydantic import ValidationError

from datahub.ingestion.source.boomi_data_integration.constants import LogicNodeType
from datahub.ingestion.source.boomi_data_integration.models import (
    ConditionBranch,
    LogicNode,
    LogicProperties,
    River,
    SourceToTargetProperties,
)

RawNode = Dict[str, object]

_SEQUENTIAL_CONTAINERS = (LogicNodeType.RUN_ONCE, LogicNodeType.LOOP)


@dataclass
class LogicStepJob:
    node: LogicNode
    step_id: str
    upstream_step_ids: List[str]


@dataclass
class FlattenedLogic:
    jobs: List[LogicStepJob] = field(default_factory=list)
    disabled: int = 0
    invalid: int = 0


def parse_source_to_target(river: River) -> SourceToTargetProperties:
    return SourceToTargetProperties.model_validate(river.properties)


def flatten_logic_steps(river: River) -> FlattenedLogic:
    return _LogicFlattener().flatten(
        LogicProperties.model_validate(river.properties).logic_steps
    )


def _dedupe(ids: List[str]) -> List[str]:
    return list(dict.fromkeys(ids))


class _LogicFlattener:
    # Steps in a sequence depend on whatever ran last before them: the previous
    # step, every branch of a preceding parallel container or condition, or the
    # enclosing container's predecessors for the first step.
    def __init__(self) -> None:
        self.result = FlattenedLogic()

    def flatten(self, steps: List[RawNode]) -> FlattenedLogic:
        self._sequence(steps, upstream=[], parallel=False)
        return self.result

    def _parse(self, raw: RawNode) -> Optional[LogicNode]:
        try:
            node = LogicNode.model_validate(raw)
        except ValidationError:
            self.result.invalid += 1
            return None
        if not node.is_enabled:
            self.result.disabled += 1
            return None
        return node

    def _sequence(
        self, steps: List[RawNode], upstream: List[str], parallel: bool
    ) -> List[str]:
        current = upstream
        branch_exits: List[str] = []
        for raw in steps:
            node = self._parse(raw)
            if node is None:
                continue
            if parallel:
                branch_exits.extend(self._visit(node, upstream))
            else:
                current = self._visit(node, current)
        if parallel:
            return _dedupe(branch_exits) or upstream
        return current

    def _visit(self, node: LogicNode, upstream: List[str]) -> List[str]:
        if node.type in _SEQUENTIAL_CONTAINERS:
            parallel = node.type == LogicNodeType.RUN_ONCE and node.is_parallel
            return self._sequence(node.steps, upstream, parallel)
        if node.type == LogicNodeType.CONDITION:
            return self._condition(node, upstream)
        if not node.step_id:
            self.result.invalid += 1
            return upstream
        self.result.jobs.append(
            LogicStepJob(
                node=node,
                step_id=node.step_id,
                upstream_step_ids=_dedupe(upstream),
            )
        )
        return [node.step_id]

    def _condition(self, node: LogicNode, upstream: List[str]) -> List[str]:
        # Only one branch runs, so each branch starts from the same predecessors
        # and whatever follows the condition depends on every branch.
        exits: List[str] = []
        for raw_branch in node.steps:
            try:
                branch = ConditionBranch.model_validate(raw_branch)
            except ValidationError:
                self.result.invalid += 1
                continue
            if branch.step is None:
                continue
            child = self._parse(branch.step)
            if child is not None:
                exits.extend(self._visit(child, upstream))
        return _dedupe(exits) or upstream
