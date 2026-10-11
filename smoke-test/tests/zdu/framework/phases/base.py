from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal


@dataclass
class PhaseResult:
    phase_name: str
    status: Literal["passed", "failed", "skipped"]
    started_at: datetime = field(default_factory=datetime.utcnow)
    duration_s: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class Phase(abc.ABC):
    """Base class for phases whose ``run`` takes only ``ctx``.

    See ``ConfiguredPhase`` for phases that also need the live
    ``ZDUTestConfig`` (most setup-style phases). Splitting the bases makes
    the runner's dispatch a typed ``isinstance`` check instead of the prior
    ``phase.run(ctx, config)  # type: ignore[call-arg]`` pattern that
    silenced mypy across five phases.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def run(self, ctx: Any) -> PhaseResult: ...


class ConfiguredPhase(abc.ABC):
    """Base class for phases that need the live ZDUTestConfig in ``run``.

    Subclassed by ``preflight``, ``build_images``, ``setup_old_stack``,
    ``nuke_and_redeploy``, and ``prepare_old_stack`` — the phases that mutate
    config (build_images sets old/new image tags) or read config-driven
    behavior flags (clean_build, refresh_token) inside ``run``.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def run(self, ctx: Any, config: Any) -> PhaseResult: ...
