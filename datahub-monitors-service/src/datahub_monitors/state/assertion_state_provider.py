from typing import Optional

from datahub_monitors.assertion.types import AssertionState, AssertionStateType


class AssertionStateProvider:
    """Base class for all monitor state providers"""

    def get_state(
        self,
        entity_urn: str,
        assertion_state_type: AssertionStateType,
    ) -> Optional[AssertionState]:
        raise NotImplementedError()

    def save_state(
        self,
        entity_urn: str,
        state: AssertionState,
    ) -> None:
        raise NotImplementedError()
