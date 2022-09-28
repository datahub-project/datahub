from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, List, Optional, TypeVar


class CommitPolicy(Enum):
    ALWAYS = auto()
    ON_NO_ERRORS = auto()
    ON_NO_ERRORS_AND_NO_WARNINGS = auto()


@dataclass
class Committable(ABC):
    name: str
    commit_policy: CommitPolicy
    committed: bool = False

    @abstractmethod
    def commit(self) -> None:
        pass


StateKeyType = TypeVar("StateKeyType")
StateType = TypeVar("StateType")
# TODO: Add a better alternative to a string for the filter.
FilterType = TypeVar("FilterType")


class StatefulCommittable(
    Committable,
    Generic[StateKeyType, StateType, FilterType],
):
    def __init__(
        self, name: str, commit_policy: CommitPolicy, state_to_commit: StateType
    ):
        super().__init__(name=name, commit_policy=commit_policy)
        self.state_to_commit: StateType = state_to_commit

    def has_successfully_committed(self) -> bool:
        return bool(not self.state_to_commit or self.committed)

    @abstractmethod
    def get_previous_states(
        self,
        state_key: StateKeyType,
        last_only: bool = True,
        filter_opt: Optional[FilterType] = None,
    ) -> List[StateType]:
        pass
