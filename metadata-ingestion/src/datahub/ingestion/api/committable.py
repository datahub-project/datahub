from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, TypeVar


class CommitPolicy(Enum):
    ALWAYS = auto()
    ON_NO_ERRORS = auto()
    ON_NO_ERRORS_AND_NO_WARNINGS = auto()


@dataclass
class Committable(ABC):
    name: str
    commit_policy: CommitPolicy

    @abstractmethod
    def commit(self) -> None:
        pass


StateType = TypeVar("StateType")


class StatefulCommittable(
    Committable,
    Generic[StateType],
):
    def __init__(
        self, name: str, commit_policy: CommitPolicy, state_to_commit: StateType
    ):
        super().__init__(name=name, commit_policy=commit_policy)
        self.committed: bool = False
        self.state_to_commit: StateType = state_to_commit

    def has_successfully_committed(self) -> bool:
        return bool(not self.state_to_commit or self.committed)
