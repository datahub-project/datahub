from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, List, Optional, TypeVar


class CommitPolicy(Enum):
    ALWAYS = auto()
    ON_NO_ERRORS = auto()
    ON_NO_ERRORS_AND_NO_WARNINGS = auto()


@dataclass
class _CommittableConcrete:
    name: str
    commit_policy: CommitPolicy
    committed: bool


# The concrete portion Committable is separated from the abstract portion due to
# https://github.com/python/mypy/issues/5374#issuecomment-568335302.
class Committable(_CommittableConcrete, ABC):
    def __init__(self, name: str, commit_policy: CommitPolicy):
        super(Committable, self).__init__(name, commit_policy, committed=False)

    @abstractmethod
    def commit(self) -> None:
        pass


StateKeyType = TypeVar("StateKeyType")
StateType = TypeVar("StateType")
# TODO: Add a better alternative to a string for the filter.
FilterType = TypeVar("FilterType")


class _StatefulCommittableConcrete(Generic[StateType]):
    def __init__(self, state_to_commit: StateType):
        self.state_to_commit: StateType = state_to_commit


class StatefulCommittable(
    Committable,
    _StatefulCommittableConcrete[StateType],
    Generic[StateKeyType, StateType, FilterType],
):
    def __init__(
        self, name: str, commit_policy: CommitPolicy, state_to_commit: StateType
    ):
        # _ConcreteCommittable will be the first from this class.
        super(StatefulCommittable, self).__init__(
            name=name, commit_policy=commit_policy
        )
        # _StatefulCommittableConcrete will be after _CommittableConcrete in the __mro__.
        super(_CommittableConcrete, self).__init__(state_to_commit=state_to_commit)

    def has_successfully_committed(self) -> bool:
        return True if not self.state_to_commit or self.committed else False

    @abstractmethod
    def get_previous_states(
        self,
        state_key: StateKeyType,
        last_only: bool = True,
        filter_opt: Optional[FilterType] = None,
    ) -> List[StateType]:
        pass
