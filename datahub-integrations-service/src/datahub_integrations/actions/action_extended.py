from abc import ABC, abstractmethod

from datahub_actions.action.action import Action


# Define the wrapper interface with additional methods
class ExtendedAction(Action, ABC):
    """
    A wrapper interface that adds additional methods like rollback to the Action class.
    """

    @abstractmethod
    def rollback(self) -> None:
        """Rollback the action."""
        pass

    @abstractmethod
    def bootstrap(self) -> None:
        """Bootstrap the action."""
        pass
