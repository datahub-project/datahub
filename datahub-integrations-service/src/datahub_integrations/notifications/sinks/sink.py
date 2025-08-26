from abc import ABC, abstractmethod

from datahub.metadata.schema_classes import NotificationRequestClass

from datahub_integrations.notifications.sinks.context import NotificationContext


class NotificationSink(ABC):
    """
    A notification sink represents a destination to which notifications can be sent.

    Notification sinks are dumb components with a single job: send a notification to a single
    recipient (DataHub Actor or custom recipient)

    By the time a sink receives it, a message is presumed safe to send. A notification sink does
    not perform any additional validation against a user's preferences.
    """

    @abstractmethod
    def type(self) -> str:
        """
        Returns the NotificationSinkType corresponding to the sink.
        """
        pass

    @abstractmethod
    def init(self) -> None:
        """
        Initializes a notification sink.
        """
        pass

    @abstractmethod
    def send(
        self, request: NotificationRequestClass, context: NotificationContext
    ) -> None:
        """
        Sends a notification to one or more recipients based on a NotificationRequest.
        """
        pass
