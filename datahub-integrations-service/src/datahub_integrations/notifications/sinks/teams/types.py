from typing import Optional

from pydantic import BaseModel


class TeamsMessageDetails(BaseModel):
    """
    Model to store details about a Teams message relevant to an incident.
    """

    conversation_id: (
        str  # The Teams conversation id associated with the incident message.
    )
    conversation_name: Optional[str] = (
        None  # The human-readable conversation name associated with the incident message.
    )
    message_id: str  # The Teams message id associated with the incident.
    recipient_id: str  # The Teams recipient id (user or channel).
