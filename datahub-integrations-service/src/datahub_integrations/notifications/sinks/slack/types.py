from typing import Optional

from pydantic import BaseModel


class SlackMessageDetails(BaseModel):
    """
    Model to store details about a Slack message relevant to an incident.
    """

    channel_id: str  # The Slack channel id associated with the incident message.
    channel_name: Optional[str] = (
        None  # The human-readable channel name associated with the incident message.
    )
    message_id: str  # The Slack message id associated with the incident.
