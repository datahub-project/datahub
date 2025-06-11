from typing import Literal, Optional

from datahub_integrations.telemetry.telemetry import BaseEvent

DEFAULT_USER_URN = "urn:li:corpuser:admin"


class InferDocsApiRequestEvent(BaseEvent):
    """Event representing a request to infer docs for a dataset."""

    type: Literal["InferDocsApiRequest"] = "InferDocsApiRequest"
    entity_type: Literal["dataset", "query"]

    entity_urn: str
    user_urn: str


class InferDocsApiResponseEvent(BaseEvent):
    """Event representing a response from the infer docs API."""

    type: Literal["InferDocsApiResponse"] = "InferDocsApiResponse"
    entity_type: Literal["dataset", "query"]

    entity_urn: str
    user_urn: str

    response_time_ms: float
    error_msg: Optional[str] = None

    has_entity_description: Optional[bool] = None
    has_column_descriptions: Optional[bool] = None
