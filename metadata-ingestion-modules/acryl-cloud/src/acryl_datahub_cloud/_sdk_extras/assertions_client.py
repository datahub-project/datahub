from __future__ import annotations

from typing import TYPE_CHECKING, Union

from acryl_datahub_cloud._sdk_extras.assertion import Assertion
from datahub.metadata.urns import AssertionUrn

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class AssertionsClient:
    def __init__(self, client: DataHubClient):
        self.client = client

    def get_assertions(
        self, urn: Union[str, list[str], AssertionUrn, list[AssertionUrn]]
    ) -> list[Assertion]:
        print(
            "get_assertions is not implemented, this is a placeholder. Returning empty list."
        )
        print(f"urn provided: {urn}")
        return []
