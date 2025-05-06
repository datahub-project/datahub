"""
This file contains the Assertion class, which is used to represent an assertion in DataHub.

The Assertion class is currently not implemented, this is a placeholder for future implementation.
"""

from typing import Union

from datahub.metadata.urns import AssertionUrn


class Assertion:
    def __init__(self, urn: Union[str, AssertionUrn]):
        print(f"The Assertion class is currently not implemented. Urn provided: {urn}")
        self.urn = AssertionUrn(urn)
