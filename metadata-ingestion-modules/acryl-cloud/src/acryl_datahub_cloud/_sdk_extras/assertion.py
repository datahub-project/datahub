"""
This module contains the classes that represent assertions. These
classes are used to provide a user-friendly interface for creating and
managing assertions.

The actual Assertion Entity classes are defined in `metadata-ingestion/src/datahub/sdk`.
"""

from typing import Union

from acryl_datahub_cloud._sdk_extras.assertion_input import _AssertionInput


class _Assertion:
    """
    Base class that represents an assertion and contains the common properties of all assertions.
    """

    # TODO: This is a placeholder for now.
    pass


class SmartFreshnessAssertion(_Assertion):
    """
    A class that represents a smart freshness assertion.
    """

    def __init__(self, assertion_input: _AssertionInput):
        self.assertion_input = assertion_input
        # TODO: Implement creation of this user facing assertion from the assertion and monitor entity

    # TODO: This is a placeholder for now.


AssertionTypes = Union[SmartFreshnessAssertion]
