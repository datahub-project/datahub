from typing import Optional

import pytest

from acryl_datahub_cloud._sdk_extras.assertion_input import (
    DEFAULT_DETECTION_MECHANISM,
    DEFAULT_NAME_PREFIX,
    DEFAULT_NAME_SUFFIX_LENGTH,
    DEFAULT_SENSITIVITY,
    AssertionUrn,
)
from acryl_datahub_cloud._sdk_extras.assertions_client import AssertionsClient


class DummyDataHubClient:
    pass


@pytest.mark.parametrize(
    "assertion_urn",
    [
        AssertionUrn("urn:li:assertion:123"),
        None,
    ],
)
def test_upsert_smart_freshness_assertion_with_default_values(
    assertion_urn: Optional[AssertionUrn],
) -> None:
    assertions_client = AssertionsClient(client=DummyDataHubClient())  # type: ignore[arg-type]  # Ignore type check for dummy client
    assertion = assertions_client.upsert_smart_freshness_assertion(
        urn=assertion_urn,
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
    )
    # TODO: Make assertions on the Assertion Entity once created, not the AssertionInput entity
    assert assertion.assertion_input.urn == assertion_urn
    assert assertion.assertion_input.name.startswith(DEFAULT_NAME_PREFIX)
    assert (
        len(assertion.assertion_input.name)
        == len(DEFAULT_NAME_PREFIX) + DEFAULT_NAME_SUFFIX_LENGTH + 1
    )  # +1 for the dash
    assert assertion.assertion_input.detection_mechanism == DEFAULT_DETECTION_MECHANISM
    assert assertion.assertion_input.sensitivity == DEFAULT_SENSITIVITY
    assert assertion.assertion_input.exclusion_windows == []
    assert assertion.assertion_input.training_data_lookback_days == 60
    assert assertion.assertion_input.incident_behavior == []
