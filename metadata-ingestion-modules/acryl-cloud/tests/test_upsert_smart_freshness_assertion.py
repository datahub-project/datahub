from datetime import datetime
from typing import Optional

import pytest

from acryl_datahub_cloud._sdk_extras.assertion import (
    AssertionIncidentBehavior,
    AssertionMode,
    DetectionMechanism,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
)
from acryl_datahub_cloud._sdk_extras.assertions_client import AssertionsClient
from datahub.metadata.urns import AssertionUrn, DatasetUrn
from datahub.utilities.urns.urn import Urn


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
    assert assertion is not None
    assert assertion.urn == AssertionUrn("urn:li:assertion:smart_freshness_assertion")
    assert assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )
    assert assertion.display_name == "Smart Freshness Assertion"
    assert assertion.mode == AssertionMode.ACTIVE
    assert assertion.sensitivity == InferenceSensitivity.LOW
    assert assertion.exclusion_windows == [
        FixedRangeExclusionWindow(start=datetime(2021, 1, 1), end=datetime(2021, 1, 2))
    ]
    assert assertion.training_data_lookback_days == 30
    assert assertion.incident_behavior == [AssertionIncidentBehavior.RAISE_ON_FAIL]
    assert assertion.detection_mechanism == DetectionMechanism.INFORMATION_SCHEMA
    assert assertion.created_by == Urn.from_string("urn:li:corpuser:acryl-cloud-user")
    assert assertion.created_at == datetime(2021, 1, 1)
    assert assertion.updated_by == Urn.from_string("urn:li:corpuser:acryl-cloud-user")
    assert assertion.updated_at == datetime(2021, 1, 1)
    assert assertion.tags == []
