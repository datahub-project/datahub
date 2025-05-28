from datetime import datetime, timezone
from typing import Optional

import pytest

from acryl_datahub_cloud._sdk_extras.assertion import (
    AssertionMode,
    DetectionMechanism,
    InferenceSensitivity,
)
from acryl_datahub_cloud._sdk_extras.assertions_client import AssertionsClient
from datahub.metadata.urns import AssertionUrn, DatasetUrn
from datahub.utilities.urns.urn import Urn
from tests.conftest import StubDataHubClient


@pytest.mark.parametrize(
    "assertion_urn",
    [
        AssertionUrn("urn:li:assertion:123"),
        # None, # TODO: Enable this
    ],
)
def test_upsert_smart_freshness_assertion_with_default_values(
    stub_datahub_client: StubDataHubClient,
    assertion_urn: Optional[AssertionUrn],
) -> None:
    assertions_client = AssertionsClient(client=stub_datahub_client)  # type: ignore[arg-type]  # Ignore type check for dummy client

    assertion = assertions_client.upsert_smart_freshness_assertion(
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
        urn=assertion_urn,
        # TODO: Remove the below placeholder fields once everything is connected and implemented, and create
        # a separate test with all of the fields set:
        display_name="Smart Freshness Assertion",
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[],
        training_data_lookback_days=60,
        incident_behavior=[],
    )
    assert assertion is not None
    assert assertion.urn == AssertionUrn("urn:li:assertion:123")
    assert assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)"
    )
    assert assertion.display_name == "Smart Freshness Assertion"
    assert assertion.mode == AssertionMode.ACTIVE
    assert assertion.sensitivity == InferenceSensitivity.LOW
    assert assertion.exclusion_windows == []
    assert assertion.training_data_lookback_days == 60
    assert assertion.incident_behavior == []
    assert assertion.detection_mechanism == DetectionMechanism.INFORMATION_SCHEMA
    assert assertion.created_by == Urn.from_string("urn:li:corpuser:acryl-cloud-user")
    assert assertion.created_at == datetime(2021, 1, 1, tzinfo=timezone.utc)
    assert assertion.updated_by == Urn.from_string("urn:li:corpuser:acryl-cloud-user")
    assert assertion.updated_at == datetime(2021, 1, 1, tzinfo=timezone.utc)
    assert assertion.tags == []
