from datetime import datetime

import pytest

from acryl_datahub_cloud._sdk_extras.assertion import (
    AssertionMode,
    SmartFreshnessAssertion,
)
from acryl_datahub_cloud._sdk_extras.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
    FixedRangeExclusionWindow,
    InferenceSensitivity,
)
from datahub.metadata.urns import AssertionUrn, DatasetUrn
from datahub.utilities.urns.urn import Urn


def test_smart_freshness_assertion_creation_min_fields() -> None:
    """Make sure the SmartFreshnessAssertion can be created with the minimum required fields."""
    SmartFreshnessAssertion(
        urn=AssertionUrn("urn:li:assertion:smart_freshness_assertion"),
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Smart Freshness Assertion",
        mode=AssertionMode.ACTIVE,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2021, 1, 1), end=datetime(2021, 1, 2)
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        created_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
        created_at=datetime(2021, 1, 1),
        updated_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
        updated_at=datetime(2021, 1, 1),
        tags=[],
    )


def test_smart_freshness_assertion_all_attributes_are_readonly() -> None:
    """Test that all attributes of SmartFreshnessAssertion are read-only."""
    assertion = SmartFreshnessAssertion(
        urn=AssertionUrn("urn:li:assertion:smart_freshness_assertion"),
        dataset_urn=DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
        ),
        display_name="Smart Freshness Assertion",
        mode=AssertionMode.ACTIVE,
        sensitivity=InferenceSensitivity.LOW,
        exclusion_windows=[
            FixedRangeExclusionWindow(
                start=datetime(2021, 1, 1), end=datetime(2021, 1, 2)
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        created_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
        created_at=datetime(2021, 1, 1),
        updated_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
        updated_at=datetime(2021, 1, 1),
        tags=[],
    )

    public_attributes_and_methods = [
        attr
        for attr in dir(assertion)
        if not attr.startswith("_") and not attr.startswith("__")
    ]

    settable_attributes = [
        attr
        for attr in public_attributes_and_methods
        if not callable(getattr(assertion, attr))
    ]

    # Test that each attribute cannot be set
    for attr in settable_attributes:
        with pytest.raises(
            AttributeError,
            # Different error message on Python 3.10+
            match=f"(can't set attribute '{attr}'|property '{attr}' of '{assertion.__class__.__name__}' object has no setter)",
        ):
            setattr(assertion, attr, getattr(assertion, attr))
