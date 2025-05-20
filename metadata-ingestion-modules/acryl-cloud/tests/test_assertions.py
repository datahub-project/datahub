from datetime import datetime, timezone

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
from acryl_datahub_cloud._sdk_extras.entities.assertion import Assertion
from datahub.metadata import schema_classes as models
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
                start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end=datetime(2021, 1, 2, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
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
                start=datetime(2021, 1, 1, tzinfo=timezone.utc),
                end=datetime(2021, 1, 2, tzinfo=timezone.utc),
            )
        ],
        training_data_lookback_days=30,
        detection_mechanism=DetectionMechanism.INFORMATION_SCHEMA,
        incident_behavior=[AssertionIncidentBehavior.RAISE_ON_FAIL],
        created_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
        created_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
        updated_by=Urn.from_string("urn:li:corpuser:acryl-cloud-user"),
        updated_at=datetime(2021, 1, 1, tzinfo=timezone.utc),
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


# TODO: Add tests for the monitor entity once it's added
def test_smart_freshness_assertion_from_entities() -> None:
    """Test that SmartFreshnessAssertion can be created from entities."""
    smart_freshness_assertion = SmartFreshnessAssertion.from_entities(
        assertion=Assertion(
            id=AssertionUrn("urn:li:assertion:smart_freshness_assertion"),
            info=models.FreshnessAssertionInfoClass(
                type=models.AssertionTypeClass.FRESHNESS,
                entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            ),
            description="Smart Freshness Assertion",
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.NATIVE,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:acryl-cloud-user-created",
                    time=1609459200000,  # 2021-01-01 00:00:00 UTC
                ),
            ),
            last_updated=models.AuditStampClass(
                actor="urn:li:corpuser:acryl-cloud-user-updated",
                time=1609545600000,  # 2021-01-02 00:00:00 UTC
            ),
            tags=[
                models.TagAssociationClass(
                    tag="urn:li:tag:smart_freshness_assertion_tag",
                )
            ],
            on_failure=[
                models.AssertionActionClass(
                    type=models.AssertionActionTypeClass.RAISE_INCIDENT,
                )
            ],
            on_success=[
                models.AssertionActionClass(
                    type=models.AssertionActionTypeClass.RESOLVE_INCIDENT,
                )
            ],
        )
    )

    assert smart_freshness_assertion.urn == AssertionUrn(
        "urn:li:assertion:smart_freshness_assertion"
    )
    assert smart_freshness_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
    )
    assert smart_freshness_assertion.display_name == "Smart Freshness Assertion"
    assert smart_freshness_assertion.incident_behavior == [
        AssertionIncidentBehavior.RAISE_ON_FAIL,
        AssertionIncidentBehavior.RESOLVE_ON_PASS,
    ]
    assert smart_freshness_assertion.created_by == Urn.from_string(
        "urn:li:corpuser:acryl-cloud-user-created"
    )
    assert smart_freshness_assertion.created_at == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )
    assert smart_freshness_assertion.updated_by == Urn.from_string(
        "urn:li:corpuser:acryl-cloud-user-updated"
    )
    assert smart_freshness_assertion.updated_at == datetime(
        2021, 1, 2, tzinfo=timezone.utc
    )
    assert smart_freshness_assertion.tags == [
        Urn.from_string("urn:li:tag:smart_freshness_assertion_tag")
    ]


def test_smart_freshness_assertion_from_entities_minimal() -> None:
    """Test that SmartFreshnessAssertion can be created from entities with minimal fields."""
    smart_freshness_assertion = SmartFreshnessAssertion.from_entities(
        assertion=Assertion(
            id=AssertionUrn("urn:li:assertion:minimal_assertion"),
            info=models.FreshnessAssertionInfoClass(
                type=models.AssertionTypeClass.FRESHNESS,
                entity="urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)",
            ),
        )
    )

    assert smart_freshness_assertion.urn == AssertionUrn(
        "urn:li:assertion:minimal_assertion"
    )
    assert smart_freshness_assertion.dataset_urn == DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:abc,def,PROD)"
    )
    assert (
        smart_freshness_assertion.display_name == ""
    )  # Default empty string when no description
    assert (
        smart_freshness_assertion.incident_behavior == []
    )  # Default empty list when no actions
    assert smart_freshness_assertion.tags == []  # Default empty list when no tags

    # No created by, created at, updated by, updated at when no source or last updated:
    assert smart_freshness_assertion.created_by is None
    assert smart_freshness_assertion.created_at is None
    assert smart_freshness_assertion.updated_by is None
    assert smart_freshness_assertion.updated_at is None
