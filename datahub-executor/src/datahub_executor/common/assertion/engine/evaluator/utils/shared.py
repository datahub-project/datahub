import base64

from datahub.metadata._urns.urn_defs import DatasetUrn
from datahub.metadata.urns import (
    AssertionUrn,
    DataHubMetricCubeUrn,
    MonitorUrn,
)

from datahub_executor.common.types import (
    Assertion,
    AssertionType,
)

ASSERTION_TYPES_REQUIRING_TRAINING = [
    AssertionType.VOLUME,
    AssertionType.FRESHNESS,
    AssertionType.FIELD,
]


def is_training_required(
    assertion: Assertion,
) -> bool:
    # Currently, we'll assume that all smart assertions
    # require some training.
    return (
        assertion.type in ASSERTION_TYPES_REQUIRING_TRAINING and assertion.is_inferred
    )


def is_field_metric_assertion(
    assertion: Assertion,
) -> bool:
    # Currently, we'll assume that all smart assertions
    # require some training.
    return (
        assertion.field_assertion is not None
        and assertion.field_assertion.field_metric_assertion is not None
    )


def encode_monitor_urn(urn: str) -> str:
    """Encodes a URN using Base64 URL encoding in a reversible way."""
    encoded_bytes = base64.urlsafe_b64encode(urn.encode("utf-8"))
    return encoded_bytes.decode("utf-8")


# TODO: Determine whether we should move the following methods.


def default_volume_assertion_urn(dataset_urn: str) -> str:
    assert DatasetUrn.from_string(dataset_urn)
    return AssertionUrn(f"{encode_monitor_urn(dataset_urn)}-__system__volume").urn()


def default_volume_monitor_urn(dataset_urn: str) -> str:
    assert DatasetUrn.from_string(dataset_urn)
    return MonitorUrn(dataset_urn, "__system__volume").urn()


def make_volume_metric_cube_urn(dataset_urn: str) -> str:
    assert DatasetUrn.from_string(dataset_urn)
    monitor_urn = default_volume_monitor_urn(dataset_urn)
    return DataHubMetricCubeUrn(encode_monitor_urn(monitor_urn)).urn()
