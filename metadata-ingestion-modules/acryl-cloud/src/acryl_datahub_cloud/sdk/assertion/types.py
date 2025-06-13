from typing import Union

from acryl_datahub_cloud.sdk.assertion.assertion_base import (
    FreshnessAssertion,
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
    SqlAssertion,
)
from acryl_datahub_cloud.sdk.assertion.smart_column_metric_assertion import (
    SmartColumnMetricAssertion,
)

AssertionTypes = Union[
    SmartFreshnessAssertion,
    SmartVolumeAssertion,
    FreshnessAssertion,
    SmartColumnMetricAssertion,
    SqlAssertion,
    # TODO: Add other assertion types here as we add them.
]
