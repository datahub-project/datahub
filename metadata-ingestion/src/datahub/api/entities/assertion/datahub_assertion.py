from typing import Union

from datahub.api.entities.assertion.field_assertion import (
    FieldMetricAssertion,
    FieldValuesAssertion,
)
from datahub.api.entities.assertion.freshness_assertion import (
    CronFreshnessAssertion,
    FixedIntervalFreshnessAssertion,
)
from datahub.api.entities.assertion.sql_assertion import (
    SqlMetricAssertion,
    SqlMetricChangeAssertion,
)
from datahub.api.entities.assertion.volume_assertion import (
    RowCountChangeVolumeAssertion,
    RowCountTotalVolumeAssertion,
)

# Pydantic v2 smart union: automatically discriminates based on the 'type' field
# (eg freshness/volume/sql/field) and unique fields within each type
DataHubAssertion = Union[
    FixedIntervalFreshnessAssertion,
    CronFreshnessAssertion,
    RowCountTotalVolumeAssertion,
    RowCountChangeVolumeAssertion,
    SqlMetricAssertion,
    SqlMetricChangeAssertion,
    FieldMetricAssertion,
    FieldValuesAssertion,
]
