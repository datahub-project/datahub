# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
