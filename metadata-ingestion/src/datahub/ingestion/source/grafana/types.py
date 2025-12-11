# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.schema_classes import (
    ChartTypeClass,
)

CHART_TYPE_MAPPINGS = {
    "graph": ChartTypeClass.LINE,
    "timeseries": ChartTypeClass.LINE,
    "table": ChartTypeClass.TABLE,
    "stat": ChartTypeClass.TEXT,
    "gauge": ChartTypeClass.TEXT,
    "bargauge": ChartTypeClass.TEXT,
    "bar": ChartTypeClass.BAR,
    "pie": ChartTypeClass.PIE,
    "heatmap": ChartTypeClass.TABLE,
    "histogram": ChartTypeClass.BAR,
}
