MICROSTRATEGY_PLATFORM = "microstrategy"

MSTR_LOGIN_MODE_STANDARD = 1
MSTR_LOGIN_MODE_GUEST = 8

MSTR_OBJECT_TYPE_DASHBOARD = 55
MSTR_OBJECT_TYPE_REPORT = 3
# Documents share object type 55 with dossiers; the subtype distinguishes them.
MSTR_OBJECT_SUBTYPE_DOCUMENT = "14081"

MEASURE_TAG_URN = "urn:li:tag:Measure"
DIMENSION_TAG_URN = "urn:li:tag:Dimension"
TEMPORAL_TAG_URN = "urn:li:tag:Temporal"

# Entity kinds usage buckets can attach to (dashboards get
# DashboardUsageStatistics, report charts get ChartUsageStatistics).
USAGE_TARGET_DASHBOARD = "dashboard"
USAGE_TARGET_CHART = "chart"
