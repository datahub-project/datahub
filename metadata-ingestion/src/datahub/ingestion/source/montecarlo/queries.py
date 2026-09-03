# GraphQL documents issued against the Monte Carlo Data Collector (MCD) gateway.
# Field selections follow the pycarlo / MCD schema.
#
# getMonitors returns a plain list (walked by MonteCarloClient._paginate_offset);
# the rest are Relay-style connections (edges/node/pageInfo, walked by ._paginate).
MONITORS_QUERY = """
query getMonitors($domainIds: [UUID!], $limit: Int, $offset: Int) {
  getMonitors(domainIds: $domainIds, limit: $limit, offset: $offset) {
    uuid
    name
    description
    monitorType
    entityMcons
    resourceId
    severity
    dataQualityDimension
    comparisons {
      comparisonType
      operator
      metric
      customMetric {
        uuid
        metricName
      }
      field
      fields
      threshold
      upperThreshold
      lowerThreshold
    }
  }
}
"""

CUSTOM_RULES_QUERY = """
query getCustomRules($first: Int, $after: String) {
  getCustomRules(first: $first, after: $after) {
    edges {
      node {
        uuid
        ruleName
        ruleType
        description
        customSql
        entityMcons
        severity
        comparisons {
          comparisonType
          operator
          metric
          customMetric {
            uuid
            metricName
          }
          field
          fields
          threshold
          upperThreshold
          lowerThreshold
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Monte Carlo surfaces both "alerts" and "incidents" through the same getAlerts
# connection — an incident is an alert whose `type` field is incident-related
# (e.g. "incident", "freshness_incident", "volume_anomaly"). There is no
# separate getIncidents endpoint; the builder maps every entry to an
# AssertionRunEvent failure regardless of its `type`, so this single query
# covers both. The `type`/`subTypes` fields are carried on nativeResults so the
# UI can distinguish an incident from a generic alert.
ALERTS_QUERY = """
query getAlerts($first: Int, $after: String, $createdTime: DateTimeRangeInput) {
  getAlerts(first: $first, after: $after, createdTime: $createdTime) {
    edges {
      node {
        id
        type
        subTypes
        severity
        priority
        status
        createdTime
        monitorUuids
        assets {
          mcon
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

GET_TABLE_QUERY = """
query getTable($mcon: String) {
  getTable(mcon: $mcon) {
    mcon
    fullTableId
    warehouse {
      connectionType
    }
  }
}
"""

# TABLE-type monitors cover many tables via an asset_selection filter, so
# getMonitors' entityMcons (scoped to single-entity METRIC monitors) is always
# empty for them. getTableMonitor exposes the actual filter/exclusion
# definition; only the FULL_TABLE_ID filter case is resolved here (a fixed,
# explicit table list) — pattern-based filters (TABLE_NAME, TABLE_TAG,
# activity filters) would need evaluateAssetSelection instead.
TABLE_MONITOR_QUERY = """
query getTableMonitor($monitorUuid: UUID!) {
  getTableMonitor(monitorUuid: $monitorUuid) {
    assetSelection {
      filters {
        type
        ... on AssetFilterFullTableId {
          fullTableId
        }
      }
    }
  }
}
"""

GET_TABLE_BY_FULL_TABLE_ID_QUERY = """
query getTable($dwId: UUID, $fullTableId: String) {
  getTable(dwId: $dwId, fullTableId: $fullTableId) {
    mcon
  }
}
"""

# Monitor run history. Returns a Relay connection (most-recent-first) of
# JobExecutionHistoryLog nodes. monitorUuid is String (not UUID!) per the MCD
# schema. historyDays bounds the query window; first caps the page size — it
# does NOT bound the total count, so callers that want only the latest N runs
# should set first=N and not paginate.
GET_JOB_EXECUTIONS_QUERY = """
query getJobExecutions($monitorUuid: String, $historyDays: Int, $first: Int) {
  getJobExecutions(monitorUuid: $monitorUuid, historyDays: $historyDays, first: $first) {
    edges {
      node {
        jobExecutionUuid
        startTime
        endTime
        status
        exceptions
        totalResultCount
        evaluatedRecordCount
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Measured metric values (time-series). metricName and metricsFilter are
# required; startTime is also required (DateTime!). metricsFilter.mcon is a
# single String (not a list). jobExecutionUuid is present on the schema but is
# null for table-level metrics, so a per-run join is not possible — the value
# is a best-effort temporal correlation to the latest run, not a proven join.
# first=1 + deduplicateValues=true returns only the most-recent point.
GET_METRICS_V4_QUERY = """
query getMetricsV4($metricName: String!, $metricsFilter: MetricsFilter!, $startTime: DateTime!, $endTime: DateTime, $first: Int, $deduplicateValues: Boolean) {
  getMetricsV4(metricName: $metricName, metricsFilter: $metricsFilter, startTime: $startTime, endTime: $endTime, first: $first, deduplicateValues: $deduplicateValues) {
    metrics {
      metric
      value
      field
      timestamp
      measurementTimestamp
      thresholds {
        upper
        lower
      }
      jobExecutionUuid
    }
  }
}
"""
