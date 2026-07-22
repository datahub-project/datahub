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
  }
}
"""

CUSTOM_RULES_QUERY = """
query getCustomRules($first: Int, $after: String) {
  getCustomRules(first: $first, after: $after) {
    edges {
      node {
        uuid
        ruleType
        description
        customSql
        entityMcons
        severity
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

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
