# GraphQL documents issued against the Monte Carlo Data Collector (MCD) gateway.
# Field selections for all operations are built in query_builder.py from the
# FieldSpec registry. AssetFilterInterface inline fragments stay here as one
# shared string interpolated into getTableMonitor.

# Shared AssetFilterInterface selection for getTableMonitor filters and
# exclusions. Interpolated twice so a new filter type cannot drift between lists.
ASSET_FILTER_INLINE_SELECTION = """\
        type
        negated
        ... on AssetFilterTableName {
          tableName
          tableNameOperator
        }
        ... on AssetFilterFullTableId {
          fullTableId
        }
        ... on AssetFilterTableType {
          tableType
        }
        ... on AssetFilterTableTag {
          tableTags
          tableTagsOperator
        }
        ... on AssetFilterActivityRead {
          readDays
        }
        ... on AssetFilterActivityWrite {
          writeDays
        }
        ... on AssetFilterActivityReadWrite {
          readWriteDays
        }
        ... on AssetFilterActivityReadAndWrite {
          readAndWriteDays
        }
        ... on AssetFilterActivityVolumeChange {
          volumeChangeDays
        }
        ... on AssetFilterActivityReadIsNull {
          readActivityIsNull
        }
        ... on AssetFilterActivityWriteIsNull {
          writeActivityIsNull
        }
        ... on AssetFilterActivityReadWriteIsNull {
          readWriteActivityIsNull
        }
        ... on AssetFilterActivityReadAndWriteIsNull {
          readAndWriteActivityIsNull
        }"""


# TABLE-type monitors cover many tables via an asset_selection filter, so
# getMonitors' entityMcons (scoped to single-entity METRIC monitors) is always
# empty for them. getTableMonitor exposes the filter/exclusion definition;
# evaluateAssetSelection then resolves that selection to concrete table MCONs.
def _table_monitor_query_with_filter_selection(asset_filter_selection: str) -> str:
    return f"""
query getTableMonitor($monitorUuid: UUID!) {{
  getTableMonitor(monitorUuid: $monitorUuid) {{
    warehouseUuid
    assetSelection {{
      filtersJoiner
      exclusionsJoiner
      databases {{
        name
        schemas
      }}
      filters {{
{asset_filter_selection}
      }}
      exclusions {{
{asset_filter_selection}
      }}
    }}
  }}
}}
"""


TABLE_MONITOR_QUERY = _table_monitor_query_with_filter_selection(
    ASSET_FILTER_INLINE_SELECTION
)
