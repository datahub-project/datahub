## Take Note as of 11 January 2023

Below are the keys differences between V2 connector and the original connector.

- **Only uses ADMIN API and Scan Results**
- `modified_since` config to ingest **ONLY MODIFIED** workspaces since configured time (limit to last 30 days)
- Multi workspaces ingestion (allow and deny Pattern for workspaces)
- Stateful ingestion State per PowerBI Workspace
- Ingestion of Powerbi Dataset fields and measures
- Workspaces as container which contain PowerBI workspace reports, dashboards, datasets and tiles

## Why only ADMIN API?

In big organization, having workspace access granted for each workspace is not easy.
Hence as PowerBI admin, we only use Powerbi Admin Apis like [Admin - WorkspaceInfo GetScanResult](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result) and only use the scan results for ingestion.
If your not granted as workspace owner, you can't call specific endpoint like [Reports - Get Report](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/get-report) even though your a PowerBI Admin.

## Configuration Notes

See the

1. [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) for the steps to create an app client ID and secret
2. Login to Power BI as Admin and from Tenant settings allow below permissions

- Allow service principles to use Power BI APIs
- Allow service principals to use read-only Power BI admin APIs
- Enhance admin APIs responses with detailed metadata

## Concept mapping

| Power BI                   | Datahub                |
| -------------------------- | ---------------------- |
| `Dashboard`                | `Dashboard`            |
| `Report`                   | `Dashboard`            |
| `Report.webUrl`            | `Chart.externalUrl`    |
| `Tile`                     | `Chart`                |
| `Workspace`                | `Container`            |
| `Dataset's Tables`         | `Dataset`              |
| `Dataset's Column/Measure` | `Dataset Schema Field` |

For PowerBI Dataset contain many tables, table contains many columns or measures.
We group all of them into one Datahub Dataset.
