## Configuration Notes
See the 
1. [Microsoft AD App Creation doc](https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal) for the steps to create an app client ID and secret
2. Login to Power BI as Admin and from Tenant settings allow below permissions
- Allow service principles to use Power BI APIs
- Allow service principals to use read-only Power BI admin APIs
- Enhance admin APIs responses with detailed metadata
## Concept mapping 

| Power BI              | Datahub             |                                                                                               
|-----------------------|---------------------|
| `Dashboard`           | `Dashboard`         |
| `Dataset, Datasource` | `Dataset`           |
| `Tile`                | `Chart`             |
| `Report.webUrl`       | `Chart.externalUrl` |
| `Workspace`           | `N/A`               |
| `Report`              | `Dashboard`         |
| `Page`                | `Chart`             |

If Tile is created from report then Chart.externalUrl is set to Report.webUrl.
