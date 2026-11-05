import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Analytics Dashboards

Analytics has two pages. Usage Stats tracks usage on a DataHub Cloud contract. Product Analytics shows asset and adoption metrics for DataHub Cloud. Usage Stats appears only when its feature flag is on. Otherwise, Product Analytics is the only page on Analytics.

# Usage Stats Dashboard

<FeatureAvailability saasOnly stage="public-beta" />

Usage Stats provides visibility of usage on a DataHub Cloud contract for the service period. Start and end dates are formatted in UTC.

![](https://raw.githubusercontent.com/datahub-project/static-assets/d92ec9d2a9db23ab041fd7fea2ea6127f7b605e3/imgs/admin/usage-stats-dashboard.png)

## Metered usage cards

Each contracted product has one card. A card is a line chart of usage over time against what the contract includes.

| Product             | What the card counts                                                                        |
| ------------------- | ------------------------------------------------------------------------------------------- |
| Stored Data Assets  | Peak count of assets (datasets, dashboards, charts) in the period                           |
| API Calls           | MCP, REST/OpenAPI and GraphQL calls combined                                                |
| AI Credits          | Running total of AI credits used in the period                                              |
| Observe Tables      | Peak count of tables monitored in the period                                                |
| Ask DataHub Answers | Usage against its own allowance. Shown only if the contract has a separate allowance for it |

Each card shows the allowances in effect for the period.

## Usage Over Time

This section charts usage over a period you choose.

- Instance selector: on the charts, select an instance to view its usage.&#32;
- Charts use stacked bars or lines.
- Period presets: current period, forecast to period end, last 24 hours, 30 days, 90 days, last year.
- Custom range: set your own start and end dates.

Two charts show product adoption:

- Monthly Active Users
- Active Data Sources

API calls can be broken down by request API: MCP, REST/OpenAPI or GraphQL.

## Upcoming Grants

This section lists credit and allowance grants that have not started yet under the contract. Each grant shows its start date and, if the grant has one, its end date. Dates are formatted in UTC.

Grants that have already started, and expired grants, do not appear here. In-effect allowances appear in the metered usage cards at the top of the dashboard.

## Fair Use

This section shows live hourly charts for each dimension the fair use policy covers. A link to the policy sits beside the charts.

Fair use policy: [DataHub SaaS terms](https://datahub.com/saasterms/)

# Product Analytics Dashboard

<FeatureAvailability />

Product Analytics shows asset and adoption metrics for DataHub Cloud.

![](https://raw.githubusercontent.com/datahub-project/static-assets/d92ec9d2a9db23ab041fd7fea2ea6127f7b605e3/imgs/admin/product-analytics-dashboard.png)

## Highlight tiles

Summarized counts of assets in DataHub Cloud. Weekly and Monthly Active Users are visible if the Usage Stats Dashboard is not enabled, otherwise refer to Usage Stats.&#32;&#32;

## Data Landscape Summary

A snapshot of the catalog, not usage.

- Entities by domain: stacked chart of entities by type by domain
- Assets by platform: number of assets by platform
- Entities with term: number of entities with associated glossary terms
- Entities by environment: number of entities by environment

## Domain Landscape Summary

The same chart types as the Data Landscape Summary, filtered by a domain dropdown.;

## Usage Analytics

How people use the UI.

- Weekly and monthly active user trends. Only visible if Usage Stats is not enabled.
- New Users (last 30 days), as a table with name, title and email.
- Top Users (last 30 days), as a table with link to user profile
- Number of searches over time
- Top Searches (past week)
- Top viewed datasets
- Top viewed dashboards
- Section views per entity type
- Actions by event type

## Ask DataHub

Only visible on DataHub Cloud.

- Ask DataHub weekly active users.&#32;
- Questions asked (past 3 months)
- Questions by platform (last 30 days)
- Top Ask DataHub users
- Top tools used.&#32;

## MCP

Only visible on DataHub Cloud.

- MCP sessions (past 3 months)
- MCP weekly active users
- Top MCP users
- Top MCP tools
