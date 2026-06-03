---
description: "Detect and resolve quality issues before they impact production. Automated anomaly detection and quality checks keep data reliable."
sidebar_custom_props:
  icon: "🔍"
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Data Quality & Observability

<FeatureAvailability selfHostedPartial comparisonLink="/docs/managed-datahub/managed-datahub-overview#data-observability" />

DataHub treats observability as a first-class capability of the metadata platform. Quality signals live alongside the data assets they describe, the lineage they propagate through, and the people accountable for them — so problems are surfaced in context and routed to the right owner.

The capability area is organized around three jobs:

- **Detection** — find quality issues before consumers do.
- **Resolution** — react fast, communicate clearly, and coordinate a fix.
- **Governance & Improvement** — codify expectations and raise the bar over time.

:::info OSS vs Cloud
[**See the feature-by-feature breakdown**](/docs/managed-datahub/managed-datahub-overview.md#data-observability).
:::

## Detection

Catch issues as close to the source as possible.

### Supported platforms

Active query runs on the major cloud warehouses; ingestion-driven assertions work on any platform that reports the relevant aspect.

👉 **[See supported platforms](/docs/managed-datahub/observe/assertions.md#capabilities-at-a-glance)**

### Capabilities

- **[Data Observability Agent](/docs/managed-datahub/observe/data-health-dashboard.md#data-observability-agent-private-beta)** — an AI assistant that scans your data landscape and provisions the right assertions for the right tables in minutes, not weeks. Tell it which slice of your data matters most (or let it figure that out from usage and ownership signals), and it creates Freshness, Volume, Field, and other checks automatically — closing coverage gaps without manual setup per table. _DataHub Cloud only. Private Beta._
- **[Assertions](/docs/managed-datahub/observe/assertions.md)** — the core data-quality test primitive in DataHub. Assertions can be **active** (DataHub Cloud issues queries against your warehouse on a schedule) or **ingestion-driven** (DataHub Cloud evaluates the assertion against profiles and operations already reported during ingestion, on any platform). Active, ingestion-driven, and anomaly-detection assertions are all DataHub Cloud features. DataHub Core can ingest and display assertion results that you self-report — from dbt, Great Expectations, Snowflake DMFs, or any custom source pushed via the SDK.
  - [Freshness](/docs/managed-datahub/observe/freshness-assertions.md) — has the table updated recently?
  - [Volume](/docs/managed-datahub/observe/volume-assertions.md) — is row count in the expected range?
  - [Column](/docs/managed-datahub/observe/column-assertions.md) — column-level metrics and value constraints (e.g. `status` must be in `active`, `pending`, or `closed`; or null rate stays below 5%).
  - [Custom SQL](/docs/managed-datahub/observe/custom-sql-assertions.md) — arbitrary SQL returning a numeric value.
  - [Schema](/docs/managed-datahub/observe/schema-assertions.md) — expected columns and types are present.
  - [Anomaly Detection](/docs/managed-datahub/observe/anomaly-detection.md) — DataHub Cloud auto-learns normal behavior for freshness, volume, and column metrics. _DataHub Cloud only._
- **[Data Health Dashboard](/docs/managed-datahub/observe/data-health-dashboard.md)** — a single pane for the health of your data landscape, including [Monitoring Rules](/docs/managed-datahub/observe/data-health-dashboard.md#monitoring-rules) that automatically apply anomaly-detection monitors and schema assertions across matching datasets as your landscape evolves. _DataHub Cloud only._
- **[SQL Profiling](/metadata-ingestion/docs/dev_guides/sql_profiles.md)** — dataset and column profiles produced by ingestion. Profiles power ingestion-driven Volume and Column assertions and feed the asset profile pages users already browse.

## Resolution

Once an issue is detected, route and fix it.

- **[Incidents](/docs/incidents/incidents.md)** — formal tracking and triage for data issues. Tied to the affected assets and visible to consumers exploring lineage. Available in DataHub Core; DataHub Cloud adds [Slack and Teams notifications](/docs/incidents/incidents.md#enabling-slack-notifications-datahub-cloud-only).
- **[Subscriptions & Notifications](/docs/managed-datahub/subscription-and-notification.md)** — let users and teams subscribe to assets, assertions, incidents, and changes, with delivery via email, Slack, or Microsoft Teams. _DataHub Cloud only._
- **[Data Observability Agent — root-cause assistance](/docs/managed-datahub/observe/data-health-dashboard.md#data-observability-agent-private-beta)** — when an assertion fires or an incident opens, the agent uses DataHub's lineage, ownership, and metadata — together with MCP-style connectors into Snowflake, dbt, and other source systems — to investigate likely causes (recent schema or query changes, upstream failures, pipeline regressions) and propose next steps. _DataHub Cloud only. Private Beta._

## Governance & Improvement

Encode quality expectations as durable artifacts and track improvement over time.

- **[Data Contracts](/docs/managed-datahub/observe/data-contract.md)** — the verifiable agreement between producers and consumers about what a dataset's freshness, volume, schema, and column-level quality should look like. Assertions are the checks that prove the contract is met. Data contracts are available in DataHub Core.
- **[Failure trends in the Data Health Dashboard](/docs/managed-datahub/observe/data-health-dashboard.md#assertions-tab)** — filter the **By Assertion** view by time range and result status to surface which checks are failing repeatedly, which are flaky, and which tables consistently lack coverage. Use these patterns to prioritize the structural fixes — better tests, contract changes, ingestion improvements — that move quality forward over months, not just minutes. _DataHub Cloud only._

## Bring your own quality signals — integrations

DataHub also captures assertion results from external quality tools you may already run, so the asset view stays unified.

- **[dbt](/docs/generated/ingestion/sources/dbt.md)** — dbt tests are ingested as assertions linked to the corresponding tables; failures show up alongside DataHub-native assertions on the asset page.
- **[Great Expectations](/metadata-ingestion/integration_docs/great-expectations.md)** — GX expectation results are pushed into DataHub as assertions via the action handler.
- **[Snowflake Data Metric Functions](/docs/assertions/snowflake/snowflake_dmfs.md)** — author assertions in YAML and compile them to native Snowflake DMFs that run inside your warehouse; results stream back into DataHub as assertion results. Externally managed DMFs you've already created in Snowflake can also be ingested as assertions via the Snowflake source's `include_externally_managed_dmfs` option.

## Programmatic access — APIs & SDKs

Every observability capability is scriptable. Common entry points:

- **[Assertions tutorial](/docs/api/tutorials/assertions.md)** — create, update, and report results for assertions.
- **[Custom Assertions](/docs/api/tutorials/custom-assertions.md)** — register and report assertion types DataHub doesn't model out of the box.
- **[Bulk Assertions SDK](/docs/api/tutorials/sdk/bulk-assertions-sdk.md)** — manage assertions at scale from Python.
- **[Incidents tutorial](/docs/api/tutorials/incidents.md)** — open, update, and resolve incidents from your pipelines.
- **[Data Contracts tutorial](/docs/api/tutorials/data-contracts.md)** — define and evolve contracts programmatically.
- **[Subscriptions tutorial](/docs/api/tutorials/subscriptions.md)** — wire up notification routing from code.
- **[Operations tutorial](/docs/api/tutorials/operations.md)** — report table-change events that power freshness and ingestion-driven assertions.
