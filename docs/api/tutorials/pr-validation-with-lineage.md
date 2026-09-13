# Using DataHub Lineage for Automated PR Validation

## Overview

This tutorial shows how to use DataHub's REST API and GraphQL to build an automated PR validation system that detects breaking changes before they reach production. By querying DataHub's lineage graph at PR review time, you can identify which downstream dashboards, reports, and ML features will be affected by schema changes in your dbt models or SQL pipelines.

**Use cases:**

- Validate dbt model changes against downstream consumers before merge
- Detect column-level breaking changes (renames, deletions, type changes)
- Compute blast radius scores for pull requests
- Post lineage-backed verdicts as GitHub PR comments

## Prerequisites

- A running DataHub instance (GMS on port 8080)
- Ingested datasets with schema metadata and lineage (e.g., via dbt ingestion or REST API)
- Node.js 18+ or Python 3.8+ for the client code
- A GitHub repository with pull request webhooks

## Step 1: Search for Affected Entities

When a PR changes a dbt model, first identify which DataHub entities correspond to the changed files.

### Using the REST API

```bash
# Search for datasets matching a model name
curl -X GET 'http://localhost:8080/entities?action=search' \
  -H 'X-DataHub-Actor: urn:li:corpuser:datahub' \
  -G --data-urlencode 'query=stg_orders' \
  --data-urlencode 'filter={"or":[{"and":[{"field":"_entityType","values":["DATASET"]},{"field":"platform","values":["snowflake"]}]}]}' \
  --data-urlencode 'start=0' \
  --data-urlencode 'count=10'
```

### Using GraphQL

```graphql
query SearchDatasets {
  search(query: "stg_orders", filters: [{ field: "entityType", values: ["DATASET"] }]) {
    total
    searchResults {
      entity {
        urn
        ... on Dataset {
          name
          platform {
            name
          }
        }
      }
    }
  }
}
```

**Response:**

```json
{
  "data": {
    "search": {
      "total": 1,
      "searchResults": [
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.stg_orders,PROD)",
            "name": "stg_orders",
            "platform": { "name": "snowflake" }
          }
        }
      ]
    }
  }
}
```

## Step 2: Retrieve Schema for Column-Level Comparison

Fetch the current schema of the changed dataset to compare against proposed changes.

### Using the REST API

```bash
# Get the SchemaMetadata aspect
curl -X GET 'http://localhost:8080/aspects/urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.stg_orders,PROD)?aspect=SchemaMetadata' \
  -H 'X-DataHub-Actor: urn:li:corpuser:datahub'
```

### Using GraphQL

```graphql
query GetSchema($urn: String!) {
  dataset(urn: $urn) {
    name
    schemaMetadata {
      fields {
        fieldPath
        type
        description
        isNullable
      }
      version
    }
  }
}
```

**Response:**

```json
{
  "data": {
    "dataset": {
      "name": "stg_orders",
      "schemaMetadata": {
        "fields": [
          { "fieldPath": "order_id", "type": "NUMBER", "isNullable": false },
          { "fieldPath": "customer_id", "type": "NUMBER", "isNullable": false },
          { "fieldPath": "order_total", "type": "NUMBER", "isNullable": true },
          { "fieldPath": "order_date", "type": "TIMESTAMP", "isNullable": false }
        ],
        "version": 3
      }
    }
  }
}
```

## Step 3: Trace Downstream Lineage

This is the critical step. Query DataHub's lineage graph to find every dataset, dashboard, and feature that depends on the changed columns.

### Using GraphQL (recommended)

```graphql
query DownstreamLineage($urn: String!) {
  dataset(urn: $urn) {
    name
    lineage {
      downstreams {
        ... on Dataset {
          urn
          name
          platform {
            name
          }
          schemaMetadata {
            fields {
              fieldPath
              type
            }
          }
        }
        ... on Dashboard {
          urn
          title
          platform {
            name
          }
        }
      }
    }
  }
}
```

### Using the REST API

```bash
# Get downsteam relationships
curl -X GET 'http://localhost:8080/entities?actionalytics' \
  -H 'X-DataHub-Actor: urn:li:corpuser:datahub' \
  -G --data-urlencode 'entity=urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.stg_orders,PROD)' \
  --data-urlencode 'category=DownstreamLineage' \
  --data-urlencode 'start=0' \
  --data-urlencode 'count=100'
```

### Mapping Column-Level Impact

To determine which specific downstream columns are affected, cross-reference the changed columns with the lineage graph:

```javascript
function findAffectedDownstream(changedColumns, downstreamDatasets) {
  const affected = [];

  for (const downstream of downstreamDatasets) {
    const impactedFields = [];

    for (const field of downstream.schemaMetadata?.fields || []) {
      // Match by column name or check upstream lineage aspects
      if (changedColumns.includes(field.fieldPath)) {
        impactedFields.push(field.fieldPath);
      }
    }

    if (impactedFields.length > 0) {
      affected.push({
        urn: downstream.urn,
        name: downstream.name,
        impactedFields,
      });
    }
  }

  return affected;
}
```

## Step 4: Compute Risk Score

Combine the lineage breadth (how many downstream assets are affected) with schema change severity to produce a risk score.

```javascript
function computeRiskScore(changedColumns, downstreamAssets, schemaChanges) {
  let score = 0;

  // Factor 1: Number of downstream assets affected (0-0.4)
  const downstreamCount = downstreamAssets.length;
  score += Math.min(downstreamCount * 0.05, 0.4);

  // Factor 2: Column change severity (0-0.3)
  for (const change of schemaChanges) {
    if (change.type === 'deleted') score += 0.15;
    else if (change.type === 'renamed') score += 0.10;
    else if (change.type === 'type_changed') score += 0.08;
    else if (change.type === 'added') score += 0.02;
  }

  // Factor 3: Downstream depth (how deep in the DAG) (0-0.3)
  const maxDepth = Math.max(...downstreamAssets.map(a => a.depth || 1));
  score += Math.min(maxDepth * 0.1, 0.3);

  return Math.min(score, 1.0);
}
```

**Risk thresholds:**

| Score | Verdict | Action |
|-------|---------|--------|
| 0.0 - 0.3 | SAFE TO MERGE | No downstream impact detected |
| 0.3 - 0.6 | CHANGES REQUESTED | Review affected downstream assets |
| 0.6 - 1.0 | BLOCK | High blast radius, requires manual review |

## Step 5: Post Verdict as PR Comment

Use the GitHub API to post the lineage-backed verdict directly on the pull request.

```javascript
const octokit = new Octokit({ auth: GITHUB_TOKEN });

await octokit.rest.issues.createComment({
  owner: repoOwner,
  repo: repoName,
  issue_number: prNumber,
  body: `## 🛑 BLOCK — Lineage Impact Analysis

**Risk Score:** 0.75 / 1.0

### Affected Downstream Assets

| Asset | Type | Impacted Columns |
|-------|------|-----------------|
| \`analytics.order_summary\` | Dataset | \`order_total\`, \`customer_id\` |
| \`analytics.revenue_dashboard\` | Dashboard | \`order_total\` |
| \`ml.daily_revenue_feature\` | Feature | \`order_total\` |

### Schema Changes

| Column | Change | Severity |
|--------|--------|----------|
| \`order_total\` | Renamed to \`order_amount\` | Breaking |
| \`tax_amount\` | Deleted | Breaking |

### Recommended Actions

- [ ] Update \`stg_orders.sql\` to maintain backward compatibility
- [ ] Notify dashboard owners before merging
- [ ] Update downstream models referencing \`order_total\`
`,
});
```

## Step 6: Full Integration Example (Node.js)

Here is a complete example that ties all steps together:

```javascript
const axios = require('axios');
const { Octokit } = require('octokit');

const DATAHUB_GMS = 'http://localhost:8080';
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;

async function validatePR(prFiles, repoOwner, repoName, prNumber) {
  const changedModels = prFiles
    .filter(f => f.filename.endsWith('.sql'))
    .map(f => f.filename.split('/').pop().replace('.sql', ''));

  const results = [];

  for (const model of changedModels) {
    // Step 1: Search for the entity
    const searchRes = await axios.get(`${DATAHUB_GMS}/entities?action=search`, {
      params: { query: model, filter: JSON.stringify({ or: [{ and: [
        { field: '_entityType', values: ['DATASET'] }
      ]}] }) }
    });
    const entity = searchRes.data.elements?.[0];
    if (!entity) continue;

    const urn = entity.urn;

    // Step 2: Get schema
    const schemaRes = await axios.get(
      `${DATAHUB_GMS}/aspects/${encodeURIComponent(urn)}?aspect=SchemaMetadata`
    );
    const fields = schemaRes.data.value?.schemaMetadata?.fields || [];

    // Step 3: Get downstream lineage
    // (Using GraphQL for richer data)
    const lineageRes = await axios.post(`${DATAHUB_GMS}/api/graphql`, {
      query: `query($urn: String!) {
        dataset(urn: $urn) {
          lineage { downstreams { ... on Dataset { urn name } ... on Dashboard { urn title } } }
        }
      }`,
      variables: { urn }
    });
    const downstreams = lineageRes.data.data?.dataset?.lineage?.downstreams || [];

    // Step 4: Compute risk
    const riskScore = Math.min(downstreams.length * 0.1, 1.0);

    results.push({ model, urn, fields, downstreams, riskScore });
  }

  // Step 5: Post comment
  if (results.length > 0) {
    const maxRisk = Math.max(...results.map(r => r.riskScore));
    const verdict = maxRisk >= 0.6 ? 'BLOCK' : maxRisk >= 0.3 ? 'CHANGES REQUESTED' : 'SAFE TO MERGE';

    const comment = formatComment(verdict, results);
    await new Octokit({ auth: GITHUB_TOKEN }).rest.issues.createComment({
      owner: repoOwner, repo: repoName, issue_number: prNumber, body: comment
    });
  }
}
```

## Key Takeaways

1. **GraphQL is preferred** for lineage queries — it returns structured entity data in a single request, avoiding the N+1 problem of REST.
2. **Column-level lineage** requires correlating schema fields across the lineage graph — DataHub stores this in the `SchemaField` aspect.
3. **Risk scoring** combines three signals: breadth (downstream count), depth (DAG depth), and severity (change type).
4. **REST API fallback** works well for simple entity lookups and aspect retrieval when GraphQL is unavailable.

## Related Resources

- [DataHub GraphQL API](../graphql/README.md)
- [Lineage Overview](../../lineage/lineage.md)
- [Ingesting dbt Models](../../quick-ingestion-guides/dbt.md)
- [Schema Metadata](../../schema-history.md)
