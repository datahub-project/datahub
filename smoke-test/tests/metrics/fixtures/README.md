# Metrics Seed Fixture

Dev-only fixture for manual verification of the Metrics UI (CAT-2567 PR 1–4). Kept on a local
branch; **do not open a PR** while the feature PRs are in flight.

## Prerequisites

1. DataHub running locally:

   ```bash
   scripts/dev/datahub-dev.sh start
   ```

2. Initialise the DataHub CLI (one-time):

   ```bash
   datahub init --username datahub --password datahub
   ```

3. Enable the metrics feature flag:

   ```bash
   scripts/dev/datahub-dev.sh env set METRICS_ENABLED=true
   scripts/dev/datahub-dev.sh env restart
   ```

## Seeding

```bash
smoke-test/venv/bin/python smoke-test/tests/metrics/fixtures/seed_metrics.py
```

Re-running is safe — all entities are upserted (idempotent).

### Options

| Flag              | Description                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| `--gms-url <url>` | Override GMS URL (default: `$DATAHUB_GMS_URL` or `http://localhost:8080`) |
| `--token <token>` | Override access token (default: `$DATAHUB_GMS_TOKEN`)                     |
| `--dry-run`       | Print all MCPs to stdout without emitting anything                        |
| `--wipe`          | Soft-delete all seeded entities (`status.removed = true`)                 |

## Verification

After seeding, confirm entities exist with:

```bash
# Expect total >= 3
datahub graphql --query '{ getSemanticModels(input:{count:10, start:0}) { total semanticModels { urn } } }'

# Expect total >= 10
datahub graphql --query '{ getRootMetrics(input:{count:20, start:0}) { total } }'
```

Re-run to confirm idempotency (same URNs, no duplicates).

## Wiping

```bash
smoke-test/venv/bin/python smoke-test/tests/metrics/fixtures/seed_metrics.py --wipe
```

## Seeded entities

### Semantic models (3)

| URN                                                                                          | Display name                   | Platform   | Purpose                              |
| -------------------------------------------------------------------------------------------- | ------------------------------ | ---------- | ------------------------------------ |
| `urn:li:semanticModel:(urn:li:dataPlatform:snowflake,b2b_sales,b2b_sales_bookings)`          | B2B Sales Bookings & Pipelines | Snowflake  | Fully-populated detail page          |
| `urn:li:semanticModel:(urn:li:dataPlatform:databricks,web_analytics,web_performance_funnel)` | Web Performance & Funnel       | Databricks | Partially populated                  |
| `urn:li:semanticModel:(urn:li:dataPlatform:tableau,accounts,accounts_semantic_layer)`        | Accounts Semantic Layer        | Tableau    | Sparse — tests conditional rendering |

### Upstream datasets (5)

| URN                                                                              |
| -------------------------------------------------------------------------------- |
| `urn:li:dataset:(urn:li:dataPlatform:snowflake,salesforce.dim_opportunity,PROD)` |
| `urn:li:dataset:(urn:li:dataPlatform:snowflake,salesforce.dim_account,PROD)`     |
| `urn:li:dataset:(urn:li:dataPlatform:snowflake,shared.dim_date,PROD)`            |
| `urn:li:dataset:(urn:li:dataPlatform:databricks,web.dim_session,PROD)`           |
| `urn:li:dataset:(urn:li:dataPlatform:databricks,web.fact_pageview,PROD)`         |

### Metrics (16)

| URN                                                                                     | Display name                  | Notes                                                     |
| --------------------------------------------------------------------------------------- | ----------------------------- | --------------------------------------------------------- |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,bookings)`                      | bookings                      | Root; has child `bookings_by_region`                      |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,won_opportunities)`             | won_opportunities             | Leaf; upstream of `win_rate`                              |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,closed_opportunities)`          | closed_opportunities          | Leaf; upstream of `win_rate`                              |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,avg_deal_size_won)`             | avg_deal_size_won             | ANSI_SQL dialect                                          |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,avg_deal_cycle_days)`           | avg_deal_cycle_days           | Snowflake dialect                                         |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,bookings_target)`               | bookings_target               | No upstream datasets                                      |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,pipeline_generated)`            | pipeline_generated            | Snowflake dialect                                         |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,win_rate)`                      | win_rate                      | Derived from `won_opportunities` + `closed_opportunities` |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,bookings_by_region)`            | bookings_by_region            | Child of `bookings`                                       |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,bookings_by_region_by_quarter)` | bookings_by_region_by_quarter | Grandchild of `bookings`                                  |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,deep_root_metric)`              | deep_root_metric              | Root of 6-level chain                                     |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,deep_level_2)`                  | deep_level_2                  | Level 2                                                   |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,deep_level_3)`                  | deep_level_3                  | Level 3                                                   |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,deep_level_4)`                  | deep_level_4                  | Level 4                                                   |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,deep_level_5)`                  | deep_level_5                  | Level 5                                                   |
| `urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,deep_leaf_metric)`              | deep_leaf_metric              | Depth 6 — header should truncate with `…`                 |
| `urn:li:metric:(urn:li:dataPlatform:databricks,web_analytics,total_sessions)`           | total_sessions                | Databricks dialect                                        |
| `urn:li:metric:(urn:li:dataPlatform:databricks,web_analytics,bounce_rate)`              | bounce_rate                   | Databricks dialect                                        |
| `urn:li:metric:(urn:li:dataPlatform:tableau,accounts,tableau_metric_sparse)`            | tableau_metric_sparse         | Only `name` set — tests sparse rendering                  |

## Manual verification checklist

Relevant items from the plan per PR:

**PR 1 (routing + entity registry)**

- Visit `/metric/urn:li:metric:(urn:li:dataPlatform:snowflake,b2b_sales,bookings)` — placeholder profile renders
- Visit `/semanticModel/urn:li:semanticModel:(urn:li:dataPlatform:snowflake,b2b_sales,b2b_sales_bookings)` — placeholder renders

**PR 2 (sidebar)**

- `/metrics` landing page shows 3 semantic models and counts
- Expanding "B2B Sales Bookings & Pipelines" reveals its metrics
- Expanding `bookings` shows `bookings_by_region` as a child

**PR 3 (SemanticModel detail)**

- Fully-populated model: all sections visible (Datasets, Metrics, Relationships, Dimensions, AI Context)
- Sparse model (`accounts_semantic_layer`): only `name` section visible

**PR 4 (Metric detail)**

- `bookings` breadcrumb: `Metric > B2B Sales Bookings & Pipelines > bookings`
- `bookings_by_region` breadcrumb walks the parent chain
- `deep_leaf_metric` triggers the 5-level `…` truncation
- `tableau_metric_sparse` shows only sections that have data
