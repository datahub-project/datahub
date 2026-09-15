---
description: "Count the assets in a DataHub Core instance with read-only SQL queries, so a DataHub Cloud environment can be sized correctly before upgrading."
---

# Counting Assets for DataHub Cloud Sizing

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Planning an upgrade to **DataHub Cloud**? The first thing we'll need is an accurate count of the assets in your **DataHub Core (OSS)** instance, so that your Cloud environment is sized correctly. In this guide, we'll show you how to get that count.

The queries below are read-only `SELECT` statements against your DataHub database. They work with both MySQL and PostgreSQL, whether your database runs in Docker, in Kubernetes, or as an external managed service such as Amazon RDS, Cloud SQL, or Azure Database.

Once you have your numbers, head over to [Upgrading from DataHub Core (OSS) to DataHub Cloud](upgrade_core_to_cloud.md) to plan the transfer itself.

## What Gets Counted

DataHub stores all of its metadata in a single table, `metadata_aspect_v2`. Every entity in your catalog has exactly one "key" aspect in that table, so counting those aspects gives you an exact count of your catalog.

The sizing count covers the following entity types:

| Entity type      | DataHub key aspect  |
| ---------------- | ------------------- |
| Dataset          | `datasetKey`        |
| Chart            | `chartKey`          |
| Dashboard        | `dashboardKey`      |
| Data Flow        | `dataFlowKey`       |
| Data Job         | `dataJobKey`        |
| Container        | `containerKey`      |
| ML Model         | `mlModelKey`        |
| ML Model Group   | `mlModelGroupKey`   |
| ML Feature       | `mlFeatureKey`      |
| ML Feature Table | `mlFeatureTableKey` |
| ML Primary Key   | `mlPrimaryKeyKey`   |

Users, groups, glossary terms, domains, tags, and schema fields are not included in the sizing count.

## Step 1: Find Your Database Connection Details

Your GMS container already knows how to reach the database, so the quickest way to find your connection details is to ask it. This works the same way whether your database runs alongside DataHub or is a managed service.

The commands below deliberately leave out `EBEAN_DATASOURCE_PASSWORD`, so nothing sensitive is printed to your terminal.

### Docker

First, find your GMS container:

```bash
docker ps --format '{{.Names}}' | grep gms
```

Then inspect its database settings:

```bash
docker exec <GMS_CONTAINER> env \
  | grep -E '^EBEAN_DATASOURCE_(HOST|URL|USERNAME|DRIVER)='
```

### Kubernetes

First, find your namespace and GMS deployment:

```bash
kubectl get deployments --all-namespaces | grep gms
```

Then inspect its database settings:

```bash
kubectl exec --namespace <NAMESPACE> deployment/<GMS_DEPLOYMENT> -- \
  printenv EBEAN_DATASOURCE_HOST EBEAN_DATASOURCE_URL \
  EBEAN_DATASOURCE_USERNAME EBEAN_DATASOURCE_DRIVER
```

### Reading the Output

You'll get back something like this:

```text
EBEAN_DATASOURCE_HOST=mysql:3306
EBEAN_DATASOURCE_URL=jdbc:mysql://mysql:3306/datahub?verifyServerCertificate=false&useSSL=true
EBEAN_DATASOURCE_USERNAME=datahub
EBEAN_DATASOURCE_DRIVER=com.mysql.cj.jdbc.Driver
```

From that you have everything you need:

- **Host and port** come from `EBEAN_DATASOURCE_HOST`, in `hostname:port` form
- **Database name** is the path in `EBEAN_DATASOURCE_URL`, before any `?`, and is usually `datahub`
- **Username** comes from `EBEAN_DATASOURCE_USERNAME`
- **Database type** is given away by `EBEAN_DATASOURCE_DRIVER`, which tells you whether you're on MySQL or PostgreSQL

:::note Splitting the host value
`EBEAN_DATASOURCE_HOST` combines the hostname and port, but the `mysql` and `psql` clients expect them separately. A value of `mysql:3306` becomes `-h mysql -P 3306`.
:::

For the password, use the credentials from your DataHub deployment configuration, or ask your database administrator. A read-only account is all you need.

## Step 2: Connect to Your Database

Pick the option below that matches your deployment.

### Docker with a Bundled Database

If you're running the standard DataHub quickstart, which uses MySQL:

```bash
docker exec -it mysql /usr/bin/mysql datahub --user=datahub --password=datahub
```

If your deployment uses PostgreSQL instead:

```bash
docker exec -it <POSTGRES_CONTAINER> psql -U datahub -d datahub
```

If Docker Compose has given the container a prefixed or generated name, use the name shown by `docker ps`. For more on inspecting the quickstart database, see [How can I check if data has been loaded into MySQL properly?](../troubleshooting/quickstart.md#how-can-i-check-if-data-has-been-loaded-into-mysql-properly).

### Docker with an External Database

Use your usual SQL client, or launch a throwaway client container:

```bash
# MySQL
docker run --rm -it mysql:8 \
  mysql -h <HOST> -P <PORT> -u <USERNAME> -p <DATABASE>

# PostgreSQL
docker run --rm -it postgres:16 \
  psql -h <HOST> -p <PORT> -U <USERNAME> -d <DATABASE>
```

Your database host needs to be reachable from that container. If it isn't, connect using a client that already has network access instead.

### Kubernetes with an In-Cluster Database

First, find the database pod:

```bash
kubectl get pods --namespace <NAMESPACE> | grep -E 'mysql|postgres'
```

Then open a client inside it:

```bash
# MySQL
kubectl exec -it --namespace <NAMESPACE> <MYSQL_POD> -- \
  mysql -u <USERNAME> -p <DATABASE>

# PostgreSQL
kubectl exec -it --namespace <NAMESPACE> <POSTGRES_POD> -- \
  psql -U <USERNAME> -d <DATABASE>
```

### Kubernetes with an External Database

Run a temporary client pod inside the cluster. This is the easiest option when your database is only reachable from the cluster network or VPC:

```bash
# MySQL
kubectl run datahub-count --rm -it --restart=Never \
  --namespace <NAMESPACE> --image=mysql:8 -- \
  mysql -h <HOST> -P <PORT> -u <USERNAME> -p <DATABASE>

# PostgreSQL
kubectl run datahub-count --rm -it --restart=Never \
  --namespace <NAMESPACE> --image=postgres:16 -- \
  psql -h <HOST> -p <PORT> -U <USERNAME> -d <DATABASE>
```

The pod is removed automatically when you exit the client. If your cluster restricts image pulls or ad-hoc pods, use your organization's approved database client instead.

## Step 3: Count Your Total Assets

:::caution
These are read-only queries, but on a large `metadata_aspect_v2` table they can put load on your primary database. Where possible, run them during a quiet period or against a read replica. The optional query in [Step 5](#step-5-count-only-active-assets-optional) is the most expensive, as it joins the table to itself.
:::

```sql
SELECT COUNT(DISTINCT urn) AS total_assets
FROM metadata_aspect_v2
WHERE version = 0
  AND aspect IN (
    'datasetKey', 'chartKey', 'dashboardKey',
    'dataFlowKey', 'dataJobKey', 'containerKey',
    'mlModelKey', 'mlModelGroupKey', 'mlFeatureKey',
    'mlFeatureTableKey', 'mlPrimaryKeyKey'
  );
```

This is the headline number for sizing. `version = 0` is DataHub's "latest value" slot, so earlier versions of the same aspect are never double-counted. The result includes assets that have been soft-deleted.

## Step 4: Count Assets by Type

This breakdown shows where the volume in your catalog actually sits, which helps with ingestion planning as well as sizing:

```sql
SELECT
    aspect,
    COUNT(DISTINCT urn) AS entity_count
FROM metadata_aspect_v2
WHERE version = 0
  AND aspect IN (
    'datasetKey', 'chartKey', 'dashboardKey',
    'dataFlowKey', 'dataJobKey', 'containerKey',
    'mlModelKey', 'mlModelGroupKey', 'mlFeatureKey',
    'mlFeatureTableKey', 'mlPrimaryKeyKey'
  )
GROUP BY aspect
ORDER BY entity_count DESC;
```

## Step 5: Count Only Active Assets (Optional)

When an asset is deleted from the DataHub UI it is soft-deleted, which means its key aspect stays in the database but the asset is hidden from search and browse. The queries above include those assets, so if you've done a lot of cleanup your total may be higher than the number you see in the UI.

To count only what's live in your catalog, run the query for your database type. Assets with no `status` aspect at all are treated as active.

<Tabs>
<TabItem value="mysql" label="MySQL" default>

```sql
SELECT COUNT(DISTINCT k.urn) AS active_assets
FROM metadata_aspect_v2 AS k
LEFT JOIN metadata_aspect_v2 AS s
  ON s.urn = k.urn
  AND s.aspect = 'status'
  AND s.version = 0
WHERE k.version = 0
  AND k.aspect IN (
    'datasetKey', 'chartKey', 'dashboardKey',
    'dataFlowKey', 'dataJobKey', 'containerKey',
    'mlModelKey', 'mlModelGroupKey', 'mlFeatureKey',
    'mlFeatureTableKey', 'mlPrimaryKeyKey'
  )
  AND COALESCE(JSON_EXTRACT(s.metadata, '$.removed'), false) = false;
```

</TabItem>
<TabItem value="postgresql" label="PostgreSQL">

```sql
SELECT COUNT(DISTINCT k.urn) AS active_assets
FROM metadata_aspect_v2 AS k
LEFT JOIN metadata_aspect_v2 AS s
  ON s.urn = k.urn
  AND s.aspect = 'status'
  AND s.version = 0
WHERE k.version = 0
  AND k.aspect IN (
    'datasetKey', 'chartKey', 'dashboardKey',
    'dataFlowKey', 'dataJobKey', 'containerKey',
    'mlModelKey', 'mlModelGroupKey', 'mlFeatureKey',
    'mlFeatureTableKey', 'mlPrimaryKeyKey'
  )
  AND COALESCE(((s.metadata::json)->>'removed')::boolean, false) = false;
```

</TabItem>
</Tabs>

It's worth running this alongside the total: the gap between the two tells us how much of your catalog is retired, which is a useful input into whether to migrate it or leave it behind.

## Step 6: Share Your Results

Send the following to your DataHub representative:

1. The total asset count from Step 3
2. The per-type breakdown from Step 4
3. The active asset count from Step 5, if you ran it

If you run more than one DataHub instance, run the queries against each one and share the results separately, as they're sized independently.

## Next Steps

Once your Cloud environment is sized, continue with [Upgrading from DataHub Core (OSS) to DataHub Cloud](upgrade_core_to_cloud.md) to transfer your metadata.
