---
title: Setup
---
# Redshift Ingestion Guide: Setup & Prerequisites

To configure ingestion from Redshift, you'll need a [User](https://docs.aws.amazon.com/redshift/latest/gsg/t_adding_redshift_user_cmd.html) configured with the proper permission sets, and an associated.

This setup guide will walk you through the steps you'll need to take via your Google Cloud Console.

## Redshift Prerequisites

1. Connect to your Amazon Redshift cluster using an SQL client such as SQL Workbench/J or Amazon Redshift Query Editor with your Admin user.
2. Create a [Redshift User](https://docs.aws.amazon.com/redshift/latest/gsg/t_adding_redshift_user_cmd.html) that will be used to perform the metadata extraction if you don't have one already.
For example:

```sql
CREATE USER datahub WITH PASSWORD 'Datahub1234';
```

## Redshift Setup

1. Grant the following permission to your `datahub` user:

```sql
ALTER USER datahub WITH SYSLOG ACCESS UNRESTRICTED;
GRANT SELECT ON pg_catalog.svv_table_info to datahub;
GRANT SELECT ON pg_catalog.svl_user_info to datahub;

```

## Next Steps

Once you've confirmed all of the above in Redshift, it's time to [move on](configuration.md) to configure the actual ingestion source within the DataHub UI.


