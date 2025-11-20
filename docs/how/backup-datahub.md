# DataHub Backup & Restore

DataHub stores metadata in two key storage systems that require separate backup approaches:

1. **Versioned Aspects**: Stored in a relational database (MySQL/PostgreSQL) in the `metadata_aspect_v2` table
2. **Time Series Aspects, Search Indexes, & Graph Relationships**: Stored in Elasticsearch/OpenSearch indexes

This guide outlines how to properly back up both components to ensure complete recoverability of your DataHub instance.

## Production Environment Backups

### Backing Up Document Store (Versioned Metadata)

The recommended backup strategy is to periodically dump the `metadata_aspect_v2` table from the `datahub` database. This table contains all versioned aspects and can be restored in case of database failure. Most managed database services (e.g., AWS RDS) provide automated backup capabilities.

#### AWS Managed RDS

**Option 1: Automated RDS Snapshots**

1. Go to **AWS Console > RDS > Databases**
2. Select your DataHub RDS instance
3. Click **Actions > Take Snapshot**
4. Name the snapshot (e.g., `datahub-backup-YYYY-MM-DD`)
5. Configure automated snapshots in RDS with appropriate retention periods (recommended: 14-30 days)

**Option 2: SQL Dump (MySQL)**

For a targeted backup of only the essential metadata:

`mysqldump -h <rds-endpoint> -u <username> -p datahub metadata_aspect_v2 > metadata_aspect_v2_backup.sql`

To compress the backup:

`mysqldump -h <rds-endpoint> -u <username> -p datahub metadata_aspect_v2 | gzip > metadata_aspect_v2_backup.sql.gz`

#### Self-Hosted MySQL

`mysqldump -u <username> -p datahub metadata_aspect_v2 > metadata_aspect_v2_backup.sql`

Compressed version:

`mysqldump -u <username> -p datahub metadata_aspect_v2 | gzip > metadata_aspect_v2_backup.sql.gz`

### Backing Up Time Series Aspects (Elasticsearch/OpenSearch)

Time Series Aspects power important features like usage statistics, dataset profiles, and assertion runs. These are stored in Elasticsearch/OpenSearch and require a separate backup strategy.

#### AWS OpenSearch Service

1. **Create an IAM Role for Snapshots**

   Create an IAM role with permissions to write to an S3 bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": ["s3:ListBucket"],
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::your-backup-bucket"]
    },
    {
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Effect": "Allow",
      "Resource": ["arn:aws:s3:::your-backup-bucket/*"]
    }
  ]
}
```

Ensure the trust relationship allows OpenSearch to assume this role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "es.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

2. **Register a Snapshot Repository**

```
   PUT _snapshot/datahub_s3_backup
   {
      "type": "s3",
      "settings": {
         "bucket": "your-backup-bucket",
         "region": "us-east-1",
         "role_arn": "arn:aws:iam::<account-id>:role/<snapshot-role>"
      }
   }
```

> ⚠️ **Important**: The S3 bucket must be in the same AWS region as your OpenSearch domain.

3. **Create a Regular Snapshot Schedule**

   Set up an automated schedule using the OpenSearch Snapshot Management:

```
   PUT _plugins/_sm/policies/datahub_backup_policy
   {
      "schedule": {
         "cron": {
            "expression": "0 0 * * *",
            "timezone": "UTC"
         }
      },
      "name": "<snapshot-{now/d}>",
      "repository": "datahub_s3_backup",
      "config": {
         "partial": false
      },
      "retention": {
         "expire_after": "15d",
         "min_count": 5,
         "max_count": 30
      }
   }
```

This configures daily snapshots with a 15-day retention period.

4. **Take a Manual Snapshot** (if needed)

   `PUT _snapshot/datahub_s3_backup/snapshot_YYYY_MM_DD?wait_for_completion=true`

5. **Verify Snapshot Status**

   `GET _snapshot/datahub_s3_backup/snapshot_YYYY_MM_DD`

#### Self-Hosted Elasticsearch

1. **Create a Local Repository**

   First, add `path.repo` setting to `elasticsearch.yml` on all nodes:

   path.repo: ["/mnt/es-backups"]

   Ensure `/mnt/es-backups` is a shared or mounted path on all Elasticsearch nodes.

2. **Register the Repository**

```
   PUT _snapshot/datahub_fs_backup
   {
      "type": "fs",
      "settings": {
         "location": "/mnt/es-backups",
         "compress": true
      }
   }
```

3. **Create a Snapshot**

   `PUT \_snapshot/datahub_fs_backup/snapshot_YYYY_MM_DD?wait_for_completion=true`

4. **Check Snapshot Status**

   `GET \_snapshot/datahub_fs_backup/snapshot_YYYY_MM_DD`

## Restoring DataHub from Backups

### Restoring the MySQL Database

1. **Restore from an RDS Snapshot** (if using AWS RDS)

   In the AWS Console, go to **RDS > Snapshots**, select your snapshot, and choose "Restore Snapshot".

2. **Restore from SQL Dump**

   `mysql -h <host> -u <user> -p datahub < metadata_aspect_v2_backup.sql`

### Restoring Elasticsearch/OpenSearch Indices

After restoring the database, you need to restore the search and graph indices using your snapshots.

Note that you can also rebuild the index from scratch after restoring the MySQL / Postgres Document Store,
as outlined [here](./restore-indices.md).

#### Restoring from Snapshots

To restore search indexes from a snapshot:

```
POST _snapshot/datahub_s3_backup/snapshot_YYYY_MM_DD/_restore
{
   "indices": "datastream*,metadataindex*",
   "include_global_state": false
}
```

## Testing Your Backup Strategy

Regularly test your backup and restore procedures to ensure they work when needed:

1. Create a test environment
2. Restore your production backups to this environment
3. Verify that all functionality works correctly
4. Document any issues encountered and update your backup/restore procedures

A good practice is to test restore procedures quarterly or after significant infrastructure changes.
