### Overview

:::info Looking for Amazon Data Firehose (formerly Kinesis Data Firehose)?

You're in the right place — Firehose streams are ingested by this same `kinesis` connector. See the `kinesis-firehose` platform section below.
:::

This connector ingests both AWS streaming services with one recipe, one IAM policy, and one ingestion job:

- **`kinesis`** (display name: _Amazon Kinesis Data Streams_) — KDS streams are emitted as **Datasets** (`Stream` subtype) under a regional Container, with `StreamARN`, shard count, retention, encryption, and stream mode in custom properties, AWS resource tags as DataHub tags, and (optionally) `schemaMetadata` resolved from AWS Glue Schema Registry.
- **`kinesis-firehose`** (display name: _Amazon Data Firehose_) — each Firehose stream is emitted as its own **DataFlow** (`Firehose Stream` subtype) containing a single **DataJob** (`Delivery` subtype), whose `dataJobInputOutput` edges draw lineage from the source Kinesis stream to the destination platform. Six destinations are supported: S3, Redshift, OpenSearch/Elasticsearch, Snowflake, Apache Iceberg, and MongoDB.

Cross-service lineage (e.g. `KDS Stream → Firehose stream → S3`) is rendered in the DataHub lineage viewer as edges crossing platform boundaries, making the data flow immediately legible.

The connector is API-based (boto3 + AWS IAM SigV4) and **region-scoped per recipe** — a multi-region setup runs multiple recipes, one per region. The region is encoded in dataset names and Firehose DataFlow ids, so multiple regions of the same account share one `platform_instance` (the account ID, by default) without colliding on URN.

### Prerequisites

#### AWS IAM Permissions

The connector needs read-only access to the Kinesis, Firehose, and (optionally) Glue services. The minimum policy is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "KinesisDataStreamsRead",
      "Effect": "Allow",
      "Action": [
        "kinesis:ListStreams",
        "kinesis:DescribeStream",
        "kinesis:ListTagsForStream"
      ],
      "Resource": "*"
    },
    {
      "Sid": "KinesisFirehoseRead",
      "Effect": "Allow",
      "Action": [
        "firehose:ListDeliveryStreams",
        "firehose:DescribeDeliveryStream",
        "firehose:ListTagsForDeliveryStream"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueSchemaRegistryRead",
      "Effect": "Allow",
      "Action": ["glue:ListRegistries", "glue:GetSchemaVersion"],
      "Resource": "*"
    }
  ]
}
```

Notes on each statement:

- **Account ID resolution** — no extra permission is needed. When you don't set `platform_instance` explicitly, the connector derives the AWS account ID from a resource ARN (parsed from the `kinesis:DescribeStream` / `firehose:DescribeDeliveryStream` calls it already makes) and uses it as the default `platform_instance` (`<account_id>`), so URNs disambiguate across accounts; the region is encoded in the dataset name and DataFlow id rather than the `platform_instance`. If no resource is available or the lookup fails, the connector logs a warning and continues with `platform_instance=None`; URNs then won't include the account ID, so cross-account collision-safety depends on you setting `platform_instance` explicitly in the recipe.
- **`KinesisFirehoseRead`** — required only when `include_firehose: true` (the default). If you don't have these permissions and Firehose extraction is enabled, the connector logs a warning ("Permission denied for Firehose") and continues with KDS only — Firehose section is skipped, KDS ingestion proceeds normally.
- **`GlueSchemaRegistryRead`** — required only when `glue_schema_registry.enabled: true`. AWS also provides a ready-made managed policy ([`AWSGlueSchemaRegistryReadonlyAccess`](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSGlueSchemaRegistryReadonlyAccess.html)) you can attach instead.
- **`KinesisDataStreamsRead`** — denial of `kinesis:ListStreams` on the first page is logged as a warning and the KDS section is skipped (the user may intentionally have Firehose-only IAM). A mid-pagination failure escalates to `report.failure` to prevent stateful ingestion from soft-deleting un-listed streams on the next run.

#### Authentication

Credentials are resolved by the standard boto3 chain, in priority order:

1. Static credentials in `aws_config` (`aws_access_key_id` + `aws_secret_access_key`, plus `aws_session_token` for STS temporary credentials).
2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables (and `AWS_SESSION_TOKEN` when applicable).
3. A profile selected by `aws_config.aws_profile` from `~/.aws/credentials`.
4. An IAM role attached to the EC2 / ECS / EKS host the ingestion runs on.
5. An AWS SSO profile.

The three patterns below cover most setups. **Prefer IAM roles or short-lived SSO credentials over long-lived access keys in checked-in recipes.**

**Environment variables (recommended for CI / containers)** — inject `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN` when using temporary credentials) as env vars, and leave only `aws_region` in the recipe:

```yaml
aws_config:
  aws_region: "us-east-1"
```

**Assume-role (recommended for cross-account access)** — set `aws_config.aws_role` to the role ARN. The credentials picked up by steps 1–5 above must have `sts:AssumeRole` on the target role:

```yaml
aws_config:
  aws_region: "us-east-1"
  aws_role: "arn:aws:iam::123456789012:role/datahub-kinesis-read"
  # aws_external_id: "${DATAHUB_EXTERNAL_ID}"  # if required by trust policy
```

**Named profile (recommended for local development)** — reference a profile from `~/.aws/credentials`:

```yaml
aws_config:
  aws_region: "us-east-1"
  aws_profile: "datahub-prod"
```
