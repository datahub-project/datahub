


# Amazon Kinesis Data Streams

## Overview

AWS Kinesis is a real-time streaming and data-delivery service. See the [official AWS Kinesis page](https://aws.amazon.com/kinesis/) for product details.

This connector covers both AWS streaming services with one recipe and one IAM policy:

- **Amazon Kinesis Data Streams (KDS)** — emitted under the `kinesis` platform (display name: _Amazon Kinesis Data Streams_) as **Datasets** (`Stream` subtype).
- **Amazon Data Firehose** (formerly _Amazon Kinesis Data Firehose / KDF_) — each Firehose stream is emitted under the `kinesis-firehose` platform (display name: _Amazon Data Firehose_) as its own **DataFlow** (`Firehose Stream` subtype) containing a single **DataJob** (`Delivery` subtype) that carries cross-platform lineage edges to the destination (S3, Redshift, OpenSearch, Snowflake, Apache Iceberg, MongoDB).

AWS resource tags become DataHub tags (and can be turned into ownership via the `extract_ownership_from_tags` transformer). Glue Schema Registry can be opted in to attach Avro / JSON / Protobuf schemas to streams.

:::info Looking specifically for Amazon Data Firehose?

Firehose streams are ingested by this same connector — see the [Concept Mapping](#concept-mapping) table below or the [Limitations](#limitations) section for cross-platform lineage configuration.
:::

## Concept Mapping

| Source Concept                                                               | DataHub Concept                                           | Notes                                                                                                                               |
| ---------------------------------------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `"kinesis"` / `"kinesis-firehose"`                                           | [Data Platform](../../metamodel/entities/dataPlatform.md) | Two platforms, mirroring AWS's own service split.                                                                                   |
| AWS Region                                                                   | [Container](../../metamodel/entities/container.md)        | Subtype `Region`. One per recipe.                                                                                                   |
| Kinesis Data Stream                                                          | [Dataset](../../metamodel/entities/dataset.md)            | Subtype `Stream`. Parent: the regional Container.                                                                                   |
| Glue Schema Registry schema (per stream)                                     | `SchemaMetadata` aspect                                   | Avro / JSON / Protobuf. Attached when `glue_schema_registry.enabled: true` and a schema is resolved for the stream.                 |
| Firehose stream                                                              | [DataFlow](../../metamodel/entities/dataFlow.md)          | Subtype `Firehose Stream`. One DataFlow per Firehose stream (the pipeline).                                                         |
| Firehose delivery (the stream's data movement)                               | [DataJob](../../metamodel/entities/dataJob.md)            | Subtype `Delivery`. The single DataJob inside each Firehose stream's DataFlow; carries the lineage.                                 |
| Firehose destination (S3, Redshift, OpenSearch, Snowflake, Iceberg, MongoDB) | Lineage edge                                              | Emitted via `dataJobInputOutput.outputDatasets`. Upstream is the source KDS stream when `DeliveryStreamType=KinesisStreamAsSource`. |
| AWS resource tag (`Key=Value`)                                               | Tag                                                       | Tag URN form: `urn:li:tag:Key:Value`.                                                                                               |
| AWS resource tag (via the `extract_ownership_from_tags` transformer)         | Owner                                                     | Ownership is derived from the emitted tags by the transformer, not by this source directly.                                         |

### Compatibility

Six Firehose destination platforms are supported: S3, Redshift, OpenSearch/Elasticsearch, Snowflake, Apache Iceberg, and MongoDB. Firehose streams targeting other destinations (HTTP, Datadog, Splunk, New Relic, etc.) are still emitted (as a DataFlow + Delivery DataJob) but without lineage output edges, and surface a warning in the ingestion report. See [Limitations](#limitations) for the `destination_platform_map` configuration required when destination platforms were ingested with a non-default `platform_instance`.


## Module `kinesis`
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Region containers. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Via stateful ingestion. |
| Extract Tags | ✅ | From AWS resource tags. |
| Schema Metadata | ✅ | Opt-in via `glue_schema_registry.enabled` (AWS Glue Schema Registry). |
| Table-Level Lineage | ✅ | Firehose -> destination lineage. |

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


### Install the Plugin
```shell
pip install 'acryl-datahub[kinesis]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
pipeline_name: "kinesis_ingestion"
source:
  type: "kinesis"
  config:
    # One recipe per AWS region. platform_instance defaults to the AWS
    # account ID (derived from a resource ARN); set it explicitly only to
    # disambiguate across accounts. The region is already encoded in dataset
    # names and the Firehose DataFlow id, so different regions of the same
    # account never collide on URN.
    # platform_instance: "prod-account"
    env: "PROD"

    # Standard DataHub AWS connection config. Credentials are resolved by
    # the boto3 chain (env vars, shared credentials file, IAM role on
    # EC2/ECS/EKS, SSO profile). Prefer IAM roles where possible; the
    # AWS_*_KEY env vars below are shown only for completeness.
    # aws_config:
      # aws_region: "us-west-2"
      # aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      # aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
      # aws_role: "arn:aws:iam::123456789012:role/datahub-kinesis-ingest"

    # --- Kinesis Data Streams (KDS) -----------------------------------
    include_streams: true
    stream_pattern:
      # Exclude internal / debug / audit streams by default.
      deny:
        - "^_.*"

    # --- Amazon Data Firehose -----------------------------------------
    include_firehose: true
    firehose_stream_pattern:
      allow:
        - ".*"
    include_table_lineage: true

    # --- Tags ---------------------------------------------------------
    # AWS resource tags are emitted as DataHub globalTags. To derive
    # ownership from a tag, apply the extract_ownership_from_tags
    # transformer (see the `transformers:` block below).
    extract_tags: true

    # --- Cross-platform lineage ---------------------------------------
    # Required when a destination platform was ingested with a
    # non-default platform_instance — without it, Firehose lineage
    # edges reference valid-but-dead URNs. See the Limitations section
    # of the connector docs.
    #
    # Allowed keys (validated at parse time):
    #   s3, redshift, snowflake, iceberg, mongodb, elasticsearch, glue
    #
    # Per-destination fields:
    #   platform_instance        — match the destination source's setting
    #   env                      — match the destination source's env
    #   convert_urns_to_lowercase — default True. Set False for
    #                              case-sensitive destinations (Iceberg,
    #                              MongoDB) or when the destination's own
    #                              source has convert_urns_to_lowercase=False.
    # destination_platform_map:
      # snowflake:
      #   platform_instance: "prod-snowflake-east"
      #   env: "PROD"
      #   convert_urns_to_lowercase: false  # only if Snowflake source also has this
      # redshift:
      #   platform_instance: "analytics-cluster"
      #   env: "PROD"
      # s3:
      #   platform_instance: "data-lake-prod"
      # iceberg:
      #   convert_urns_to_lowercase: false  # Iceberg catalogs are case-sensitive

    # --- Glue Schema Registry (optional) ------------------------------
    # Disabled by default. Requires the extra `glue:Get*` / `glue:List*`
    # IAM statement listed in the connector prerequisites.
    glue_schema_registry:
      enabled: true
      registry_name: "default-registry"
      # Explicit stream -> schema overrides — the recommended way to attach
      # schemas. Unlike Kafka + Confluent Schema Registry, AWS does NOT define
      # a relationship between a Kinesis stream and a schema; schemas are
      # chosen per-record by producers. List your known stream->schema pairs
      # explicitly:
      stream_schema_map: {}
      #   events: "events-v2"
      #   clicks: "click-events-schema"
      # Optional heuristic: look up a schema whose name == stream name.
      # Off by default (no AWS-defined convention). Enable only if your
      # organization has adopted schema-name == stream-name as an internal
      # convention.
      use_naming_convention: false

    # --- Stateful ingestion (deletion detection) ----------------------
    # Soft-deletes entities that have disappeared from AWS since the last
    # successful run (diffed per ingestion job — independent of platform_instance).
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true
      fail_safe_threshold: 100

# Derive ownership from an AWS `owner` tag (emitted above as the globalTag
# `owner:<value>`). Uncomment to map it to a corpuser owner.
# transformers:
#   - type: "extract_ownership_from_tags"
#     config:
#       tag_pattern: "owner:"

# Default sink is datahub-rest, picked up from ~/.datahubenv (set via
# `datahub init`). Override here only when you need a different target.
# See https://docs.datahub.com/docs/metadata-ingestion/sink_docs/datahub
# for customization options.

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">extract_tags</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract AWS resource tags as DataHub globalTags. To derive ownership from a tag, apply the `extract_ownership_from_tags` transformer to the emitted tags (see the connector docs). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_firehose</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Amazon Data Firehose streams. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_streams</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract Kinesis Data Streams (KDS). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_table_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit Firehose-stream → destination lineage edges. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">aws_config</span></div> <div className="type-name-line"><span className="type-name">AwsConnectionConfig</span></div> | Common AWS credentials config. <br />  <br /> Currently used by: <br />     - Glue source <br />     - SageMaker source <br />     - dbt source  |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_access_key_id</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_advanced_config</span></div> <div className="type-name-line"><span className="type-name">object</span></div> | Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).  |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_endpoint_url</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_profile</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_proxy</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_region</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | AWS region code. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_retry_mode</span></div> <div className="type-name-line"><span className="type-name">Enum</span></div> | One of: "legacy", "standard", "adaptive" <div className="default-line default-line-with-docs">Default: <span className="default-value">standard</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_retry_num</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">5</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_secret_access_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_session_token</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">read_timeout</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | The timeout for reading from the connection (in seconds). <div className="default-line default-line-with-docs">Default: <span className="default-value">60</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.</span><span className="path-main">aws_role</span></div> <div className="type-name-line"><span className="type-name">One of string, array, null</span></div> | AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role). <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">aws_config.aws_role.</span><span className="path-main">union</span></div> <div className="type-name-line"><span className="type-name">One of string, AwsAssumeRoleConfig</span></div> |   |
| <div className="path-line"><span className="path-prefix">aws_config.aws_role.union.</span><span className="path-main">RoleArn</span>&nbsp;<abbr title="Required if union is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | ARN of the role to assume.  |
| <div className="path-line"><span className="path-prefix">aws_config.aws_role.union.</span><span className="path-main">ExternalId</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | External ID to use when assuming the role. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">destination_platform_map</span></div> <div className="type-name-line"><span className="type-name">map(str,DestinationPlatformDetail)</span></div> | Per-destination-platform override for cross-platform lineage URN construction. <br />  <br /> Used by `KinesisSourceConfig.destination_platform_map` so Firehose lineage edges <br /> to S3 / Redshift / Snowflake / etc. resolve to the same URNs those platforms' <br /> own DataHub ingestion sources emit.  |
| <div className="path-line"><span className="path-prefix">destination_platform_map.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | platform_instance of the destination platform (must match the ingestion of that platform). If unset, the downstream URN has no platform_instance slot. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">destination_platform_map.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether to lowercase the URN name (`<db>.<schema>.<table>` etc.) before emitting it. Mirrors the same-named field on platforms' own DataHub source configs (notably Snowflake). Defaults to `True`, matching the Snowflake source's default behavior. Set to `False` when your destination's source recipe set `convert_urns_to_lowercase=False` and your identifiers are case-sensitive (Iceberg, MongoDB, or quoted-identifier Snowflake / Redshift). <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">destination_platform_map.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | env (e.g., PROD, DEV) of the destination. If unset, inherits from this source's env. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">firehose_stream_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">firehose_stream_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">glue_schema_registry</span></div> <div className="type-name-line"><span className="type-name">KinesisGlueSchemaRegistryConfig</span></div> | Glue Schema Registry attachment for Kinesis streams. <br />  <br /> Follows the Kafka source's `topic_subject_map` pattern (see <br /> `confluent_schema_registry.py:81-112`). AWS doesn't store stream→schema <br /> associations server-side; we resolve via explicit map OR naming convention.  |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | When True, fetch Glue Schema Registry schemas for streams. Requires the additional `glue:Get*` / `glue:List*` IAM permissions listed in the connector prerequisites. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">registry_name</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | Glue Schema Registry registry name to query. <div className="default-line default-line-with-docs">Default: <span className="default-value">default-registry</span></div> |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">stream_schema_map</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">glue_schema_registry.</span><span className="path-main">use_naming_convention</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Heuristic fallback: when `stream_schema_map` has no entry for a stream, look up a Glue schema whose name matches the stream name. Disabled by default — unlike Kafka + Confluent Schema Registry (which has a documented `TopicNameStrategy`), AWS Glue Schema Registry has NO AWS-defined relationship between a Kinesis Data Stream and a schema: schemas are chosen per-record by producers, and one stream may carry multiple schemas. Enable only if your organization has adopted schema-name == stream-name as an internal convention. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">stream_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes  |
| <div className="path-line"><span className="path-prefix">stream_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Stateful ingestion configuration for stale entity removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "AwsAssumeRoleConfig": {
      "additionalProperties": true,
      "properties": {
        "RoleArn": {
          "description": "ARN of the role to assume.",
          "title": "Rolearn",
          "type": "string"
        },
        "ExternalId": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "External ID to use when assuming the role.",
          "title": "Externalid"
        }
      },
      "required": [
        "RoleArn"
      ],
      "title": "AwsAssumeRoleConfig",
      "type": "object"
    },
    "AwsConnectionConfig": {
      "additionalProperties": false,
      "description": "Common AWS credentials config.\n\nCurrently used by:\n    - Glue source\n    - SageMaker source\n    - dbt source",
      "properties": {
        "aws_access_key_id": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS access key ID. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Access Key Id"
        },
        "aws_secret_access_key": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS secret access key. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Secret Access Key"
        },
        "aws_session_token": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS session token. Can be auto-detected, see [the AWS boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for details.",
          "title": "Aws Session Token"
        },
        "aws_role": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "items": {
                "anyOf": [
                  {
                    "type": "string"
                  },
                  {
                    "$ref": "#/$defs/AwsAssumeRoleConfig"
                  }
                ]
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS roles to assume. If using the string format, the role ARN can be specified directly. If using the object format, the role can be specified in the RoleArn field and additional available arguments are the same as [boto3's STS.Client.assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html?highlight=assume_role#STS.Client.assume_role).",
          "title": "Aws Role"
        },
        "aws_profile": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The [named profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to use from AWS credentials. Falls back to default profile if not specified and no access keys provided. Profiles are configured in ~/.aws/credentials or ~/.aws/config.",
          "title": "Aws Profile"
        },
        "aws_region": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "AWS region code.",
          "title": "Aws Region"
        },
        "aws_endpoint_url": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The AWS service endpoint. This is normally [constructed automatically](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html), but can be overridden here.",
          "title": "Aws Endpoint Url"
        },
        "aws_proxy": {
          "anyOf": [
            {
              "additionalProperties": {
                "type": "string"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "A set of proxy configs to use with AWS. See the [botocore.config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html) docs for details.",
          "title": "Aws Proxy"
        },
        "aws_retry_num": {
          "default": 5,
          "description": "Number of times to retry failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
          "title": "Aws Retry Num",
          "type": "integer"
        },
        "aws_retry_mode": {
          "default": "standard",
          "description": "Retry mode to use for failed AWS requests. See the [botocore.retry](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html) docs for details.",
          "enum": [
            "legacy",
            "standard",
            "adaptive"
          ],
          "title": "Aws Retry Mode",
          "type": "string"
        },
        "read_timeout": {
          "default": 60,
          "description": "The timeout for reading from the connection (in seconds).",
          "title": "Read Timeout",
          "type": "number"
        },
        "aws_advanced_config": {
          "additionalProperties": true,
          "description": "Advanced AWS configuration options. These are passed directly to [botocore.config.Config](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html).",
          "title": "Aws Advanced Config",
          "type": "object"
        }
      },
      "title": "AwsConnectionConfig",
      "type": "object"
    },
    "DestinationPlatformDetail": {
      "additionalProperties": false,
      "description": "Per-destination-platform override for cross-platform lineage URN construction.\n\nUsed by `KinesisSourceConfig.destination_platform_map` so Firehose lineage edges\nto S3 / Redshift / Snowflake / etc. resolve to the same URNs those platforms'\nown DataHub ingestion sources emit.",
      "properties": {
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "platform_instance of the destination platform (must match the ingestion of that platform). If unset, the downstream URN has no platform_instance slot.",
          "title": "Platform Instance"
        },
        "env": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "env (e.g., PROD, DEV) of the destination. If unset, inherits from this source's env.",
          "title": "Env"
        },
        "convert_urns_to_lowercase": {
          "default": true,
          "description": "Whether to lowercase the URN name (`<db>.<schema>.<table>` etc.) before emitting it. Mirrors the same-named field on platforms' own DataHub source configs (notably Snowflake). Defaults to `True`, matching the Snowflake source's default behavior. Set to `False` when your destination's source recipe set `convert_urns_to_lowercase=False` and your identifiers are case-sensitive (Iceberg, MongoDB, or quoted-identifier Snowflake / Redshift).",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "DestinationPlatformDetail",
      "type": "object"
    },
    "KinesisGlueSchemaRegistryConfig": {
      "additionalProperties": false,
      "description": "Glue Schema Registry attachment for Kinesis streams.\n\nFollows the Kafka source's `topic_subject_map` pattern (see\n`confluent_schema_registry.py:81-112`). AWS doesn't store stream\u2192schema\nassociations server-side; we resolve via explicit map OR naming convention.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "When True, fetch Glue Schema Registry schemas for streams. Requires the additional `glue:Get*` / `glue:List*` IAM permissions listed in the connector prerequisites.",
          "title": "Enabled",
          "type": "boolean"
        },
        "registry_name": {
          "default": "default-registry",
          "description": "Glue Schema Registry registry name to query.",
          "title": "Registry Name",
          "type": "string"
        },
        "stream_schema_map": {
          "additionalProperties": {
            "type": "string"
          },
          "description": "Explicit override map: stream name \u2192 schema name within registry_name. Wins over use_naming_convention.",
          "title": "Stream Schema Map",
          "type": "object"
        },
        "use_naming_convention": {
          "default": false,
          "description": "Heuristic fallback: when `stream_schema_map` has no entry for a stream, look up a Glue schema whose name matches the stream name. Disabled by default \u2014 unlike Kafka + Confluent Schema Registry (which has a documented `TopicNameStrategy`), AWS Glue Schema Registry has NO AWS-defined relationship between a Kinesis Data Stream and a schema: schemas are chosen per-record by producers, and one stream may carry multiple schemas. Enable only if your organization has adopted schema-name == stream-name as an internal convention.",
          "title": "Use Naming Convention",
          "type": "boolean"
        }
      },
      "title": "KinesisGlueSchemaRegistryConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Top-level Kinesis source config.\n\nComposes AwsConnectionConfig (creds + region + endpoint override) with\nfeature toggles, filters, and cross-platform lineage overrides.",
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Stateful ingestion configuration for stale entity removal."
    },
    "aws_config": {
      "$ref": "#/$defs/AwsConnectionConfig",
      "description": "Standard DataHub AWS connection config (creds, region, role assumption, endpoint override). Optional \u2014 when omitted, boto3 resolves everything from the standard credential chain (env vars, shared credentials file, IAM role, SSO profile)."
    },
    "include_streams": {
      "default": true,
      "description": "Extract Kinesis Data Streams (KDS).",
      "title": "Include Streams",
      "type": "boolean"
    },
    "stream_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns for which Kinesis streams to include."
    },
    "include_firehose": {
      "default": true,
      "description": "Extract Amazon Data Firehose streams.",
      "title": "Include Firehose",
      "type": "boolean"
    },
    "firehose_stream_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "description": "Regex patterns for which Firehose streams to include."
    },
    "include_table_lineage": {
      "default": true,
      "description": "Emit Firehose-stream \u2192 destination lineage edges.",
      "title": "Include Table Lineage",
      "type": "boolean"
    },
    "extract_tags": {
      "default": true,
      "description": "Extract AWS resource tags as DataHub globalTags. To derive ownership from a tag, apply the `extract_ownership_from_tags` transformer to the emitted tags (see the connector docs).",
      "title": "Extract Tags",
      "type": "boolean"
    },
    "glue_schema_registry": {
      "$ref": "#/$defs/KinesisGlueSchemaRegistryConfig",
      "description": "Glue Schema Registry integration (disabled by default)."
    },
    "destination_platform_map": {
      "additionalProperties": {
        "$ref": "#/$defs/DestinationPlatformDetail"
      },
      "description": "Map destination platform name (one of 's3', 'redshift', 'snowflake', 'iceberg', 'mongodb', 'elasticsearch', 'glue') to platform_instance + env overrides. REQUIRED when the destination platform was ingested with a platform_instance \u2014 without this, lineage edges reference non-existent URNs (silent failure mode). Unknown platform keys are rejected at parse time by the DestinationPlatform Literal.",
      "propertyNames": {
        "enum": [
          "s3",
          "redshift",
          "elasticsearch",
          "snowflake",
          "iceberg",
          "mongodb",
          "glue"
        ]
      },
      "title": "Destination Platform Map",
      "type": "object"
    }
  },
  "title": "KinesisSourceConfig",
  "type": "object"
}
```





### Capabilities

See the **Important Capabilities** table above for the full list of supported capabilities. The sections below cover the connector-specific configuration behind them.

#### Firehose lineage — supported destinations

The following destination types produce a `dataJobInputOutput.outputDatasets` edge:

| AWS destination                   | DataHub platform | URN format                                                                 |
| --------------------------------- | ---------------- | -------------------------------------------------------------------------- |
| Amazon S3 / Extended S3           | `s3`             | `urn:li:dataset:(urn:li:dataPlatform:s3,<bucket>[/<prefix>],...)`          |
| Amazon Redshift                   | `redshift`       | `urn:li:dataset:(urn:li:dataPlatform:redshift,<db>.<schema>.<table>,...)`  |
| Amazon OpenSearch / Elasticsearch | `elasticsearch`  | `urn:li:dataset:(urn:li:dataPlatform:elasticsearch,<index>,...)`           |
| Snowflake                         | `snowflake`      | `urn:li:dataset:(urn:li:dataPlatform:snowflake,<db>.<schema>.<table>,...)` |
| Apache Iceberg                    | `iceberg`        | `urn:li:dataset:(urn:li:dataPlatform:iceberg,<namespace>.<table>,...)`     |
| MongoDB                           | `mongodb`        | `urn:li:dataset:(urn:li:dataPlatform:mongodb,<database>.<collection>,...)` |

URN names are lowercased by default (matching the Snowflake source's `convert_urns_to_lowercase=True` default). Override per destination via `destination_platform_map.<platform>.convert_urns_to_lowercase: false` — see [Cross-platform lineage](#cross-platform-lineage-with-destination_platform_map) below.

Unsupported Firehose destinations (HTTP, Datadog, Splunk, New Relic, Coralogix, LogicMonitor, Dynatrace, Honeycomb, Sumo Logic, etc.) do **not** produce a lineage edge — the connector logs a warning (_"Unsupported Firehose destination"_) and records the destination configuration as a custom property on the DataJob. The DataJob itself is still emitted.

#### Cross-platform lineage with `destination_platform_map`

Firehose destinations live on other platforms (S3, Redshift, Snowflake, etc.), so Kinesis-emitted lineage URNs must match the URN convention of those platforms' own DataHub sources. The `destination_platform_map` lets you override per-destination URN parameters:

```yaml
destination_platform_map:
  snowflake:
    platform_instance: "prod-snowflake-east"
    env: "PROD"
    # Set to false if your Snowflake source recipe also has
    # `convert_urns_to_lowercase: false` (preserves UPPER_CASE identifiers):
    convert_urns_to_lowercase: false
  redshift:
    platform_instance: "analytics-cluster"
    env: "PROD"
  iceberg:
    # Iceberg catalogs are case-sensitive — disable lowercasing
    # to preserve the table's original case:
    convert_urns_to_lowercase: false
```

Three knobs are available per destination platform:

- **`platform_instance`** — the same string the destination platform's own source recipe used. Without this, Firehose lineage edges resolve to dead URNs in the DataHub UI.
- **`env`** — `PROD` / `DEV` / etc. Inherits from this source's `env` when unset.
- **`convert_urns_to_lowercase`** — default `true`. Set to `false` for case-sensitive destinations (Iceberg, MongoDB) or for Snowflake / Redshift sources ingested with their own `convert_urns_to_lowercase=false`.

#### Glue table lineage (Firehose format conversion)

When a Firehose stream has **Parquet/ORC format conversion** enabled, its `SchemaConfiguration` references a Glue table that defines the output schema. The connector surfaces this as an additional upstream input on the Firehose delivery DataJob:

- The source Kinesis stream remains an input (existing behavior).
- The destination S3 path remains an output (existing behavior).
- The Glue table is added as a second input — its schema governs what gets written to the S3 path.

For the Glue table URN to be emitted, `SchemaConfiguration` must include `DatabaseName` and `TableName`. `CatalogId` is **not** required — AWS omits it from `DescribeDeliveryStream` responses when it equals the caller's account (per AWS docs, it's an input-side default). SchemaConfigurations present but missing `DatabaseName` / `TableName` are recorded in the source report's `firehose_glue_schema_skipped` field for diagnosis.

If your Glue catalog was ingested under a non-default `platform_instance`, set the override:

```yaml
destination_platform_map:
  glue:
    platform_instance: "central-catalog"
    env: "PROD"
```

This whole behavior is gated by the `include_table_lineage` flag — when that's off, no Glue lineage is emitted.

#### Filtering streams and Firehose streams

`stream_pattern` and `firehose_stream_pattern` use the standard DataHub `AllowDenyPattern` shape. A common deny rule excludes internal / audit / debug streams:

```yaml
stream_pattern:
  deny:
    - "^_.*"
    - ".*-debug$"
```

#### Deriving ownership from tags

The connector emits AWS resource tags as DataHub `globalTags` (a tag `Key=Value` becomes `urn:li:tag:Key:Value`). To turn a tag into ownership, apply the built-in [`extract_ownership_from_tags`](../../../../metadata-ingestion/docs/transformer/dataset_transformer.md#extract-ownership-from-tags) transformer to the emitted tags — this keeps ownership handling consistent with every other DataHub source rather than reimplementing it per connector.

For example, to treat the value of an `owner` tag as a corpuser owner:

```yaml
transformers:
  - type: "extract_ownership_from_tags"
    config:
      tag_pattern: "owner:"
```

The transformer also supports corp groups, owner types, and appending an email domain — see its documentation for the full set of options.

#### Glue Schema Registry

GSR is **opt-in** because it requires extra IAM permissions (`glue:Get*` / `glue:List*`).

**What you get / miss:**

- **Disabled (default)** — streams are emitted **without** a `schemaMetadata` aspect. All other metadata (properties, tags, ownership, lineage) is unaffected. No `glue:*` permissions are needed.
- **Enabled** — for each stream that resolves to a schema (see resolution order below), the connector fetches the schema from AWS Glue Schema Registry and attaches a `schemaMetadata` aspect with the parsed fields (Avro / JSON / Protobuf). Streams that don't resolve to a schema are still emitted, just without `schemaMetadata` — enabling GSR never drops a stream. Requires the `GlueSchemaRegistryRead` IAM statement.

To enable it:

```yaml
glue_schema_registry:
  enabled: true
  registry_name: "default-registry"
  # Recommended: declare known stream -> schema associations explicitly.
  stream_schema_map:
    events: "events-v2"
    clicks: "click-events-schema"
  # Optional heuristic — see explanation below:
  use_naming_convention: false
```

Schema resolution order for each stream:

1. If the stream name is a key in `stream_schema_map`, use the mapped schema name.
2. Otherwise, if `use_naming_convention: true`, look up a schema whose name equals the stream name in the configured `registry_name`.
3. Otherwise, emit the stream without `schemaMetadata`.

#### Why is `use_naming_convention` off by default?

Unlike Kafka + Confluent Schema Registry — which defines a standardized `TopicNameStrategy` (`<topic>-key/-value` subject naming) — **AWS does not define any relationship between a Kinesis Data Stream and a Glue schema**. Schemas are chosen per-record by producers (the `GlueSchemaRegistrySerializer` embeds the schema-id in each record), and the stream itself has no schema binding. Multiple producers can write to one stream using different schemas, and one schema can be reused across multiple streams.

Some organizations adopt "schema name == stream name" as an internal convention, but it's not AWS best practice. If your organization follows that convention, set `use_naming_convention: true`. Otherwise, declare your known associations in `stream_schema_map` — that's the most predictable mode.

(For **Firehose** streams with Parquet/ORC format conversion, AWS _does_ define a relationship via `SchemaConfiguration` — see [Glue table lineage](#glue-table-lineage-firehose-format-conversion) above. That extraction is on by default and unaffected by this flag.)

### Limitations

1. **`destination_platform_map` is required for any destination platform ingested with a non-default `platform_instance`.** Without it, Firehose lineage edges reference URNs that are syntactically valid but resolve to nothing in DataHub — the lineage looks correct in the emitted JSON but the target is a dead link in the UI. Always populate `destination_platform_map` for destinations whose own source recipe set a `platform_instance`. Example:

   ```yaml
   destination_platform_map:
     snowflake:
       platform_instance: "prod-snowflake-east"
     redshift:
       platform_instance: "analytics-cluster"
   ```

2. **Glue Schema Registry cross-schema references aren't resolved.** Schemas with `$ref` (JSON Schema) or named imports (Avro / Protobuf) emit the top-level schema only — nested references aren't followed. Streams whose schemas depend on cross-schema imports will be missing some fields.

3. **One region per recipe.** The connector ingests a single AWS region per run. For multi-region accounts, run one recipe per region with a distinct `platform_instance`.

4. **One Glue Schema Registry per recipe.** Only the registry named in `glue_schema_registry.registry_name` is queried. If your schemas span multiple registries, run multiple recipes.

5. **No `USAGE_STATS` capability.** Read/write throughput per stream is available in CloudWatch but isn't read by this connector.

6. **Unsupported Firehose destinations.** Splunk, HTTP, Datadog, New Relic, Coralogix, LogicMonitor, Dynatrace, Honeycomb, and Sumo Logic destinations emit a DataJob without an output lineage edge and surface a warning in the report. The six supported destinations are listed in the [Firehose lineage table](#firehose-lineage--supported-destinations) above.

7. **No schema inference from record sampling.** The connector relies on AWS Glue Schema Registry for schemas — streams without a registered schema are emitted without `schemaMetadata`.

8. **No Lambda consumer discovery.** The connector doesn't enumerate Lambda functions consuming a stream, so `Stream → Lambda` lineage is not emitted.

9. **No Kinesis Data Analytics (KDA / managed Flink) support.** KDA applications are not ingested by this connector.

### Troubleshooting

**Lineage edges show but the target dataset isn't found in DataHub.**
Your `destination_platform_map` doesn't match the destination platform's own ingestion settings. Check that `platform_instance` and `env` in `destination_platform_map.<platform>` exactly match the values that platform's source recipe used. See Limitation #1.

**Snowflake or Redshift lineage doesn't resolve, and the destination identifiers are mixed-case.**
The connector lowercases destination URN names by default. If your Snowflake / Redshift source recipe set `convert_urns_to_lowercase: false`, mirror that on the Kinesis side:

```yaml
destination_platform_map:
  snowflake:
    convert_urns_to_lowercase: false
```

**Iceberg or MongoDB lineage doesn't resolve.**
Both platforms have case-sensitive identifiers. Disable lowercasing for them as above (`convert_urns_to_lowercase: false`).

**Ingestion succeeded but search doesn't show the entities.**
Your DataHub backend may have entered a read-only state due to disk pressure on the search index. From the host running DataHub:

```shell
docker exec <opensearch-or-elasticsearch-container> \
  curl -s localhost:9200/_cluster/allocation/explain
```

If the cluster reports `disk.watermark.low` exceeded, free disk space and re-index.

**`AccessDeniedException` on `kinesis:ListStreams`.**
The IAM identity used by the recipe is missing the `KinesisDataStreamsRead` permissions in the [AWS IAM Permissions](#aws-iam-permissions) section. A first-page denial is logged as a warning and skips the KDS section (the user may intentionally have Firehose-only IAM); a mid-pagination failure escalates to `report.failure` to prevent stateful soft-deletion of un-listed streams.

**Firehose section is silently empty even though Firehose streams exist.**
The IAM identity is missing `firehose:ListDeliveryStreams` and/or `firehose:DescribeDeliveryStream` — Kinesis Data Streams permissions don't cover Firehose. A missing Firehose permission is logged as a warning (_"Permission denied for Firehose"_) rather than a failure; check the ingestion run report.

**Schema not found for a stream.**
`glue:GetSchemaVersion` returned `EntityNotFoundException` for the expected schema name. Either add an explicit entry in `glue_schema_registry.stream_schema_map`, rename the GSR schema to match the stream name (and enable `use_naming_convention: true`), or accept that the stream will be emitted without `schemaMetadata`.


### Code Coordinates
- Class Name: `datahub.ingestion.source.kinesis.kinesis.KinesisSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/kinesis/kinesis.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Amazon Kinesis Data Streams, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
