### Overview

The `s3` module ingests metadata from S3 into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This connector ingests AWS S3 datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub.
Refer to the section [Path Specs](https://docs.datahub.com/docs/generated/ingestion/sources/s3/#path-specs) for more details.

:::tip
This connector can also be used to ingest local files.
Just replace `s3://` in your path_specs with an absolute path to files on the machine running ingestion.
:::

### Prerequisites

Grant necessary S3 permissions to an IAM user or role:

**1. Create an IAM Policy**

Grant read access to the S3 bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

**Permissions:**

- `s3:ListBucket`: List objects in the bucket
- `s3:GetBucketLocation`: Retrieve bucket location
- `s3:GetObject`: Read object content (required for schema inference)

**2. Attach the Policy**

Attach the policy to the IAM user or role used by the S3 ingestion source.

**3. Configure the Source**

Use the IAM user/role credentials in your S3 ingestion recipe.

#### Cross-Account Access

The S3 connector supports cross-account access via AWS STS AssumeRole. This allows DataHub running in one AWS account to ingest S3 metadata from buckets in a different AWS account.

**Setup steps:**

1. **In the target account** (where the S3 bucket lives), create an IAM role with:
   - The S3 permissions policy shown above
   - A trust policy allowing the source account to assume the role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::SOURCE-ACCOUNT-ID:role/DataHubExecutionRole"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "your-unique-external-id"
        }
      }
    }
  ]
}
```

2. **In the ingestion recipe**, configure `aws_config.aws_role` with the target role ARN:

**Simple ARN format:**

```yaml
source:
  type: s3
  config:
    aws_config:
      aws_role: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubS3ReadRole"
    path_specs:
      - include: "s3://target-account-bucket/**"
```

**With External ID** (recommended for security):

```yaml
source:
  type: s3
  config:
    aws_config:
      aws_role:
        RoleArn: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubS3ReadRole"
        ExternalId: "your-unique-external-id"
    path_specs:
      - include: "s3://target-account-bucket/**"
```

**Role chaining** (assume multiple roles in sequence):

```yaml
source:
  type: s3
  config:
    aws_config:
      aws_role:
        - "arn:aws:iam::INTERMEDIARY-ACCOUNT-ID:role/IntermediateRole"
        - RoleArn: "arn:aws:iam::TARGET-ACCOUNT-ID:role/DataHubS3ReadRole"
          ExternalId: "your-unique-external-id"
    path_specs:
      - include: "s3://target-account-bucket/**"
```

The connector uses [boto3's assume_role](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.assume_role), so additional parameters like `RoleSessionName`, `DurationSeconds`, and `Policy` are also supported.

#### S3-Compatible Object Stores

The source plugin talks to Amazon S3 through boto3, so it also works against other object stores that implement the S3 API (for example Backblaze B2, Cloudflare R2, and MinIO). Set `aws_config.aws_endpoint_url` to the provider's S3 endpoint, and keep `aws_config.aws_region` set to the region that endpoint belongs to, since the region is still used to sign requests. Continue to use `s3://` in `path_specs`: the endpoint override decides which host is contacted.

```yaml
source:
  type: s3
  config:
    aws_config:
      aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
      aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
      aws_region: example-region
      aws_endpoint_url: "https://s3.example-region.example.com"
      # Only needed if the provider requires path-style addressing.
      aws_advanced_config:
        s3:
          addressing_style: path
    path_specs:
      - include: "s3://my-bucket/{table}/*.parquet"
```

The IAM policy steps above are AWS-specific. On other providers, grant the equivalent read and list access with that provider's own credential mechanism.
