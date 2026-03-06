### Overview

The `s3` module ingests metadata from S3 into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

### Prerequisites

Grant necessary S3 permissions to an IAM user or role:

### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

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
