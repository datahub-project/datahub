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
