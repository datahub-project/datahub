### Prerequisites

To allow the S3 ingestion source ingest metadata from an S3 bucket, you need to grant the necessary permissions to an IAM user or role. Follow these steps:

1. **Create an IAM Policy**: Create a policy that grants read access to the S3 bucket.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

**Permissions Explanation**:
- `s3:ListBucket`: Allows listing the objects in the bucket. This permission is necessary for the S3 ingestion source to know which objects are available to read.
- `s3:GetBucketLocation`: Allows retrieving the location of the bucket.
- `s3:GetObject`: Allows reading the actual content of the objects in the bucket. This is needed to infer schema from sample files.


2. **Attach the Policy to an IAM User or Role**: Attach the created policy to the IAM user or role that the S3 ingestion source will use.


3. **Configure the S3 Ingestion Source**: Configure the user in s3 ingestion who you attached the role above.
