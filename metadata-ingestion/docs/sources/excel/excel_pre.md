This connector ingests Excel worksheet datasets into DataHub. Workbooks (Excel files) can be ingested from the local filesystem or from S3 buckets. An asterisk (`*`) can be used in place of a directory or as part of a file name to match multiple directories or files with a single path specification.

:::tip
By default, this connector will ingest all worksheets in a workbook (an Excel file). To filter worksheets use the `worksheet_pattern` config option, or to only ingest the active worksheet use the `active_sheet_only` config option.
:::

### Supported file types

Supported file types are as follows:

- Excel workbook (\*.xlsx)
- Excel macro-enabled workbook (\*.xlsm)

The connector will attempt to identify which cells contain table data. A table is defined as a header row, which is used to derive the column names, followed by data rows. The schema is inferred from the data types that are present in a column.

Rows that are directly above or directly below the table where only the first two columns have values are assumed to contain metadata. If such rows are located, they are converted to custom properties where the first column is the key, and the second column is the value. Additionally, the workbook standard and custom properties are also imported as dataset custom properties.

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
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"],
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
