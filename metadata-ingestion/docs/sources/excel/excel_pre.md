This connector ingests Excel worksheet datasets into DataHub. Workbooks (Excel files) can be ingested from the local filesystem, from S3 buckets, or from Azure Blob Storage. An asterisk (`*`) can be used in place of a directory or as part of a file name to match multiple directories or files with a single path specification.

:::tip
By default, this connector will ingest all worksheets in a workbook (an Excel file). To filter worksheets use the `worksheet_pattern` config option, or to only ingest the active worksheet use the `active_sheet_only` config option.
:::

### Supported file types

Supported file types are as follows:

- Excel workbook (\*.xlsx)
- Excel macro-enabled workbook (\*.xlsm)

The connector will attempt to identify which cells contain table data. A table is defined as a header row, which is used to derive the column names, followed by data rows. The schema is inferred from the data types that are present in a column.

Rows that are directly above or directly below the table where only the first two columns have values are assumed to contain metadata. If such rows are located, they are converted to custom properties where the first column is the key, and the second column is the value. Additionally, the workbook standard and custom properties are also imported as dataset custom properties.

## Prerequisites

### AWS S3

When configuring an S3 ingestion source to access files in an S3 bucket, the AWS account referenced in your ingestion recipe must have appropriate S3 permissions. Create a policy with the minimum required permissions by following these steps:

1. **Create an IAM Policy**: Create a policy that grants read access to specific S3 buckets.

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

2. **Link Policy to Identity**: Associate your newly created policy with the appropriate IAM user or role that will be used by the S3 ingestion process.

3. **Set Up S3 Data Source**: When configuring your S3 ingestion source, specify the IAM user to whom you assigned the permissions in the previous step.

### Azure Blob Storage

To access files on Azure Blob Storage, you will need the following:

1. **Azure Storage Account**: A storage account that provides a unique namespace for your data in Azure.

2. **Authentication Credentials**: One of these supported authentication methods:

   - **Account Key**: Use your storage account's access key
   - **Client Secret**: Use a service principal with client ID and client secret for Microsoft Entra ID authentication
   - **SAS Token**: Use a Shared Access Signature token that provides limited, time-bound access

3. **Container**: A blob container that organizes your blobs (similar to a directory in a file system).

4. **Access Permissions**: Appropriate authorization for the authentication method:
   - For account key: Full access to the storage account
   - For client secret: Appropriate Azure role assignments (like Storage Blob Data Contributor)
   - For SAS token: Permissions are defined within the token itself

## Starter Recipes

Check out the following recipes to get started with ingestion.

For general pointers on writing and running a recipe, see our [main recipe guide](https://docs.datahub.com/docs/metadata-ingestion#recipes).

### S3

```yaml
source:
  type: excel
  config:
    path_list:
      - "s3://bucket/data/excel/*/*.xlsx"
    aws_config:
      aws_access_key_id: AKIAIOSFODNN7EXAMPLE
      aws_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
      aws_region: us-east-1
    profiling:
      enabled: false
```

### Azure Blob Storage

```yaml
source:
  type: excel
  config:
    path_list:
      - "https://storageaccountname.blob.core.windows.net/abs-data/excel/*/*.xlsx"
    azure_config:
      account_name: storageaccountname
      sas_token: sv=2022-11-02&ss=b&srt=sco&sp=rwdlacx&se=2025-06-07T21:00:00Z&st=2025-05-07T13:00:00Z&spr=https&sig=a1B2c3D4%3D
      container_name: abs-data
    profiling:
      enabled: false
```

### Local Files

```yaml
source:
  type: excel
  config:
    path_list:
      - "/data/path/reporting/excel/*.xlsx"
    profiling:
      enabled: false
```
