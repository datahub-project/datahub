### Prerequisities

In order to execute this source, you will need to create a policy with below permissions and attach it to the the aws role or credentials used in ingestion recipe.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "athena:GetTableMetadata", 
        "athena:StartQueryExecution", 
        "athena:GetQueryResults", 
        "athena:GetDatabase", 
        "athena:ListDataCatalogs",
        "athena:GetDataCatalog", 
        "athena:ListQueryExecutions", 
        "athena:GetWorkGroup", 
        "athena:StopQueryExecution", 
        "athena:GetQueryResultsStream", 
        "athena:ListDatabases", 
        "athena:GetQueryExecution", 
        "athena:ListTableMetadata", 
        "athena:BatchGetQueryExecution", 
        "glue:GetTables", 
        "glue:GetDatabases", 
        "glue:GetTable",
        "glue:GetDatabase",
        "glue:SearchTables",
        "glue:GetTableVersions",
        "glue:GetTableVersion",
        "glue:GetPartition", 
        "glue:GetPartitions", 
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:athena:${region-id}:${account-id}:datacatalog/*",
        "arn:aws:athena:${region-id}:${account-id}:workgroup/*",
        "arn:aws:glue:${region-id}:${account-id}:tableVersion/*/*/*",
        "arn:aws:glue:${region-id}:${account-id}:table/*/*", 
        "arn:aws:glue:${region-id}:${account-id}:catalog", 
        "arn:aws:glue:${region-id}:${account-id}:database/*", 
        "arn:aws:s3:::${datasets-bucket}",
        "arn:aws:s3:::${datasets-bucket}/*"
      ]
    },
    {
      "Sid": "VisualEditor1",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucketMultipartUploads",
        "s3:AbortMultipartUpload",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::${athena-query-result-bucket}/*",
        "arn:aws:s3:::${athena-query-result-bucket}"
      ]
    }
  ]
}
```

Replace `${var}` with appropriate values as per your athena setup.
