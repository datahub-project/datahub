### Prerequisities
1Generate a Databrick Personal Access token following the guide here: https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token
2. Get your workspace Id where Unity Catalog is following: https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids
3. Check our sample recipe and replace Token and Workspace Id with the ones above.

## Troubleshooting

### No data lineage captured or missing lineage
Please, check you meets the following requirements:
    https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements

And also check limitations:
https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html#requirements

### Lineage extraction is slow

Currently, there is no way to get table/column lineag in bulk from the Databricks Unity Catalog rest api therfore if 
table lineage is enabled we have to run one rest call per table or if column level lineage is enabled then one rest
call per column.
Even though we try our best to minimise the number of rest calls to send it can happen lineage extraction slows down
extraction.