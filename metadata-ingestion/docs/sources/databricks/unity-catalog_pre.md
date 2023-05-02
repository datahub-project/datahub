### Prerequisities
- Generate a Databrick Personal Access token following the guide here:
https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token
- Get your catalog's workspace id by following:
https://docs.databricks.com/workspace/workspace-details.html#workspace-instance-names-urls-and-ids
- To enable usage ingestion, ensure the account associated with your access token has
`CAN_MANAGE` permissions on any SQL Warehouses you want to ingest:
  https://docs.databricks.com/security/auth-authz/access-control/sql-endpoint-acl.html
- Check the starter recipe below and replace Token and Workspace id with the ones above.
