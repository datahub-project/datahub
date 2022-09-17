### Prerequisites
#### Create a datahub profile in GCP
1. Create a custom role for datahub as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-custom-roles#creating_a_custom_role)
2. Grant the following permissions to this role:
```   
   # basic requirements

   # This needs to list datasets
   bigquery.datasets.get
   bigquery.datasets.getIamPolicy
   # Needs to submit queries.
   bigquery.jobs.create
   # Needs to check submitet queries status
   bigquery.jobs.list
   # Needs for lineage generation and usage to see all the queries were run on a project
   bigquery.jobs.listAll
   # Needs to list/get metadata about Bigquery Routines
   bigquery.routines.get
   bigquery.routines.list
   # Needs to list/get metadata about Bigquery Tables   
   bigquery.tables.get
   bigquery.tables.list
   # Needs to get resutlset of queries
   bigquery.readsessions.create
   bigquery.readsessions.getData
   # Needs to get project names/metadata
   resourcemanager.projects.get

   # needed if profiling enabled
 
   # profiler needs to access data to do the profiling
   bigquery.tables.getData
   # It needs to create temporary tables to profile partitioned/sharded tables and that is why it needs 
   # table create/delete privilege.
   # Use profiling.bigquery_temp_table_schema to restrict to one specific dataset the create/delete permission
   bigquery.tables.create
   bigquery.tables.delete

   # needed for lineage generation via GCP logging
   logging.logEntries.list
   logging.privateLogEntries.list
```
#### Create a service account

1. Setup a ServiceAccount as per [BigQuery docs](https://cloud.google.com/iam/docs/creating-managing-service-accounts#iam-service-accounts-create-console)
and assign the previously created role to this service account.
2. Download a service account JSON keyfile.
   Example credential file:
```json
{
  "type": "service_account",
  "project_id": "project-id-1234567",
  "private_key_id": "d0121d0000882411234e11166c6aaa23ed5d74e0",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----",
  "client_email": "test@suppproject-id-1234567.iam.gserviceaccount.com",
  "client_id": "113545814931671546333",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%suppproject-id-1234567.iam.gserviceaccount.com"
}
```
3. To provide credentials to the source, you can either:
   Set an environment variable:
       $ export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"

   *or*

   Set credential config in your source based on the credential json file. For example: 

```yml
     credential:
       project_id: project-id-1234567
       private_key_id: "d0121d0000882411234e11166c6aaa23ed5d74e0"
       private_key: "-----BEGIN PRIVATE KEY-----\nMIIyourkey\n-----END PRIVATE KEY-----\n"
       client_email: "test@suppproject-id-1234567.iam.gserviceaccount.com"
       client_id: "123456678890"
```

### Lineage Computation Details

When `use_exported_bigquery_audit_metadata` is set to `true`, lineage information will be computed using exported bigquery logs. On how to setup exported bigquery audit logs, refer to the following [docs](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud) on BigQuery audit logs. Note that only protoPayloads with "type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" are supported by the current ingestion version. The `bigquery_audit_metadata_datasets` parameter will be used only if `use_exported_bigquery_audit_metadat` is set to `true`.

Note: the `bigquery_audit_metadata_datasets` parameter receives a list of datasets, in the format $PROJECT.$DATASET. This way queries from a multiple number of projects can be used to compute lineage information.

Note: Since bigquery source also supports dataset level lineage, the auth client will require additional permissions to be able to access the google audit logs. Refer the permissions section in bigquery-usage section below which also accesses the audit logs.

### Profiling Details

Profiling can profile normal/partitioned and sharded tables as well but due to performance reasons, we only profile the latest partition for Partitioned tables and the latest shard for sharded tables.

If limit/offset parameter is set or partitioning partitioned or sharded table Great Expectation (the profiling framework we use) needs to create temporary
views. By default, these views are created in the schema where the profiled table is but you can control to create all these
tables into a predefined schema by setting `profiling.bigquery_temp_table_schema` property. 
Temporary tables are removed after profiling.

```yaml
     profiling:
       enabled: true
       bigquery_temp_table_schema: my-project-id.my-schema-where-views-can-be-created
```

:::note

Due to performance reasons, we only profile the latest partition for Partitioned tables and the latest shard for sharded tables.
You can set partition explicitly with `partition.partition_datetime` property if you want. (partition will be applied to all partitioned tables)
:::

### Working with multi-project GCP setups

Sometimes you may have multiple GCP project with one only giving you view access rights and other project where you have view/modify rights.

The GCP roles with which this setup has been tested are as follows
- Storage Project
   - BigQuery Data Viewer
   - BigQuery Metadata Viewer
   - Logs Viewer
   - Private Logs Viewer
- Compute Project
   - BigQuery Admin
   - BigQuery Data Editor
   - BigQuery Job User

If you are using `use_exported_bigquery_audit_metadata = True` then make sure you prefix the datasets in `bigquery_audit_metadata_datasets` with storage project id.

### Caveats
- For Materialized views lineage is dependent on logs being retained. If your GCP logging is retained for 30 days (default) and 30 days have passed since the creation of the materialized view we won't be able to get lineage for them.
