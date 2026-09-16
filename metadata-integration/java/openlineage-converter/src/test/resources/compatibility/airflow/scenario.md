# Description

Scenario contains events from airflow dag run performing basic operations using

- BigQueryToBigQueryOperator
- BigQueryToGCSOperator
- GCSToBigQueryOperator
- BashOperator
- PythonOperator

Simplified steps:

1. GCSToBigQueryOperator uploads `gs://mock-bucket/copied.csv` to bigquery `mock-project.test.upload`
2. BigQueryToBigQueryOperator copies `mock-project.test.upload` to `mock-project.test.upload_cp`
3. BigQueryToGCSOperator downloads `mock-project.test.upload_cp` to `gs://mock-bucket/result.csv`
4. BashOperator executes bash script without any inputs or outputs
5. PythonOperator executes few functions from GCS python api
   1. upload file to gcs, then delete it
   2. upload file to gcs and copy it
   3. upload data to gcs file
   4. copy files using copy and rewrite
   5. read gcs file
   6. download gcs file to local

# Entities

BQ

- `mock-project.test.upload`
- `mock-project.test.upload_cp`
- GCS
- `gs://mock-bucket/copied.csv`
- `gs://mock-bucket/result.csv`
- `gs://mock-bucket/uploaded_file.txt`
- `gs://mock-bucket/copy_of_uploaded_file.txt`
- `gs://mock-bucket/uploaded_data.txt`
- `gs://mock-bucket/copy_of_uploaded_data.txt`
- `/files/temp/downloaded_file.txt`
- `/files/temp/compose_result.txt`

# Facets

- ColumnLineageDatasetFacet
- ExternalQueryRunFacet
- JobFacet
- JobTypeJobFacet
- NominalTimeRunFacet
- OwnershipJobFacet
- ParentRunFacet
- ProcessingEngineRunFacet
- RunEvent
- RunFacet
- SchemaDatasetFacet
- SourceCodeJobFacet
