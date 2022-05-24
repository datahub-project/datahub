# Lineage sample code

The following samples will cover emitting dataset-to-dataset, dataset-to-job-to-dataset, chart-to-dataset, dashboard-to-chart and job-to-dataflow lineages.
- [lineage_emitter_mcpw_rest.py](../../metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py) - emits simple bigquery table-to-table (dataset-to-dataset) lineage via REST as MetadataChangeProposalWrapper.
- [lineage_dataset_job_dataset.py](../../metadata-ingestion/examples/library/lineage_dataset_job_dataset.py) - emits mysql-to-airflow-to-kafka (dataset-to-job-to-dataset) lineage via REST as MetadataChangeProposalWrapper.
- [lineage_dataset_chart.py](../../metadata-ingestion/examples/library/lineage_dataset_chart.py) - emits the dataset-to-chart lineage via REST as MetadataChangeProposalWrapper.
- [lineage_chart_dashboard.py](../../metadata-ingestion/examples/library/lineage_chart_dashboard.py) - emits the chart-to-dashboard lineage via REST as MetadataChangeProposalWrapper.
- [lineage_job_dataflow.py](../../metadata-ingestion/examples/library/lineage_job_dataflow.py) - emits the job-to-dataflow lineage via REST as MetadataChangeProposalWrapper.
- [lineage_emitter_rest.py](../../metadata-ingestion/examples/library/lineage_emitter_rest.py) - emits simple dataset-to-dataset lineage via REST as MetadataChangeEvent.
- [lineage_emitter_kafka.py](../../metadata-ingestion/examples/library/lineage_emitter_kafka.py) - emits simple dataset-to-dataset lineage via Kafka as MetadataChangeEvent.
- [lineage_emitter_dataset_finegrained.py](../../metadata-ingestion/examples/library/lineage_emitter_dataset_finegrained.py) - emits fine-grained dataset-dataset lineage via REST as MetadataChangeProposalWrapper.
- [lineage_emitter_datajob_finegrained.py](../../metadata-ingestion/examples/library/lineage_emitter_datajob_finegrained.py) - emits fine-grained datajob-dataset lineage via REST as MetadataChangeProposalWrapper.
- [Datahub Snowflake Lineage](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/sql/snowflake.py#L249) - emits Datahub's Snowflake lineage as MetadataChangeProposalWrapper.
- [Datahub Bigquery Lineage](https://github.com/datahub-project/datahub/blob/a1bf95307b040074c8d65ebb86b5eb177fdcd591/metadata-ingestion/src/datahub/ingestion/source/sql/bigquery.py#L229) - emits Datahub's Bigquery lineage as MetadataChangeProposalWrapper.
- [Datahub Dbt Lineage](https://github.com/datahub-project/datahub/blob/a9754ebe83b6b73bc2bfbf49d9ebf5dbd2ca5a8f/metadata-ingestion/src/datahub/ingestion/source/dbt.py#L625,L630) - emits Datahub's DBT lineage as MetadataChangeEvent.

NOTE:
- Emitting aspects as MetadataChangeProposalWrapper is recommended over emitting aspects via the
MetadataChangeEvent.
- Emitting any aspect associated with an entity completely overwrites the previous
value of the aspect associated with the entity. This means that emitting a lineage aspect associated with a dataset will overwrite lineage edges that already exist.