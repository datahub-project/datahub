from datahub.emitter.mce_builder import (
    make_data_job_urn,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.specific.datajob import DataJobPatchBuilder

# Create DataHub Client
datahub_client = DataHubGraph(DataHubGraphConfig(server="http://localhost:8080"))

# Create DataJob URN
datajob_urn = make_data_job_urn(
    orchestrator="airflow", flow_id="dag_abc", job_id="task_456"
)

# Create DataJob Patch to Add Lineage
patch_builder = DataJobPatchBuilder(datajob_urn)
patch_builder.add_input_dataset(
    make_dataset_urn(platform="kafka", name="SampleKafkaDataset", env="PROD")
)
patch_builder.add_output_dataset(
    make_dataset_urn(platform="hive", name="SampleHiveDataset", env="PROD")
)
patch_builder.add_input_datajob(
    make_data_job_urn(orchestrator="airflow", flow_id="dag_abc", job_id="task_123")
)
patch_builder.add_input_dataset_field(
    make_schema_field_urn(
        parent_urn=make_dataset_urn(
            platform="hive", name="fct_users_deleted", env="PROD"
        ),
        field_path="user_id",
    )
)
patch_builder.add_output_dataset_field(
    make_schema_field_urn(
        parent_urn=make_dataset_urn(
            platform="hive", name="fct_users_created", env="PROD"
        ),
        field_path="user_id",
    )
)
patch_mcps = patch_builder.build()

# Emit DataJob Patch
for patch_mcp in patch_mcps:
    datahub_client.emit(patch_mcp)
