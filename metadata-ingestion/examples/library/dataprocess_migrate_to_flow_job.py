"""
Example: Migrate from deprecated dataProcess to modern dataFlow and dataJob entities.

This example shows how to create the modern dataFlow and dataJob entities
that replace the deprecated dataProcess entity.

The dataProcess entity used a flat structure:
  dataProcess -> inputs/outputs

The new model uses a hierarchical structure:
  dataFlow (pipeline) -> dataJob (task) -> inputs/outputs

This provides better organization and more precise lineage tracking.
"""

from datahub.metadata.urns import DatasetUrn
from datahub.sdk import DataFlow, DataHubClient, DataJob

client = DataHubClient.from_env()

# Old dataProcess would have been:
# urn:li:dataProcess:(customer_etl_job,airflow,PROD)
# with inputs and outputs at the process level

# New approach: Create a DataFlow for the pipeline
dataflow = DataFlow(
    platform="airflow",  # Same as the old 'orchestrator' field
    name="customer_etl_job",  # Same as the old 'name' field
    platform_instance="prod",  # Based on old 'origin' field
    description="ETL pipeline for customer data processing",
)

# Create DataJob(s) within the flow
# For a simple single-task process, create one job
# For complex multi-step processes, create multiple jobs
datajob = DataJob(
    name="customer_etl_task",  # Task name within the flow
    flow=dataflow,  # Link to parent flow
    description="Main ETL task for customer data",
    # Inputs and outputs now live at the job level for precise lineage
    inlets=[
        DatasetUrn(platform="mysql", name="raw_db.customers", env="PROD"),
        DatasetUrn(platform="mysql", name="raw_db.orders", env="PROD"),
    ],
    outlets=[
        DatasetUrn(platform="postgres", name="analytics.customer_summary", env="PROD"),
    ],
)

# Upsert both entities
client.entities.upsert(dataflow)
client.entities.upsert(datajob)

print("Successfully migrated from dataProcess to dataFlow + dataJob!")
print(f"DataFlow URN: {dataflow.urn}")
print(f"DataJob URN: {datajob.urn}")
print("\nKey improvements over dataProcess:")
print("- Clear separation between pipeline (DataFlow) and task (DataJob)")
print("- Support for multi-step pipelines with multiple DataJobs")
print("- More precise lineage at the task level")
print("- Better integration with modern orchestration platforms")
