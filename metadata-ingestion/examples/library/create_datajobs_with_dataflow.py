from datahub.metadata.urns import TagUrn
from datahub.sdk import DataFlow, DataHubClient

client = DataHubClient.from_env()

dataflow = DataFlow(
    name="example_dataflow",
    platform="airflow",
)
datajobs = dataflow.create_jobs(
    job_names=["datajob1", "datajob1"], tags=[TagUrn("airflow")]
)
# datajob will inherit the platform from the flow_urn
# you can pass additional DataJob properties to the create_jobs method along with the job names
# e.g. dataflow.create_jobs(job_names=["datajob1", "datajob2"], description="example datajob", tags=[TagUrn("example")])

client.entities.upsert(dataflow)
for datajob in datajobs:
    client.entities.create(datajob)
