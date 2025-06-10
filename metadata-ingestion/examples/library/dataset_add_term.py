from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hdfs", name="SampleHdfsDataset", env="PROD")
)

dataset.add_term(GlossaryTermUrn("rateofreturn"))

client.entities.upsert(dataset)
