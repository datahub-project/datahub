from datahub.sdk import CorpUserUrn, DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(DatasetUrn(platform="hdfs", name="SampleHdfsDataset"))

dataset.add_owner(CorpUserUrn("jdoe"))

client.entities.upsert(dataset)
