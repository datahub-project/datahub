from datahub.sdk import DataHubClient, DatasetUrn, GlossaryTermUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="realestate_db.sales", env="PROD")
)
dataset.add_term(GlossaryTermUrn("Classification.HighlyConfidential"))

# Or, if you know the term name but not the term urn:
term_urn = client.resolve.term(name="PII")
dataset.add_term(term_urn)

client.entities.update(dataset)
