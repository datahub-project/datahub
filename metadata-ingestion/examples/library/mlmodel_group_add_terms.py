from datahub.metadata.urns import MlModelGroupUrn
from datahub.sdk import DataHubClient, GlossaryTermUrn

client = DataHubClient.from_env()

group_urn = MlModelGroupUrn(platform="mlflow", name="recommendation-models", env="PROD")

mlmodel_group = client.entities.get(group_urn)

mlmodel_group.add_term(GlossaryTermUrn("Recommendation"))

client.entities.update(mlmodel_group)

print(f"Added term {GlossaryTermUrn('Recommendation')} to ML model group {group_urn}")
