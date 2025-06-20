from datahub.metadata.schema_classes import DeprecationClass
from datahub.sdk import DataHubClient, DatasetUrn

client = DataHubClient.from_env()

dataset = client.entities.get(
    DatasetUrn(platform="hive", name="fct_users_created", env="PROD")
)

# Check if dataset is deprecated
deprecation = dataset._get_aspect(DeprecationClass)
if deprecation and deprecation.deprecated:
    print(f"Dataset is deprecated: {deprecation.note}")
    if deprecation.decommissionTime:
        print(f"Decommission time: {deprecation.decommissionTime}")
else:
    print("Dataset is not deprecated")
