from datahub.emitter.mce_builder import make_tag_urn
from datahub.metadata.schema_classes import TagAssociationClass
from datahub.sdk import DataHubClient, DatasetUrn
from datahub.specific.dataset import DatasetPatchBuilder

client = DataHubClient.from_env()

# Create the Dataset updater.
patch_builder = DatasetPatchBuilder(
    DatasetUrn(platform="snowflake", name="fct_users_created", env="PROD")
)
patch_builder.add_tag(TagAssociationClass(make_tag_urn("tag-to-add-id")))
patch_builder.remove_tag("urn:li:tag:tag-to-remove-id")

# Do the update.
client.entities.update(patch_builder)
