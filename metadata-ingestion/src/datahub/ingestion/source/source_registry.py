from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.source import Source

source_registry = PluginRegistry[Source]()
source_registry.register_from_entrypoint("datahub.ingestion.source.plugins")

# Deprecations.
# source_registry.register_alias(<new_name>, <old_name>, <deprecation_message>)

# The MSSQL source has two possible sets of dependencies. We alias
# the second to the first so that we maintain the 1:1 mapping between
# source type and pip extra.
source_registry.register_alias(
    "mssql-odbc",
    "mssql",
)

# Use databricks as alias for unity-catalog ingestion source.
# As mentioned here - https://docs.databricks.com/en/data-governance/unity-catalog/enable-workspaces.html,
# Databricks is rolling out Unity Catalog gradually across accounts.
# TODO: Rename unity-catalog source to databricks source, once it is rolled out for all accounts
source_registry.register_alias(
    "databricks",
    "unity-catalog",
)
