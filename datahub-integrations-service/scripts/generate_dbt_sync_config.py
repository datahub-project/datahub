from datahub_integrations.dbt.dbt_sync_back import DbtSyncBackConfig

print(DbtSyncBackConfig.schema_json(indent=2))
