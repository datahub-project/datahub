import datahub.metadata.schema_classes as models
from datahub.sdk import DashboardUrn, DataHubClient

client = DataHubClient.from_env()

dashboard_urn = DashboardUrn("looker", "dashboards.999999")

dashboard = client.entities.get(dashboard_urn)

current_editable = dashboard._get_aspect(models.EditableDashboardPropertiesClass)

if current_editable:
    current_editable.description = "Updated description added via DataHub UI"
else:
    current_editable = models.EditableDashboardPropertiesClass(
        description="Updated description added via DataHub UI"
    )
    dashboard._set_aspect(current_editable)

client.entities.update(dashboard)
print(f"Updated editable properties for dashboard {dashboard_urn}")
