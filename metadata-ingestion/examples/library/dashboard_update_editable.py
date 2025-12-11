# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
