<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

## Prerequisites and Permissions

**Important:**  
The user account used for MongoDB ingestion must have the `readWrite` role on each database to be ingested. Schema inference and sampling logic executes on system collections (such as `system.profile` and `system.views`), which are not permitted with only `read` or `readAnyDatabase` roles. Without `readWrite`, ingestion will fail with an authorization error.
