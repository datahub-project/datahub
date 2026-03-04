### Prerequisites and Permissions

**Important:** Requires `readWrite` role on each database to be ingested.

Schema inference requires access to system collections (`system.profile`, `system.views`), which are not accessible with `read` or `readAnyDatabase` roles. Without `readWrite`, ingestion will fail with authorization errors.
