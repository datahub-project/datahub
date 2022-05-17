# DataHub Telemetry

## Overview of DataHub Telemetry

To effectively build and maintain the DataHub Project, we must understand how end-users work within DataHub. Beginning in version X.X.X, DataHub collects anonymous usage statistics and errors to inform our roadmap priorities and to enable us to proactively address errors.

Deployments are assigned a UUID which is sent along with event details, Java version, OS, and timestamp; telemetry collection is disabled by default and can be disabled by setting `DATAHUB_TELEMETRY_ENABLED=false` in your Docker Compose config.


The source code is available [here.](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/telemetry/TelemetryUtils.java)