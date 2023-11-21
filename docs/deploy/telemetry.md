# DataHub Telemetry

## Overview of DataHub Telemetry

To effectively build and maintain the DataHub Project, we must understand how end-users work within DataHub. Beginning in version 0.8.35, DataHub collects anonymous usage statistics and errors to inform our roadmap priorities and to enable us to proactively address errors.

Both the DataHub backend and the ingestion framework collect telemetry.

## DataHub Backend Telemetry

Deployments are assigned a UUID which is sent along with event details, Java version, OS, and timestamp.
The source code is available [here](../../metadata-service/factories/src/main/java/com/linkedin/gms/factory/telemetry/TelemetryUtils.java).

## Ingestion Framework Telemetry

The ingestion framework collects telemetry including CLI invocations, source/sink types, error types, versions, and timestamps. If you run with `datahub --debug`, all telemetry calls will be logged.

On first invocation, the CLI will generate a randomized UUID, which will be sent alongside every telemetry event. This config is stored in `~/.datahub/telemetry-config.json`.

The source code is available [here](../../metadata-ingestion/src/datahub/telemetry/telemetry.py).

## Disabling Telemetry

Telemetry is enabled by default. While we are careful to anonymize all telemetry data and encourage users to keep it enabled so that we can improve DataHub, we understand that some users may wish to disable it.

You can disable backend telemetry by setting the `DATAHUB_TELEMETRY_ENABLED` environment variable to `false`. You'll need to set this on both the datahub-gms and datahub-actions containers.

If you're using the DataHub CLI, ingestion framework telemetry will be disabled when the `DATAHUB_TELEMETRY_ENABLED` environment variable is set to `false`. To persist this change for your machine, run `datahub telemetry disable`.
