---
title: DataHub Telemetry
sidebar_label: Telemetry
slug: /deploy/telemetry
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/deploy/telemetry.md
---

# DataHub Telemetry

## Overview of DataHub Telemetry

To effectively build and maintain the DataHub Project, we must understand how end-users work within DataHub. Beginning in version 0.8.35, DataHub collects anonymous usage statistics and errors to inform our roadmap priorities and to enable us to proactively address errors.

Deployments are assigned a UUID which is sent along with event details, Java version, OS, and timestamp; telemetry collection is enabled by default and can be disabled by setting `DATAHUB_TELEMETRY_ENABLED=false` in your Docker Compose config.

The source code is available [here.](https://github.com/datahub-project/datahub/blob/master/metadata-service/factories/src/main/java/com/linkedin/gms/factory/telemetry/TelemetryUtils.java)
