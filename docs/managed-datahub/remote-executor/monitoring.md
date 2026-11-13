---
title: Monitoring Remote Executor
description: Learn how to monitor and observe Remote Executor health and performance
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Monitoring Remote Executor

<FeatureAvailability saasOnly />

## Overview

This guide covers all aspects of monitoring your Remote Executor deployment:

1. File-based health checks
2. UI-based health monitoring
3. Advanced Prometheus metrics configuration

## Health Checks

### File-Based Health Checks

The Remote Executor uses file-based health checks that can be monitored by your container platform:

- Liveness: `/tmp/worker_liveness_heartbeat`
- Readiness: `/tmp/worker_readiness_heartbeat`

These files are automatically managed by the Remote Executor and can be used by Kubernetes liveness/readiness probes or ECS health checks.

### UI-Based Health Monitoring

Monitor Remote Executor health directly in the DataHub UI:

1. Navigate to **Data Sources > Executors**
2. View health information for each Pool:
   - Active Remote Executor instances
   - Last reported time for each executor
   - Status (Active/Stale)
   - Currently running Ingestion tasks and their details

<p align="center">
  <img width="90%"  src="https://github.com/datahub-project/static-assets/blob/main/imgs/remote-executor/remote-executor-view-running-tasks.png?raw=true"/>
</p>

## Advanced: Prometheus Metrics

The Remote Executor exposes metrics on port `9087/tcp` in Prometheus/OpenMetrics format. Metrics can be collected by Prometheus stack or compatible agents, such as DataDog.

### Metric Categories

1. **Ingestion Metrics**

   - `datahub_executor_worker_ingestion_requests` - Total jobs received
   - `datahub_executor_worker_ingestion_errors` - Failed jobs (v0.3.9+)

2. **Resource Metrics** (v0.3.9+)
   - Memory: `datahub_executor_memory_*`
   - CPU: `datahub_executor_cpu_*`
   - Disk: `datahub_executor_disk_*`
   - Network: `datahub_executor_net_*`

:::note
From DataHub 2.4.0, the executor reads its memory and CPU limits from its own container's cgroup, and on ECS also from the task's limits. Before, it read them from the root of the cgroup filesystem, which on some platforms (for example older Docker versions, ECS on EC2 and ECS on Fargate) shows no limit, so the whole machine's size was reported. After upgrading, `datahub_executor_memory_limit_processgroup` and `datahub_executor_cpu_limit_processgroup` can report lower, more accurate values on those platforms. Review any alerts that compare usage against these limits.
:::

### Prometheus Configuration

Example ServiceMonitor resource to allow scraping Remote Executor metrics in Prometheus:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  labels:
  name: datahub-remote-executor
spec:
  endpoints:
    - port: metrics
  selector:
    matchLabels:
      app.kubernetes.io/name: datahub-remote-executor
```

### Discovering Available Metrics

1. View metrics endpoint directly:

   ```bash
   curl http://your-executor:9087/metrics
   ```

2. Read annotations in Prometheus UI
3. Search `datahub_executor_*` in your monitoring system

:::note
Platform-specific metrics (e.g., container restarts) should be monitored through native tooling (CloudWatch for ECS, Kubernetes metrics for K8s).
:::
