---
title: Monitoring Remote Executor
description: Learn how to monitor and observe Remote Executor health and performance
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
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

The Remote Executor exposes metrics on port `9087/tcp` in Prometheus/OpenMetrics format.

### Metric Categories

1. **Ingestion Metrics**
   - `datahub_executor_worker_ingestion_requests` - Total jobs received
   - `datahub_executor_worker_ingestion_errors` - Failed jobs (v0.3.9+)

2. **Resource Metrics** (v0.3.9+)
   - Memory: `datahub_executor_memory_*`
   - CPU: `datahub_executor_cpu_*`
   - Disk: `datahub_executor_disk_*`
   - Network: `datahub_executor_net_*`

### Prometheus Configuration

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: datahub-remote-executor
spec:
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
    scrapeTimeout: 10s
  selector:
    matchLabels:
      app.kubernetes.io/name: datahub-remote-executor
  namespaceSelector:
    matchNames:
      - default  # adjust to your namespace
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