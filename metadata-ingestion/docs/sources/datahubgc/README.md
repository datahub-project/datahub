# DataHub Garbage Collection Source Documentation

## Overview

The DataHub Garbage Collection (GC) source is a maintenance component responsible for cleaning up various types of metadata to maintain system performance and data quality. It performs multiple cleanup tasks, each focusing on different aspects of DataHub's metadata.

## Cleanup Tasks

### 1. Index Cleanup

Manages Elasticsearch indices in DataHub, particularly focusing on time-series data.

#### Configuration

```yaml
source:
  type: datahub-gc
  config:
    truncate_indices: true
    truncate_index_older_than_days: 30
    truncation_watch_until: 10000
    truncation_sleep_between_seconds: 30
```

#### Features

- Truncates old Elasticsearch indices for the following timeseries aspects:
  - DatasetOperations
  - DatasetUsageStatistics
  - ChartUsageStatistics
  - DashboardUsageStatistics
  - QueryUsageStatistics
  - Timeseries Aspects
- Monitors truncation progress
- Implements safe deletion with monitoring thresholds
- Supports gradual truncation with sleep intervals

### 2. Expired Token Cleanup

Manages access tokens in DataHub to maintain security and prevent token accumulation.

#### Configuration

```yaml
source:
  type: datahub-gc
  config:
    cleanup_expired_tokens: true
```

#### Features

- Automatically identifies and revokes expired access tokens
- Processes tokens in batches for efficiency
- Maintains system security by removing outdated credentials
- Reports number of tokens revoked
- Uses GraphQL API for token management

### 3. Data Process Cleanup

Manages the lifecycle of data processes, jobs, and their instances (DPIs) within DataHub.

#### Features

- Cleans up Data Process Instances (DPIs) based on age and count
- Can remove empty DataJobs and DataFlows
- Supports both soft and hard deletion
- Uses parallel processing for efficient cleanup
- Maintains configurable retention policies

#### Configuration

```yaml
source:
  type: datahub-gc
  config:
    dataprocess_cleanup:
      enabled: true
      retention_days: 10
      keep_last_n: 5
      delete_empty_data_jobs: false
      delete_empty_data_flows: false
      hard_delete_entities: false
      batch_size: 500
      max_workers: 10
      delay: 0.25
```

### Limitations

- Maximum 9000 DPIs per job for performance

### 4. Execution Request Cleanup

Manages DataHub execution request records to prevent accumulation of historical execution data.

#### Features

- Maintains execution history per ingestion source
- Preserves minimum number of recent requests
- Removes old requests beyond retention period
- Special handling for running/pending requests
- Automatic cleanup of corrupted records

#### Configuration

```yaml
source:
  type: datahub-gc
  config:
    execution_request_cleanup:
      enabled: true
      keep_history_min_count: 10
      keep_history_max_count: 1000
      keep_history_max_days: 30
      batch_read_size: 100
      runtime_limit_seconds: 3600
      max_read_errors: 10
```

### 5. Soft-Deleted Entities Cleanup

Manages the permanent removal of soft-deleted entities after a retention period.

#### Features

- Permanently removes soft-deleted entities after retention period
- Handles entity references cleanup
- Special handling for query entities
- Supports filtering by entity type, platform, or environment
- Concurrent processing with safety limits

#### Configuration

```yaml
source:
  type: datahub-gc
  config:
    soft_deleted_entities_cleanup:
      enabled: true
      retention_days: 10
      batch_size: 500
      max_workers: 10
      delay: 0.25
      entity_types: null # Optional list of entity types to clean
      platform: null # Optional platform filter
      env: null # Optional environment filter
      query: null # Optional custom query filter
      limit_entities_delete: 25000
      futures_max_at_time: 1000
      runtime_limit_seconds: 7200
```

### Performance Considerations

- Concurrent processing using thread pools
- Configurable batch sizes for optimal performance
- Rate limiting through configurable delays
- Maximum limits on concurrent operations

## Reporting

Each cleanup task maintains detailed reports including:

- Number of entities processed
- Number of entities removed
- Errors encountered
- Sample of affected entities
- Runtime statistics
- Task-specific metrics
