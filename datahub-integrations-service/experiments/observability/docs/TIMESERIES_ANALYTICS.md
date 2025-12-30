# Time-Series Analytics for Observability Dashboard

The observability dashboard now includes comprehensive time-series analytics with DuckDB persistence, enabling historical tracking of metrics even across service restarts and dashboard downtime.

## Features

### 1. **DuckDB Time-Series Storage**

- **Persistent Storage**: All metric snapshots are stored in a local DuckDB database
- **Automatic Collection**: Metrics are captured on each dashboard refresh (every 5 seconds when auto-refresh is enabled)
- **Retention Management**: Old metrics are automatically cleaned up based on retention policy (default: 24 hours)
- **Zero Configuration**: Database is automatically created on first run

### 2. **Counter Reset Detection (Prometheus-Style)**

When the integrations service restarts, Prometheus counters reset to 0. The dashboard detects these resets automatically:

- **Detection**: Identifies when counter values decrease (indicating a restart)
- **Handling**: Query-time adjustment following Prometheus conventions
- **Visual Indicators**: Shows warning when restarts are detected in the time window

### 3. **Monitoring Gap Detection**

Identifies when the dashboard itself stopped collecting data:

- **Gap Threshold**: Configurable (default: 10 seconds)
- **Detection**: Finds intervals where no data was collected
- **Reporting**: Shows total gap count and duration
- **Distinction**: Separates service restarts from dashboard downtime

### 4. **Multiple Chart Views**

#### Rate Charts (Primary View)

- **Metric**: Messages per minute, Cost per minute, etc.
- **Benefits**:
  - Automatically handles counter resets
  - Shows actual activity rate over time
  - Easy to identify usage spikes
- **Calculation**: Delta between consecutive samples divided by time interval

#### Raw Cumulative Charts

- **Metric**: Actual counter values from Prometheus
- **Benefits**:
  - See exact counter values
  - Visible drops indicate service restarts
  - Understand raw metric behavior
- **Use Case**: Debugging, understanding service lifecycle

#### Adjusted Cumulative Charts

- **Metric**: Cumulative values with offsets applied across restarts
- **Benefits**:
  - Always increasing line
  - Shows true total across restarts
  - Better for long-term trend analysis
- **Calculation**: Adds offset at each restart to maintain continuity

## Configuration

### Environment Variables

```bash
# URL of the integrations service (default: http://localhost:9003)
export INTEGRATIONS_SERVICE_URL="http://localhost:9003"

# Path to DuckDB database (default: /tmp/observability_metrics.duckdb)
export METRICS_DB_PATH="/tmp/observability_metrics.duckdb"

# Hours to retain metric history (default: 24)
export METRICS_RETENTION_HOURS="24"
```

### Dashboard Controls

- **Time-series window slider**: Choose lookback period (1-24 hours)
- **Chart type selector**: Switch between Rate, Raw Cumulative, and Adjusted Cumulative
- **AI Module selector**: View metrics for specific AI modules (chat, description_generation, etc.)
- **Auto-refresh**: Enable 5-second auto-refresh to continuously collect data

## How It Works

### Data Collection Flow

```
1. Dashboard fetches /metrics endpoint (every 5s with auto-refresh)
2. Parses Prometheus metrics into structured format
3. Stores snapshot with current timestamp in DuckDB
4. Cleans up metrics older than retention period
```

### Counter Reset Detection

```python
# Detect reset: if counter decreased, assume restart from 0
if current_value < previous_value:
    delta = current_value  # Started from 0
else:
    delta = current_value - previous_value
```

This follows Prometheus's `rate()` and `increase()` function behavior.

### Gap Detection

```python
# Identify monitoring gaps
time_diff = current_timestamp - previous_timestamp
if time_diff > GAP_THRESHOLD_SECONDS:
    # Gap detected - dashboard stopped collecting
    record_gap(start=previous_timestamp, end=current_timestamp)
```

## Database Schema

```sql
CREATE TABLE metric_snapshots (
    timestamp TIMESTAMP,
    service_url VARCHAR,   -- Which service these metrics came from
    metric_name VARCHAR,
    labels VARCHAR,        -- JSON string of all metric labels
    value DOUBLE,
    PRIMARY KEY (timestamp, service_url, metric_name, labels)
);

CREATE INDEX idx_timestamp ON metric_snapshots(timestamp DESC);
CREATE INDEX idx_service_metric ON metric_snapshots(service_url, metric_name);
```

**Design Notes**:

- **Multi-Service Support**: The `service_url` column allows tracking metrics from multiple integrations services in the same database. You can switch `INTEGRATIONS_SERVICE_URL` and the dashboard will automatically query the appropriate data.
- **Flexible Labels**: Labels are stored as JSON strings to handle arbitrary label combinations. This prevents PRIMARY KEY conflicts when different metrics have different label sets (e.g., GenAI metrics have `ai_module`, `status`, `tool` labels, while system metrics like `python_gc_objects_collected_total` have different labels).

## Metrics Tracked Over Time

### Three-Tier GenAI Metrics

- **Tier 1**: User Messages (with success/error breakdown)
- **Tier 2**: LLM Calls
- **Tier 3**: Tool Calls (by tool name)
- **Cost**: Total USD cost

### Per AI Module

All metrics are tracked separately for each AI module:

- `chat`: Interactive chat conversations
- `description_generation`: AI-powered entity descriptions
- Future modules as they're added

## Multi-Service Tracking

The dashboard can track metrics from multiple integrations services in a single database. This is useful for:

- **Multiple Environments**: Track dev, staging, and production separately
- **Comparison**: Compare metrics across different service instances
- **Historical Tracking**: Keep data even when switching between services

### Example Workflow

```bash
# Track production service
export INTEGRATIONS_SERVICE_URL="https://prod.example.com"
./experiments/observability/scripts/run_dashboard.sh
# Data stored with service_url = "https://prod.example.com"

# Later, switch to staging
export INTEGRATIONS_SERVICE_URL="https://staging.example.com"
# Same database, but queries will show staging data
# Production data is preserved and can be viewed by switching back

# Use different database per environment (optional)
export METRICS_DB_PATH="/var/lib/datahub/prod_metrics.duckdb"  # Production
export METRICS_DB_PATH="/var/lib/datahub/staging_metrics.duckdb"  # Staging
```

All metrics are automatically scoped by `service_url`, so:

- No data conflicts between services
- Clean separation of metrics
- Easy to switch contexts

## Usage Patterns

### Scenario 1: Normal Operations

- Dashboard running continuously with auto-refresh
- Metrics collected every 5 seconds
- Rate charts show smooth activity over time
- No gaps, no restarts

### Scenario 2: Service Restart

- Integrations service restarts (counters reset to 0)
- Dashboard detects counter decrease
- Warning shown: "⚠️ Detected 1 service restart(s)"
- Rate charts automatically handle the reset
- Adjusted cumulative maintains continuity

### Scenario 3: Dashboard Downtime

- Dashboard closed or not refreshing
- Service continues running, counters keep increasing
- Dashboard restarted later
- Gap detection identifies missing time periods
- Warning shown: "⚠️ Detected 1 monitoring gap(s) totaling 300 seconds"

### Scenario 4: Both Service and Dashboard Restart

- Both components restarted
- Dashboard shows both restart and gap warnings
- Historical data preserved in DuckDB
- Trends visible across the restart

## Performance Considerations

### Storage

- Each snapshot stores ~10-20 rows (one per metric/label combination)
- At 5-second intervals: ~17,280 rows per 24 hours per metric
- DuckDB is highly efficient for analytical queries on this scale
- Automatic cleanup prevents unbounded growth

### Query Performance

- Indexed by timestamp for fast time-range queries
- Typical query time: <10ms for 24 hours of data
- In-memory query results cached by Streamlit

### Memory

- DuckDB runs embedded (no separate process)
- Database file size: ~1-5 MB per day depending on metric cardinality
- Minimal memory overhead

## Comparison with Prometheus

| Feature         | Prometheus                  | This Dashboard                  |
| --------------- | --------------------------- | ------------------------------- |
| Storage         | TSDB (persistent)           | DuckDB (persistent)             |
| Retention       | Configurable (weeks/months) | 24 hours (configurable)         |
| Reset Detection | Query-time (`rate()`)       | Query-time (same approach)      |
| Gap Detection   | No                          | Yes (monitors client-side gaps) |
| Visualization   | Grafana, etc.               | Built-in Streamlit              |
| Setup           | Separate service            | Embedded, zero config           |
| Use Case        | Production monitoring       | Local development, debugging    |

## Troubleshooting

### Database Locked Error

**Symptom**: `database is locked` error when running multiple dashboard instances

**Solution**: Only run one dashboard instance at a time, or use different `METRICS_DB_PATH` values

### Missing Historical Data

**Symptom**: Charts show "No time-series data available yet"

**Causes**:

1. Dashboard just started (need to collect for a few minutes)
2. Retention period expired (data older than 24 hours cleaned up)
3. Different `METRICS_DB_PATH` than previous runs

**Solution**: Keep auto-refresh enabled and wait for data collection

### Incorrect Reset Detection

**Symptom**: Restarts detected when service didn't restart

**Causes**:

1. Metric actually decreased (should be a counter, not gauge)
2. Service had actual restart but not noticed

**Solution**: Verify metrics are counters (monotonically increasing)

## Future Enhancements

Potential improvements for future versions:

1. **Export Functionality**: Export time-series data to CSV or Parquet
2. **Alert Thresholds**: Set thresholds and get notifications
3. **Comparison Views**: Compare current vs previous time periods
4. **Cost Projections**: Extrapolate cost based on current rates
5. **Custom Time Ranges**: Select specific start/end timestamps
6. **Metric Annotations**: Add manual notes about incidents/changes
7. **Multi-Service Support**: Track metrics from multiple services

## See Also

- [Prometheus rate() function](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate)
- [DuckDB Analytics](https://duckdb.org/why_duckdb)
