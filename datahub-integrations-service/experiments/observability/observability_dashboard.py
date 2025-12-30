"""Observability Dashboard for DataHub Integrations Service.

A simple Streamlit dashboard to visualize OpenTelemetry metrics from the
integrations service, with focus on GenAI cost tracking and usage metrics.

Features time-series persistence with DuckDB to track metrics over time, even
across service restarts. Handles counter resets Prometheus-style and detects
monitoring gaps.

Run locally:
    streamlit run experiments/observability/observability_dashboard.py

Optional environment variables:
    INTEGRATIONS_SERVICE_URL: URL of the integrations service (default: http://localhost:9003)
    METRICS_DB_PATH: Path to DuckDB database for time-series storage (default: /tmp/observability_metrics.duckdb)
    METRICS_RETENTION_HOURS: Hours to retain metric history (default: 24)
"""

import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import requests
import streamlit as st

# Add src to path to import constants
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from datahub_integrations.observability.metrics_constants import (
    ACTIONS_ACTIONS_EXECUTED,
    ACTIONS_ASSETS_IMPACTED,
    ACTIONS_ASSETS_PROCESSED,
    ACTIONS_ERROR_TOTAL,
    ACTIONS_EVENTS_PROCESSED,
    ACTIONS_EXECUTION_TOTAL,
    ACTIONS_INFO,
    ACTIONS_RUNNING_COUNT,
    ACTIONS_SUCCESS_TOTAL,
    GENAI_LLM_CALL_DURATION,
    GENAI_LLM_CALLS_TOTAL,
    GENAI_LLM_COST_TOTAL_WITH_UNIT,
    GENAI_LLM_TOKENS_TOTAL,
    GENAI_TOOL_CALL_DURATION,
    GENAI_TOOL_CALLS_TOTAL,
    GENAI_USER_MESSAGE_DURATION,
    GENAI_USER_MESSAGES_TOTAL,
    HTTP_SERVER_DURATION_COUNT,
    KAFKA_CONSUMER_LAG,
    LABEL_ACTION_NAME,
    LABEL_ACTION_TYPE,
    LABEL_ACTION_URN,
    LABEL_AI_MODULE,
    LABEL_EXECUTOR_ID,
    LABEL_HTTP_STATUS_CODE,
    LABEL_MODEL,
    LABEL_PIPELINE_NAME,
    LABEL_PROVIDER,
    LABEL_STAGE,
    LABEL_STATE,
    LABEL_STATUS,
    LABEL_TOKEN_TYPE,
    LABEL_TOOL,
    LABEL_TOPIC,
    PROCESS_CPU_UTILIZATION,
    PROCESS_MEMORY_USAGE,
    TOKEN_TYPE_CACHE_READ,
    TOKEN_TYPE_CACHE_WRITE,
    TOKEN_TYPE_COMPLETION,
    TOKEN_TYPE_PROMPT,
)

# Configuration
INTEGRATIONS_SERVICE_URL = os.environ.get(
    "INTEGRATIONS_SERVICE_URL", "http://localhost:9003"
)
METRICS_ENDPOINT = f"{INTEGRATIONS_SERVICE_URL}/metrics"
METRICS_DB_PATH = os.environ.get("METRICS_DB_PATH", "/tmp/observability_metrics.duckdb")
METRICS_RETENTION_HOURS = int(os.environ.get("METRICS_RETENTION_HOURS", "24"))
EXPECTED_COLLECTION_INTERVAL_SECONDS = 5
GAP_THRESHOLD_SECONDS = 10  # Consider gaps > 10s as monitoring breaks


@dataclass
class MetricSample:
    """Represents a single metric sample with labels and value."""

    name: str
    labels: dict[str, str]
    value: float


def fetch_metrics() -> str:
    """Fetch raw metrics text from the integrations service."""
    try:
        response = requests.get(
            METRICS_ENDPOINT, timeout=60
        )  # Increased for large metric payloads
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch metrics from {METRICS_ENDPOINT}: {e}")
        return ""


def parse_prometheus_metrics(metrics_text: str) -> list[MetricSample]:
    """Parse Prometheus metrics text into structured samples.

    Args:
        metrics_text: Raw Prometheus metrics text

    Returns:
        List of MetricSample objects
    """
    samples = []

    for line in metrics_text.split("\n"):
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Parse metric line: metric_name{label1="value1",label2="value2"} 123.45
        if "{" in line:
            # Metric with labels
            metric_name = line.split("{")[0]
            labels_part = line.split("{")[1].split("}")[0]
            value_part = line.split("}")[1].strip()

            # Parse labels
            labels = {}
            for label_pair in labels_part.split(","):
                key, label_value = label_pair.split("=", 1)
                labels[key.strip()] = label_value.strip('"')

            # Parse value
            try:
                metric_value = float(value_part)
            except ValueError:
                continue

            samples.append(
                MetricSample(name=metric_name, labels=labels, value=metric_value)
            )
        else:
            # Metric without labels
            parts = line.split()
            if len(parts) == 2:
                metric_name, value_str = parts
                try:
                    metric_value = float(value_str)
                    samples.append(
                        MetricSample(name=metric_name, labels={}, value=metric_value)
                    )
                except ValueError:
                    continue

    return samples


def group_by_metric(samples: list[MetricSample]) -> dict[str, list[MetricSample]]:
    """Group metric samples by metric name."""
    grouped = defaultdict(list)
    for sample in samples:
        grouped[sample.name].append(sample)
    return dict(grouped)


# === Time-Series Storage with DuckDB ===


@st.cache_resource
def init_timeseries_db() -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB database for time-series storage.

    Uses @st.cache_resource to maintain a single connection across Streamlit reruns.
    """
    conn = duckdb.connect(METRICS_DB_PATH)

    # Create table for metric snapshots with labels as JSON for flexibility
    # Include service_url to support tracking multiple services
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metric_snapshots (
            timestamp TIMESTAMP,
            service_url VARCHAR,  -- Which service these metrics came from
            metric_name VARCHAR,
            labels VARCHAR,  -- JSON string of all labels
            value DOUBLE,
            PRIMARY KEY (timestamp, service_url, metric_name, labels)
        )
    """)

    # Skip index creation to avoid concurrency conflicts
    # Indexes are optional for performance, the dashboard works without them
    return conn


def store_snapshot(
    conn: duckdb.DuckDBPyConnection,
    samples: list[MetricSample],
    service_url: str,
) -> None:
    """Store a snapshot of current metrics with timestamp.

    Args:
        conn: DuckDB connection
        samples: List of metric samples to store
        service_url: URL of the service these metrics came from
    """
    import json

    timestamp = datetime.now()

    # Prepare rows for insertion
    rows = []
    for sample in samples:
        # Serialize all labels as JSON for consistent key
        labels_json = json.dumps(sample.labels, sort_keys=True)

        rows.append(
            {
                "timestamp": timestamp,
                "service_url": service_url,
                "metric_name": sample.name,
                "labels": labels_json,
                "value": sample.value,
            }
        )

    # Bulk insert - insert each row individually to handle duplicates gracefully
    if rows:
        inserted_count = 0
        for row in rows:
            try:
                conn.execute(
                    """
                    INSERT INTO metric_snapshots (timestamp, service_url, metric_name, labels, value)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [row["timestamp"], row["service_url"], row["metric_name"], row["labels"], row["value"]]
                )
                inserted_count += 1
            except Exception as e:
                # Ignore duplicate key errors
                if "PRIMARY KEY" in str(e) or "UNIQUE" in str(e) or "Constraint" in str(e):
                    pass
                else:
                    raise
        conn.commit()  # Ensure data is visible to other connections


def cleanup_old_metrics(conn: duckdb.DuckDBPyConnection) -> None:
    """Remove metrics older than retention period."""
    cutoff = datetime.now() - timedelta(hours=METRICS_RETENTION_HOURS)
    conn.execute(
        """
        DELETE FROM metric_snapshots
        WHERE timestamp < ?
    """,
        [cutoff],
    )
    conn.commit()  # Ensure changes are visible


def detect_counter_resets(df: pd.DataFrame) -> list[datetime]:
    """Detect counter resets (when value decreases).

    Returns list of timestamps where resets occurred.
    """
    resets = []
    for i in range(1, len(df)):
        if df.iloc[i]["value"] < df.iloc[i - 1]["value"]:
            resets.append(df.iloc[i]["timestamp"])
    return resets


def detect_monitoring_gaps(
    df: pd.DataFrame, threshold_seconds: int = GAP_THRESHOLD_SECONDS
) -> list[dict]:
    """Detect gaps in monitoring (when dashboard stopped collecting).

    Returns list of gap intervals with start, end, and duration.
    """
    gaps = []
    for i in range(1, len(df)):
        prev_time = df.iloc[i - 1]["timestamp"]
        curr_time = df.iloc[i]["timestamp"]
        time_diff = (curr_time - prev_time).total_seconds()

        if time_diff > threshold_seconds:
            gaps.append(
                {
                    "start": prev_time,
                    "end": curr_time,
                    "duration_seconds": time_diff,
                }
            )

    return gaps


def calculate_rate_with_resets(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate rate per minute, handling counter resets.

    When a counter reset is detected (value decreases), we assume it
    restarted from 0, following Prometheus behavior.
    """
    rates = []

    for i in range(1, len(df)):
        prev_row = df.iloc[i - 1]
        curr_row = df.iloc[i]

        prev_time = prev_row["timestamp"]
        prev_value = prev_row["value"]
        curr_time = curr_row["timestamp"]
        curr_value = curr_row["value"]

        # Calculate time difference in minutes
        time_diff_minutes = (curr_time - prev_time).total_seconds() / 60

        if time_diff_minutes == 0:
            continue

        # Detect reset: if counter decreased, assume it started from 0
        if curr_value < prev_value:
            delta = curr_value
        else:
            delta = curr_value - prev_value

        rate = delta / time_diff_minutes

        rates.append(
            {
                "timestamp": curr_time,
                "rate": rate,
            }
        )

    return pd.DataFrame(rates)


def calculate_adjusted_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate adjusted cumulative values that always increase.

    When counter resets are detected, add an offset to maintain continuity.
    """
    if df.empty:
        return df

    adjusted = []
    cumulative_offset = 0.0

    for i in range(len(df)):
        curr_value = df.iloc[i]["value"]

        # Detect reset
        if i > 0 and curr_value < df.iloc[i - 1]["value"]:
            # Add the previous value to the offset before reset
            cumulative_offset += df.iloc[i - 1]["value"]

        adjusted_value = curr_value + cumulative_offset

        adjusted.append(
            {
                "timestamp": df.iloc[i]["timestamp"],
                "adjusted_value": adjusted_value,
            }
        )

    return pd.DataFrame(adjusted)


def render_metric_timeseries(
    title: str,
    df: pd.DataFrame,
    metric_key: str,
    caption: str = "",
    unit: str = "",
) -> None:
    """Render a time-series metric with Rate/Raw/Adjusted selector.

    Args:
        title: Metric display title
        df: Time-series dataframe with timestamp and value columns
        metric_key: Unique key for this metric's state
        caption: Optional caption text
        unit: Unit of measurement (e.g., "msgs", "calls", "USD", "s")
    """
    if df.empty:
        st.info(f"No {title.lower()} data yet")
        return

    # Individual chart type selector for this metric
    chart_type = st.radio(
        f"{title} View",
        ["Rate/min", "Raw", "Adjusted"],
        horizontal=True,
        key=f"chart_type_{metric_key}",
        label_visibility="collapsed",
        help="Rate: per minute | Raw: cumulative | Adjusted: always-increasing",
    )

    # Prepare data based on selection
    if chart_type == "Rate/min":
        rate_df = calculate_rate_with_resets(df)
        if not rate_df.empty:
            st.line_chart(rate_df.set_index("timestamp")[["rate"]])
            if caption:
                st.caption(f"{caption} (rate per minute)")
    elif chart_type == "Raw":
        st.line_chart(df.set_index("timestamp")[["value"]])
        if caption:
            st.caption(f"{caption} (raw cumulative)")
    else:  # Adjusted
        adj_df = calculate_adjusted_cumulative(df)
        if not adj_df.empty:
            st.line_chart(adj_df.set_index("timestamp")[["adjusted_value"]])
            if caption:
                st.caption(f"{caption} (adjusted cumulative)")


def calculate_percentile_from_buckets(
    bucket_counts: dict[float, float], percentile: float
) -> float:
    """Calculate a percentile from histogram bucket counts.

    Args:
        bucket_counts: Dict mapping bucket upper bound (le value) to cumulative count
        percentile: Percentile to calculate (0-100)

    Returns:
        Estimated percentile value
    """
    if not bucket_counts:
        return 0.0

    # Get total count (highest bucket's count)
    sorted_buckets = sorted(bucket_counts.items())
    total_count = sorted_buckets[-1][1]

    if total_count == 0:
        return 0.0

    target_count = total_count * (percentile / 100.0)

    # Find the bucket containing the percentile
    prev_le = 0.0
    prev_count = 0.0

    for le, count in sorted_buckets:
        if count >= target_count:
            # Linear interpolation within the bucket
            if count == prev_count:
                return le
            bucket_fraction = (target_count - prev_count) / (count - prev_count)
            return prev_le + (le - prev_le) * bucket_fraction
        prev_le = le
        prev_count = count

    return sorted_buckets[-1][0]


def query_latency_percentiles(
    conn: duckdb.DuckDBPyConnection,
    service_url: str,
    metric_base_name: str,
    ai_module: str,
    status: str = "success",
    hours: int = 1,
) -> pd.DataFrame:
    """Query histogram buckets and calculate percentiles over time.

    Returns DataFrame with columns: timestamp, avg, p95, p99
    """
    # Query all bucket metrics
    bucket_metric_name = f"{metric_base_name}_bucket"

    # Match JSON with spaces after colons (standard formatting)
    ai_module_pattern = f'%"ai_module": "{ai_module}"%'
    if status:
        ai_module_pattern = f'%"ai_module": "{ai_module}"%"status": "{status}"%'

    # Use f-string for INTERVAL since DuckDB doesn't support parameterized INTERVAL values
    query = f"""
        SELECT timestamp, labels, value
        FROM metric_snapshots
        WHERE service_url = ?
          AND metric_name = ?
          AND labels LIKE ?
          AND timestamp >= NOW() - INTERVAL {hours} HOUR
        ORDER BY timestamp, labels
    """

    # Force DuckDB to see latest data by creating fresh connection
    # This works around transaction isolation issues
    fresh_conn = duckdb.connect(METRICS_DB_PATH)

    try:
        try:
            df = fresh_conn.execute(
                query, [service_url, bucket_metric_name, ai_module_pattern]
            ).df()
        except Exception:
            return pd.DataFrame(columns=["timestamp", "avg", "p95", "p99"])

        if df.empty:
            return pd.DataFrame(columns=["timestamp", "avg", "p95", "p99"])

        # Also get sum and count for average calculation
        sum_df = fresh_conn.execute(
            f"SELECT timestamp, value FROM metric_snapshots WHERE service_url = ? AND metric_name = ? AND labels LIKE ? AND timestamp >= NOW() - INTERVAL {hours} HOUR ORDER BY timestamp",
            [service_url, f"{metric_base_name}_sum", ai_module_pattern],
        ).df()

        count_df = fresh_conn.execute(
            f"SELECT timestamp, value FROM metric_snapshots WHERE service_url = ? AND metric_name = ? AND labels LIKE ? AND timestamp >= NOW() - INTERVAL {hours} HOUR ORDER BY timestamp",
            [service_url, f"{metric_base_name}_count", ai_module_pattern],
        ).df()

        # Group by timestamp and calculate percentiles
        results = []
        for ts in df["timestamp"].unique():
            ts_buckets = df[df["timestamp"] == ts]

            # Parse bucket upper bounds from labels and build bucket_counts
            bucket_counts = {}
            for _, row in ts_buckets.iterrows():
                labels_str = row["labels"]
                # Extract le value from labels JSON
                import json

                try:
                    labels = json.loads(labels_str)
                    le_str = labels.get("le", "+Inf")
                    if le_str == "+Inf":
                        le = float("inf")
                    else:
                        le = float(le_str)
                    bucket_counts[le] = row["value"]
                except (json.JSONDecodeError, ValueError):
                    continue

            if not bucket_counts:
                continue

            # Calculate percentiles from current bucket counts (not diffs)
            p95 = calculate_percentile_from_buckets(bucket_counts, 95)
            p99 = calculate_percentile_from_buckets(bucket_counts, 99)

            # Calculate average from sum/count
            ts_sum_rows = sum_df[sum_df["timestamp"] == ts]
            ts_count_rows = count_df[count_df["timestamp"] == ts]

            avg = 0.0
            if not ts_sum_rows.empty and not ts_count_rows.empty:
                total_sum = ts_sum_rows["value"].iloc[0]
                total_count = ts_count_rows["value"].iloc[0]
                if total_count > 0:
                    avg = total_sum / total_count

            results.append({"timestamp": ts, "avg": avg, "p95": p95, "p99": p99})

        return pd.DataFrame(results)
    finally:
        fresh_conn.close()


def render_latency_timeseries(
    title: str,
    sum_df: pd.DataFrame,
    count_df: pd.DataFrame,
    metric_key: str,
    caption: str = "",
    conn: duckdb.DuckDBPyConnection | None = None,
    metric_base_name: str = "",
    ai_module: str = "",
    service_url: str = "",
    hours: int = 1,
) -> None:
    """Render latency time-series showing average and P95 over time.

    Args:
        title: Chart title
        sum_df: DataFrame with timestamp and sum values
        count_df: DataFrame with timestamp and count values
        metric_key: Unique key for this metric (for widget state)
        caption: Optional caption explaining the metric
        conn: DuckDB connection to query bucket data for percentiles
        metric_base_name: Base metric name for querying buckets
        ai_module: AI module name for filtering
        service_url: Service URL for filtering
        hours: Hours of history to show
    """
    import plotly.graph_objects as go  # type: ignore[import-untyped]

    if sum_df.empty or count_df.empty:
        st.info(f"No {title.lower()} data yet")
        return

    # Calculate average latency from sum/count
    merged = pd.merge(
        sum_df[["timestamp", "value"]].rename(columns={"value": "sum"}),
        count_df[["timestamp", "value"]].rename(columns={"value": "count"}),
        on="timestamp",
        how="inner",
    )

    if merged.empty:
        st.info(f"No {title.lower()} data yet")
        return

    merged = merged.sort_values("timestamp")
    merged["sum_diff"] = merged["sum"].diff().fillna(merged["sum"])
    merged["count_diff"] = merged["count"].diff().fillna(merged["count"])
    merged["avg_latency"] = merged.apply(
        lambda row: row["sum_diff"] / row["count_diff"] if row["count_diff"] > 0 else 0,
        axis=1,
    )

    chart_df = merged[merged["avg_latency"] > 0].copy()
    if chart_df.empty:
        st.info("No latency data with requests yet")
        return

    # Try to get P95 from histogram buckets
    p95_data = None
    if conn and metric_base_name:
        try:
            percentile_df = query_latency_percentiles(
                conn,
                service_url,
                metric_base_name,
                ai_module,
                status="success",
                hours=hours,
            )
            if not percentile_df.empty:
                p95_data = percentile_df[["timestamp", "p95"]].copy()
        except Exception:
            pass  # Silently skip P95 if calculation fails

    # Create plot
    fig = go.Figure()  # type: ignore[import-untyped]

    # Average line
    fig.add_trace(
        go.Scatter(
            x=chart_df["timestamp"],
            y=chart_df["avg_latency"],
            mode="lines",
            name="Average",
            line=dict(color="#1f77b4", width=2),
        )
    )

    # P95 line if available
    if p95_data is not None and not p95_data.empty:
        fig.add_trace(
            go.Scatter(
                x=p95_data["timestamp"],
                y=p95_data["p95"],
                mode="lines",
                name="P95",
                line=dict(color="#ff7f0e", width=2, dash="dash"),
            )
        )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Latency (seconds)",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=30, b=0),
        height=300,
    )

    st.plotly_chart(fig, use_container_width=True)
    if caption:
        st.caption(caption)


def query_timeseries(
    conn: duckdb.DuckDBPyConnection,
    service_url: str,
    metric_name: str,
    ai_module: str,
    status: str = "",
    tool: str = "",
    provider: str = "",
    hours: int = 1,
) -> pd.DataFrame:
    """Query time-series data for a specific metric.

    Args:
        conn: DuckDB connection
        service_url: URL of the service to query
        metric_name: Name of the metric
        ai_module: AI module label value
        status: Status label value (optional)
        tool: Tool label value (optional)
        provider: Provider label value (optional, for LLM metrics)
        hours: Number of hours to look back

    Returns:
        DataFrame with timestamp and value columns, sorted by timestamp
    """
    import json

    cutoff = datetime.now() - timedelta(hours=hours)

    # Build label filters
    query = """
        SELECT timestamp, labels, value
        FROM metric_snapshots
        WHERE service_url = ?
          AND metric_name = ?
          AND timestamp >= ?
        ORDER BY timestamp ASC
    """
    params = [service_url, metric_name, cutoff]

    df = conn.execute(query, params).df()

    if df.empty:
        return pd.DataFrame(columns=["timestamp", "value"])

    # Filter by labels in Python (DuckDB JSON functions would be more complex)
    filtered_rows = []
    for _, row in df.iterrows():
        try:
            labels = json.loads(row["labels"])
            # Check if labels match the filters
            if labels.get(LABEL_AI_MODULE) != ai_module:
                continue
            if status and labels.get(LABEL_STATUS) != status:
                continue
            if tool and labels.get(LABEL_TOOL) != tool:
                continue
            if provider and labels.get(LABEL_PROVIDER) != provider:
                continue
            filtered_rows.append({"timestamp": row["timestamp"], "value": row["value"]})
        except json.JSONDecodeError:
            continue

    return pd.DataFrame(filtered_rows)


def calculate_cost_breakdown(
    samples: list[MetricSample],
) -> dict[str, dict[str, float]]:
    """Calculate cost breakdown by provider and model.

    Returns:
        Dict mapping provider -> model -> total_cost
    """
    cost_by_provider_model: dict[str, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for sample in samples:
        provider = sample.labels.get(LABEL_PROVIDER, "unknown")
        model = sample.labels.get(LABEL_MODEL, "unknown")
        cost_by_provider_model[provider][model] += sample.value

    return dict(cost_by_provider_model)


def calculate_token_breakdown(
    samples: list[MetricSample],
) -> dict[str, dict[str, int]]:
    """Calculate token usage breakdown by model and token type.

    Returns:
        Dict mapping model -> token_type -> total_tokens
    """
    tokens_by_model: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for sample in samples:
        model = sample.labels.get(LABEL_MODEL, "unknown")
        token_type = sample.labels.get(LABEL_TOKEN_TYPE, "unknown")
        tokens_by_model[model][token_type] += int(sample.value)

    return dict(tokens_by_model)


def extract_histogram_stats(
    samples: list[MetricSample],
    metric_name: str,
) -> dict[str, dict[str, float]]:
    """Extract histogram statistics from Prometheus histogram metrics.

    Args:
        samples: List of all metric samples
        metric_name: Base name of the histogram metric (without _bucket/_sum/_count suffix)

    Returns:
        Dict mapping label_key -> {count, sum, avg, p50, p95, p99}
    """
    import json

    stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "count": 0.0,
            "sum": 0.0,
            "avg": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }
    )

    # Group by labels (excluding 'le' for buckets)
    buckets_by_labels: dict[str, list[tuple[float, float]]] = defaultdict(list)

    for sample in samples:
        # Extract sum and count
        if sample.name == f"{metric_name}_sum":
            # Create label key (without 'le')
            labels_copy = {k: v for k, v in sample.labels.items() if k != "le"}
            label_key = json.dumps(labels_copy, sort_keys=True)
            stats[label_key]["sum"] = sample.value

        elif sample.name == f"{metric_name}_count":
            labels_copy = {k: v for k, v in sample.labels.items() if k != "le"}
            label_key = json.dumps(labels_copy, sort_keys=True)
            stats[label_key]["count"] = sample.value

        elif sample.name == f"{metric_name}_bucket":
            # Store bucket data for percentile calculation
            labels_copy = {k: v for k, v in sample.labels.items() if k != "le"}
            label_key = json.dumps(labels_copy, sort_keys=True)
            le = sample.labels.get("le", "+Inf")
            if le != "+Inf":
                buckets_by_labels[label_key].append((float(le), sample.value))

    # Calculate averages and percentiles
    for label_key in stats:
        if stats[label_key]["count"] > 0:
            stats[label_key]["avg"] = (
                stats[label_key]["sum"] / stats[label_key]["count"]
            )

            # Calculate percentiles from buckets
            if label_key in buckets_by_labels:
                buckets = sorted(buckets_by_labels[label_key])
                count = stats[label_key]["count"]

                # Find bucket containing p50, p95, p99
                for percentile_name, percentile in [
                    ("p50", 0.50),
                    ("p95", 0.95),
                    ("p99", 0.99),
                ]:
                    target = count * percentile
                    for bucket_le, bucket_count in buckets:
                        if bucket_count >= target:
                            stats[label_key][percentile_name] = bucket_le
                            break

    return dict(stats)


def main() -> None:
    st.set_page_config(
        page_title="DataHub Integrations Observability",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 DataHub Integrations Service - Observability Dashboard")
    st.markdown(
        f"Connected to: **{INTEGRATIONS_SERVICE_URL}** | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    # Add refresh button and time-series controls
    col1, col2, col3 = st.columns([1, 2, 3])
    with col1:
        if st.button("🔄 Refresh", type="primary"):
            st.rerun()

    with col2:
        auto_refresh = st.checkbox("Auto-refresh (5s)")

    with col3:
        timeseries_hours = st.slider(
            "Time-series window (hours)",
            min_value=1,
            max_value=24,
            value=1,
            help="How far back to show in time-series charts",
        )

    st.divider()

    # Initialize DuckDB connection
    db_conn = init_timeseries_db()

    # Fetch and parse metrics
    metrics_text = fetch_metrics()
    if not metrics_text:
        st.warning(
            "No metrics available. Make sure the integrations service is running."
        )
        return

    samples = parse_prometheus_metrics(metrics_text)
    metrics_by_name = group_by_metric(samples)

    # Store snapshot in time-series database
    try:
        store_snapshot(db_conn, samples, INTEGRATIONS_SERVICE_URL)
        cleanup_old_metrics(db_conn)
    except Exception as e:
        st.warning(f"Failed to store time-series snapshot: {e}")

    # === GenAI Cost Overview ===
    st.header("💰 GenAI Cost Tracking")

    # OpenTelemetry appends unit to metric name, so look for USD suffix
    cost_samples = metrics_by_name.get(GENAI_LLM_COST_TOTAL_WITH_UNIT, [])
    token_samples = metrics_by_name.get(GENAI_LLM_TOKENS_TOTAL, [])

    if cost_samples:
        total_cost = sum(s.value for s in cost_samples)
        cost_breakdown = calculate_cost_breakdown(cost_samples)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Cost", f"${total_cost:.6f} USD")

        with col2:
            num_providers = len(cost_breakdown)
            st.metric("Providers", num_providers)

        with col3:
            total_models = sum(len(models) for models in cost_breakdown.values())
            st.metric("Models Used", total_models)

        # Cost breakdown by provider and model
        st.subheader("Cost Breakdown by Provider & Model")

        for provider, models in sorted(cost_breakdown.items()):
            with st.expander(
                f"**{provider.upper()}** - Total: ${sum(models.values()):.6f}"
            ):
                for model, cost in sorted(
                    models.items(), key=lambda x: x[1], reverse=True
                ):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.text(f"  • {model}")
                    with col2:
                        st.text(f"${cost:.6f}")
    else:
        st.info("No GenAI cost metrics available yet. Make LLM calls to see data.")

    st.divider()

    # === Token Usage ===
    st.header("🎯 Token Usage")

    if token_samples:
        token_breakdown = calculate_token_breakdown(token_samples)

        # Calculate totals
        total_tokens = sum(s.value for s in token_samples)
        prompt_tokens = sum(
            s.value
            for s in token_samples
            if s.labels.get(LABEL_TOKEN_TYPE) == TOKEN_TYPE_PROMPT
        )
        completion_tokens = sum(
            s.value
            for s in token_samples
            if s.labels.get(LABEL_TOKEN_TYPE) == TOKEN_TYPE_COMPLETION
        )
        cache_read_tokens = sum(
            s.value
            for s in token_samples
            if s.labels.get(LABEL_TOKEN_TYPE) == TOKEN_TYPE_CACHE_READ
        )
        cache_write_tokens = sum(
            s.value
            for s in token_samples
            if s.labels.get(LABEL_TOKEN_TYPE) == TOKEN_TYPE_CACHE_WRITE
        )

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Tokens", f"{int(total_tokens):,}")

        with col2:
            st.metric("Prompt", f"{int(prompt_tokens):,}")

        with col3:
            st.metric("Completion", f"{int(completion_tokens):,}")

        with col4:
            cache_total = int(cache_read_tokens + cache_write_tokens)
            st.metric("Cache Tokens", f"{cache_total:,}")

        # Cache efficiency metric
        if cache_read_tokens > 0:
            cache_efficiency = (
                cache_read_tokens / (prompt_tokens + cache_read_tokens)
            ) * 100
            st.success(
                f"🚀 **Cache Hit Rate:** {cache_efficiency:.1f}% "
                f"({int(cache_read_tokens):,} cache reads vs {int(prompt_tokens):,} regular prompts)"
            )

        # Token breakdown by model
        st.subheader("Token Usage by Model")

        for model, token_types in sorted(token_breakdown.items()):
            model_total = sum(token_types.values())
            with st.expander(f"**{model}** - Total: {model_total:,} tokens"):
                for token_type, count in sorted(
                    token_types.items(), key=lambda x: x[1], reverse=True
                ):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        # Add emoji indicators
                        emoji = {
                            "prompt": "📝",
                            "completion": "✍️",
                            "cache_read": "💾",
                            "cache_write": "📦",
                        }.get(token_type, "❓")
                        st.text(f"  {emoji} {token_type}")
                    with col2:
                        st.text(f"{count:,}")
    else:
        st.info("No token usage metrics available yet. Make LLM calls to see data.")

    st.divider()

    # === Three-Tier AI Module Metrics ===
    st.header("🤖 AI Modules - Three-Tier Tracking")

    # Get three-tier metrics
    user_messages = metrics_by_name.get(GENAI_USER_MESSAGES_TOTAL, [])
    llm_calls = metrics_by_name.get(GENAI_LLM_CALLS_TOTAL, [])
    tool_calls = metrics_by_name.get(GENAI_TOOL_CALLS_TOTAL, [])

    if user_messages or llm_calls or cost_samples:
        # Aggregate by AI module
        modules_data: dict[str, dict[str, int | float]] = defaultdict(
            lambda: {
                "user_messages": 0,
                "user_messages_failed": 0,
                "llm_calls": 0,
                "tool_calls": 0,
                "cost": 0.0,
            }
        )

        # Tier 1: User messages (track both success and error)
        for sample in user_messages:
            ai_module = sample.labels.get(LABEL_AI_MODULE, "unknown")
            status = sample.labels.get(LABEL_STATUS)
            if status == "success":
                modules_data[ai_module]["user_messages"] += int(sample.value)
            elif status == "error":
                modules_data[ai_module]["user_messages_failed"] += int(sample.value)

        # Tier 2: LLM calls (only success - failures don't make LLM calls)
        for sample in llm_calls:
            ai_module = sample.labels.get(LABEL_AI_MODULE, "unknown")
            if sample.labels.get(LABEL_STATUS) == "success":
                modules_data[ai_module]["llm_calls"] += int(sample.value)

        # Tier 3: Tool calls (only success - failures don't make tool calls)
        for sample in tool_calls:
            ai_module = sample.labels.get(LABEL_AI_MODULE, "unknown")
            if sample.labels.get(LABEL_STATUS) == "success":
                modules_data[ai_module]["tool_calls"] += int(sample.value)

        # Cost
        for sample in cost_samples:
            ai_module = sample.labels.get(LABEL_AI_MODULE, "unknown")
            modules_data[ai_module]["cost"] += sample.value

        # Display each AI module
        for ai_module in sorted(modules_data.keys()):
            data = modules_data[ai_module]
            user_msgs = data["user_messages"]
            user_msgs_failed = data["user_messages_failed"]
            total_user_msgs = user_msgs + user_msgs_failed
            llm = data["llm_calls"]
            tools = data["tool_calls"]
            cost = data["cost"]

            # Calculate ratios (based on successful messages only)
            llm_per_msg = (llm / user_msgs) if user_msgs > 0 else 0
            tools_per_msg = (tools / user_msgs) if user_msgs > 0 else 0
            cost_per_msg = (cost / user_msgs) if user_msgs > 0 else 0

            # Calculate success rate
            success_rate = (
                (user_msgs / total_user_msgs * 100) if total_user_msgs > 0 else 0
            )

            with st.expander(f"**{ai_module}** - ${cost:.4f} total", expanded=True):
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric(
                        "User Messages",
                        f"{user_msgs:,}",
                        delta=f"{success_rate:.1f}% success",
                    )
                    if user_msgs_failed > 0:
                        st.caption(
                            f"Tier 1: {user_msgs:,} success, {user_msgs_failed:,} failed"
                        )
                    else:
                        st.caption("Tier 1: User requests")

                with col2:
                    st.metric(
                        "LLM Calls", f"{llm:,}", delta=f"{llm_per_msg:.1f} per msg"
                    )
                    st.caption("Tier 2: API calls")

                with col3:
                    st.metric(
                        "Tool Calls", f"{tools:,}", delta=f"{tools_per_msg:.1f} per msg"
                    )
                    st.caption("Tier 3: Tool invocations")

                # Cost breakdown
                st.markdown("**Cost Analysis:**")
                col1, col2 = st.columns(2)
                with col1:
                    st.text(f"Total Cost: ${cost:.6f}")
                    st.text(f"Cost per Message: ${cost_per_msg:.6f}")
                with col2:
                    cost_per_llm = (cost / llm) if llm > 0 else 0
                    st.text(f"Cost per LLM Call: ${cost_per_llm:.6f}")
                    if llm_per_msg > 0:
                        efficiency = (
                            "🟢 Efficient"
                            if llm_per_msg < 2
                            else "🟡 Moderate"
                            if llm_per_msg < 3
                            else "🔴 Complex"
                        )
                        st.text(f"Agentic Complexity: {efficiency}")

                # Tool breakdown if available
                module_tools = [
                    s for s in tool_calls if s.labels.get(LABEL_AI_MODULE) == ai_module
                ]
                if module_tools:
                    st.markdown("**Top Tools:**")
                    tool_counts: defaultdict[str, int] = defaultdict(int)
                    for sample in module_tools:
                        tool_name = sample.labels.get(LABEL_TOOL, "unknown")
                        tool_counts[tool_name] += int(sample.value)

                    for tool, count in sorted(
                        tool_counts.items(), key=lambda x: x[1], reverse=True
                    )[:5]:
                        st.text(f"  • {tool}: {count:,} calls")

    else:
        st.info(
            "No AI module metrics available yet. Make some AI requests to see data."
        )

    st.divider()

    # === Latency Tracking (Three-Tier) ===
    st.header("⏱️ Latency Tracking - Three-Tier Performance")

    # Collect all histogram samples
    all_samples = [s for samples_list in metrics_by_name.values() for s in samples_list]

    # Extract histogram statistics for each tier
    user_latency_stats = extract_histogram_stats(
        all_samples, GENAI_USER_MESSAGE_DURATION
    )
    llm_latency_stats = extract_histogram_stats(all_samples, GENAI_LLM_CALL_DURATION)
    tool_latency_stats = extract_histogram_stats(all_samples, GENAI_TOOL_CALL_DURATION)

    if user_latency_stats or llm_latency_stats or tool_latency_stats:
        import json

        # Display each AI module's latency
        if user_latency_stats:
            st.subheader("Tier 1: User Message Latency")
            st.caption("Overall request → response time from user's perspective")

            for label_key, stats in user_latency_stats.items():
                labels = json.loads(label_key)
                ai_module = labels.get(LABEL_AI_MODULE, "unknown")
                status = labels.get(LABEL_STATUS, "unknown")

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        f"{ai_module} ({status})", f"{stats['count']:.0f} requests"
                    )
                with col2:
                    st.metric("Avg", f"{stats['avg']:.2f}s")
                with col3:
                    st.metric("P95", f"{stats['p95']:.2f}s")
                with col4:
                    st.metric("P99", f"{stats['p99']:.2f}s")

        if llm_latency_stats:
            st.subheader("Tier 2: LLM Call Latency")
            st.caption(
                "Individual LLM API call duration (multiple calls per user message)"
            )

            for label_key, stats in llm_latency_stats.items():
                labels = json.loads(label_key)
                ai_module = labels.get(LABEL_AI_MODULE, "unknown")
                provider = labels.get(LABEL_PROVIDER, "unknown")
                model = labels.get(LABEL_MODEL, "unknown")
                status = labels.get(LABEL_STATUS, "unknown")

                with st.expander(f"**{provider}** / {model} ({status})"):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Calls", f"{stats['count']:.0f}")
                    with col2:
                        st.metric("Avg", f"{stats['avg']:.2f}s")
                    with col3:
                        st.metric("P50", f"{stats['p50']:.2f}s")
                    with col4:
                        st.metric("P95", f"{stats['p95']:.2f}s")
                    with col5:
                        st.metric("P99", f"{stats['p99']:.2f}s")

        if tool_latency_stats:
            st.subheader("Tier 3: Tool Call Latency")
            st.caption(
                "Individual tool execution duration (multiple tool calls per LLM call)"
            )

            for label_key, stats in tool_latency_stats.items():
                labels = json.loads(label_key)
                ai_module = labels.get(LABEL_AI_MODULE, "unknown")
                tool = labels.get(LABEL_TOOL, "unknown")
                status = labels.get(LABEL_STATUS, "unknown")

                with st.expander(f"**{tool}** ({status})"):
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Calls", f"{stats['count']:.0f}")
                    with col2:
                        st.metric("Avg", f"{stats['avg']:.2f}s")
                    with col3:
                        st.metric("P50", f"{stats['p50']:.2f}s")
                    with col4:
                        st.metric("P95", f"{stats['p95']:.2f}s")
                    with col5:
                        st.metric("P99", f"{stats['p99']:.2f}s")

        # Latency breakdown insights
        if user_latency_stats and llm_latency_stats:
            st.markdown("---")
            st.markdown("**⚡ Performance Insights:**")

            # Calculate overhead (non-LLM time)
            for user_key, user_stats in user_latency_stats.items():
                user_labels = json.loads(user_key)
                ai_module = user_labels.get(LABEL_AI_MODULE)

                # Find matching LLM stats for same module
                matching_llm_stats = [
                    (key, stats)
                    for key, stats in llm_latency_stats.items()
                    if json.loads(key).get(LABEL_AI_MODULE) == ai_module
                ]

                if matching_llm_stats and user_stats["count"] > 0:
                    # Calculate total LLM time
                    total_llm_time = sum(
                        stats["sum"] for _, stats in matching_llm_stats
                    )
                    total_user_time = user_stats["sum"]
                    overhead_time = total_user_time - total_llm_time
                    overhead_pct = (
                        (overhead_time / total_user_time * 100)
                        if total_user_time > 0
                        else 0
                    )

                    col1, col2 = st.columns(2)
                    with col1:
                        st.text(f"📊 {ai_module}: LLM time = {total_llm_time:.2f}s")
                        st.text(f"   User time = {total_user_time:.2f}s")
                    with col2:
                        st.text(
                            f"🔧 Overhead = {overhead_time:.2f}s ({overhead_pct:.1f}%)"
                        )
                        if overhead_pct > 20:
                            st.caption(
                                "High overhead - tool execution or processing time"
                            )
                        else:
                            st.caption("Efficient - most time spent in LLM calls")
    else:
        st.info("No latency metrics available yet. Make some AI requests to see data.")

    st.divider()

    # === Time-Series Charts ===
    st.header("📈 Time-Series Analytics")

    # Only show if we have AI module data
    if user_messages or llm_calls or cost_samples:
        # Get list of AI modules
        ai_modules_list = sorted(modules_data.keys())

        if ai_modules_list:
            # Module selector
            selected_module = st.selectbox(
                "Select AI Module",
                ai_modules_list,
                help="Choose which AI module to visualize over time",
            )

            st.caption(
                "Each metric can be viewed as rate/minute, raw cumulative, or adjusted cumulative independently"
            )

            # Query time-series data for selected module
            try:
                # User messages (success)
                ts_user_success = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    GENAI_USER_MESSAGES_TOTAL,
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )

                # User messages (error)
                ts_user_error = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    GENAI_USER_MESSAGES_TOTAL,
                    selected_module,
                    status="error",
                    hours=timeseries_hours,
                )

                # LLM calls
                ts_llm = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    GENAI_LLM_CALLS_TOTAL,
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )

                # Tool calls
                ts_tools = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    GENAI_TOOL_CALLS_TOTAL,
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )

                # Cost
                ts_cost = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    GENAI_LLM_COST_TOTAL_WITH_UNIT,
                    selected_module,
                    hours=timeseries_hours,
                )

                # Latency metrics (histogram sum and count values)
                ts_user_latency_sum = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    f"{GENAI_USER_MESSAGE_DURATION}_sum",
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )
                ts_user_latency_count = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    f"{GENAI_USER_MESSAGE_DURATION}_count",
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )

                ts_llm_latency_sum = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    f"{GENAI_LLM_CALL_DURATION}_sum",
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )
                ts_llm_latency_count = query_timeseries(
                    db_conn,
                    INTEGRATIONS_SERVICE_URL,
                    f"{GENAI_LLM_CALL_DURATION}_count",
                    selected_module,
                    status="success",
                    hours=timeseries_hours,
                )

                # Check if we have data
                has_data = any(
                    [
                        not ts_user_success.empty,
                        not ts_user_error.empty,
                        not ts_llm.empty,
                        not ts_tools.empty,
                        not ts_cost.empty,
                        not ts_user_latency_sum.empty,
                        not ts_llm_latency_sum.empty,
                    ]
                )

                if has_data:
                    # Detect resets and gaps for user messages
                    resets = []
                    gaps = []
                    if not ts_user_success.empty:
                        resets = detect_counter_resets(ts_user_success)
                        gaps = detect_monitoring_gaps(ts_user_success)

                    # Show warnings for resets and gaps
                    if resets:
                        st.warning(
                            f"⚠️ Detected {len(resets)} service restart(s) in this time window"
                        )
                    if gaps:
                        total_gap_time = sum(g["duration_seconds"] for g in gaps)
                        st.warning(
                            f"⚠️ Detected {len(gaps)} monitoring gap(s) totaling {total_gap_time:.0f} seconds"
                        )

                    # === Render metrics with individual selectors ===
                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader("User Messages")
                        # Combine success and error for display
                        if not ts_user_success.empty or not ts_user_error.empty:
                            # For now, show success only with the new helper
                            # (combining would need a different approach)
                            if not ts_user_success.empty:
                                render_metric_timeseries(
                                    "User Messages (Success)",
                                    ts_user_success,
                                    f"{selected_module}_user_success",
                                    "Successful user requests",
                                )
                            if not ts_user_error.empty:
                                render_metric_timeseries(
                                    "User Messages (Error)",
                                    ts_user_error,
                                    f"{selected_module}_user_error",
                                    "Failed user requests",
                                )
                        else:
                            st.info("No user message data yet")

                        st.subheader("Tool Calls")
                        render_metric_timeseries(
                            "Tool Calls",
                            ts_tools,
                            f"{selected_module}_tools",
                            "Tool invocations",
                        )

                    with col2:
                        st.subheader("LLM Calls")
                        render_metric_timeseries(
                            "LLM Calls",
                            ts_llm,
                            f"{selected_module}_llm",
                            "LLM API calls",
                        )

                        st.subheader("Cost")
                        render_metric_timeseries(
                            "Cost",
                            ts_cost,
                            f"{selected_module}_cost",
                            "GenAI cost in USD",
                        )

                    # === Latency Metrics ===
                    st.markdown("---")
                    st.markdown("**⏱️ Latency Metrics Over Time**")
                    st.caption(
                        "Historical latency trends showing average, 95th percentile (P95), and 99th percentile (P99) calculated from histogram buckets."
                    )

                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader("User Message Latency")
                        render_latency_timeseries(
                            "User Message Latency",
                            ts_user_latency_sum,
                            ts_user_latency_count,
                            f"{selected_module}_user_latency",
                            "User-facing latency (avg, P95, P99)",
                            conn=db_conn,
                            metric_base_name=GENAI_USER_MESSAGE_DURATION,
                            ai_module=selected_module,
                            service_url=INTEGRATIONS_SERVICE_URL,
                            hours=timeseries_hours,
                        )

                    with col2:
                        st.subheader("LLM Call Latency")
                        render_latency_timeseries(
                            "LLM Call Latency",
                            ts_llm_latency_sum,
                            ts_llm_latency_count,
                            f"{selected_module}_llm_latency",
                            "LLM API time (avg, P95, P99)",
                            conn=db_conn,
                            metric_base_name=GENAI_LLM_CALL_DURATION,
                            ai_module=selected_module,
                            service_url=INTEGRATIONS_SERVICE_URL,
                            hours=timeseries_hours,
                        )

                else:
                    st.info(
                        f"No time-series data available yet for {selected_module}. Keep the dashboard running to collect metrics over time."
                    )

            except Exception as e:
                st.error(f"Failed to query time-series data: {e}")
        else:
            st.info("No AI modules to display")
    else:
        st.info("Make some AI requests to see time-series analytics.")

    st.divider()

    # === Actions Metrics ===
    st.header("⚙️ DataHub Actions - Health & Performance")

    # Get Actions metrics
    actions_running_count = metrics_by_name.get(ACTIONS_RUNNING_COUNT, [])
    actions_info = metrics_by_name.get(ACTIONS_INFO, [])
    actions_success_total = metrics_by_name.get(ACTIONS_SUCCESS_TOTAL, [])
    actions_error_total = metrics_by_name.get(ACTIONS_ERROR_TOTAL, [])
    actions_execution_total = metrics_by_name.get(ACTIONS_EXECUTION_TOTAL, [])

    # Phase 3 metrics: Event and asset processing
    actions_assets_processed = metrics_by_name.get(ACTIONS_ASSETS_PROCESSED, [])
    actions_assets_impacted = metrics_by_name.get(ACTIONS_ASSETS_IMPACTED, [])
    actions_events_processed = metrics_by_name.get(ACTIONS_EVENTS_PROCESSED, [])
    actions_actions_executed = metrics_by_name.get(ACTIONS_ACTIONS_EXECUTED, [])
    # Note: actions_event_lag could be added here for lag visualization in the future

    if actions_execution_total or actions_running_count or actions_success_total:
        # === Global Actions Health ===
        st.subheader("🌍 Global Actions Health")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            # Total currently running actions across all stages
            total_running = sum(s.value for s in actions_running_count)
            st.metric("Currently Running", f"{int(total_running):,}")

        with col2:
            # Total successful executions
            total_success = sum(s.value for s in actions_success_total)
            st.metric("Total Success", f"{int(total_success):,}", delta="✅")

        with col3:
            # Total failed executions
            total_errors = sum(s.value for s in actions_error_total)
            st.metric(
                "Total Errors",
                f"{int(total_errors):,}",
                delta="❌" if total_errors > 0 else None,
            )

        with col4:
            # Success rate
            total_executions = total_success + total_errors
            success_rate = (
                (total_success / total_executions * 100) if total_executions > 0 else 0
            )
            st.metric("Success Rate", f"{success_rate:.1f}%")

        # Phase 3 metrics: Event and asset processing
        if actions_assets_processed or actions_events_processed:
            st.markdown("**📊 Processing Statistics:**")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                # Total assets processed
                total_assets_processed = sum(s.value for s in actions_assets_processed)
                st.metric("Assets Processed", f"{int(total_assets_processed):,}")

            with col2:
                # Total assets impacted
                total_assets_impacted = sum(s.value for s in actions_assets_impacted)
                st.metric("Assets Impacted", f"{int(total_assets_impacted):,}")

            with col3:
                # Total events processed
                total_events = sum(s.value for s in actions_events_processed)
                st.metric("Events Processed", f"{int(total_events):,}")

            with col4:
                # Event success rate
                if actions_events_processed:
                    success_events = sum(
                        s.value
                        for s in actions_events_processed
                        if s.labels.get("success") == "true"
                    )
                    total_events_all = sum(s.value for s in actions_events_processed)
                    event_success_rate = (
                        (success_events / total_events_all * 100)
                        if total_events_all > 0
                        else 0
                    )
                    st.metric("Event Success Rate", f"{event_success_rate:.1f}%")
                else:
                    st.metric("Event Success Rate", "N/A")

        # Running actions by stage
        if actions_running_count:
            st.markdown("**Actions Running by Stage:**")
            col1, col2, col3 = st.columns(3)

            stage_counts: dict[str, int] = {}
            for sample in actions_running_count:
                stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
                stage_counts[stage] = int(sample.value)

            with col1:
                st.metric("Bootstrap", f"{stage_counts.get('BOOTSTRAP', 0)}")

            with col2:
                st.metric("Live", f"{stage_counts.get('LIVE', 0)}")

            with col3:
                st.metric("Rollback", f"{stage_counts.get('ROLLBACK', 0)}")

        st.divider()

        # === Per Action Health ===
        st.subheader("📋 Per Action Health")

        # Aggregate metrics by action_urn and stage
        from typing import Any

        action_data: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "action_urn": "",
                "action_name": "",
                "action_type": "",
                "executor_id": "",
                "stages": defaultdict(
                    lambda: {
                        "state": "unknown",
                        "success": 0,
                        "error": 0,
                        "total_executions": 0,
                        # Phase 3 metrics
                        "assets_processed": 0,
                        "assets_impacted": 0,
                        "events_processed": 0,
                        "events_success": 0,
                        "events_failed": 0,
                        "actions_executed": 0,
                        # Kafka lag tracking (per topic)
                        "kafka_topics": defaultdict(lambda: {"lag": 0}),
                    }
                ),
            }
        )

        # Collect success counts per stage
        for sample in actions_success_total:
            action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
            stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
            action_data[action_urn]["action_urn"] = action_urn
            action_data[action_urn]["action_name"] = sample.labels.get(
                LABEL_ACTION_NAME, "unknown"
            )
            action_data[action_urn]["action_type"] = sample.labels.get(
                LABEL_ACTION_TYPE, "unknown"
            )
            action_data[action_urn]["stages"][stage]["success"] += int(sample.value)

        # Collect error counts per stage
        for sample in actions_error_total:
            action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
            stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
            action_data[action_urn]["action_urn"] = action_urn
            action_data[action_urn]["action_name"] = sample.labels.get(
                LABEL_ACTION_NAME, "unknown"
            )
            action_data[action_urn]["action_type"] = sample.labels.get(
                LABEL_ACTION_TYPE, "unknown"
            )
            action_data[action_urn]["stages"][stage]["error"] += int(sample.value)

        # Collect current state from actions_info per stage
        for sample in actions_info:
            if sample.value > 0:  # Only consider active states
                action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
                state = sample.labels.get(LABEL_STATE, "unknown")
                stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
                executor_id = sample.labels.get(LABEL_EXECUTOR_ID, "")

                # Populate action metadata from actions_info
                action_data[action_urn]["action_urn"] = action_urn
                action_data[action_urn]["action_name"] = sample.labels.get(
                    LABEL_ACTION_NAME, "unknown"
                )
                action_data[action_urn]["action_type"] = sample.labels.get(
                    LABEL_ACTION_TYPE, "unknown"
                )
                action_data[action_urn]["executor_id"] = executor_id
                action_data[action_urn]["stages"][stage]["state"] = state

        # Collect Phase 3 metrics per action per stage
        for sample in actions_assets_processed:
            action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
            stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
            action_data[action_urn]["stages"][stage]["assets_processed"] += int(
                sample.value
            )
            # Ensure action metadata is populated
            if not action_data[action_urn]["action_name"]:
                action_data[action_urn]["action_name"] = sample.labels.get(
                    LABEL_ACTION_NAME, "unknown"
                )
                action_data[action_urn]["action_type"] = sample.labels.get(
                    LABEL_ACTION_TYPE, "unknown"
                )
                action_data[action_urn]["action_urn"] = action_urn

        for sample in actions_assets_impacted:
            action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
            stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
            action_data[action_urn]["stages"][stage]["assets_impacted"] += int(
                sample.value
            )

        for sample in actions_events_processed:
            action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
            stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
            success = sample.labels.get("success", "unknown")
            count = int(sample.value)
            action_data[action_urn]["stages"][stage]["events_processed"] += count
            if success == "true":
                action_data[action_urn]["stages"][stage]["events_success"] += count
            elif success == "false":
                action_data[action_urn]["stages"][stage]["events_failed"] += count

        for sample in actions_actions_executed:
            action_urn = sample.labels.get(LABEL_ACTION_URN, "unknown")
            stage = sample.labels.get(LABEL_STAGE, "unknown").upper()  # Normalize to uppercase
            action_data[action_urn]["stages"][stage]["actions_executed"] += int(
                sample.value
            )

        # Collect Kafka consumer lag metrics
        # Note: Kafka lag is tracked by pipeline_name (action name) and topic,
        # but doesn't have stage labels since it's emitted by datahub-actions library.
        # We'll aggregate at the action level rather than per-stage.
        kafka_lag_samples = metrics_by_name.get(KAFKA_CONSUMER_LAG, [])

        # Build mapping from action_name to action_urn for Kafka lag lookup
        action_name_to_urn: dict[str, str] = {}
        for urn, data in action_data.items():
            if data["action_name"] and data["action_name"] != "unknown":
                action_name_to_urn[data["action_name"]] = urn

        for sample in kafka_lag_samples:
            pipeline_name = sample.labels.get(LABEL_PIPELINE_NAME, "")
            topic = sample.labels.get(LABEL_TOPIC, "unknown")
            lag = sample.value

            # Map pipeline_name to action_urn
            action_urn = action_name_to_urn.get(pipeline_name, "")
            if not action_urn:
                continue

            # Store lag per topic in LIVE stage (Kafka lag is for LIVE actions)
            # Access will auto-create LIVE stage if it doesn't exist (defaultdict)
            action_data[action_urn]["stages"]["LIVE"]["kafka_topics"][topic][
                "lag"
            ] = lag

        # Calculate totals per stage
        for _action_urn, data in action_data.items():
            for _stage, stage_data in data["stages"].items():
                stage_data["total_executions"] = (
                    stage_data["success"] + stage_data["error"]
                )

        if action_data:
            # Sort by whether action has any LIVE stage running (most active first)
            def sort_key(x):
                data = x[1]
                has_live_running = (
                    data["stages"].get("LIVE", {}).get("state") == "running"
                )
                total_executions = sum(
                    stage_data.get("total_executions", 0)
                    for stage_data in data["stages"].values()
                )
                return (has_live_running, total_executions)

            sorted_actions = sorted(
                action_data.items(),
                key=sort_key,
                reverse=True,
            )

            for action_urn, data in sorted_actions:
                # Action name for display (prefer name over URN)
                # If action_name is a URN, use action_type instead
                action_name = data["action_name"]
                if (
                    action_name.startswith(("urn:", "datahub_urn:"))
                    or action_name == "unknown"
                ):
                    display_name = (
                        data["action_type"]
                        if data["action_type"] != "unknown"
                        else action_urn
                    )
                else:
                    display_name = action_name

                # Determine primary state (LIVE if running, otherwise first stage)
                primary_state = "unknown"
                if data["stages"].get("LIVE", {}).get("state") == "running":
                    primary_state = "running"
                else:
                    for stage_data in data["stages"].values():
                        if stage_data.get("state") != "unknown":
                            primary_state = stage_data["state"]
                            break

                # State emoji
                state_emoji = {
                    "running": "🟢",
                    "stopped": "⏸️",
                    "failed": "🔴",
                    "unknown": "⚪",
                }.get(primary_state, "⚪")

                # Calculate total stats across all stages
                total_executions = sum(
                    stage_data.get("total_executions", 0)
                    for stage_data in data["stages"].values()
                )
                total_success = sum(
                    stage_data.get("success", 0)
                    for stage_data in data["stages"].values()
                )
                total_success_pct = (
                    (total_success / total_executions * 100)
                    if total_executions > 0
                    else 0
                )

                # Build status text
                if primary_state == "running" and total_executions == 0:
                    # LIVE action with no batch completions
                    events_processed = sum(
                        stage_data.get("events_processed", 0)
                        for stage_data in data["stages"].values()
                    )
                    status_text = f"{events_processed} events processed"
                else:
                    # Batch stages or action with completions
                    status_text = (
                        f"{total_success_pct:.1f}% success ({total_executions} total)"
                    )

                with st.expander(
                    f"{state_emoji} **{display_name}** - {status_text}",
                    expanded=(primary_state == "running"),
                ):
                    # Action metadata
                    col1, col2 = st.columns(2)
                    with col1:
                        st.text(f"URN: {action_urn}")
                    with col2:
                        st.text(f"Type: {data['action_type']}")

                    if data["executor_id"]:
                        st.caption(f"Executor: {data['executor_id']}")

                    st.divider()

                    # Display each stage separately
                    stage_order = ["LIVE", "BOOTSTRAP", "ROLLBACK"]
                    for stage_name in stage_order:
                        if stage_name not in data["stages"]:
                            continue

                        stage_data = data["stages"][stage_name]

                        # Check if stage has kafka topics data
                        kafka_topics = stage_data.get("kafka_topics", {})
                        if isinstance(kafka_topics, defaultdict):
                            kafka_topics = dict(kafka_topics)
                        has_kafka_data = bool(kafka_topics)

                        # Skip stages with no data
                        if (
                            stage_data["total_executions"] == 0
                            and stage_data["assets_processed"] == 0
                            and not has_kafka_data
                        ):
                            continue

                        # Normalize stage name for display
                        display_stage = stage_name.upper()
                        state = stage_data["state"]

                        # Stage header
                        stage_emoji = {
                            "running": "🟢",
                            "stopped": "⏸️",
                            "failed": "🔴",
                            "unknown": "⚪",
                        }.get(state, "⚪")

                        if display_stage == "LIVE":
                            stage_label = (
                                f"{stage_emoji} **{display_stage} Stage** (Continuous)"
                            )
                        elif display_stage == "BOOTSTRAP":
                            stage_label = (
                                f"{stage_emoji} **{display_stage} Stage** (Last Run)"
                            )
                        elif display_stage == "ROLLBACK":
                            stage_label = (
                                f"{stage_emoji} **{display_stage} Stage** (Last Run)"
                            )
                        else:
                            stage_label = f"{stage_emoji} **{display_stage} Stage**"

                        st.markdown(stage_label)
                        st.caption(f"State: {state}")

                        # Execution stats for this stage
                        if stage_data["total_executions"] > 0:
                            success_count = stage_data["success"]
                            error_count = stage_data["error"]
                            total = stage_data["total_executions"]
                            success_pct = (
                                (success_count / total * 100) if total > 0 else 0
                            )

                            col1, col2, col3, col4 = st.columns(4)

                            with col1:
                                st.metric("Executions", f"{total:,}")

                            with col2:
                                st.metric("Success", f"{success_count:,}")

                            with col3:
                                st.metric("Errors", f"{error_count:,}")

                            with col4:
                                st.metric("Success Rate", f"{success_pct:.1f}%")

                        # Phase 3: Processing statistics for this stage
                        if (
                            stage_data["assets_processed"] > 0
                            or stage_data["events_processed"] > 0
                            or stage_data["actions_executed"] > 0
                        ):
                            col1, col2, col3, col4 = st.columns(4)

                            with col1:
                                st.metric(
                                    "Assets Processed",
                                    f"{stage_data['assets_processed']:,}",
                                )

                            with col2:
                                st.metric(
                                    "Assets Impacted",
                                    f"{stage_data['assets_impacted']:,}",
                                )

                            with col3:
                                st.metric(
                                    "Events Processed",
                                    f"{stage_data['events_processed']:,}",
                                )

                            with col4:
                                # Calculate impact ratio
                                if stage_data["assets_processed"] > 0:
                                    impact_ratio = (
                                        stage_data["assets_impacted"]
                                        / stage_data["assets_processed"]
                                        * 100
                                    )
                                    st.metric("Impact Ratio", f"{impact_ratio:.1f}%")
                                else:
                                    st.metric("Impact Ratio", "N/A")

                            # Event success rate if events were processed
                            if stage_data["events_processed"] > 0:
                                col1, col2, col3 = st.columns(3)

                                with col1:
                                    st.metric(
                                        "Events Success",
                                        f"{stage_data['events_success']:,}",
                                    )

                                with col2:
                                    st.metric(
                                        "Events Failed",
                                        f"{stage_data['events_failed']:,}",
                                    )

                                with col3:
                                    event_success_pct = (
                                        stage_data["events_success"]
                                        / stage_data["events_processed"]
                                        * 100
                                    )
                                    st.metric(
                                        "Event Success Rate",
                                        f"{event_success_pct:.1f}%",
                                    )

                            # Actions executed
                            if stage_data["actions_executed"] > 0:
                                st.metric(
                                    "Actions Executed",
                                    f"{stage_data['actions_executed']:,}",
                                    help="Individual operations performed (e.g., descriptions written)",
                                )

                            # Kafka consumer lag (per topic)
                            kafka_topics = stage_data.get("kafka_topics", {})
                            # Convert defaultdict to dict for proper checking
                            if isinstance(kafka_topics, defaultdict):
                                kafka_topics = dict(kafka_topics)

                            if kafka_topics:
                                st.markdown("**📊 Kafka Consumer Lag**")

                                # Show lag per topic
                                for topic, topic_data in kafka_topics.items():
                                    lag = topic_data.get("lag", 0)

                                    # Format topic name (shorten if too long)
                                    display_topic = topic
                                    if len(topic) > 50:
                                        display_topic = topic[:47] + "..."

                                    # Color code lag (green < 100, yellow < 1000, red >= 1000)
                                    if lag < 100:
                                        lag_color = "🟢"
                                    elif lag < 1000:
                                        lag_color = "🟡"
                                    else:
                                        lag_color = "🔴"

                                    col1, col2 = st.columns([3, 1])
                                    with col1:
                                        st.caption(f"{lag_color} {display_topic}")
                                    with col2:
                                        st.caption(f"{int(lag):,} messages")

                                # Time-series chart for lag trends
                                st.markdown("**📈 Lag Trend**")

                                try:
                                    import json

                                    import plotly.graph_objects as go

                                    cutoff = datetime.now() - timedelta(hours=timeseries_hours)

                                    query = """
                                        SELECT timestamp, labels, value
                                        FROM metric_snapshots
                                        WHERE service_url = ?
                                          AND metric_name = ?
                                          AND timestamp >= ?
                                        ORDER BY timestamp ASC
                                    """
                                    params = [INTEGRATIONS_SERVICE_URL, KAFKA_CONSUMER_LAG, cutoff]
                                    df_lag = db_conn.execute(query, params).df()

                                    if not df_lag.empty:
                                        # Filter by this action's name
                                        action_name = data["action_name"]
                                        topic_series = {}

                                        for _, row in df_lag.iterrows():
                                            try:
                                                labels = json.loads(row["labels"])
                                                if labels.get(LABEL_PIPELINE_NAME) == action_name:
                                                    topic = labels.get(LABEL_TOPIC)
                                                    if topic not in topic_series:
                                                        topic_series[topic] = []
                                                    topic_series[topic].append({
                                                        "timestamp": row["timestamp"],
                                                        "value": row["value"]
                                                    })
                                            except json.JSONDecodeError:
                                                continue

                                        if topic_series:
                                            fig = go.Figure()

                                            for topic, points in topic_series.items():
                                                ts_df = pd.DataFrame(points)
                                                fig.add_trace(go.Scatter(
                                                    x=ts_df["timestamp"],
                                                    y=ts_df["value"],
                                                    mode="lines+markers",
                                                    name=topic,
                                                    marker=dict(size=3),
                                                ))

                                            fig.update_layout(
                                                xaxis_title="Time",
                                                yaxis_title="Lag (messages)",
                                                hovermode="x unified",
                                                height=300,
                                                margin=dict(l=0, r=0, t=10, b=0),
                                                showlegend=True,
                                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                                            )

                                            st.plotly_chart(fig, use_container_width=True)
                                        else:
                                            st.caption("Enable auto-refresh to collect lag trends")
                                    else:
                                        st.caption("Enable auto-refresh to collect lag trends")
                                except Exception as e:
                                    st.caption(f"Error loading trend: {e}")

                        # Divider between stages
                        st.divider()
        else:
            st.info(
                "No per-action metrics available yet. Actions need to complete at least one execution."
            )

    else:
        st.info(
            "No Actions metrics available yet. Start some DataHub Actions to see metrics."
        )

    st.divider()

    # === Service Health Metrics ===
    st.header("📊 Service Health")

    # HTTP metrics - use the count metric from duration histogram
    http_requests = metrics_by_name.get(HTTP_SERVER_DURATION_COUNT, [])
    if http_requests:
        total_requests = sum(s.value for s in http_requests)
        st.metric("Total HTTP Requests", f"{int(total_requests):,}")

        # Group by status code (http_status_code label)
        status_codes: dict[str, int] = defaultdict(int)
        for sample in http_requests:
            status = sample.labels.get(LABEL_HTTP_STATUS_CODE, "unknown")
            status_codes[status] += int(sample.value)

        col1, col2, col3 = st.columns(3)
        with col1:
            success_requests = sum(
                count
                for status, count in status_codes.items()
                if status.startswith("2")
            )
            st.metric("2xx Success", f"{success_requests:,}", delta=None)
        with col2:
            client_errors = sum(
                count
                for status, count in status_codes.items()
                if status.startswith("4")
            )
            st.metric("4xx Client Errors", f"{client_errors:,}", delta=None)
        with col3:
            server_errors = sum(
                count
                for status, count in status_codes.items()
                if status.startswith("5")
            )
            st.metric("5xx Server Errors", f"{server_errors:,}", delta=None)

    # Process metrics - using OpenTelemetry metric names
    col1, col2 = st.columns(2)

    with col1:
        cpu_samples = metrics_by_name.get(PROCESS_CPU_UTILIZATION, [])
        if cpu_samples:
            cpu_usage = (
                cpu_samples[0].value * 100
            )  # Already a ratio, convert to percentage
            st.metric("CPU Usage", f"{cpu_usage:.1f}%")

    with col2:
        memory_samples = metrics_by_name.get(PROCESS_MEMORY_USAGE, [])
        if memory_samples:
            memory_mb = memory_samples[0].value / (1024 * 1024)
            st.metric("Memory Usage", f"{memory_mb:.1f} MB")

    # Auto-refresh
    if auto_refresh:
        import time

        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
