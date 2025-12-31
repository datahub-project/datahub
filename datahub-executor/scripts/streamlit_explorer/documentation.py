# ruff: noqa: INP001
"""
Documentation page for the Observe Control Panel.

Provides documentation of the core data models used for assertions,
monitors, metric cubes, and anomaly detection.
"""

import streamlit as st


def render_documentation_page() -> None:
    """Render the documentation page with data model descriptions."""

    st.title("📖 Documentation")
    st.markdown(
        """
        This page describes the core data models used in the Observe platform
        for tracking metrics and detecting anomalies.
        """
    )

    # ==========================================================================
    # Core Entities
    # ==========================================================================
    st.header("Core Entities")

    # Dataset
    with st.expander("📁 Dataset", expanded=True):
        st.markdown(
            """
            **Dataset** represents a data asset being monitored (table, view, etc.).

            | Field | Type | Description |
            |-------|------|-------------|
            | `urn` | string | Unique identifier (e.g., `urn:li:dataset:(platform,name,env)`) |
            | `name` | string | Human-readable name of the dataset |
            | `platform` | string | Data platform (e.g., snowflake, bigquery, databricks) |

            **Relationships:**
            - A dataset can have **many assertions** that validate its data quality
            """
        )

    # Assertion
    with st.expander("✓ Assertion", expanded=True):
        st.markdown(
            """
            **Assertion** is a data quality rule applied to a dataset.

            | Field | Type | Description |
            |-------|------|-------------|
            | `urn` | string | Unique identifier (e.g., `urn:li:assertion:uuid`) |
            | `type` | string | Assertion type: `VOLUME`, `FRESHNESS`, `FIELD`, `SQL`, etc. |
            | `asserteeUrn` | string | URN of the dataset being validated |
            | `info` | object | Type-specific configuration (thresholds, operators, etc.) |

            **Assertion Types:**
            - **VOLUME**: Validates row counts and data size
            - **FRESHNESS**: Validates data recency and update frequency
            - **FIELD**: Validates column-level metrics (null count, uniqueness, etc.)
            - **SQL**: Custom SQL-based validation rules

            **Relationships:**
            - An assertion belongs to **one dataset** (via `asserteeUrn`)
            - An assertion may be evaluated by **one monitor** (optional)

            **Timeseries Aspects:**
            - `assertionRunEvent`: Records each evaluation result with metrics
            """
        )

    # Monitor
    with st.expander("📊 Monitor", expanded=True):
        st.markdown(
            """
            **Monitor** schedules and evaluates assertions, producing metric cube data.

            | Field | Type | Description |
            |-------|------|-------------|
            | `urn` | string | Unique identifier (e.g., `urn:li:monitor:(entityUrn,id)`) |
            | `entityUrn` | string | URN of the dataset being monitored |
            | `status` | object | Current mode (`ACTIVE`, `PAUSED`) |
            | `schedule` | object | Evaluation schedule (cron expression, timezone) |
            | `assertionMonitor` | object | Links to the assertion being evaluated |

            **Key Properties:**
            - Contains scheduling configuration (cron, timezone)
            - Tracks evaluation status and history
            - Produces metric cube data for timeseries analysis

            **Relationships:**
            - A monitor evaluates **one assertion**
            - A monitor produces **one metric cube** (URN derived from monitor URN)
            - A monitor can detect **many anomaly events**

            **Timeseries Aspects:**
            - `monitorAnomalyEvent`: Records detected anomalies with state
            - `monitorTimeseriesState`: Stores model state for anomaly detection
            """
        )

    # Metric Cube
    with st.expander("📈 Metric Cube (dataHubMetricCube)", expanded=True):
        st.markdown(
            """
            **Metric Cube** stores timeseries metric data produced by monitors.

            | Field | Type | Description |
            |-------|------|-------------|
            | `urn` | string | Derived from monitor URN via Base64 encoding |
            | `name` | string | Optional display name |
            | `type` | string | Metric cube type |
            | `entity` | string | URN of the associated entity |

            **URN Derivation:**
            ```python
            import base64
            metric_cube_urn = f"urn:li:dataHubMetricCube:{base64.urlsafe_b64encode(monitor_urn.encode()).decode()}"
            ```

            **Why Metric Cubes?**
            - Cleaner data model than extracting metrics from `assertionRunEvent`
            - Measure values are directly available (no parsing `nativeResults`)
            - Pre-joined with anomaly events via `listMonitorMetrics` GraphQL query
            - Better suited for timeseries analysis and forecasting

            **Relationships:**
            - One metric cube per monitor (1:1 relationship)
            - Contains **many metric cube events** (timeseries data points)
            """
        )

    # Metric Cube Event
    with st.expander("📉 Metric Cube Event (dataHubMetricCubeEvent)", expanded=True):
        st.markdown(
            """
            **Metric Cube Event** is a timeseries aspect storing individual metric values.

            | Field | Type | Description |
            |-------|------|-------------|
            | `timestampMillis` | long | Event timestamp in milliseconds |
            | `reportedTimeMillis` | long | Time the metric was reported |
            | `measure` | double | The metric value |
            | `dim1`, `dim2`, `dim3` | array | Optional dimension arrays for grouping |
            | `origin` | object | Source information (entity URN, event timestamp) |

            **Data Source Hierarchy:**
            The application prefers metric cube events when available:
            1. **Preferred**: `dataHubMetricCubeEvent` - clean measure values
            2. **Fallback**: `assertionRunEvent` - extract from `nativeResults`

            **Usage:**
            - Primary source for timeseries visualization
            - Used for anomaly detection model training
            - Cached locally in parquet format for fast access
            """
        )

    # Anomaly Event
    with st.expander("⚠️ Anomaly Event (monitorAnomalyEvent)", expanded=True):
        st.markdown(
            """
            **Anomaly Event** records a detected deviation from expected behavior.

            | Field | Type | Description |
            |-------|------|-------------|
            | `timestampMillis` | long | When the anomaly was detected |
            | `state` | string | `PENDING`, `CONFIRMED`, `REJECTED` |
            | `source` | object | What triggered the anomaly detection |

            **Source Properties:**
            | Field | Type | Description |
            |-------|------|-------------|
            | `source.type` | string | Detection source type |
            | `source.sourceUrn` | string | URN of the source entity (assertion) |
            | `source.sourceEventTimestampMillis` | long | Original event timestamp |
            | `source.properties.assertionMetric` | object | Metric value that triggered detection |

            **Anomaly States:**
            - **PENDING**: Detected but not yet reviewed
            - **CONFIRMED**: User confirmed as a true anomaly
            - **REJECTED**: User rejected as a false positive

            **Usage:**
            - Linked to metric cube events via timestamp
            - User can review and update state in the UI
            - Confirmed anomalies can be excluded from model training
            """
        )

    # ==========================================================================
    # Entity Relationships
    # ==========================================================================
    st.header("Entity Relationships")

    st.markdown(
        """
        | From | To | Relationship | Description |
        |------|-----|--------------|-------------|
        | Dataset | Assertion | 1:many | A dataset can have multiple assertions |
        | Assertion | Monitor | 1:0..1 | An assertion may have one monitor (optional) |
        | Monitor | Metric Cube | 1:1 | Each monitor produces exactly one metric cube |
        | Monitor | Anomaly Events | 1:many | A monitor can detect multiple anomalies |
        | Metric Cube | Events | 1:many | A cube contains many timeseries events |
        """
    )

    # ==========================================================================
    # Glossary
    # ==========================================================================
    st.header("Glossary")

    st.markdown(
        """
        | Term | Definition |
        |------|------------|
        | **URN** | Uniform Resource Name - unique identifier for entities |
        | **Aspect** | A facet of metadata attached to an entity |
        | **Timeseries Aspect** | An aspect that stores time-indexed data points |
        | **Bootstrap** | Backfilling metric cube data from historical run events |
        | **Assertee** | The entity (usually dataset) that an assertion validates |
        """
    )


# Export for page registration
__all__ = ["render_documentation_page"]
