# ruff: noqa: INP001

import json
import math
import random

# Import Streamlit pages and shared config
# Handle both direct execution (streamlit run) and module import
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import datahub.metadata.schema_classes as models
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit_ext as ste
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.urns import DatasetUrn
from plotly.subplots import make_subplots
from pydantic import TypeAdapter
from pydantic.json import pydantic_encoder

from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    default_volume_assertion_urn,
    default_volume_monitor_urn,
    make_monitor_metric_cube_urn,
)
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_GET_MONITOR_OPERATION,
    GRAPHQL_LIST_MONITORS_OPERATION,
    GRAPHQL_LIST_MONITORS_QUERY,
)
from datahub_executor.common.client.fetcher.monitors.mapper import (
    SkippableMonitorMappingError,
    graphql_to_monitor,
    graphql_to_monitors,
)
from datahub_executor.common.constants import LIST_MONITORS_BATCH_SIZE
from datahub_executor.common.helpers import paginate_datahub_query_results
from datahub_executor.common.metric.client.client import (
    MetricClient,
)
from datahub_executor.common.metric.types import (
    Metric,
)
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.base_assertion_trainer import (
    BaseAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.utils import (
    get_metric_ceiling_value,
    get_metric_floor_value,
    is_metric_anomaly,
)
from datahub_executor.common.types import (
    AssertionAdjustmentSettings,
    AssertionExclusionWindow,
    AssertionMonitorSensitivity,
    Monitor,
)
from datahub_executor.config import VOLUME_DEFAULT_SENSITIVITY_LEVEL

# Add parent directory to path for direct script execution
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from shared_config import (  # noqa: E402
    GRAPH_CLIENT_KEY,
    METRICS_CLIENT_KEY,
    MONITOR_CLIENT_KEY,
    get_configured_graph,
    render_connection_settings_page,
    render_connection_status,
    set_connection_settings_page,
)
from streamlit_explorer import EXPLORER_PAGES  # noqa: E402

_SELECTED_MONITOR_KEY = "selected_monitor"


def _get_graph() -> Optional[DataHubGraph]:
    """
    Get the DataHub graph client using the centralized shared config.

    The client is cached in session state and automatically cleared when
    connection settings change. All pages (Create Assertion, All Monitors,
    Monitor Details, etc.) share the same client instance.
    """
    graph = get_configured_graph()

    # Fall back to default graph if not configured
    if graph is None:
        try:
            graph = get_default_graph()
            # Cache the fallback graph too
            if graph is not None:
                graph.execute_graphql = st.cache_data(graph.execute_graphql)  # type: ignore
                st.session_state[GRAPH_CLIENT_KEY] = graph
        except Exception:
            return None

    return graph


def _get_metrics_client() -> Optional[MetricClient]:
    """Get the metrics client, creating if needed."""
    if METRICS_CLIENT_KEY in st.session_state:
        return st.session_state[METRICS_CLIENT_KEY]

    graph = _get_graph()
    if graph is None:
        return None

    client = MetricClient(graph)
    st.session_state[METRICS_CLIENT_KEY] = client
    return client


def _get_monitor_client() -> Optional[MonitorClient]:
    """Get the monitor client, creating if needed."""
    if MONITOR_CLIENT_KEY in st.session_state:
        return st.session_state[MONITOR_CLIENT_KEY]

    graph = _get_graph()
    if graph is None:
        return None

    client = MonitorClient(graph)
    st.session_state[MONITOR_CLIENT_KEY] = client
    return client


# =============================================================================
# Random Dataset Generation
# =============================================================================

# Fake data for generating realistic-looking datasets
_PLATFORMS = ["snowflake", "bigquery", "redshift", "postgres", "mysql", "databricks"]
_DATABASES = [
    "analytics",
    "warehouse",
    "reporting",
    "raw_data",
    "staging",
    "production",
]
_SCHEMAS = ["public", "dbt", "core", "marts", "staging", "raw", "analytics"]
_TABLES = [
    "users",
    "orders",
    "transactions",
    "events",
    "sessions",
    "products",
    "customers",
    "inventory",
    "payments",
    "logs",
    "accounts",
    "subscriptions",
    "invoices",
    "shipments",
    "reviews",
]
# Valid FabricType enum values in DataHub
_ENVS = ["PROD", "DEV", "STG", "CORP", "TEST"]

# Column templates for different table types
_COLUMN_TEMPLATES = {
    "users": [
        ("user_id", "BIGINT", "Unique user identifier"),
        ("email", "VARCHAR(255)", "User email address"),
        ("username", "VARCHAR(100)", "User login name"),
        ("first_name", "VARCHAR(50)", "User first name"),
        ("last_name", "VARCHAR(50)", "User last name"),
        ("created_at", "TIMESTAMP", "Account creation timestamp"),
        ("updated_at", "TIMESTAMP", "Last update timestamp"),
        ("is_active", "BOOLEAN", "Whether user is active"),
        ("country_code", "VARCHAR(2)", "Two-letter country code"),
    ],
    "orders": [
        ("order_id", "BIGINT", "Unique order identifier"),
        ("user_id", "BIGINT", "Foreign key to users"),
        ("order_date", "DATE", "Date order was placed"),
        ("total_amount", "DECIMAL(10,2)", "Total order amount"),
        ("currency", "VARCHAR(3)", "Three-letter currency code"),
        ("status", "VARCHAR(20)", "Order status"),
        ("shipping_address_id", "BIGINT", "Foreign key to addresses"),
        ("created_at", "TIMESTAMP", "Record creation timestamp"),
    ],
    "transactions": [
        ("transaction_id", "BIGINT", "Unique transaction identifier"),
        ("user_id", "BIGINT", "Foreign key to users"),
        ("amount", "DECIMAL(12,2)", "Transaction amount"),
        ("currency", "VARCHAR(3)", "Currency code"),
        ("transaction_type", "VARCHAR(20)", "Type of transaction"),
        ("status", "VARCHAR(20)", "Transaction status"),
        ("processed_at", "TIMESTAMP", "When transaction was processed"),
        ("reference_id", "VARCHAR(100)", "External reference"),
    ],
    "events": [
        ("event_id", "BIGINT", "Unique event identifier"),
        ("event_type", "VARCHAR(50)", "Type of event"),
        ("user_id", "BIGINT", "User who triggered event"),
        ("session_id", "VARCHAR(100)", "Session identifier"),
        ("timestamp", "TIMESTAMP", "Event timestamp"),
        ("properties", "JSON", "Event properties"),
        ("page_url", "VARCHAR(500)", "Page URL where event occurred"),
    ],
    "products": [
        ("product_id", "BIGINT", "Unique product identifier"),
        ("sku", "VARCHAR(50)", "Stock keeping unit"),
        ("name", "VARCHAR(200)", "Product name"),
        ("description", "TEXT", "Product description"),
        ("price", "DECIMAL(10,2)", "Product price"),
        ("category_id", "BIGINT", "Foreign key to categories"),
        ("inventory_count", "INTEGER", "Current inventory"),
        ("is_active", "BOOLEAN", "Whether product is available"),
    ],
    "default": [
        ("id", "BIGINT", "Primary key"),
        ("name", "VARCHAR(100)", "Name"),
        ("description", "TEXT", "Description"),
        ("created_at", "TIMESTAMP", "Creation timestamp"),
        ("updated_at", "TIMESTAMP", "Last update timestamp"),
        ("status", "VARCHAR(20)", "Status"),
        ("metadata", "JSON", "Additional metadata"),
    ],
}

# Lorem ipsum style descriptions for datasets
_DATASET_DESCRIPTIONS = [
    "Core business entity table containing essential operational data.",
    "Analytics-ready dataset optimized for reporting and dashboards.",
    "Staging table for data pipeline processing and transformation.",
    "Historical record of all transactions for audit and compliance.",
    "Real-time event stream data for behavioral analytics.",
    "Master data table synchronized from source systems daily.",
    "Aggregated metrics table for executive reporting.",
    "Raw ingestion table from external data sources.",
]


def _generate_random_dataset_urn() -> str:
    """Generate a random dataset URN without creating it in DataHub."""
    platform = random.choice(_PLATFORMS)
    db = random.choice(_DATABASES)
    schema = random.choice(_SCHEMAS)
    table = random.choice(_TABLES)
    env = random.choice(_ENVS)

    # Add random suffix to make unique
    suffix = random.randint(1000, 9999)
    qualified_name = f"{db}.{schema}.{table}_{suffix}"

    # Dataset URN format: urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{qualified_name},{env})"


def _create_random_dataset_in_datahub() -> str:
    """Create a random dataset with schema and properties in DataHub.

    Returns:
        The dataset URN.
    """
    graph = _get_graph()
    if graph is None:
        raise ValueError("No DataHub connection configured")

    # Generate random dataset details
    platform = random.choice(_PLATFORMS)
    db = random.choice(_DATABASES)
    schema = random.choice(_SCHEMAS)
    table_base = random.choice(_TABLES)
    env = random.choice(_ENVS)

    suffix = random.randint(1000, 9999)
    table_name = f"{table_base}_{suffix}"
    qualified_name = f"{db}.{schema}.{table_name}"

    # Dataset URN format: urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})
    dataset_urn = (
        f"urn:li:dataset:(urn:li:dataPlatform:{platform},{qualified_name},{env})"
    )

    # Get column template based on table type
    columns = _COLUMN_TEMPLATES.get(table_base, _COLUMN_TEMPLATES["default"])

    # Build schema fields
    schema_fields = []
    for col_name, col_type, col_desc in columns:
        # Map SQL types to DataHub schema types
        field_type = _sql_type_to_schema_field_type(col_type)
        schema_fields.append(
            models.SchemaFieldClass(
                fieldPath=col_name,
                type=field_type,
                nativeDataType=col_type,
                description=col_desc,
                nullable=col_name
                not in (
                    "id",
                    "user_id",
                    "order_id",
                    "event_id",
                    "product_id",
                    "transaction_id",
                ),
            )
        )

    # Create aspects
    now_ms = int(time.time() * 1000)

    dataset_properties = models.DatasetPropertiesClass(
        name=table_name,
        qualifiedName=qualified_name,
        description=random.choice(_DATASET_DESCRIPTIONS),
        created=models.TimeStampClass(time=now_ms),
        lastModified=models.TimeStampClass(time=now_ms),
        customProperties={
            "source": "sample_data_generator",
            "environment": env.lower(),
            "database": db,
            "schema": schema,
        },
    )

    schema_metadata = models.SchemaMetadataClass(
        schemaName=qualified_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=models.MySqlDDLClass(tableSchema=""),
        fields=schema_fields,
    )

    # Add global tags to identify datasets created by this tool
    global_tags = models.GlobalTagsClass(
        tags=[models.TagAssociationClass(tag="urn:li:tag:datahub:observe_streamlit")]
    )

    # Status aspect makes the entity visible in DataHub search
    status = models.StatusClass(removed=False)

    # Emit the MCPs
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=status,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=global_tags,
        ),
    ]

    graph.emit_mcps(mcps)

    return dataset_urn


def _sql_type_to_schema_field_type(sql_type: str) -> models.SchemaFieldDataTypeClass:
    """Convert SQL type string to DataHub SchemaFieldDataType."""
    sql_type_upper = sql_type.upper()

    if "BIGINT" in sql_type_upper or "INT" in sql_type_upper:
        return models.SchemaFieldDataTypeClass(type=models.NumberTypeClass())
    elif (
        "DECIMAL" in sql_type_upper
        or "NUMERIC" in sql_type_upper
        or "FLOAT" in sql_type_upper
        or "DOUBLE" in sql_type_upper
    ):
        return models.SchemaFieldDataTypeClass(type=models.NumberTypeClass())
    elif (
        "VARCHAR" in sql_type_upper
        or "CHAR" in sql_type_upper
        or "TEXT" in sql_type_upper
    ):
        return models.SchemaFieldDataTypeClass(type=models.StringTypeClass())
    elif "TIMESTAMP" in sql_type_upper or "DATETIME" in sql_type_upper:
        return models.SchemaFieldDataTypeClass(type=models.TimeTypeClass())
    elif "DATE" in sql_type_upper:
        return models.SchemaFieldDataTypeClass(type=models.DateTypeClass())
    elif "BOOLEAN" in sql_type_upper or "BOOL" in sql_type_upper:
        return models.SchemaFieldDataTypeClass(type=models.BooleanTypeClass())
    elif "JSON" in sql_type_upper:
        return models.SchemaFieldDataTypeClass(type=models.RecordTypeClass())
    else:
        return models.SchemaFieldDataTypeClass(type=models.StringTypeClass())


def generate_sample_metrics(
    num_points: int = 60,
    trend_factor: float = 1.0,
    seasonality: float = 20.0,
    noise_level: float = 5.0,
    base_value: float = 100.0,
    monotonic: bool = False,
    daily_increment_range: tuple[float, float] = (100.0, 500.0),
) -> List[Metric]:
    """
    Generate synthetic time series data with trend, seasonality, and noise.

    Args:
        num_points: Number of data points to generate
        trend_factor: Factor for upward trend
        seasonality: Amplitude of weekly seasonality
        noise_level: Standard deviation of random noise
        base_value: Starting base value
        monotonic: If True, generate strictly increasing values (append-only)
        daily_increment_range: (min, max) daily increment for monotonic mode

    Returns:
        List of Metric objects
    """
    metrics = []
    now = int(time.time() * 1000)
    current_value = base_value

    # Generate data points going back in time
    for i in range(num_points, 0, -1):
        # Generate timestamp (going backward from now)
        timestamp_ms = now - (i * 24 * 60 * 60 * 1000)  # daily data

        if monotonic:
            # Monotonically increasing: each day adds rows (append-only table)
            # Use consistent increments based on day-of-week for realistic patterns
            day_of_week = (num_points - i) % 7
            # Weekends have lower activity
            if day_of_week in (5, 6):  # Saturday, Sunday
                increment_factor = 0.3
            elif day_of_week == 0:  # Monday (catch-up)
                increment_factor = 1.5
            else:
                increment_factor = 1.0

            min_inc, max_inc = daily_increment_range
            base_increment = random.uniform(min_inc, max_inc)
            increment = base_increment * increment_factor

            # Add small random variation
            increment *= random.uniform(0.9, 1.1)

            current_value += increment
            value = int(current_value)
        else:
            # Original non-monotonic behavior with trend, seasonality, and noise
            trend = trend_factor * (num_points - i)
            season = seasonality * (0.5 + 0.5 * (1 + (i % 7) / 7))  # weekly seasonality
            noise = random.normalvariate(0, noise_level)

            value = base_value + trend + season + noise
            value = max(0, value)  # Ensure non-negative values

        metrics.append(Metric(timestamp_ms=timestamp_ms, value=value))

    return metrics


def create_volume_smart_assertion(dataset_urn: str) -> tuple[str, str]:
    """Creates a monitor and assertion for a dataset. Returns the URNs."""
    graph = _get_graph()
    if graph is None:
        raise ValueError("No DataHub connection configured")

    monitor_urn = default_volume_monitor_urn(dataset_urn)
    assertion_urn = default_volume_assertion_urn(dataset_urn)

    if graph.exists(monitor_urn):
        raise ValueError(f"Monitor {monitor_urn} already exists")

    if graph.exists(assertion_urn):
        raise ValueError(f"Assertion {assertion_urn} already exists")

    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=monitor_urn,
            aspect=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                assertionMonitor=models.AssertionMonitorClass(
                    assertions=[
                        models.AssertionEvaluationSpecClass(
                            assertion=assertion_urn,
                            parameters=models.AssertionEvaluationParametersClass(
                                type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
                                datasetVolumeParameters=models.DatasetVolumeAssertionParametersClass(
                                    sourceType=models.DatasetVolumeSourceTypeClass.DATAHUB_DATASET_PROFILE
                                ),
                            ),
                            schedule=models.CronScheduleClass(
                                cron="0 0 * * *",
                                timezone="UTC",
                            ),
                        )
                    ]
                ),
                status=models.MonitorStatusClass(mode=models.MonitorModeClass.INACTIVE),
            ),
        ),
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=models.AssertionInfoClass(
                source=models.AssertionSourceClass(
                    type=models.AssertionSourceTypeClass.INFERRED
                ),
                type=models.AssertionTypeClass.VOLUME,
                volumeAssertion=models.VolumeAssertionInfoClass(
                    type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                    entity=dataset_urn,
                ),
            ),
        ),
    ]

    graph.emit_mcps(mcps)
    return assertion_urn, monitor_urn


def generate_sample_run_events(
    assertion_urn: str,
    dataset_urn: str,
    num_events: int = 30,
    base_value: float = 1000.0,
    monotonic: bool = True,
    daily_increment_range: tuple[float, float] = (100.0, 500.0),
    inject_anomaly: bool = False,
    anomaly_position: float = 0.7,  # Position in time series (0.0-1.0)
    anomaly_magnitude: float = 3.0,  # Multiplier for anomaly spike
) -> list:
    """Generate sample assertion run events with realistic patterns.

    Args:
        assertion_urn: The assertion URN
        dataset_urn: The dataset URN (assertee)
        num_events: Number of events to generate
        base_value: Base metric value (row count)
        monotonic: If True, generate strictly increasing values (append-only)
        daily_increment_range: (min, max) daily increment for monotonic mode
        inject_anomaly: If True, inject an anomaly spike
        anomaly_position: Where to inject anomaly (0.0-1.0)
        anomaly_magnitude: How much larger the anomaly increment should be

    Returns:
        List of MetadataChangeProposalWrapper objects
    """
    mcps = []
    now = int(time.time() * 1000)
    prev_value = None
    current_value = base_value
    anomaly_index = int(num_events * (1 - anomaly_position)) if inject_anomaly else -1

    for i in range(num_events, 0, -1):
        # Generate timestamp (going backward from now, daily)
        timestamp_ms = now - (i * 24 * 60 * 60 * 1000)
        event_index = num_events - i

        if monotonic:
            # Monotonically increasing: each day adds rows (append-only table)
            day_of_week = event_index % 7

            # Weekends have lower activity
            if day_of_week in (5, 6):  # Saturday, Sunday
                increment_factor = 0.3
            elif day_of_week == 0:  # Monday (catch-up)
                increment_factor = 1.5
            else:
                increment_factor = 1.0

            min_inc, max_inc = daily_increment_range
            base_increment = random.uniform(min_inc, max_inc)
            increment = base_increment * increment_factor

            # Inject anomaly if requested
            if inject_anomaly and event_index == anomaly_index:
                increment *= anomaly_magnitude

            # Add small random variation
            increment *= random.uniform(0.95, 1.05)

            current_value += increment
            value = int(current_value)
        else:
            # Non-monotonic behavior with trend and noise
            trend = 10 * event_index  # slight upward trend
            noise = random.normalvariate(0, base_value * 0.1)
            value = base_value + trend + noise
            value = max(0, int(value))  # Row counts are integers

        # For monotonic data, success/failure is based on whether growth is reasonable
        # For non-monotonic, use random failures
        if monotonic:
            # In real systems, assertions pass when growth is within expected bounds
            is_success = True
            # Mark anomaly as failure if present
            if inject_anomaly and event_index == anomaly_index:
                is_success = False
        else:
            # Occasionally generate failures (about 10%)
            is_success = random.random() > 0.1

        result_type = (
            models.AssertionResultTypeClass.SUCCESS
            if is_success
            else models.AssertionResultTypeClass.FAILURE
        )

        run_id = f"sample-{assertion_urn}-{timestamp_ms}"

        # Build nativeResults like the real executor does for volume assertions
        native_results = {
            "Metric Value": str(value),  # For display in Time Series Explorer
        }
        if prev_value is not None:
            native_results["Previous Row Count"] = str(prev_value)
            if monotonic:
                native_results["Row Increment"] = str(value - prev_value)

        run_event = models.AssertionRunEventClass(
            timestampMillis=timestamp_ms,
            runId=run_id,
            asserteeUrn=dataset_urn,
            status=models.AssertionRunStatusClass.COMPLETE,
            assertionUrn=assertion_urn,
            result=models.AssertionResultClass(
                type=result_type,
                rowCount=value,  # Row count for volume assertions
                nativeResults=native_results,
                actualAggValue=float(value),
                # Include metric for proper display
                metric=models.AssertionMetricClass(
                    value=float(value),
                    timestampMs=timestamp_ms,
                ),
            ),
        )

        mcpw = MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=run_event,
            systemMetadata=models.SystemMetadataClass(
                runId=run_id, lastObserved=timestamp_ms
            ),
        )
        mcps.append(mcpw)
        prev_value = value

    return mcps


@dataclass
class SampleDataConfig:
    """Configuration for sample data generation."""

    # Basic settings
    num_days: int = 60
    base_row_count: int = 10000

    # Volume assertion behavior
    monotonic: bool = True  # Append-only table (realistic)
    daily_increment_range: tuple[float, float] = (100.0, 500.0)

    # Metric cube settings
    create_metric_cube: bool = True

    # Anomaly injection
    inject_anomaly: bool = False
    anomaly_position: float = 0.7  # 70% through the time series
    anomaly_magnitude: float = 3.0  # 3x normal increment


def generate_volume_sample_data(
    dataset_urn: str,
    config: Optional[SampleDataConfig] = None,
) -> str:
    """Generates sample volume assertion data for a dataset.

    Creates the monitor/assertion if they don't exist, then generates:
    - Sample metrics (for AI assertion evaluation) - optional via config
    - Sample assertion run events (for Time Series Explorer)

    Args:
        dataset_urn: The dataset URN to create sample data for
        config: Configuration for sample data generation

    Returns:
        The monitor URN.
    """
    if config is None:
        config = SampleDataConfig()

    graph = _get_graph()
    if graph is None:
        raise ValueError("No DataHub connection configured")

    metrics_client = _get_metrics_client()
    if metrics_client is None:
        raise ValueError("No DataHub connection configured")

    monitor_urn = default_volume_monitor_urn(dataset_urn)
    assertion_urn = default_volume_assertion_urn(dataset_urn)

    # Always ensure monitor and assertion are properly configured
    # Even if they exist, they might not be properly linked (e.g., created by tests)

    # Create/update the assertion
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=models.AssertionInfoClass(
                source=models.AssertionSourceClass(
                    type=models.AssertionSourceTypeClass.INFERRED
                ),
                type=models.AssertionTypeClass.VOLUME,
                volumeAssertion=models.VolumeAssertionInfoClass(
                    type=models.VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                    entity=dataset_urn,
                ),
            ),
        )
    )

    # Create/update the monitor with correct assertion reference
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=monitor_urn,
            aspect=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                assertionMonitor=models.AssertionMonitorClass(
                    assertions=[
                        models.AssertionEvaluationSpecClass(
                            assertion=assertion_urn,
                            parameters=models.AssertionEvaluationParametersClass(
                                type=models.AssertionEvaluationParametersTypeClass.DATASET_VOLUME,
                                datasetVolumeParameters=models.DatasetVolumeAssertionParametersClass(
                                    sourceType=models.DatasetVolumeSourceTypeClass.DATAHUB_DATASET_PROFILE
                                ),
                            ),
                            schedule=models.CronScheduleClass(
                                cron="0 0 * * *",
                                timezone="UTC",
                            ),
                        )
                    ]
                ),
                status=models.MonitorStatusClass(mode=models.MonitorModeClass.INACTIVE),
            ),
        )
    )

    # Generate sample metrics (for AI evaluation) - optional
    if config.create_metric_cube:
        metrics = generate_sample_metrics(
            num_points=config.num_days,
            base_value=float(config.base_row_count),
            monotonic=config.monotonic,
            daily_increment_range=config.daily_increment_range,
        )
        metric_urn = make_monitor_metric_cube_urn(monitor_urn)

        for metric in metrics:
            metrics_client.save_metric_value(
                metric_urn=metric_urn,
                metric=metric,
            )

    # Generate sample assertion run events (for Time Series Explorer)
    run_event_mcps = generate_sample_run_events(
        assertion_urn=assertion_urn,
        dataset_urn=dataset_urn,
        num_events=config.num_days,
        base_value=float(config.base_row_count),
        monotonic=config.monotonic,
        daily_increment_range=config.daily_increment_range,
        inject_anomaly=config.inject_anomaly,
        anomaly_position=config.anomaly_position,
        anomaly_magnitude=config.anomaly_magnitude,
    )

    for mcpw in run_event_mcps:
        graph.emit_mcp(mcpw)

    return monitor_urn


def render_create_assertion_page() -> None:
    """Render the create assertion page."""
    st.header("Create New Volume Assertion")

    # Show connection status
    if not render_connection_status():
        st.info("Please configure a DataHub connection to create assertions.")
        return

    with st.form("create_assertion"):
        dataset_urn = st.text_input(
            "Dataset URN", help="Format: urn:li:dataset:(platform,name,env)"
        )

        submitted = st.form_submit_button("Create")

        if submitted:
            try:
                # Validate URN format
                DatasetUrn.from_string(dataset_urn)

                # Create assertion and monitor
                assertion_urn, monitor_urn = create_volume_smart_assertion(dataset_urn)

                st.success("Successfully created volume assertion!")
                st.write("**Assertion URN:**", assertion_urn)
                st.write("**Monitor URN:**", monitor_urn)

            except ValueError as e:
                st.error(f"Error: {str(e)}")
            except Exception as e:
                st.error(f"Unexpected error: {str(e)}")


def _get_monitor(monitor_urn: str) -> Optional[Monitor]:
    """Get a monitor from the DataHub GraphQL API."""
    graph = _get_graph()
    if graph is None:
        return None

    graphql_monitor = graph.execute_graphql(
        GRAPHQL_LIST_MONITORS_QUERY,
        operation_name=GRAPHQL_GET_MONITOR_OPERATION,
        variables={
            "urn": monitor_urn,
        },
    )["entity"]
    return graphql_to_monitor(graphql_monitor)


def render_monitor_detail_page() -> None:
    """Render details for a specific monitor."""

    st.header("Monitor Details")

    # Show connection status
    if not render_connection_status():
        st.info("Please configure a DataHub connection to view monitor details.")
        return

    # Initialize session state from query params if available
    if _SELECTED_MONITOR_KEY not in st.session_state:
        # Check query params for monitor URN
        query_monitor = st.query_params.get(_SELECTED_MONITOR_KEY, "")
        st.session_state[_SELECTED_MONITOR_KEY] = query_monitor

    # Use the session state key directly (no value= to avoid conflict)
    monitor_urn = ste.text_input("Monitor URN", key=_SELECTED_MONITOR_KEY)
    if not monitor_urn:
        st.error("Monitor URN not provided")
        return

    try:
        monitor = _get_monitor(monitor_urn)
    except SkippableMonitorMappingError as e:
        st.warning(str(e))
        st.info(
            "This monitor cannot be loaded because it doesn't have exactly 1 assertion. "
            "This can happen if:\n"
            "- The monitor was just created and the assertion hasn't been fully set up\n"
            "- The monitor has multiple assertions (not yet supported)\n"
            "- The assertion was deleted"
        )
        return

    if monitor is None:
        st.error("Failed to fetch monitor. Check your connection settings.")
        return
    st.json(monitor.model_dump_json(), expanded=False)

    # TODO: only render for volume assertions.
    render_volume_assertion_simulation_ui(monitor)


def render_volume_assertion_simulation_ui(monitor: Monitor) -> None:
    """Render the assertion simulation UI."""
    st.header("Assertion Simulation")

    assertion = (
        monitor.assertion_monitor.assertions[0] if monitor.assertion_monitor else None
    )
    dataset = assertion.assertion.entity if assertion else None
    if not dataset:
        st.error("No dataset found for assertion")
        return
    # dataset_urn = dataset.urn

    original_adjustment_settings = (
        monitor.assertion_monitor.settings.inference_settings
        if monitor.assertion_monitor and monitor.assertion_monitor.settings
        else None
    )

    # Settings.

    last_day_cutoff_date = st.date_input(
        "Last day of metrics to include", value=datetime.now(timezone.utc)
    )
    last_day_cutoff = datetime.combine(
        last_day_cutoff_date, datetime.min.time(), tzinfo=timezone.utc
    )

    exclusion_windows_raw = st.text_area(
        "Exclusion Windows",
        value=(
            json.dumps(
                original_adjustment_settings.exclusion_windows, default=pydantic_encoder
            )
            if original_adjustment_settings
            and original_adjustment_settings.exclusion_windows
            else ""
        ),
        help="""\
Example exclusion windows:

{
    "type": "FIXED_RANGE",
    "fixedRange": {
        "startTimeMillis": "2025-02-27T00:00:00+00:00",
        "endTimeMillis": "2025-02-28T00:00:00+00:00"
    }
}

{ "type": "WEEKLY", "weekly": { "days_of_week": ["SATURDAY", "SUNDAY"] } }
""",
    )
    default_lookback = (
        BaseAssertionTrainer.extract_lookback_days_from_adjustment_settings(
            original_adjustment_settings
        )
    )
    lookback = st.number_input(
        "Lookback", value=default_lookback, min_value=1, max_value=365, step=1
    )

    sensitivity = st.slider(
        "Sensitivity",
        min_value=1,
        max_value=10,
        value=VOLUME_DEFAULT_SENSITIVITY_LEVEL,
        step=1,
    )

    user_config_raw = st.text_area("User Config", value="{}")

    prediction_unit_options = {
        "Hourly": timedelta(hours=1),
        "Daily": timedelta(days=1),
    }
    selected_unit = st.select_slider(
        "Prediction Boundary Unit",
        options=list(prediction_unit_options.keys()),
        value="Daily",
    )
    unit = prediction_unit_options[selected_unit]
    multiple = math.ceil(timedelta(days=14) / unit)

    # Simulation logic.
    metrics_client = _get_metrics_client()
    monitor_client = _get_monitor_client()
    if metrics_client is None or monitor_client is None:
        st.error("Failed to initialize clients. Check your connection settings.")
        return

    metric_urn = make_monitor_metric_cube_urn(monitor.urn)
    st.write(f"Fetching all metrics for metric_urn {metric_urn}")
    all_metrics = metrics_client.fetch_metric_values(
        metric_urn,
        lookback=timedelta(days=lookback + 5),
        limit=2000,
    )
    st.json(
        {"all_metrics": {m.timestamp().isoformat(): m.value for m in all_metrics}},
        expanded=False,
    )

    exclusion_windows: List[AssertionExclusionWindow] = TypeAdapter(
        List[AssertionExclusionWindow]  # type: ignore
    ).validate_python(json.loads(exclusion_windows_raw or "[]"))
    user_config = json.loads(user_config_raw)

    adjustment_settings = AssertionAdjustmentSettings(
        exclusion_windows=exclusion_windows,
        training_data_lookback_window_days=lookback,
        sensitivity=AssertionMonitorSensitivity(level=sensitivity),
    )

    historical_metrics = metrics_client.fetch_metric_values(
        metric_urn,
        lookback=timedelta(days=lookback),
        limit=2000,
    )

    # Fetch anomalies
    anomalies = monitor_client.fetch_monitor_anomalies(
        urn=monitor.urn,
        lookback=timedelta(days=lookback),
        limit=2000,
    )
    st.write(f"Fetched {len(anomalies)} anomalies")

    # Filter out anomalies with no metric data for display
    anomalies_with_metrics = [a for a in anomalies if a.metric is not None]
    st.json(
        {
            "anomalies": {
                a.metric.timestamp().isoformat(): a.metric.value
                for a in anomalies_with_metrics
            },
            "anomalies_without_metrics": len(anomalies) - len(anomalies_with_metrics),
        },
        expanded=False,
    )

    # Filter out anomalies to avoid using in training
    metrics_without_anomalies = [
        metric
        for metric in historical_metrics
        if not is_metric_anomaly(metric, anomalies)
    ]

    st.write(
        f"Fetched {len(metrics_without_anomalies)} after filtering out anomalies (vs {len(historical_metrics)} prior to filtering)."
    )

    training_metrics = BaseAssertionTrainer.filter_training_timeseries(
        metrics_without_anomalies, adjustment_settings
    )
    st.write(
        f"Fetched {len(training_metrics)} training samples after applying exclusions (vs {len(metrics_without_anomalies)} prior to exclusion)."
    )
    st.json(
        {
            "training_metrics": {
                m.timestamp().isoformat(): m.value for m in training_metrics
            }
        },
        expanded=False,
    )

    if not training_metrics:
        st.info("No training data found")
        return

    metric_predictor = MetricPredictor(user_config=user_config)

    metric_type = (
        assertion.assertion.field_assertion.field_metric_assertion.metric
        if assertion.assertion.field_assertion
        else None
    )
    metric_floor_value = None
    metric_ceiling_value = None
    if metric_type:
        metric_floor_value = get_metric_floor_value(metric_type.name)
        metric_ceiling_value = get_metric_ceiling_value(metric_type.name)
    st.write(
        f"Predicting metric boundaries for {metric_type} with floor value {metric_floor_value} and ceiling value {metric_ceiling_value}"
    )
    metric_boundaries = metric_predictor.predict_metric_boundaries(
        training_metrics,
        unit=unit,
        multiple=multiple,
        sensitivity_level=sensitivity,
        floor_value=metric_floor_value,
        ceiling_value=metric_ceiling_value,
    )

    fig = plot_metrics_and_predictions(
        training_metrics=training_metrics,
        all_metrics=all_metrics,
        prediction_start_time=last_day_cutoff,
        metric_boundaries=metric_boundaries,
    )
    st.plotly_chart(fig)


def plot_metrics_and_predictions(
    training_metrics: List[Metric],
    metric_boundaries: List[MetricBoundary],
    prediction_start_time: datetime,
    all_metrics: List[Metric] | None = None,
) -> go.Figure:
    """
    Plot the original metrics and the predicted boundaries using Plotly.
    """

    training_timestamps = [m.timestamp_ms for m in training_metrics]
    non_training_metrics = [
        m for m in (all_metrics or []) if m.timestamp_ms not in training_timestamps
    ]

    # Create a DataFrame for easier plotting
    training_df = pd.DataFrame(
        {
            "timestamp": [m.timestamp() for m in training_metrics],
            "value": [m.value for m in training_metrics],
        }
    )
    non_training_df = pd.DataFrame(
        {
            "timestamp": [m.timestamp() for m in non_training_metrics],
            "value": [m.value for m in non_training_metrics],
        }
    )

    # Initialize the figure using make_subplots
    fig = make_subplots()

    # Add historical data
    fig.add_trace(
        go.Scatter(
            x=training_df["timestamp"],
            y=training_df["value"],
            mode="lines+markers",
            name="Training Data",
            line=dict(color="#1f77b4", width=2),
            opacity=0.7,
        )
    )

    # Add future boundaries as filled regions and lines
    lower_bound_x = []
    lower_bound_y = []
    upper_bound_x = []
    upper_bound_y = []

    for i, boundary in enumerate(metric_boundaries):
        start_time = datetime.fromtimestamp(boundary.start_time_ms / 1000)
        end_time = datetime.fromtimestamp(boundary.end_time_ms / 1000)
        lower = boundary.lower_bound.value
        upper = boundary.upper_bound.value

        # Add to arrays for connecting lines
        lower_bound_x.extend([start_time, end_time])
        lower_bound_y.extend([lower, lower])
        upper_bound_x.extend([start_time, end_time])
        upper_bound_y.extend([upper, upper])

        # Add filled area
        fig.add_trace(
            go.Scatter(
                x=[start_time, end_time],
                y=[upper, upper],
                mode="lines",
                line=dict(width=0),
                showlegend=False if i > 0 else False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[start_time, end_time],
                y=[lower, lower],
                mode="lines",
                line=dict(width=0),
                fill="tonexty",
                fillcolor="rgba(44, 160, 44, 0.2)",
                name="Future Prediction Range" if i == 0 else None,
                showlegend=i == 0,
            )
        )

    # Add connecting lines for bounds
    fig.add_trace(
        go.Scatter(
            x=lower_bound_x,
            y=lower_bound_y,
            mode="lines",
            line=dict(color="green", width=1, dash=None),
            name="Lower Bound",
            opacity=0.7,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=upper_bound_x,
            y=upper_bound_y,
            mode="lines",
            line=dict(color="green", width=1, dash=None),
            name="Upper Bound",
            opacity=0.7,
        )
    )

    # Add non-training data as orange dots.
    if not non_training_df.empty:
        fig.add_trace(
            go.Scatter(
                x=non_training_df["timestamp"],
                y=non_training_df["value"],
                mode="markers",
                name="Non-Training Data",
                marker=dict(color="#ff7f0e", size=4),
                opacity=0.7,
            )
        )

    # Add vertical line for "now"
    fig.add_shape(
        type="line",
        x0=prediction_start_time,
        x1=prediction_start_time,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(color="black", width=1, dash="dash"),
        opacity=0.5,
    )

    # Update layout
    fig.update_layout(
        title="Metric Prediction Simulation",
        xaxis_title="Time",
        yaxis_title="Value",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
        ),
    )

    return fig


def _list_monitors(paginate: bool = False) -> Optional[List[Monitor]]:
    """List the monitors using the DataHub GraphQL API."""
    graph = _get_graph()
    if graph is None:
        return None

    params: dict = {
        "input": {
            "types": ["MONITOR"],
            "query": "*",
            "searchFlags": {"skipCache": True},
        }
    }

    if paginate:
        result = paginate_datahub_query_results(
            graph=graph,
            query=GRAPHQL_LIST_MONITORS_QUERY,
            operation_name=GRAPHQL_LIST_MONITORS_OPERATION,
            query_key="searchAcrossEntities",
            result_key="searchResults",
            user_params=params,
            page_size=LIST_MONITORS_BATCH_SIZE,
        )
    else:
        params["input"]["count"] = 100
        result = graph.execute_graphql(
            GRAPHQL_LIST_MONITORS_QUERY,
            operation_name=GRAPHQL_LIST_MONITORS_OPERATION,
            variables=params,
        )["searchAcrossEntities"]["searchResults"]

    monitors = [item["entity"] for item in result]
    return graphql_to_monitors(monitors)


def render_monitor_list_page() -> None:
    """Render the list of monitors."""
    st.header("Monitors")

    # Show connection status
    if not render_connection_status():
        st.info("Please configure a DataHub connection to view monitors.")
        return

    limited = st.checkbox("Limit to first 100", value=True)

    monitors = _list_monitors(paginate=not limited)
    if monitors is None:
        st.error("Failed to fetch monitors. Check your connection settings.")
        return

    # Create a dataframe with key monitor information
    monitor_data = []
    for monitor in monitors:
        assertion = (
            monitor.assertion_monitor.assertions[0]
            if monitor.assertion_monitor
            else None
        )
        dataset = assertion.assertion.entity if assertion else None

        monitor_info = {
            "Monitor": monitor.urn,
            "Mode": monitor.mode.value if monitor.mode else None,
            "Dataset": dataset.table_name if dataset else None,
            "Assertion Type": assertion.assertion.type.value if assertion else None,
            "Assertion": assertion.model_dump_json() if assertion else None,
            "Settings": (
                monitor.assertion_monitor.settings.model_dump_json()
                if monitor.assertion_monitor and monitor.assertion_monitor.settings
                else None
            ),
            "Executor ID": monitor.executor_id,
        }
        monitor_data.append(monitor_info)

    monitor_data_df = pd.DataFrame(monitor_data)

    st.text(
        "Select a row using the checkbox on the left to view details. "
        "Click the 'View Monitor Details' button to view the details of the selected monitor."
    )
    monitor_df = st.dataframe(
        monitor_data_df,
        column_config={
            "Monitor": st.column_config.TextColumn(width="small"),
            "Dataset": st.column_config.TextColumn(width="medium"),
            "Assertion": st.column_config.JsonColumn(width="small"),
            "Settings": st.column_config.JsonColumn(width="small"),
        },
        on_select="rerun",
        selection_mode="single-row",
    )

    if row_selection := monitor_df.get("selection", {}).get("rows"):
        # Go to details when a row is selected.
        monitor_urn = monitor_data_df.iloc[row_selection[0]]["Monitor"]
        st.session_state[_SELECTED_MONITOR_KEY] = monitor_urn
        st.switch_page(detail_page)


def render_load_sample_data_page() -> None:
    """Render the load sample data page."""
    st.header("Load Sample Data")

    # Show connection status first
    if not render_connection_status():
        st.info("Please configure a DataHub connection to load sample data.")
        return

    st.markdown(
        "Generate realistic sample volume assertion data for testing and development. "
        "Volume assertions track row counts over time - by default, data is generated "
        "as **monotonically increasing** (append-only), which is realistic for most "
        "production tables."
    )

    # Track generation result in session state
    _SAMPLE_DATA_RESULT_KEY = "_sample_data_result"

    # Random URN generator (outside form so it can update state)
    _RANDOM_URN_KEY = "_random_dataset_urn"

    col_btn1, col_btn2 = st.columns([1, 1])

    with col_btn1:
        if st.button("🎲 Generate Random Dataset URN"):
            random_urn = _generate_random_dataset_urn()
            st.session_state[_RANDOM_URN_KEY] = random_urn
            st.rerun()  # Rerun to populate the form field

    with col_btn2:
        if st.button("✨ Create Random Dataset in DataHub"):
            try:
                with st.spinner("Creating dataset..."):
                    random_urn = _create_random_dataset_in_datahub()
                    st.session_state[_RANDOM_URN_KEY] = random_urn
                st.session_state["_dataset_created_success"] = random_urn
                st.rerun()  # Rerun to populate the form field
            except Exception as e:
                st.error(f"Failed to create dataset: {e}")

    # Show success message after rerun (outside the button handler)
    if "_dataset_created_success" in st.session_state:
        created_urn = st.session_state.pop("_dataset_created_success")
        st.success(f"✅ Created dataset: `{created_urn}`")

    # Get default value from session state if available
    default_urn = st.session_state.get(_RANDOM_URN_KEY, "")

    with st.form("load_sample"):
        dataset_urn = st.text_input(
            "Dataset URN",
            value=default_urn,
            help="Format: urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})",
            placeholder="urn:li:dataset:(urn:li:dataPlatform:snowflake,my_database.my_schema.my_table,PROD)",
        )

        st.markdown("---")
        st.markdown("### Data Generation Settings")

        col1, col2 = st.columns(2)

        with col1:
            num_days = st.number_input(
                "Number of Days",
                min_value=7,
                max_value=365,
                value=60,
                help="How many days of historical data to generate",
            )

            base_row_count = st.number_input(
                "Starting Row Count",
                min_value=100,
                max_value=10000000,
                value=10000,
                step=1000,
                help="Initial row count for the table",
            )

        with col2:
            daily_increment_min = st.number_input(
                "Min Daily Increment",
                min_value=0,
                max_value=100000,
                value=100,
                step=50,
                help="Minimum rows added per day",
            )

            daily_increment_max = st.number_input(
                "Max Daily Increment",
                min_value=1,
                max_value=100000,
                value=500,
                step=50,
                help="Maximum rows added per day",
            )

        st.markdown("---")
        st.markdown("### Options")

        opt_col1, opt_col2 = st.columns(2)

        with opt_col1:
            monotonic = st.checkbox(
                "Monotonically Increasing (Append-Only)",
                value=True,
                help="Row counts always increase - realistic for append-only tables. "
                "Unchecking this creates fluctuating values with trend/seasonality.",
            )

            create_metric_cube = st.checkbox(
                "Create Metric Cube",
                value=True,
                help="Generate metric cube events for AI model training and evaluation. "
                "Disable if you only need assertion run events.",
            )

        with opt_col2:
            inject_anomaly = st.checkbox(
                "Inject Anomaly",
                value=False,
                help="Insert an anomaly spike in the data for testing anomaly detection",
            )

        # Anomaly settings (shown only if inject_anomaly is checked)
        if inject_anomaly:
            anom_col1, anom_col2 = st.columns(2)
            with anom_col1:
                anomaly_position = st.slider(
                    "Anomaly Position",
                    min_value=0.1,
                    max_value=0.9,
                    value=0.7,
                    step=0.1,
                    help="Where in the time series to inject the anomaly (0.1=early, 0.9=late)",
                )
            with anom_col2:
                anomaly_magnitude = st.slider(
                    "Anomaly Magnitude",
                    min_value=2.0,
                    max_value=10.0,
                    value=3.0,
                    step=0.5,
                    help="How much larger the anomaly increment is (e.g., 3.0 = 3x normal)",
                )
        else:
            anomaly_position = 0.7
            anomaly_magnitude = 3.0

        st.markdown("---")

        submitted = st.form_submit_button("Generate Sample Data", type="primary")

        if submitted:
            try:
                # Validate URN format
                if not dataset_urn:
                    st.error("Please enter a Dataset URN")
                    return

                DatasetUrn.from_string(dataset_urn)

                # Validate increment range
                if daily_increment_min > daily_increment_max:
                    st.error("Min daily increment must be less than or equal to max")
                    return

                # Build config
                config = SampleDataConfig(
                    num_days=int(num_days),
                    base_row_count=int(base_row_count),
                    monotonic=monotonic,
                    daily_increment_range=(
                        float(daily_increment_min),
                        float(daily_increment_max),
                    ),
                    create_metric_cube=create_metric_cube,
                    inject_anomaly=inject_anomaly,
                    anomaly_position=anomaly_position,
                    anomaly_magnitude=anomaly_magnitude,
                )

                with st.spinner("Generating sample data..."):
                    # Generate sample data (creates/updates monitor/assertion)
                    monitor_urn = generate_volume_sample_data(dataset_urn, config)
                    assertion_urn = default_volume_assertion_urn(dataset_urn)

                # Store result in session state for display outside form
                st.session_state[_SAMPLE_DATA_RESULT_KEY] = {
                    "success": True,
                    "monitor_urn": monitor_urn,
                    "assertion_urn": assertion_urn,
                    "config": {
                        "num_days": config.num_days,
                        "base_row_count": config.base_row_count,
                        "monotonic": config.monotonic,
                        "create_metric_cube": config.create_metric_cube,
                        "inject_anomaly": config.inject_anomaly,
                    },
                }
                st.session_state[_SELECTED_MONITOR_KEY] = monitor_urn
                st.rerun()

            except ValueError as e:
                st.error(f"Error: {str(e)}")
            except Exception as e:
                st.error(f"Unexpected error: {str(e)}")

    # Display result outside form (so we can use st.button)
    if _SAMPLE_DATA_RESULT_KEY in st.session_state:
        result = st.session_state[_SAMPLE_DATA_RESULT_KEY]
        if result.get("success"):
            st.success("✅ Successfully generated sample data!")

            # Show what was created
            st.markdown("**Created/Updated:**")
            st.code(
                f"Monitor: {result['monitor_urn']}\nAssertion: {result['assertion_urn']}",
                language=None,
            )

            # Show config summary
            config_info = result.get("config", {})
            config_details = []
            if config_info.get("num_days"):
                config_details.append(f"📅 {config_info['num_days']} days of data")
            if config_info.get("monotonic"):
                config_details.append("📈 Monotonically increasing (append-only)")
            if config_info.get("create_metric_cube"):
                config_details.append("📊 Metric cube created")
            if config_info.get("inject_anomaly"):
                config_details.append("⚠️ Anomaly injected")

            if config_details:
                st.markdown("**Configuration:**")
                for detail in config_details:
                    st.markdown(f"- {detail}")

            st.info(
                "💡 **Note:** It may take a few seconds for the data to be indexed. "
                "If the monitor doesn't load immediately, wait a moment and try again."
            )

            # Button to navigate to detail page (user-initiated, gives time for indexing)
            if st.button("View Monitor Details →", type="primary"):
                del st.session_state[_SAMPLE_DATA_RESULT_KEY]
                st.switch_page(detail_page)


st.set_page_config(
    page_title="Observe Control Panel",
    layout="wide",
)

# Define pages
connection_settings_page = st.Page(
    render_connection_settings_page,
    title="Connection Settings",
    icon="⚙️",
    url_path="/settings",
)

# Register the connection settings page for navigation from other pages
set_connection_settings_page(connection_settings_page)

create_page = st.Page(
    lambda: render_create_assertion_page(),
    title="Create Assertion",
    icon="➕",
    url_path="/create",
)

view_page = st.Page(
    lambda: render_monitor_list_page(),
    title="All Monitors",
    icon="📋",
    url_path="/view",
    default=True,
)

sample_data_page = st.Page(
    lambda: render_load_sample_data_page(),
    title="Load Sample Row Counts",
    icon="💾",
    url_path="/sample_data",
)

detail_page = st.Page(
    lambda: render_monitor_detail_page(),
    title="Monitor Details",
    icon="🔍",
    url_path="/detail",
)

# Navigation menu
pages = {
    "Settings": [connection_settings_page],
    "Monitors": [create_page, view_page, detail_page],
    "Tools": [sample_data_page],
}
# Add time series explorer pages
pages.update(EXPLORER_PAGES)

page = st.navigation(pages)
page.run()

# Historical note: Pydantic v1 required clearing validator cache for Streamlit reruns
# via pydantic.v1.class_validators._FUNCS.clear() to avoid duplicate validator errors.
# See https://github.com/streamlit/streamlit/issues/3218
# This workaround is NOT needed in Pydantic v2 - the validator system was redesigned
# and no longer uses a global cache that causes conflicts on script reruns.
