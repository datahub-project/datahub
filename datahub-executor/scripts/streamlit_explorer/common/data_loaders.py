# ruff: noqa: INP001
"""
Unified data loading interface.

This module provides a unified interface for loading time series data from
multiple sources: local parquet files, S3, cached API data, or live API.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd

try:
    from .cache_manager import RunEventCache, get_cache_dir
    from .metric_cube_extractor import (
        MetricCubeMetadata,
        MonitoredAssertionMetadata,
        build_metric_cube_urn,
        decode_metric_cube_urn,
        extract_timeseries as extract_metric_cube_timeseries,
        extract_timeseries_with_anomalies,
        list_metric_cubes,
        list_monitored_assertions,
    )
    from .run_event_extractor import (
        MonitorMetadata,
        extract_entity_from_monitor_urn,
        list_monitors,
    )
except ImportError:
    from cache_manager import (  # type: ignore[import-not-found,no-redef]
        RunEventCache,
        get_cache_dir,
    )
    from metric_cube_extractor import (  # type: ignore[import-not-found,no-redef]
        MetricCubeMetadata,
        MonitoredAssertionMetadata,
        build_metric_cube_urn,
        decode_metric_cube_urn,
        extract_timeseries as extract_metric_cube_timeseries,
        extract_timeseries_with_anomalies,
        list_metric_cubes,
        list_monitored_assertions,
    )
    from run_event_extractor import (  # type: ignore[import-not-found,no-redef]
        MonitorMetadata,
        extract_entity_from_monitor_urn,
        list_monitors,
    )


class DataLoader:
    """Unified data loader supporting multiple data sources."""

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or get_cache_dir()
        self.cache = RunEventCache(self.cache_dir)
        # Use the same registry as the cache to avoid state inconsistencies
        self.registry = self.cache.registry

    # =========================================================================
    # Local Parquet Loading
    # =========================================================================

    def load_local_parquet(
        self,
        path: Union[str, Path],
        use_duckdb: bool = True,
        sample_fraction: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load data from a local parquet file.

        Args:
            path: Path to the parquet file
            use_duckdb: If True, use DuckDB for efficient loading (if available)
            sample_fraction: If set, sample this fraction of rows (0.0 to 1.0)
            limit: If set, limit to this many rows

        Returns:
            DataFrame with the loaded data
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        if use_duckdb:
            try:
                import duckdb

                query = f"SELECT * FROM read_parquet('{path}')"

                if sample_fraction is not None:
                    pct = int(sample_fraction * 100)
                    query += f" USING SAMPLE {pct} PERCENT"

                if limit is not None:
                    query += f" LIMIT {limit}"

                return duckdb.query(query).to_df()
            except ImportError:
                pass  # Fall through to pandas

        # Pandas fallback
        df = pd.read_parquet(path)

        if sample_fraction is not None:
            df = df.sample(frac=sample_fraction)

        if limit is not None:
            df = df.head(limit)

        return df

    def get_local_parquet_metadata(
        self,
        path: Union[str, Path],
    ) -> dict:
        """Get metadata about a local parquet file without loading it fully.

        Args:
            path: Path to the parquet file

        Returns:
            Dictionary with metadata (row_count, columns, size_mb, etc.)
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        try:
            import duckdb

            # Get row count efficiently
            result = duckdb.query(
                f"SELECT COUNT(*) FROM read_parquet('{path}')"
            ).fetchone()
            row_count = result[0] if result else 0

            # Get column info
            schema_df = duckdb.query(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')"
            ).to_df()

            return {
                "path": str(path),
                "size_mb": path.stat().st_size / (1024 * 1024),
                "row_count": row_count,
                "columns": schema_df["column_name"].tolist(),
                "column_types": dict(
                    zip(
                        schema_df["column_name"], schema_df["column_type"], strict=False
                    )
                ),
            }
        except ImportError:
            # Pandas fallback - need to load the file
            df = pd.read_parquet(path)
            return {
                "path": str(path),
                "size_mb": path.stat().st_size / (1024 * 1024),
                "row_count": len(df),
                "columns": list(df.columns),
                "column_types": {col: str(df[col].dtype) for col in df.columns},
            }

    # =========================================================================
    # S3 Parquet Loading
    # =========================================================================

    def load_s3_parquet(
        self,
        bucket: str,
        key: str,
        aws_profile: Optional[str] = None,
        aws_region: Optional[str] = None,
        sample_fraction: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Load data from an S3 parquet file.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path within bucket)
            aws_profile: AWS profile to use for credentials
            aws_region: AWS region
            sample_fraction: If set, sample this fraction of rows (0.0 to 1.0)
            limit: If set, limit to this many rows

        Returns:
            DataFrame with the loaded data
        """
        s3_path = f"s3://{bucket}/{key}"

        try:
            import duckdb

            # Set up AWS credentials if needed
            if aws_profile:
                import boto3

                session = boto3.Session(
                    profile_name=aws_profile, region_name=aws_region
                )
                credentials = session.get_credentials()
                frozen_credentials = credentials.get_frozen_credentials()

                duckdb.query(f"""
                    CREATE SECRET IF NOT EXISTS s3_secret (
                        TYPE S3,
                        KEY_ID '{frozen_credentials.access_key}',
                        SECRET '{frozen_credentials.secret_key}',
                        REGION '{aws_region or "us-east-1"}'
                    )
                """)

            query = f"SELECT * FROM read_parquet('{s3_path}')"

            if sample_fraction is not None:
                pct = int(sample_fraction * 100)
                query += f" USING SAMPLE {pct} PERCENT"

            if limit is not None:
                query += f" LIMIT {limit}"

            return duckdb.query(query).to_df()

        except ImportError:
            # Pandas with boto3 fallback
            import io

            import boto3

            session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
            s3_client = session.client("s3")

            response = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(response["Body"].read()))

            if sample_fraction is not None:
                df = df.sample(frac=sample_fraction)

            if limit is not None:
                df = df.head(limit)

            return df

    # =========================================================================
    # Cached API Data Loading
    # =========================================================================

    def load_cached_events(
        self,
        hostname: str,
        aspect_name: str = "dataHubMetricCubeEvent",
        entity_urn: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """Load cached events for an endpoint and aspect.

        Args:
            hostname: The endpoint hostname
            aspect_name: The timeseries aspect name (default: dataHubMetricCubeEvent)
            entity_urn: Optional filter by entity URN
            start_time: Optional filter by start time
            end_time: Optional filter by end time

        Returns:
            DataFrame with filtered events, or None if not cached
        """
        return self.cache.get_cached_aspect_events(
            hostname,
            aspect_name=aspect_name,
            entity_urn=entity_urn,
            start_time=start_time,
            end_time=end_time,
        )

    def cache_from_parquet(
        self,
        hostname: str,
        parquet_path: Union[str, Path],
        alias: Optional[str] = None,
        sync_type: str = "full",
    ) -> None:
        """Cache run events from a local parquet file.

        Args:
            hostname: The hostname to associate with this data
            parquet_path: Path to the parquet file
            alias: Optional friendly name for the endpoint
            sync_type: 'full' to replace, 'incremental' to append
        """
        # sync_from_parquet will register the endpoint if needed
        self.cache.sync_from_parquet(hostname, Path(parquet_path), sync_type=sync_type)

        # Update alias if provided
        if alias and self.registry.get_endpoint(hostname):
            self.registry.update_endpoint(hostname, alias=alias)

    # =========================================================================
    # Endpoint and Cache Management
    # =========================================================================

    def list_endpoints(self) -> list:
        """List all registered endpoints with cache info."""
        return self.registry.list_endpoints()

    def get_endpoint_info(self, hostname: str) -> Optional[dict]:
        """Get detailed info about an endpoint and its cache.

        Args:
            hostname: The endpoint hostname

        Returns:
            Dictionary with endpoint and cache info
        """
        endpoint = self.registry.get_endpoint(hostname)
        if endpoint is None:
            return None

        cache = self.cache.get_endpoint_cache(hostname)
        index = cache.index.data

        # Build per-aspect info
        aspects_info = {}
        for aspect_name in cache.list_cached_aspects():
            aspect_info = index.get_aspect_info(aspect_name)
            if aspect_info:
                aspects_info[aspect_name] = {
                    "event_count": aspect_info.event_count,
                    "unique_entities": aspect_info.unique_entities,
                    "first_sync": aspect_info.first_sync,
                    "last_sync": aspect_info.last_sync,
                    "data_range": {
                        "start": aspect_info.data_range_start,
                        "end": aspect_info.data_range_end,
                    },
                }

        return {
            "hostname": hostname,
            "alias": endpoint.alias,
            "url": endpoint.url,
            "last_used": endpoint.last_used,
            "cache_exists": any(cache.exists(a) for a in cache.list_cached_aspects())
            if cache.list_cached_aspects()
            else False,
            "cache_size_mb": cache.get_cache_size_mb(),
            "event_count": index.get_total_event_count(),
            "last_sync": index.last_sync,
            "aspects": aspects_info,
        }

    def get_cached_monitors(
        self,
        hostname: str,
        aspect_name: str = "monitorAnomalyEvent",
    ) -> list:
        """List monitors available in the cache for an endpoint.

        Args:
            hostname: The endpoint hostname
            aspect_name: The timeseries aspect name (monitorAnomalyEvent or monitorTimeseriesState)

        Returns:
            List of MonitorMetadata objects
        """
        events_df = self.load_cached_events(hostname, aspect_name=aspect_name)

        if events_df is None or len(events_df) == 0:
            return []

        return list_monitors(events_df, aspect_name=aspect_name)

    def get_cached_monitors_paginated(
        self,
        hostname: str,
        page: int = 0,
        page_size: int = 100,
        search_filter: Optional[str] = None,
        aspect_name: str = "monitorAnomalyEvent",
    ) -> tuple[list[MonitorMetadata], int]:
        """List monitors with pagination using DuckDB for efficiency.

        This method avoids loading all events into memory by using DuckDB's
        GROUP BY with LIMIT/OFFSET directly on the parquet file.

        Args:
            hostname: The endpoint hostname
            page: Page number (0-indexed)
            page_size: Number of results per page
            search_filter: Optional search string to filter by URN
            aspect_name: The timeseries aspect name

        Returns:
            Tuple of (list of MonitorMetadata for the page, total count)
        """
        cache = self.cache.get_endpoint_cache(hostname)
        results, total_count = cache.list_entities_paginated(
            aspect_name=aspect_name,
            page=page,
            page_size=page_size,
            search_filter=search_filter,
            type_filter=None,  # No type filter for monitors
        )

        # Determine event type from aspect name
        event_type = "anomaly" if "anomaly" in aspect_name.lower() else "state"

        # Convert dicts to MonitorMetadata objects
        monitors = []
        for r in results:
            first_event = None
            last_event = None
            if r.get("first_event_ms"):
                first_event = pd.to_datetime(
                    r["first_event_ms"], unit="ms"
                ).to_pydatetime()
            if r.get("last_event_ms"):
                last_event = pd.to_datetime(
                    r["last_event_ms"], unit="ms"
                ).to_pydatetime()

            # Extract the monitored entity URN from the monitor URN
            # Monitor URN format: urn:li:monitor:(entity_urn,monitor_name)
            monitor_urn = r["entity_urn"]
            entity_urn = extract_entity_from_monitor_urn(monitor_urn)

            monitors.append(
                MonitorMetadata(
                    monitor_urn=monitor_urn,
                    entity_urn=entity_urn,
                    event_type=event_type,
                    point_count=r.get("point_count", 0),
                    first_event=first_event,
                    last_event=last_event,
                    anomaly_count=None,
                    state_values=None,
                )
            )

        return monitors, total_count

    def get_confirmed_anomalies_as_exclusions(
        self,
        hostname: str,
        entity_urn: Optional[str] = None,
        use_local_edits: bool = True,
        window_minutes: int = 0,
    ) -> list:
        """Load confirmed monitor anomalies as exclusion time ranges.

        Uses effective state: original API state merged with local edits.

        Args:
            hostname: The endpoint hostname
            entity_urn: Optional entity URN to filter by
            use_local_edits: Whether to include local edits in state calculation
            window_minutes: Window around each anomaly point in minutes (0 = point only)

        Returns:
            List of TimeRange objects for exclusion
        """
        from datetime import timedelta

        try:
            from datahub_observe.algorithms.preprocessing import (  # type: ignore[import-untyped]
                TimeRange,
            )
        except ImportError:
            return []

        # Load anomaly events
        events_df = self.load_cached_events(hostname, aspect_name="monitorAnomalyEvent")

        if events_df is None or len(events_df) == 0:
            return []

        # Filter by entity if specified
        if entity_urn:
            # Anomalies link to assertions via source_sourceUrn (the assertion URN)
            # or can be filtered by monitorUrn if that's what's passed
            if "source_sourceUrn" in events_df.columns:
                events_df = events_df[events_df["source_sourceUrn"] == entity_urn]
            elif "assertionUrn" in events_df.columns:
                events_df = events_df[events_df["assertionUrn"] == entity_urn]
            elif "monitorUrn" in events_df.columns:
                events_df = events_df[events_df["monitorUrn"] == entity_urn]

        if len(events_df) == 0:
            return []

        # Get edit tracker for local state overrides
        edit_tracker = None
        if use_local_edits:
            from .cache_manager import AnomalyEditTracker

            endpoint_cache = self.cache.get_endpoint_cache(hostname)
            edit_tracker = AnomalyEditTracker(endpoint_cache.cache_dir)

        exclusions = []
        window_delta = timedelta(minutes=window_minutes)

        for _, row in events_df.iterrows():
            # anomaly_timestamp_ms is the anomaly event creation time (used for edit lookup)
            anomaly_timestamp_ms = row.get("timestampMillis")
            if anomaly_timestamp_ms is None:
                continue

            # Get original state
            original_state = row.get("state")

            # Get effective state (considering local edits)
            # Edits are keyed by anomaly event timestamp, not source event timestamp
            effective_state = original_state
            if edit_tracker:
                monitor_urn = row.get("monitorUrn", "")
                effective_state = edit_tracker.get_effective_state(
                    monitor_urn, anomaly_timestamp_ms, original_state
                )

            # Only include confirmed anomalies
            if effective_state != "CONFIRMED":
                continue

            # Use source event timestamp (frequency-aligned run time) for the exclusion range,
            # as that corresponds to the actual data point that should be excluded.
            # Fall back to anomaly timestamp if source timestamp not available.
            source_ts = row.get("source_sourceEventTimestampMillis")
            exclusion_timestamp_ms = (
                source_ts
                if source_ts is not None and not pd.isna(source_ts)
                else anomaly_timestamp_ms
            )

            # Convert to datetime using pd.to_datetime for UTC consistency
            # (matches how timeseries 'ds' column is created in run_event_extractor)
            timestamp = pd.to_datetime(
                exclusion_timestamp_ms, unit="ms"
            ).to_pydatetime()

            # Create exclusion range
            if window_minutes == 0:
                # Point exclusion - create a tiny range
                exclusions.append(
                    TimeRange(start=timestamp, end=timestamp + timedelta(seconds=1))
                )
            else:
                exclusions.append(
                    TimeRange(
                        start=timestamp - window_delta,
                        end=timestamp + window_delta,
                    )
                )

        return exclusions

    def get_confirmed_anomaly_timestamps(
        self,
        hostname: str,
        entity_urn: Optional[str] = None,
        use_local_edits: bool = True,
    ) -> list[int]:
        """Get confirmed anomaly timestamps in milliseconds.

        Returns the source event timestamps for confirmed anomalies, which
        can be used to mark rows in the type column for AnomalyDataFilterTransformer.

        Args:
            hostname: The endpoint hostname
            entity_urn: Optional entity URN to filter by
            use_local_edits: Whether to include local edits in state calculation

        Returns:
            List of timestamps in milliseconds for confirmed anomalies
        """
        # Load anomaly events
        events_df = self.load_cached_events(hostname, aspect_name="monitorAnomalyEvent")

        if events_df is None or len(events_df) == 0:
            return []

        # Filter by entity if specified
        if entity_urn:
            # Anomalies link to assertions via source_sourceUrn (the assertion URN)
            # or can be filtered by monitorUrn if that's what's passed
            if "source_sourceUrn" in events_df.columns:
                events_df = events_df[events_df["source_sourceUrn"] == entity_urn]
            elif "assertionUrn" in events_df.columns:
                events_df = events_df[events_df["assertionUrn"] == entity_urn]
            elif "monitorUrn" in events_df.columns:
                events_df = events_df[events_df["monitorUrn"] == entity_urn]

        if len(events_df) == 0:
            return []

        # Get edit tracker for local state overrides
        edit_tracker = None
        if use_local_edits:
            from .cache_manager import AnomalyEditTracker

            endpoint_cache = self.cache.get_endpoint_cache(hostname)
            edit_tracker = AnomalyEditTracker(endpoint_cache.cache_dir)

        timestamps = []

        for _, row in events_df.iterrows():
            # anomaly_timestamp_ms is the anomaly event creation time (used for edit lookup)
            anomaly_timestamp_ms = row.get("timestampMillis")
            if anomaly_timestamp_ms is None:
                continue

            # Get original state
            original_state = row.get("state")

            # Get effective state (considering local edits)
            effective_state = original_state
            if edit_tracker:
                monitor_urn = row.get("monitorUrn", "")
                effective_state = edit_tracker.get_effective_state(
                    monitor_urn, anomaly_timestamp_ms, original_state
                )

            # Only include confirmed anomalies
            if effective_state != "CONFIRMED":
                continue

            # Use source event timestamp (frequency-aligned run time) for the data point,
            # as that corresponds to the actual data point in the timeseries.
            # Fall back to anomaly timestamp if source timestamp not available.
            source_ts = row.get("source_sourceEventTimestampMillis")
            timestamp_ms = (
                int(source_ts)
                if source_ts is not None and not pd.isna(source_ts)
                else int(anomaly_timestamp_ms)
            )

            timestamps.append(timestamp_ms)

        return timestamps

    def clear_cache(self, hostname: Optional[str] = None) -> None:
        """Clear cached data.

        Args:
            hostname: If provided, clear only this endpoint's cache.
                     If None, clear all caches.
        """
        self.cache.clear_cache(hostname)

    def delete_monitor_cache(
        self,
        hostname: str,
        monitor_urn: str,
        aspect_name: str = "dataHubMetricCubeEvent",
    ) -> int:
        """Delete cached data for a specific monitor.

        Args:
            hostname: The endpoint hostname
            monitor_urn: The monitor URN to delete
            aspect_name: The aspect name

        Returns:
            Number of events deleted
        """
        cache = self.cache.get_endpoint_cache(hostname)
        # For metric cube events, convert monitor URN to metric cube URN
        if aspect_name == "dataHubMetricCubeEvent":
            entity_urn = build_metric_cube_urn(monitor_urn)
        else:
            entity_urn = monitor_urn
        return cache.delete_entity_events(entity_urn, aspect_name)

    # =========================================================================
    # Metric Cube Data Loading
    # =========================================================================

    def load_cached_metric_cube_events(
        self,
        hostname: str,
        monitor_urn: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """Load cached metric cube events for an endpoint.

        Args:
            hostname: The endpoint hostname
            monitor_urn: Optional filter by monitor URN
            start_time: Optional filter by start time
            end_time: Optional filter by end time

        Returns:
            DataFrame with metric cube events, or None if not cached
        """
        entity_urn = None
        if monitor_urn:
            # Convert monitor URN to metric cube URN for entity filtering
            entity_urn = build_metric_cube_urn(monitor_urn)

        return self.cache.get_cached_aspect_events(
            hostname,
            aspect_name="dataHubMetricCubeEvent",
            entity_urn=entity_urn,
            start_time=start_time,
            end_time=end_time,
        )

    def load_cached_metric_cube_timeseries(
        self,
        hostname: str,
        monitor_urn: str,
        include_anomalies: bool = True,
    ) -> pd.DataFrame:
        """Load a time series from cached metric cube data.

        This provides cleaner time series data than assertion run events
        since the measure value is directly available.

        Args:
            hostname: The endpoint hostname
            monitor_urn: The monitor URN
            include_anomalies: If True, include anomaly_state column

        Returns:
            DataFrame with columns 'ds', 'y', and optionally 'anomaly_state'
        """
        events_df = self.load_cached_metric_cube_events(
            hostname, monitor_urn=monitor_urn
        )

        if events_df is None or len(events_df) == 0:
            cols = ["ds", "y", "anomaly_state"] if include_anomalies else ["ds", "y"]
            return pd.DataFrame(columns=cols)

        return extract_metric_cube_timeseries(
            events_df,
            monitor_urn=monitor_urn,
            include_anomalies=include_anomalies,
        )

    def load_cached_metric_cube_timeseries_for_assertion(
        self,
        hostname: str,
        assertion_urn: str,
    ) -> tuple[pd.DataFrame, str]:
        """Load time series data from metric cube for an assertion.

        This method finds metric cube data for a given assertion URN by looking
        for metric cube events where the assertionUrn column matches.

        Args:
            hostname: The endpoint hostname
            assertion_urn: The assertion URN

        Returns:
            Tuple of (DataFrame with 'ds' and 'y' columns, data source name)
        """
        # Load all metric cube events and filter by assertionUrn
        events_df = self.load_cached_events(
            hostname, aspect_name="dataHubMetricCubeEvent"
        )

        if events_df is None or len(events_df) == 0:
            return pd.DataFrame(columns=["ds", "y"]), "none"

        # Filter by assertion URN if the column exists
        if "assertionUrn" in events_df.columns:
            events_df = events_df[events_df["assertionUrn"] == assertion_urn]

        if len(events_df) == 0:
            return pd.DataFrame(columns=["ds", "y"]), "none"

        # Extract the monitor URN from the first matching event
        monitor_urn = (
            events_df["monitorUrn"].iloc[0]
            if "monitorUrn" in events_df.columns
            else None
        )

        if monitor_urn:
            return extract_metric_cube_timeseries(
                events_df, monitor_urn=monitor_urn, include_anomalies=False
            ), "metric_cube"

        return pd.DataFrame(columns=["ds", "y"]), "none"

    def get_cached_metric_cubes(
        self,
        hostname: str,
    ) -> list[MetricCubeMetadata]:
        """List metric cubes available in the cache for an endpoint.

        Args:
            hostname: The endpoint hostname

        Returns:
            List of MetricCubeMetadata objects
        """
        events_df = self.load_cached_events(
            hostname, aspect_name="dataHubMetricCubeEvent"
        )

        if events_df is None or len(events_df) == 0:
            return []

        return list_metric_cubes(events_df)

    def get_cached_metric_cubes_paginated(
        self,
        hostname: str,
        page: int = 0,
        page_size: int = 100,
        search_filter: Optional[str] = None,
    ) -> tuple[list[MetricCubeMetadata], int]:
        """List metric cubes with pagination using DuckDB for efficiency.

        Args:
            hostname: The endpoint hostname
            page: Page number (0-indexed)
            page_size: Number of results per page
            search_filter: Optional search string to filter by URN

        Returns:
            Tuple of (list of MetricCubeMetadata for the page, total count)
        """
        cache = self.cache.get_endpoint_cache(hostname)
        results, total_count = cache.list_entities_paginated(
            aspect_name="dataHubMetricCubeEvent",
            page=page,
            page_size=page_size,
            search_filter=search_filter,
            type_filter=None,
        )

        # Convert dicts to MetricCubeMetadata objects
        cubes = []
        for r in results:
            first_event = None
            last_event = None
            if r.get("first_event_ms"):
                first_event = pd.to_datetime(
                    r["first_event_ms"], unit="ms"
                ).to_pydatetime()
            if r.get("last_event_ms"):
                last_event = pd.to_datetime(
                    r["last_event_ms"], unit="ms"
                ).to_pydatetime()

            # entity_urn is the metric cube URN
            metric_cube_urn = r["entity_urn"]

            # Decode to get monitor URN
            monitor_urn = decode_metric_cube_urn(metric_cube_urn)

            cubes.append(
                MetricCubeMetadata(
                    metric_cube_urn=metric_cube_urn,
                    monitor_urn=monitor_urn,
                    point_count=r.get("point_count", 0),
                    first_event=first_event,
                    last_event=last_event,
                    value_min=r.get("value_min"),
                    value_max=r.get("value_max"),
                    value_mean=r.get("value_mean"),
                    anomaly_count=0,  # Would need separate query
                )
            )

        return cubes, total_count

    def get_monitored_assertions(
        self,
        hostname: str,
    ) -> list[MonitoredAssertionMetadata]:
        """List assertions that have metric cube data available.

        This provides a monitor-centric view of assertions - only assertions
        that have associated monitors and metric cube data are returned.

        Args:
            hostname: The endpoint hostname

        Returns:
            List of MonitoredAssertionMetadata objects
        """
        events_df = self.load_cached_events(
            hostname, aspect_name="dataHubMetricCubeEvent"
        )

        if events_df is None or len(events_df) == 0:
            return []

        return list_monitored_assertions(events_df)

    def get_monitored_assertions_paginated(
        self,
        hostname: str,
        page: int = 0,
        page_size: int = 100,
        search_filter: Optional[str] = None,
        status_filter: Optional[str] = None,
    ) -> tuple[list[MonitoredAssertionMetadata], int]:
        """List monitored assertions with pagination.

        Args:
            hostname: The endpoint hostname
            page: Page number (0-indexed)
            page_size: Number of results per page
            search_filter: Optional search string to filter by URN
            status_filter: Optional status filter ("ACTIVE" or "PAUSED")

        Returns:
            Tuple of (list of MonitoredAssertionMetadata for the page, total count)
        """
        # Get all monitored assertions
        all_assertions = self.get_monitored_assertions(hostname)

        # Apply search filter
        if search_filter:
            search_lower = search_filter.lower()
            all_assertions = [
                a for a in all_assertions if search_lower in a.assertion_urn.lower()
            ]

        # Apply status filter
        if status_filter:
            all_assertions = [
                a for a in all_assertions if a.monitor_status == status_filter
            ]

        # Get total count
        total_count = len(all_assertions)

        # Apply pagination
        start_idx = page * page_size
        end_idx = start_idx + page_size
        page_assertions = all_assertions[start_idx:end_idx]

        return page_assertions, total_count

    def get_metric_cube_timeseries_with_anomalies(
        self,
        hostname: str,
        monitor_urn: str,
    ) -> pd.DataFrame:
        """Load time series with detailed anomaly information from metric cube.

        Args:
            hostname: The endpoint hostname
            monitor_urn: The monitor URN

        Returns:
            DataFrame with columns:
            - 'ds': datetime
            - 'y': the metric value
            - 'is_anomaly': boolean indicating if point is anomalous
            - 'anomaly_state': the anomaly state (e.g., CONFIRMED, REJECTED)
            - 'anomaly_source_type': source of anomaly detection
        """
        events_df = self.load_cached_metric_cube_events(
            hostname, monitor_urn=monitor_urn
        )

        if events_df is None or len(events_df) == 0:
            return pd.DataFrame(
                columns=[
                    "ds",
                    "y",
                    "is_anomaly",
                    "anomaly_state",
                    "anomaly_source_type",
                ]
            )

        return extract_timeseries_with_anomalies(events_df, monitor_urn=monitor_urn)

    # =========================================================================
    # Utility Functions
    # =========================================================================

    def discover_local_parquet_files(
        self,
        directory: Union[str, Path],
        pattern: str = "*.parquet",
    ) -> list[dict]:
        """Discover parquet files in a directory.

        Args:
            directory: Directory to search
            pattern: Glob pattern for matching files

        Returns:
            List of dictionaries with file info
        """
        directory = Path(directory)

        if not directory.exists():
            return []

        files = []
        for path in directory.glob(pattern):
            if path.is_file():
                files.append(
                    {
                        "path": str(path),
                        "name": path.name,
                        "size_mb": path.stat().st_size / (1024 * 1024),
                        "modified": datetime.fromtimestamp(path.stat().st_mtime),
                    }
                )

        # Sort by modification time, newest first
        files.sort(key=lambda x: x["modified"], reverse=True)  # type: ignore[arg-type,return-value]

        return files


# Convenience function for creating a DataLoader
def get_data_loader(cache_dir: Optional[Path] = None) -> DataLoader:
    """Get a DataLoader instance."""
    return DataLoader(cache_dir)
