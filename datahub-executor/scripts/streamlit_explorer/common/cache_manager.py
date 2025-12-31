# ruff: noqa: INP001
"""
Multi-endpoint cache management for time series data.

This module provides caching infrastructure for storing raw timeseries aspects
from multiple DataHub API endpoints, with support for incremental sync and
multiple aspect types (assertion events, monitor events, etc.).
"""

import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Default cache location
DEFAULT_CACHE_DIR = Path.home() / ".datahub-executor" / "cache"

# Supported timeseries aspects
# NOTE: ASSERTION_ASPECTS removed as part of monitor-centric architecture refactoring.
# The application now uses monitors and metric cubes as the primary data source.

MONITOR_ASPECTS = {
    "monitorAnomalyEvent": {
        "label": "Anomaly Events",
        "entity_type": "monitor",
        "default": True,
    },
    "monitorTimeseriesState": {
        "label": "State Snapshots",
        "entity_type": "monitor",
        "default": False,
    },
}

METRIC_CUBE_ASPECTS = {
    "dataHubMetricCubeEvent": {
        "label": "Metric Cube Events",
        "entity_type": "dataHubMetricCube",
        "default": True,
    },
}

ALL_ASPECTS = {**MONITOR_ASPECTS, **METRIC_CUBE_ASPECTS}


def get_cache_dir() -> Path:
    """Get the cache directory, creating it if necessary."""
    cache_dir = Path(os.environ.get("DATAHUB_CACHE_DIR", str(DEFAULT_CACHE_DIR)))
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def hostname_to_dir(hostname: str) -> str:
    """Convert a hostname to a safe directory name.

    Replaces special characters with underscores.
    Examples:
        - 'gms.prod.example.com' -> 'gms.prod.example.com'
        - 'localhost:8080' -> 'localhost_8080'
        - 'https://gms.example.com' -> 'gms.example.com'
    """
    # Remove protocol prefix
    hostname = re.sub(r"^https?://", "", hostname)
    # Remove trailing slashes
    hostname = hostname.rstrip("/")
    # Replace special characters
    return re.sub(r"[:\\/]", "_", hostname)


def url_to_hostname(url: str) -> str:
    """Extract hostname from a URL.

    Examples:
        - 'https://gms.example.com:8080/path' -> 'gms.example.com:8080'
        - 'http://localhost:8080' -> 'localhost:8080'
    """
    # Remove protocol prefix
    url = re.sub(r"^https?://", "", url)
    # Remove path
    url = url.split("/")[0]
    return url


@dataclass
class EndpointInfo:
    """Information about a registered endpoint."""

    hostname: str
    alias: str
    url: str
    last_used: Optional[str] = None
    cache_size_mb: float = 0.0
    event_count: int = 0


@dataclass
class SyncHistoryEntry:
    """A single sync history entry."""

    timestamp: str
    events_added: int
    sync_type: str  # 'full' or 'incremental'


@dataclass
class AspectCacheInfo:
    """Cache info for a single timeseries aspect."""

    aspect_name: str
    entity_type: str  # "monitor" or "dataHubMetricCube"
    cache_path: str
    event_count: int = 0
    unique_entities: int = 0  # Unique assertions or monitors
    first_sync: Optional[str] = None
    last_sync: Optional[str] = None
    data_range_start: Optional[str] = None
    data_range_end: Optional[str] = None
    sync_history: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "aspect_name": self.aspect_name,
            "entity_type": self.entity_type,
            "cache_path": self.cache_path,
            "event_count": self.event_count,
            "unique_entities": self.unique_entities,
            "first_sync": self.first_sync,
            "last_sync": self.last_sync,
            "data_range": {
                "start": self.data_range_start,
                "end": self.data_range_end,
            },
            "sync_history": self.sync_history[-100:],  # Keep last 100
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AspectCacheInfo":
        """Create from dictionary."""
        data_range = data.get("data_range", {})
        return cls(
            aspect_name=data.get("aspect_name", ""),
            entity_type=data.get("entity_type", "monitor"),
            cache_path=data.get("cache_path", ""),
            event_count=data.get("event_count", 0),
            unique_entities=data.get("unique_entities", 0),
            first_sync=data.get("first_sync"),
            last_sync=data.get("last_sync"),
            data_range_start=data_range.get("start"),
            data_range_end=data_range.get("end"),
            sync_history=data.get("sync_history", []),
        )


@dataclass
class CacheIndexData:
    """Cache index data for a single endpoint with multi-aspect support."""

    version: str = "2.0"
    endpoint: str = ""
    aspects: dict = field(default_factory=dict)  # aspect_name -> AspectCacheInfo

    def get_aspect_info(self, aspect_name: str) -> Optional[AspectCacheInfo]:
        """Get cache info for a specific aspect."""
        info = self.aspects.get(aspect_name)
        if isinstance(info, dict):
            return AspectCacheInfo.from_dict(info)
        return info

    def set_aspect_info(self, aspect_name: str, info: AspectCacheInfo) -> None:
        """Set cache info for a specific aspect."""
        self.aspects[aspect_name] = info

    def get_total_event_count(self) -> int:
        """Get total event count across all aspects."""
        total = 0
        for info in self.aspects.values():
            if isinstance(info, dict):
                total += info.get("event_count", 0)
            elif isinstance(info, AspectCacheInfo):
                total += info.event_count
        return total

    @property
    def last_sync(self) -> Optional[str]:
        """Get the most recent sync time across all aspects."""
        latest = None
        for info in self.aspects.values():
            if isinstance(info, dict):
                sync = info.get("last_sync")
            elif isinstance(info, AspectCacheInfo):
                sync = info.last_sync
            else:
                continue
            if sync and (latest is None or sync > latest):
                latest = sync
        return latest


@dataclass
class AnomalyEdit:
    """A single local anomaly state edit or new anomaly creation.

    For existing anomalies (is_new=False):
        - original_state is the state from the API
        - local_state is the new desired state

    For new anomalies (is_new=True):
        - original_state is None
        - local_state is CONFIRMED (marking a point as anomalous)
        - run_event_timestamp_ms is the assertion run event timestamp
    """

    monitor_urn: str
    assertion_urn: str
    timestamp_ms: int  # The anomaly event timestamp (for existing) or run event timestamp (for new)
    original_state: Optional[str]  # State from API (None, CONFIRMED, REJECTED)
    local_state: str  # New local state (CONFIRMED, REJECTED)
    edited_at: str  # ISO timestamp
    is_new: bool = False  # True if this is a new anomaly to be created
    run_event_timestamp_ms: Optional[int] = (
        None  # For new anomalies: the run event timestamp
    )


class AnomalyEditTracker:
    """Manages local anomaly state edits for an endpoint.

    Stores edits locally in anomaly_edits.json, allowing users to
    review and modify anomaly states before optionally publishing
    changes to the API.
    """

    def __init__(self, cache_dir: Path):
        """Initialize the edit tracker.

        Args:
            cache_dir: The endpoint's cache directory
        """
        self.cache_dir = cache_dir
        self.edits_path = cache_dir / "anomaly_edits.json"
        self._data: dict = self._load()

    def _load(self) -> dict:
        """Load edits from disk."""
        if self.edits_path.exists():
            try:
                with open(self.edits_path, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return {"edits": [], "last_modified": None}

    def _save(self) -> None:
        """Save edits to disk."""
        self._data["last_modified"] = datetime.now(timezone.utc).isoformat()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with open(self.edits_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def _find_edit_index(self, monitor_urn: str, timestamp_ms: int) -> int:
        """Find index of an existing edit, or -1 if not found."""
        for i, edit in enumerate(self._data.get("edits", [])):
            if (
                edit.get("monitor_urn") == monitor_urn
                and edit.get("timestamp_ms") == timestamp_ms
            ):
                return i
        return -1

    def set_local_state(
        self,
        monitor_urn: str,
        assertion_urn: str,
        timestamp_ms: int,
        original_state: Optional[str],
        new_state: str,
        is_new: bool = False,
        run_event_timestamp_ms: Optional[int] = None,
    ) -> None:
        """Set a local state override for an anomaly or create a new anomaly.

        Args:
            monitor_urn: The monitor URN
            assertion_urn: The assertion URN (for API publish)
            timestamp_ms: The anomaly timestamp in milliseconds
            original_state: The original state from the API (None for new anomalies)
            new_state: The new local state (CONFIRMED or REJECTED)
            is_new: True if this is a new anomaly to be created
            run_event_timestamp_ms: For new anomalies, the run event timestamp
        """
        idx = self._find_edit_index(monitor_urn, timestamp_ms)
        edit_data = {
            "monitor_urn": monitor_urn,
            "assertion_urn": assertion_urn,
            "timestamp_ms": timestamp_ms,
            "original_state": original_state,
            "local_state": new_state,
            "edited_at": datetime.now(timezone.utc).isoformat(),
            "is_new": is_new,
        }

        if run_event_timestamp_ms is not None:
            edit_data["run_event_timestamp_ms"] = run_event_timestamp_ms

        if idx >= 0:
            self._data["edits"][idx] = edit_data
        else:
            self._data.setdefault("edits", []).append(edit_data)

        self._save()

    def create_new_anomaly(
        self,
        monitor_urn: str,
        assertion_urn: str,
        run_event_timestamp_ms: int,
    ) -> None:
        """Create a new local anomaly marking for a run event.

        The anomaly will be marked as CONFIRMED and staged for publishing
        via the reportAnomalyFeedback API.

        Args:
            monitor_urn: The monitor URN
            assertion_urn: The assertion URN
            run_event_timestamp_ms: The timestamp of the assertion run event
        """
        # For new anomalies, we use the run event timestamp as the identifier
        self.set_local_state(
            monitor_urn=monitor_urn,
            assertion_urn=assertion_urn,
            timestamp_ms=run_event_timestamp_ms,  # Use run event timestamp as key
            original_state=None,
            new_state="CONFIRMED",
            is_new=True,
            run_event_timestamp_ms=run_event_timestamp_ms,
        )

    def clear_local_edit(self, monitor_urn: str, timestamp_ms: int) -> bool:
        """Remove a local edit, reverting to original state.

        Returns:
            True if an edit was removed, False if not found
        """
        idx = self._find_edit_index(monitor_urn, timestamp_ms)
        if idx >= 0:
            self._data["edits"].pop(idx)
            self._save()
            return True
        return False

    def get_effective_state(
        self,
        monitor_urn: str,
        timestamp_ms: int,
        original_state: Optional[str],
    ) -> Optional[str]:
        """Get the effective state for an anomaly (local override or original).

        Args:
            monitor_urn: The monitor URN
            timestamp_ms: The anomaly timestamp in milliseconds
            original_state: The original state from the API

        Returns:
            The effective state (local override if present, else original)
        """
        idx = self._find_edit_index(monitor_urn, timestamp_ms)
        if idx >= 0:
            return self._data["edits"][idx].get("local_state")
        return original_state

    def get_local_edit(
        self, monitor_urn: str, timestamp_ms: int
    ) -> Optional[AnomalyEdit]:
        """Get a specific local edit if it exists."""
        idx = self._find_edit_index(monitor_urn, timestamp_ms)
        if idx >= 0:
            edit = self._data["edits"][idx]
            return AnomalyEdit(
                monitor_urn=edit["monitor_urn"],
                assertion_urn=edit["assertion_urn"],
                timestamp_ms=edit["timestamp_ms"],
                original_state=edit.get("original_state"),
                local_state=edit["local_state"],
                edited_at=edit["edited_at"],
                is_new=edit.get("is_new", False),
                run_event_timestamp_ms=edit.get("run_event_timestamp_ms"),
            )
        return None

    def get_pending_changes(self) -> list[AnomalyEdit]:
        """Get all pending local changes.

        Returns:
            List of AnomalyEdit objects for all local edits
        """
        return [
            AnomalyEdit(
                monitor_urn=e["monitor_urn"],
                assertion_urn=e["assertion_urn"],
                timestamp_ms=e["timestamp_ms"],
                original_state=e.get("original_state"),
                local_state=e["local_state"],
                edited_at=e["edited_at"],
                is_new=e.get("is_new", False),
                run_event_timestamp_ms=e.get("run_event_timestamp_ms"),
            )
            for e in self._data.get("edits", [])
        ]

    def get_new_anomalies(self) -> list[AnomalyEdit]:
        """Get all pending new anomaly creations.

        Returns:
            List of AnomalyEdit objects for new anomalies only
        """
        return [
            AnomalyEdit(
                monitor_urn=e["monitor_urn"],
                assertion_urn=e["assertion_urn"],
                timestamp_ms=e["timestamp_ms"],
                original_state=e.get("original_state"),
                local_state=e["local_state"],
                edited_at=e["edited_at"],
                is_new=True,
                run_event_timestamp_ms=e.get("run_event_timestamp_ms"),
            )
            for e in self._data.get("edits", [])
            if e.get("is_new", False)
        ]

    def get_new_anomaly_count(self) -> int:
        """Get the number of pending new anomaly creations."""
        return sum(1 for e in self._data.get("edits", []) if e.get("is_new", False))

    def get_pending_count(self) -> int:
        """Get the number of pending local changes."""
        return len(self._data.get("edits", []))

    def clear_all_edits(self) -> int:
        """Remove all local edits.

        Returns:
            Number of edits cleared
        """
        count = len(self._data.get("edits", []))
        self._data["edits"] = []
        self._save()
        return count

    def mark_as_published(self, edits: list[AnomalyEdit]) -> int:
        """Mark edits as published by removing them from local storage.

        Args:
            edits: List of edits that were successfully published

        Returns:
            Number of edits removed
        """
        removed = 0
        for edit in edits:
            if self.clear_local_edit(edit.monitor_urn, edit.timestamp_ms):
                removed += 1
        return removed

    def get_edits_in_range(
        self,
        start_ms: int,
        end_ms: int,
        monitor_urn: Optional[str] = None,
    ) -> list[AnomalyEdit]:
        """Get all edits within a time range.

        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            monitor_urn: Optional monitor URN to filter by

        Returns:
            List of matching edits
        """
        result = []
        for e in self._data.get("edits", []):
            ts = e.get("timestamp_ms", 0)
            if start_ms <= ts <= end_ms:
                if monitor_urn is None or e.get("monitor_urn") == monitor_urn:
                    result.append(
                        AnomalyEdit(
                            monitor_urn=e["monitor_urn"],
                            assertion_urn=e["assertion_urn"],
                            timestamp_ms=e["timestamp_ms"],
                            original_state=e.get("original_state"),
                            local_state=e["local_state"],
                            edited_at=e["edited_at"],
                            is_new=e.get("is_new", False),
                            run_event_timestamp_ms=e.get("run_event_timestamp_ms"),
                        )
                    )
        return result

    def remove_new_anomaly(
        self,
        monitor_urn: str,
        run_event_timestamp_ms: int,
    ) -> bool:
        """Remove a new anomaly marking.

        Args:
            monitor_urn: The monitor URN
            run_event_timestamp_ms: The run event timestamp

        Returns:
            True if removed, False if not found
        """
        # For new anomalies, the timestamp_ms is the run_event_timestamp_ms
        return self.clear_local_edit(monitor_urn, run_event_timestamp_ms)


class EndpointRegistry:
    """Manages the global registry of known endpoints."""

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or get_cache_dir()
        self.registry_path = self.cache_dir / "endpoints.json"
        self._data = self._load()

    def _load(self) -> dict:
        """Load the registry from disk."""
        if self.registry_path.exists():
            with open(self.registry_path, "r") as f:
                return json.load(f)
        return {"version": "1.0", "endpoints": {}, "default_endpoint": None}

    def _save(self) -> None:
        """Save the registry to disk."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        with open(self.registry_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def list_endpoints(self) -> list[EndpointInfo]:
        """List all registered endpoints."""
        endpoints = []
        for hostname, info in self._data.get("endpoints", {}).items():
            endpoints.append(
                EndpointInfo(
                    hostname=hostname,
                    alias=info.get("alias", hostname),
                    url=info.get("url", f"https://{hostname}"),
                    last_used=info.get("last_used"),
                    cache_size_mb=info.get("cache_size_mb", 0.0),
                    event_count=info.get("event_count", 0),
                )
            )
        return endpoints

    def get_endpoint(self, hostname: str) -> Optional[EndpointInfo]:
        """Get endpoint info by hostname."""
        info = self._data.get("endpoints", {}).get(hostname)
        if info:
            return EndpointInfo(
                hostname=hostname,
                alias=info.get("alias", hostname),
                url=info.get("url", f"https://{hostname}"),
                last_used=info.get("last_used"),
                cache_size_mb=info.get("cache_size_mb", 0.0),
                event_count=info.get("event_count", 0),
            )
        return None

    def add_endpoint(self, url: str, alias: Optional[str] = None) -> EndpointInfo:
        """Register a new endpoint."""
        hostname = url_to_hostname(url)
        if alias is None:
            alias = hostname

        self._data["endpoints"][hostname] = {
            "alias": alias,
            "url": url,
            "last_used": datetime.now(timezone.utc).isoformat(),
            "cache_size_mb": 0.0,
            "event_count": 0,
        }
        self._save()
        return self.get_endpoint(hostname)  # type: ignore

    def update_endpoint(self, hostname: str, **kwargs: Any) -> None:
        """Update endpoint info."""
        if hostname in self._data.get("endpoints", {}):
            self._data["endpoints"][hostname].update(kwargs)
            self._save()

    def remove_endpoint(self, hostname: str) -> bool:
        """Remove an endpoint from the registry."""
        if hostname in self._data.get("endpoints", {}):
            del self._data["endpoints"][hostname]
            if self._data.get("default_endpoint") == hostname:
                self._data["default_endpoint"] = None
            self._save()
            return True
        return False

    def get_default_endpoint(self) -> Optional[str]:
        """Get the default endpoint hostname."""
        return self._data.get("default_endpoint")

    def set_default_endpoint(self, hostname: str) -> bool:
        """Set the default endpoint."""
        if hostname in self._data.get("endpoints", {}):
            self._data["default_endpoint"] = hostname
            self._save()
            return True
        return False


class CacheIndex:
    """Manages the cache index for a single endpoint."""

    def __init__(self, endpoint_dir: Path):
        self.endpoint_dir = endpoint_dir
        self.index_path = endpoint_dir / "cache_index.json"
        self._data: Optional[CacheIndexData] = None

    def _load(self) -> CacheIndexData:
        """Load the cache index from disk."""
        if self.index_path.exists():
            with open(self.index_path, "r") as f:
                data = json.load(f)

            aspects = {}
            for aspect_name, aspect_data in data.get("aspects", {}).items():
                aspects[aspect_name] = AspectCacheInfo.from_dict(aspect_data)

            return CacheIndexData(
                version=data.get("version", "2.0"),
                endpoint=data.get("endpoint", ""),
                aspects=aspects,
            )

        return CacheIndexData()

    @property
    def data(self) -> CacheIndexData:
        """Get the cache index data, loading from disk if necessary."""
        if self._data is None:
            self._data = self._load()
        return self._data

    def save(self) -> None:
        """Save the cache index to disk in v2.0 format."""
        self.endpoint_dir.mkdir(parents=True, exist_ok=True)

        # Always save in v2.0 format
        aspects_data = {}
        for aspect_name, info in self.data.aspects.items():
            if isinstance(info, AspectCacheInfo):
                aspects_data[aspect_name] = info.to_dict()
            elif isinstance(info, dict):
                aspects_data[aspect_name] = info

        data = {
            "version": "2.0",
            "endpoint": self.data.endpoint,
            "aspects": aspects_data,
        }

        with open(self.index_path, "w") as f:
            json.dump(data, f, indent=2)

    def update_aspect_from_dataframe(
        self,
        aspect_name: str,
        entity_type: str,
        df: pd.DataFrame,
        cache_path: str,
        sync_type: str = "full",
    ) -> None:
        """Update cache index for a specific aspect based on DataFrame contents."""
        now = datetime.now(timezone.utc).isoformat()

        # Get or create aspect info
        existing = self.data.get_aspect_info(aspect_name)
        if existing:
            info = existing
        else:
            info = AspectCacheInfo(
                aspect_name=aspect_name,
                entity_type=entity_type,
                cache_path=cache_path,
            )

        if info.first_sync is None:
            info.first_sync = now
        info.last_sync = now
        info.event_count = len(df)
        info.cache_path = cache_path

        # Count unique entities based on entity type
        if entity_type == "monitor" and "monitorUrn" in df.columns:
            info.unique_entities = df["monitorUrn"].nunique()
        elif entity_type == "dataHubMetricCube" and "metricCubeUrn" in df.columns:
            info.unique_entities = df["metricCubeUrn"].nunique()
        elif "entityUrn" in df.columns:
            info.unique_entities = df["entityUrn"].nunique()

        # Update data range
        if "timestampMillis" in df.columns and len(df) > 0:
            ts_min = pd.to_datetime(df["timestampMillis"].min(), unit="ms")
            ts_max = pd.to_datetime(df["timestampMillis"].max(), unit="ms")
            info.data_range_start = ts_min.isoformat()
            info.data_range_end = ts_max.isoformat()

        # Add to sync history
        info.sync_history.append(
            {
                "timestamp": now,
                "events_added": len(df),
                "type": sync_type,
            }
        )
        info.sync_history = info.sync_history[-100:]

        self.data.set_aspect_info(aspect_name, info)
        self.save()


class EndpointCache:
    """Manages the cache for a single endpoint with multi-aspect support."""

    def __init__(self, hostname: str, cache_dir: Optional[Path] = None):
        self.hostname = hostname
        self.cache_dir = cache_dir or get_cache_dir()
        self.endpoint_dir = self.cache_dir / hostname_to_dir(hostname)
        self.timeseries_dir = self.endpoint_dir / "timeseries"
        self.preprocessings_dir = self.endpoint_dir / "preprocessings"
        self._index: Optional[CacheIndex] = None
        self._duckdb_conn: Optional[Any] = None  # Lazy-initialized DuckDB connection
        self._schema_cache: dict[str, set[str]] = {}  # aspect_name -> available columns

    @property
    def duckdb_conn(self) -> Any:
        """Get or create a reusable DuckDB connection.

        Returns an in-memory DuckDB connection that persists for the lifetime
        of this EndpointCache instance, avoiding connection creation overhead.
        """
        if self._duckdb_conn is None:
            try:
                import duckdb

                self._duckdb_conn = duckdb.connect(":memory:")
            except ImportError:
                raise ImportError("DuckDB is required for cache operations")
        return self._duckdb_conn

    def get_schema_columns(self, aspect_name: str) -> set[str]:
        """Get available columns for an aspect, using cache if available.

        Args:
            aspect_name: The timeseries aspect name

        Returns:
            Set of column names in the parquet file
        """
        if aspect_name in self._schema_cache:
            return self._schema_cache[aspect_name]

        # Query schema and cache it
        path = self.get_aspect_path(aspect_name)
        if not path.exists():
            return set()

        schema_query = f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        schema_df = self.duckdb_conn.execute(schema_query).fetchdf()
        columns = set(schema_df["column_name"].tolist())
        self._schema_cache[aspect_name] = columns
        return columns

    def invalidate_schema_cache(self, aspect_name: Optional[str] = None) -> None:
        """Invalidate schema cache after data changes.

        Args:
            aspect_name: If provided, invalidate only that aspect.
                        If None, invalidate all cached schemas.
        """
        if aspect_name:
            self._schema_cache.pop(aspect_name, None)
        else:
            self._schema_cache.clear()

    @property
    def index(self) -> CacheIndex:
        """Get the cache index for this endpoint."""
        if self._index is None:
            self._index = CacheIndex(self.endpoint_dir)
            self._index.data.endpoint = self.hostname
        return self._index

    def get_aspect_path(self, aspect_name: str) -> Path:
        """Get the parquet file path for a specific aspect.

        Structure:
            timeseries/monitor/monitorAnomalyEvent.parquet
            timeseries/assertion/assertionDryRunEvent.parquet
            timeseries/monitor/monitorAnomalyEvent.parquet
        """
        aspect_info = ALL_ASPECTS.get(aspect_name)
        if aspect_info:
            entity_type = str(aspect_info["entity_type"])
        else:
            entity_type = "monitor"  # Default

        return self.timeseries_dir / entity_type / f"{aspect_name}.parquet"

    def exists(self, aspect_name: str = "dataHubMetricCubeEvent") -> bool:
        """Check if the cache exists for a specific aspect."""
        return self.get_aspect_path(aspect_name).exists()

    def get_cache_size_mb(
        self, aspect_name: Optional[str] = None, include_all: bool = True
    ) -> float:
        """Get the size of the cache in MB.

        Args:
            aspect_name: If provided, returns size of just that aspect.
            include_all: If True and aspect_name is None, includes preprocessings
                         and training runs in the total size.

        If aspect_name is None, returns total size of all cached data.
        """
        if aspect_name:
            path = self.get_aspect_path(aspect_name)
            if path.exists():
                return path.stat().st_size / (1024 * 1024)
            return 0.0
        else:
            # Total size across all aspects
            total = 0.0
            if self.timeseries_dir.exists():
                for parquet_file in self.timeseries_dir.rglob("*.parquet"):
                    total += parquet_file.stat().st_size / (1024 * 1024)

            if include_all:
                # Include preprocessings
                if self.preprocessings_dir.exists():
                    for f in self.preprocessings_dir.iterdir():
                        if f.is_file():
                            total += f.stat().st_size / (1024 * 1024)

                # Include training runs
                if self.training_runs_dir.exists():
                    for run_dir in self.training_runs_dir.iterdir():
                        if run_dir.is_dir():
                            for f in run_dir.rglob("*"):
                                if f.is_file():
                                    total += f.stat().st_size / (1024 * 1024)

            return total

    def get_preprocessings_count(self) -> int:
        """Get the number of saved preprocessings."""
        if not self.preprocessings_dir.exists():
            return 0
        return len(list(self.preprocessings_dir.glob("*.parquet")))

    def get_training_runs_count(self) -> int:
        """Get the number of saved training runs."""
        if not self.training_runs_dir.exists():
            return 0
        return len([d for d in self.training_runs_dir.iterdir() if d.is_dir()])

    def save_aspect_events(
        self,
        aspect_name: str,
        df: pd.DataFrame,
        sync_type: str = "full",
    ) -> None:
        """Save events for a specific aspect to the cache.

        Args:
            aspect_name: The timeseries aspect name
            df: DataFrame containing raw events
            sync_type: 'full' for complete replacement, 'incremental' for append
        """
        if df is None or len(df) == 0:
            return

        path = self.get_aspect_path(aspect_name)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Determine entity URN column for deduplication
        entity_urn_col = None
        if "assertionUrn" in df.columns:
            entity_urn_col = "assertionUrn"
        elif "monitorUrn" in df.columns:
            entity_urn_col = "monitorUrn"
        elif "entityUrn" in df.columns:
            entity_urn_col = "entityUrn"

        if sync_type == "incremental" and path.exists():
            existing_df = pd.read_parquet(path)
            combined = pd.concat([existing_df, df], ignore_index=True)

            # Deduplicate
            if "timestampMillis" in combined.columns and entity_urn_col:
                combined = combined.drop_duplicates(
                    subset=["timestampMillis", entity_urn_col], keep="last"
                )
            df = combined

        df.to_parquet(path, index=False)

        # Invalidate schema cache since columns might have changed
        self.invalidate_schema_cache(aspect_name)

        # Update index
        aspect_info = ALL_ASPECTS.get(aspect_name, {"entity_type": "monitor"})
        self.index.update_aspect_from_dataframe(
            aspect_name=aspect_name,
            entity_type=str(aspect_info["entity_type"]),
            df=df,
            cache_path=str(path.relative_to(self.endpoint_dir)),
            sync_type=sync_type,
        )

    def list_entities_paginated(
        self,
        aspect_name: str,
        page: int = 0,
        page_size: int = 100,
        search_filter: Optional[str] = None,
        type_filter: Optional[str] = None,
        metric_filter: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """List entities with pagination using DuckDB for efficient aggregation.

        This method avoids loading all events into memory by using DuckDB's
        GROUP BY with LIMIT/OFFSET directly on the parquet file.

        Optimizations:
        - Single-pass query using window function for total count
        - Reusable DuckDB connection
        - Cached schema discovery

        Args:
            aspect_name: The timeseries aspect name (e.g., 'dataHubMetricCubeEvent')
            page: Page number (0-indexed)
            page_size: Number of results per page
            search_filter: Optional search string to filter by URN
            type_filter: Optional assertion type filter (e.g., 'FIELD', 'VOLUME')
            metric_filter: Optional metric name filter (e.g., 'NULL_COUNT', 'ROW_COUNT')

        Returns:
            Tuple of (list of entity dicts for the page, total count)
        """
        path = self.get_aspect_path(aspect_name)
        if not path.exists():
            return [], 0

        # Determine entity URN column based on aspect type
        is_monitor = aspect_name in MONITOR_ASPECTS
        is_metric_cube = aspect_name in METRIC_CUBE_ASPECTS
        if is_monitor:
            entity_urn_col = "monitorUrn"
        elif is_metric_cube:
            entity_urn_col = "metricCubeUrn"
        else:
            entity_urn_col = "assertionUrn"

        try:
            # Use cached schema and reusable connection
            available_cols = self.get_schema_columns(aspect_name)
            conn = self.duckdb_conn

            # Build WHERE conditions - only use columns that exist
            conditions = []
            if search_filter:
                escaped_filter = search_filter.replace("'", "''")
                # Simple search on entity URN column for all types
                conditions.append(f"{entity_urn_col} ILIKE '%{escaped_filter}%'")

            # Type filter only applies to assertion aspects (legacy)
            if type_filter and not is_monitor and not is_metric_cube:
                if "event_result_assertion_type" in available_cols:
                    escaped_type = type_filter.replace("'", "''")
                    conditions.append(f"event_result_assertion_type = '{escaped_type}'")

            # Metric filter only applies to assertion aspects (legacy)
            metric_col = (
                "event_result_assertion_fieldAssertion_fieldMetricAssertion_metric"
            )
            if metric_filter and not is_monitor and not is_metric_cube:
                if metric_col in available_cols:
                    escaped_metric = metric_filter.replace("'", "''")
                    conditions.append(f"{metric_col} = '{escaped_metric}'")

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # Build aggregation query based on entity type and available columns
            # Base select parts common to all entity types
            select_parts = [
                f"{entity_urn_col} as entity_urn",
                "COUNT(*) as point_count",
                "MIN(timestampMillis) as first_event_ms",
                "MAX(timestampMillis) as last_event_ms",
            ]

            if is_monitor:
                # For monitors, add assertion and entity URN
                if "assertionUrn" in available_cols:
                    select_parts.append("ANY_VALUE(assertionUrn) as assertion_urn")
                else:
                    select_parts.append("NULL as assertion_urn")

                if "entityUrn" in available_cols:
                    select_parts.append("ANY_VALUE(entityUrn) as monitored_entity_urn")
                else:
                    select_parts.append("NULL as monitored_entity_urn")

            elif is_metric_cube:
                # For metric cubes, add monitor and assertion URN
                if "monitorUrn" in available_cols:
                    select_parts.append("ANY_VALUE(monitorUrn) as monitor_urn")
                else:
                    select_parts.append("NULL as monitor_urn")

                if "assertionUrn" in available_cols:
                    select_parts.append("ANY_VALUE(assertionUrn) as assertion_urn")
                else:
                    select_parts.append("NULL as assertion_urn")

                # Add measure statistics for metric cubes
                if "measure" in available_cols:
                    select_parts.extend(
                        [
                            "MIN(CASE WHEN measure IS NOT NULL "
                            "THEN measure ELSE NULL END) as value_min",
                            "MAX(CASE WHEN measure IS NOT NULL "
                            "THEN measure ELSE NULL END) as value_max",
                            "AVG(CASE WHEN measure IS NOT NULL "
                            "THEN measure ELSE NULL END) as value_mean",
                        ]
                    )
                else:
                    select_parts.extend(
                        [
                            "NULL as value_min",
                            "NULL as value_max",
                            "NULL as value_mean",
                        ]
                    )

            else:
                # For assertions (legacy), aggregate with metadata
                if "asserteeUrn" in available_cols:
                    select_parts.append("ANY_VALUE(asserteeUrn) as assertee_urn")
                else:
                    select_parts.append("NULL as assertee_urn")

                if "event_result_assertion_type" in available_cols:
                    select_parts.append(
                        "ANY_VALUE(event_result_assertion_type) as assertion_type"
                    )
                else:
                    select_parts.append("NULL as assertion_type")

                metric_col = (
                    "event_result_assertion_fieldAssertion_fieldMetricAssertion_metric"
                )
                if metric_col in available_cols:
                    select_parts.append(f"ANY_VALUE({metric_col}) as metric_name")
                else:
                    select_parts.append("NULL as metric_name")

                field_col = "event_result_assertion_fieldAssertion_fieldMetricAssertion_field_path"
                if field_col in available_cols:
                    select_parts.append(f"ANY_VALUE({field_col}) as field_path")
                else:
                    select_parts.append("NULL as field_path")

                if "event_result_metric_value" in available_cols:
                    select_parts.extend(
                        [
                            "MIN(CASE WHEN event_result_metric_value IS NOT NULL "
                            "THEN event_result_metric_value ELSE NULL END) as value_min",
                            "MAX(CASE WHEN event_result_metric_value IS NOT NULL "
                            "THEN event_result_metric_value ELSE NULL END) as value_max",
                            "AVG(CASE WHEN event_result_metric_value IS NOT NULL "
                            "THEN event_result_metric_value ELSE NULL END) as value_mean",
                        ]
                    )
                else:
                    select_parts.extend(
                        [
                            "NULL as value_min",
                            "NULL as value_max",
                            "NULL as value_mean",
                        ]
                    )

            select_clause = ",\n                    ".join(select_parts)

            # Single-pass query using window function for total count
            # This avoids scanning the parquet file twice
            agg_query = f"""
                SELECT *
                FROM (
                    SELECT
                        {select_clause},
                        COUNT(*) OVER() as _total_groups
                    FROM (
                        SELECT *
                        FROM read_parquet('{path}')
                        WHERE {where_clause}
                    ) filtered
                    GROUP BY {entity_urn_col}
                    ORDER BY MAX(timestampMillis) DESC
                ) aggregated
                LIMIT {page_size} OFFSET {page * page_size}
            """

            result_df = conn.execute(agg_query).fetchdf()

            if len(result_df) == 0:
                return [], 0

            # Extract total count from window function result
            total_count = int(result_df["_total_groups"].iloc[0])

            # Remove the helper column and convert to list of dicts
            result_df = result_df.drop(columns=["_total_groups"])
            results: list[dict[str, Any]] = [
                {str(k): v for k, v in row.items()}
                for row in result_df.to_dict("records")
            ]
            return results, total_count

        except ImportError:
            # Fallback to pandas (less efficient, but works)
            df = pd.read_parquet(path)

            # Apply filters
            if search_filter:
                mask = df[entity_urn_col].str.contains(
                    search_filter, case=False, na=False
                )
                df = df[mask]

            # Type filter only applies to assertion aspects (legacy)
            if type_filter and not is_monitor and not is_metric_cube:
                if "event_result_assertion_type" in df.columns:
                    df = df[df["event_result_assertion_type"] == type_filter]

            if len(df) == 0:
                return [], 0

            # Get unique entities and count
            unique_urns = df[entity_urn_col].dropna().unique()
            total_count = len(unique_urns)

            # Paginate unique URNs
            start_idx = page * page_size
            end_idx = start_idx + page_size
            page_urns = unique_urns[start_idx:end_idx]

            fallback_results: list[dict[str, Any]] = []
            for urn in page_urns:
                entity_df = df[df[entity_urn_col] == urn]

                # Base result fields common to all entity types
                result: dict[str, Any] = {
                    "entity_urn": urn,
                    "point_count": len(entity_df),
                    "first_event_ms": entity_df["timestampMillis"].min()
                    if "timestampMillis" in entity_df.columns
                    else None,
                    "last_event_ms": entity_df["timestampMillis"].max()
                    if "timestampMillis" in entity_df.columns
                    else None,
                }

                if is_monitor:
                    result["assertion_urn"] = (
                        entity_df["assertionUrn"].iloc[0]
                        if "assertionUrn" in entity_df.columns
                        else None
                    )
                    result["monitored_entity_urn"] = (
                        entity_df["entityUrn"].iloc[0]
                        if "entityUrn" in entity_df.columns
                        else None
                    )
                elif is_metric_cube:
                    result["monitor_urn"] = (
                        entity_df["monitorUrn"].iloc[0]
                        if "monitorUrn" in entity_df.columns
                        else None
                    )
                    result["assertion_urn"] = (
                        entity_df["assertionUrn"].iloc[0]
                        if "assertionUrn" in entity_df.columns
                        else None
                    )
                    if "measure" in entity_df.columns:
                        measure_col = entity_df["measure"].dropna()
                        result["value_min"] = (
                            measure_col.min() if len(measure_col) > 0 else None
                        )
                        result["value_max"] = (
                            measure_col.max() if len(measure_col) > 0 else None
                        )
                        result["value_mean"] = (
                            measure_col.mean() if len(measure_col) > 0 else None
                        )
                    else:
                        result["value_min"] = None
                        result["value_max"] = None
                        result["value_mean"] = None
                else:
                    # Legacy assertion fallback
                    result["assertee_urn"] = (
                        entity_df["asserteeUrn"].iloc[0]
                        if "asserteeUrn" in entity_df.columns
                        else None
                    )
                    result["assertion_type"] = (
                        entity_df["event_result_assertion_type"].iloc[0]
                        if "event_result_assertion_type" in entity_df.columns
                        else None
                    )
                    result["metric_name"] = None
                    result["field_path"] = None
                    result["value_min"] = None
                    result["value_max"] = None
                    result["value_mean"] = None

                fallback_results.append(result)

            return fallback_results, total_count

    def load_aspect_events(
        self,
        aspect_name: str,
        entity_urn: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        columns: Optional[list[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Load events for a specific aspect with optional filtering.

        Args:
            aspect_name: The timeseries aspect name
            entity_urn: Filter to a specific entity (assertion or monitor)
            start_time: Filter events after this time
            end_time: Filter events before this time
            columns: Optional list of columns to select. If None, selects all.
                    For better performance, specify only the columns you need.

        Returns:
            DataFrame with filtered events, or None if cache doesn't exist
        """
        path = self.get_aspect_path(aspect_name)
        if not path.exists():
            return None

        # Determine entity URN column based on aspect type
        entity_urn_col = "assertionUrn"
        if aspect_name in MONITOR_ASPECTS:
            entity_urn_col = "monitorUrn"
        elif aspect_name in METRIC_CUBE_ASPECTS:
            entity_urn_col = "metricCubeUrn"

        # Use DuckDB for efficient filtering if available
        try:
            conn = self.duckdb_conn

            conditions = []
            if entity_urn:
                escaped_urn = entity_urn.replace("'", "''")
                conditions.append(f"{entity_urn_col} = '{escaped_urn}'")
            if start_time:
                ts_ms = int(start_time.timestamp() * 1000)
                conditions.append(f"timestampMillis >= {ts_ms}")
            if end_time:
                ts_ms = int(end_time.timestamp() * 1000)
                conditions.append(f"timestampMillis <= {ts_ms}")

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # Column projection - only read columns we need
            if columns:
                # Validate columns exist in schema
                available_cols = self.get_schema_columns(aspect_name)
                valid_cols = [c for c in columns if c in available_cols]
                if not valid_cols:
                    # Fall back to all columns if none of the requested exist
                    select_clause = "*"
                else:
                    select_clause = ", ".join(valid_cols)
            else:
                select_clause = "*"

            query = f"SELECT {select_clause} FROM read_parquet('{path}') WHERE {where_clause}"

            return conn.execute(query).fetchdf()
        except ImportError:
            # Fall back to pandas
            df = pd.read_parquet(path, columns=columns)

            if entity_urn and entity_urn_col in df.columns:
                df = df[df[entity_urn_col] == entity_urn]
            if start_time and "timestampMillis" in df.columns:
                ts_ms = int(start_time.timestamp() * 1000)
                df = df[df["timestampMillis"] >= ts_ms]
            if end_time and "timestampMillis" in df.columns:
                ts_ms = int(end_time.timestamp() * 1000)
                df = df[df["timestampMillis"] <= ts_ms]

            return df

    def update_anomaly_events_after_publish(
        self,
        published_changes: list,
        aspect_name: str = "monitorAnomalyEvent",
    ) -> int:
        """Update the local cache after successfully publishing anomaly changes.

        - For DELETE: Remove the event from the cache
        - For state changes (CONFIRMED/REJECTED): Update the event's state
        - For NEW anomalies (is_new=True): Add new rows to the cache

        Args:
            published_changes: List of AnomalyEdit objects that were published
            aspect_name: The aspect name (default: monitorAnomalyEvent)

        Returns:
            Number of cache entries modified
        """
        path = self.get_aspect_path(aspect_name)

        # Load existing data or create empty DataFrame
        if path.exists():
            df = pd.read_parquet(path)
        else:
            df = pd.DataFrame()

        modified_count = 0

        # Build a set of (monitorUrn, timestampMillis) to delete
        deletes: set[tuple[str, int]] = set()
        # Build a dict of (monitorUrn, timestampMillis) -> new_state for updates
        updates: dict[tuple[str, int], str] = {}
        # Build list of new anomalies to add
        new_rows: list[dict] = []

        for change in published_changes:
            if change.is_new and change.run_event_timestamp_ms:
                # New anomaly - add to cache
                new_rows.append(
                    {
                        "monitorUrn": change.monitor_urn,
                        "timestampMillis": int(
                            datetime.now(timezone.utc).timestamp() * 1000
                        ),
                        "state": change.local_state,
                        "source_type": "USER_FEEDBACK",
                        "source_sourceUrn": change.assertion_urn,
                        "source_sourceEventTimestampMillis": change.run_event_timestamp_ms,
                    }
                )
            elif change.local_state == "DELETE":
                key = (change.monitor_urn, change.timestamp_ms)
                deletes.add(key)
            else:
                key = (change.monitor_urn, change.timestamp_ms)
                updates[key] = change.local_state

        # Process deletes
        if deletes and len(df) > 0:
            if "monitorUrn" in df.columns and "timestampMillis" in df.columns:
                original_len = len(df)
                df = df[
                    ~df.apply(
                        lambda row: (row["monitorUrn"], row["timestampMillis"])
                        in deletes,
                        axis=1,
                    )
                ]
                modified_count += original_len - len(df)

        # Process state updates
        if updates and len(df) > 0:
            if "monitorUrn" in df.columns and "timestampMillis" in df.columns:
                for (monitor_urn, timestamp_ms), new_state in updates.items():
                    mask = (df["monitorUrn"] == monitor_urn) & (
                        df["timestampMillis"] == timestamp_ms
                    )
                    if mask.any():
                        df.loc[mask, "state"] = new_state
                        modified_count += 1

        # Process new anomalies
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            df = pd.concat([df, new_df], ignore_index=True)
            modified_count += len(new_rows)

        # Save back to parquet and update index
        if modified_count > 0:
            # Ensure directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, index=False)
            # Update the cache index with new counts
            self.index.update_aspect_from_dataframe(
                aspect_name, "monitor", df, str(path), sync_type="update"
            )

        return modified_count

    def list_cached_aspects(self) -> list[str]:
        """List all aspects that have cached data."""
        aspects = []
        if self.timeseries_dir.exists():
            for parquet_file in self.timeseries_dir.rglob("*.parquet"):
                aspect_name = parquet_file.stem
                if aspect_name not in aspects:
                    aspects.append(aspect_name)
        return aspects

    def delete_entity_events(
        self, entity_urn: str, aspect_name: str = "dataHubMetricCubeEvent"
    ) -> int:
        """Delete all cached events for a specific entity.

        Args:
            entity_urn: The URN of the entity to delete events for
            aspect_name: The aspect to delete from

        Returns:
            Number of events deleted
        """
        path = self.get_aspect_path(aspect_name)
        if not path.exists():
            return 0

        # Determine the URN column based on aspect type
        if aspect_name in MONITOR_ASPECTS:
            entity_urn_col = "monitorUrn"
        elif aspect_name in METRIC_CUBE_ASPECTS:
            entity_urn_col = "metricCubeUrn"
        else:
            entity_urn_col = "assertionUrn"

        try:
            conn = self.duckdb_conn

            # Count events to delete and remaining
            count_query = f"""
                SELECT
                    SUM(CASE WHEN {entity_urn_col} = ? THEN 1 ELSE 0 END) as to_delete,
                    SUM(CASE WHEN {entity_urn_col} != ? THEN 1 ELSE 0 END) as remaining
                FROM read_parquet('{path}')
            """
            count_result = conn.execute(
                count_query, [entity_urn, entity_urn]
            ).fetchone()
            deleted_count = count_result[0] if count_result and count_result[0] else 0
            remaining_count = count_result[1] if count_result and count_result[1] else 0

            if deleted_count == 0:
                return 0

            # If no events remaining, delete the file entirely
            if remaining_count == 0:
                path.unlink()
                self._remove_aspect_from_index(aspect_name)
                # Invalidate schema cache since file is gone
                self.invalidate_schema_cache(aspect_name)
                return int(deleted_count)

            # Create new parquet without the entity's events
            temp_path = path.with_suffix(".tmp.parquet")
            filter_query = f"""
                COPY (
                    SELECT * FROM read_parquet('{path}')
                    WHERE {entity_urn_col} != ?
                ) TO '{temp_path}' (FORMAT PARQUET)
            """
            conn.execute(filter_query, [entity_urn])

            # Replace old file with new one
            path.unlink()
            temp_path.rename(path)

            # Update index and invalidate schema cache
            self._update_index_after_delete(aspect_name)
            self.invalidate_schema_cache(aspect_name)

            return int(deleted_count)
        except Exception as e:
            logger.warning("Failed to delete entity events: %s", e)
            # Fallback to pandas if DuckDB fails
            try:
                df = pd.read_parquet(path)
                original_count = len(df)
                df = df[df[entity_urn_col] != entity_urn]
                deleted_count = original_count - len(df)

                if deleted_count > 0:
                    if len(df) == 0:
                        # No events remaining, delete the file
                        path.unlink()
                        self._remove_aspect_from_index(aspect_name)
                    else:
                        df.to_parquet(path, index=False)
                        self._update_index_after_delete(aspect_name)
                    # Invalidate schema cache after any changes
                    self.invalidate_schema_cache(aspect_name)

                return deleted_count
            except Exception as e2:
                logger.exception("Fallback delete also failed: %s", e2)
                return 0

    def _update_index_after_delete(self, aspect_name: str) -> None:
        """Update the cache index after deleting events."""
        path = self.get_aspect_path(aspect_name)
        if not path.exists():
            self._remove_aspect_from_index(aspect_name)
            return

        try:
            conn = self.duckdb_conn

            # Determine entity URN column
            entity_urn_col = "assertionUrn"
            if aspect_name in MONITOR_ASPECTS:
                entity_urn_col = "monitorUrn"
            elif aspect_name in METRIC_CUBE_ASPECTS:
                entity_urn_col = "metricCubeUrn"

            # Get updated stats
            stats_query = f"""
                SELECT
                    COUNT(*) as event_count,
                    COUNT(DISTINCT {entity_urn_col}) as unique_entities
                FROM read_parquet('{path}')
            """
            result = conn.execute(stats_query).fetchone()

            if result:
                event_count = result[0]
                unique_entities = result[1]

                # If no events remain, remove aspect from index and delete file
                if event_count == 0:
                    path.unlink()
                    self._remove_aspect_from_index(aspect_name)
                else:
                    aspect_info = self.index.data.get_aspect_info(aspect_name)
                    if aspect_info:
                        aspect_info.event_count = event_count
                        aspect_info.unique_entities = unique_entities
                        self.index.save()
        except Exception as e:
            logger.warning("Failed to update index after delete: %s", e)

    def _remove_aspect_from_index(self, aspect_name: str) -> None:
        """Remove an aspect from the cache index entirely."""
        if aspect_name in self.index.data.aspects:
            del self.index.data.aspects[aspect_name]
            self.index.save()

    def clear(self, aspect_name: Optional[str] = None) -> None:
        """Clear the cache for this endpoint.

        Args:
            aspect_name: If provided, clear only this aspect. Otherwise clear all.
        """
        if aspect_name:
            path = self.get_aspect_path(aspect_name)
            if path.exists():
                path.unlink()
            # Invalidate schema cache for this aspect
            self.invalidate_schema_cache(aspect_name)
        else:
            # Clear everything
            if self.endpoint_dir.exists():
                shutil.rmtree(self.endpoint_dir)
            # Invalidate all schema caches
            self.invalidate_schema_cache()
        self._index = None

    # =========================================================================
    # Preprocessing Storage Methods
    # =========================================================================

    def save_preprocessing(
        self,
        preprocessing_id: str,
        df: pd.DataFrame,
        metadata: Optional[dict] = None,
    ) -> None:
        """Save a preprocessed DataFrame with an ID.

        Args:
            preprocessing_id: Unique identifier for this preprocessing
            df: Preprocessed DataFrame with 'ds' and 'y' columns
            metadata: Optional metadata (source assertion, config summary, etc.)
        """
        self.preprocessings_dir.mkdir(parents=True, exist_ok=True)

        # Save the DataFrame
        parquet_path = self.preprocessings_dir / f"{preprocessing_id}.parquet"
        df.to_parquet(parquet_path, index=False)

        # Save metadata
        meta_path = self.preprocessings_dir / f"{preprocessing_id}.meta.json"
        meta = metadata or {}
        meta.update(
            {
                "preprocessing_id": preprocessing_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "row_count": len(df),
            }
        )
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

    def load_preprocessing(self, preprocessing_id: str) -> Optional[pd.DataFrame]:
        """Load a saved preprocessing by ID.

        Args:
            preprocessing_id: The preprocessing ID to load

        Returns:
            DataFrame with 'ds' and 'y' columns, or None if not found
        """
        parquet_path = self.preprocessings_dir / f"{preprocessing_id}.parquet"
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
        return None

    def get_preprocessing_metadata(self, preprocessing_id: str) -> Optional[dict]:
        """Get metadata for a saved preprocessing.

        Args:
            preprocessing_id: The preprocessing ID

        Returns:
            Metadata dict or None if not found
        """
        meta_path = self.preprocessings_dir / f"{preprocessing_id}.meta.json"
        if meta_path.exists():
            with open(meta_path, "r") as f:
                return json.load(f)
        return None

    def list_saved_preprocessings(
        self, assertion_urn: Optional[str] = None
    ) -> list[dict]:
        """List all saved preprocessings for this endpoint.

        Args:
            assertion_urn: Optional filter to show only preprocessings for this assertion

        Returns:
            List of dicts with preprocessing_id and metadata
        """
        preprocessings: list[dict[str, object]] = []
        if not self.preprocessings_dir.exists():
            return preprocessings

        for parquet_file in self.preprocessings_dir.glob("*.parquet"):
            preprocessing_id = parquet_file.stem
            meta = self.get_preprocessing_metadata(preprocessing_id)

            # Filter by assertion URN if specified
            if assertion_urn and meta:
                source_assertion = meta.get("source_assertion_urn")
                if source_assertion and source_assertion != assertion_urn:
                    continue

            preprocessings.append(
                {
                    "preprocessing_id": preprocessing_id,
                    "created_at": meta.get("created_at") if meta else None,
                    "row_count": meta.get("row_count") if meta else None,
                    "source_assertion_urn": meta.get("source_assertion_urn")
                    if meta
                    else None,
                    "metadata": meta,
                }
            )

        # Sort by created_at descending (newest first)
        preprocessings.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
        return preprocessings

    def delete_preprocessing(self, preprocessing_id: str) -> bool:
        """Delete a saved preprocessing.

        Args:
            preprocessing_id: The preprocessing ID to delete

        Returns:
            True if deleted, False if not found
        """
        parquet_path = self.preprocessings_dir / f"{preprocessing_id}.parquet"
        meta_path = self.preprocessings_dir / f"{preprocessing_id}.meta.json"

        deleted = False
        if parquet_path.exists():
            parquet_path.unlink()
            deleted = True
        if meta_path.exists():
            meta_path.unlink()
            deleted = True

        return deleted

    def preprocessing_exists(self, preprocessing_id: str) -> bool:
        """Check if a preprocessing ID already exists.

        Args:
            preprocessing_id: The preprocessing ID to check

        Returns:
            True if exists, False otherwise
        """
        parquet_path = self.preprocessings_dir / f"{preprocessing_id}.parquet"
        return parquet_path.exists()

    # =========================================================================
    # Training Run Storage Methods
    # =========================================================================

    @property
    def training_runs_dir(self) -> Path:
        """Directory for storing training runs."""
        return self.endpoint_dir / "training_runs"

    def save_training_run(
        self,
        run_id: str,
        model_key: str,
        model_name: str,
        preprocessing_id: str,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        forecast: pd.DataFrame,
        metrics: dict[str, float],
        color: str,
        dash: Optional[str],
        assertion_urn: Optional[str] = None,
        is_observe_model: bool = False,
        registry_key: Optional[str] = None,
    ) -> bool:
        """Save a training run to disk.

        Note: The model object itself is NOT persisted. Only metadata and
        DataFrames are saved. To use the model for predictions, re-train it.

        Args:
            run_id: Unique identifier for this run
            model_key: Key identifying the model type
            model_name: Display name of the model
            preprocessing_id: ID of the preprocessing used
            train_df: Training DataFrame
            test_df: Test DataFrame
            forecast: Forecast DataFrame
            metrics: Dict of metric name to value
            color: Color for visualization
            dash: Dash style for visualization
            assertion_urn: Optional assertion URN
            is_observe_model: Whether from observe-models registry
            registry_key: Key in observe-models registry

        Returns:
            True if saved successfully, False otherwise
        """
        run_dir = self.training_runs_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Save DataFrames as parquet
            train_df.to_parquet(run_dir / "train.parquet", index=False)
            test_df.to_parquet(run_dir / "test.parquet", index=False)
            forecast.to_parquet(run_dir / "forecast.parquet", index=False)

            # Save metadata (model object is NOT persisted for security/compatibility)
            metadata = {
                "run_id": run_id,
                "model_key": model_key,
                "model_name": model_name,
                "preprocessing_id": preprocessing_id,
                "metrics": metrics,
                "color": color,
                "dash": dash,
                "assertion_urn": assertion_urn,
                "is_observe_model": is_observe_model,
                "registry_key": registry_key,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "train_rows": len(train_df),
                "test_rows": len(test_df),
            }
            with open(run_dir / "metadata.json", "w") as f:
                json.dump(metadata, f, indent=2)

            return True

        except Exception:
            # Clean up on failure
            if run_dir.exists():
                shutil.rmtree(run_dir)
            return False

    def load_training_run(self, run_id: str) -> Optional[dict]:
        """Load a saved training run by ID.

        Note: The model object is NOT loaded (not persisted for security).
        Only metadata and DataFrames are returned.

        Args:
            run_id: The training run ID to load

        Returns:
            Dict with training run data (model=None), or None if not found
        """
        run_dir = self.training_runs_dir / run_id
        if not run_dir.exists():
            return None

        try:
            # Load metadata
            meta_path = run_dir / "metadata.json"
            if not meta_path.exists():
                return None

            with open(meta_path, "r") as f:
                metadata = json.load(f)

            # Load DataFrames
            train_df = pd.read_parquet(run_dir / "train.parquet")
            test_df = pd.read_parquet(run_dir / "test.parquet")
            forecast = pd.read_parquet(run_dir / "forecast.parquet")

            return {
                "run_id": metadata["run_id"],
                "model_key": metadata["model_key"],
                "model_name": metadata["model_name"],
                "preprocessing_id": metadata["preprocessing_id"],
                "train_df": train_df,
                "test_df": test_df,
                "forecast": forecast,
                "model": None,  # Model is not persisted
                "metrics": metadata["metrics"],
                "color": metadata["color"],
                "dash": metadata.get("dash"),
                "assertion_urn": metadata.get("assertion_urn"),
                "is_observe_model": metadata.get("is_observe_model", False),
                "registry_key": metadata.get("registry_key"),
                "timestamp": datetime.fromisoformat(metadata["created_at"]),
            }

        except Exception:
            return None

    def list_saved_training_runs(
        self, assertion_urn: Optional[str] = None
    ) -> list[dict]:
        """List all saved training runs.

        Args:
            assertion_urn: Optional filter by assertion URN

        Returns:
            List of dicts with run metadata (without DataFrames)
        """
        runs: list[dict] = []

        if not self.training_runs_dir.exists():
            return runs

        for run_dir in self.training_runs_dir.iterdir():
            if not run_dir.is_dir():
                continue

            meta_path = run_dir / "metadata.json"
            if meta_path.exists():
                try:
                    with open(meta_path, "r") as f:
                        metadata = json.load(f)

                    # Filter by assertion URN if specified
                    if assertion_urn and metadata.get("assertion_urn") != assertion_urn:
                        continue

                    runs.append(
                        {
                            "run_id": metadata["run_id"],
                            "model_key": metadata["model_key"],
                            "model_name": metadata["model_name"],
                            "preprocessing_id": metadata["preprocessing_id"],
                            "metrics": metadata["metrics"],
                            "assertion_urn": metadata.get("assertion_urn"),
                            "is_observe_model": metadata.get("is_observe_model", False),
                            "registry_key": metadata.get("registry_key"),
                            "created_at": metadata.get("created_at"),
                            "train_rows": metadata.get("train_rows", 0),
                            "test_rows": metadata.get("test_rows", 0),
                        }
                    )
                except Exception:
                    continue

        # Sort by creation time (newest first)
        runs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return runs

    def delete_training_run(self, run_id: str) -> bool:
        """Delete a saved training run.

        Args:
            run_id: The training run ID to delete

        Returns:
            True if deleted, False if not found
        """
        run_dir = self.training_runs_dir / run_id
        if run_dir.exists():
            shutil.rmtree(run_dir)
            return True
        return False

    def training_run_exists(self, run_id: str) -> bool:
        """Check if a training run ID already exists.

        Args:
            run_id: The training run ID to check

        Returns:
            True if exists, False otherwise
        """
        run_dir = self.training_runs_dir / run_id
        return run_dir.exists() and (run_dir / "metadata.json").exists()


class RunEventCache:
    """High-level interface for managing timeseries caches across all endpoints."""

    def __init__(self, cache_dir: Optional[Path] = None):
        self.cache_dir = cache_dir or get_cache_dir()
        self.registry = EndpointRegistry(self.cache_dir)

    def get_endpoint_cache(self, hostname: str) -> EndpointCache:
        """Get or create a cache for an endpoint."""
        return EndpointCache(hostname, self.cache_dir)

    def sync_from_parquet(
        self,
        hostname: str,
        parquet_path: Path,
        aspect_name: str = "dataHubMetricCubeEvent",
        sync_type: str = "full",
    ) -> None:
        """Sync events from a local parquet file.

        This is useful for loading data exported from DataHub or other sources.
        """
        df = pd.read_parquet(parquet_path)
        self.save_aspect_events(hostname, aspect_name, df, sync_type=sync_type)

    def save_aspect_events(
        self,
        hostname: str,
        aspect_name: str,
        df: pd.DataFrame,
        alias: Optional[str] = None,
        sync_type: str = "full",
    ) -> None:
        """Save events for a specific aspect to the cache.

        Args:
            hostname: The endpoint hostname
            aspect_name: The timeseries aspect name
            df: DataFrame containing events
            alias: Optional friendly name for the endpoint
            sync_type: 'full' to replace or 'incremental' to append
        """
        if df is None or len(df) == 0:
            return

        # Ensure endpoint is registered
        if not self.registry.get_endpoint(hostname):
            self.registry.add_endpoint(
                url=f"https://{hostname}",
                alias=alias or hostname,
            )
        elif alias:
            self.registry.update_endpoint(hostname, alias=alias)

        cache = self.get_endpoint_cache(hostname)
        cache.save_aspect_events(aspect_name, df, sync_type)

        # Update registry with cache stats
        self.registry.update_endpoint(
            hostname,
            last_used=datetime.now(timezone.utc).isoformat(),
            cache_size_mb=cache.get_cache_size_mb(),
            event_count=cache.index.data.get_total_event_count(),
        )

    def get_cached_aspect_events(
        self,
        hostname: str,
        aspect_name: str,
        **filter_kwargs: Any,
    ) -> Optional[pd.DataFrame]:
        """Load cached events for a specific aspect with optional filtering."""
        cache = self.get_endpoint_cache(hostname)
        return cache.load_aspect_events(aspect_name, **filter_kwargs)

    def clear_cache(
        self, hostname: Optional[str] = None, aspect_name: Optional[str] = None
    ) -> None:
        """Clear cache for a specific endpoint/aspect or all endpoints.

        Args:
            hostname: If provided, clear only this endpoint's cache.
            aspect_name: If provided, clear only this aspect.
        """
        if hostname:
            cache = self.get_endpoint_cache(hostname)
            cache.clear(aspect_name)
            self.registry.update_endpoint(
                hostname,
                cache_size_mb=cache.get_cache_size_mb(),
                event_count=cache.index.data.get_total_event_count(),
            )
        else:
            # Clear all endpoint caches
            for endpoint in self.registry.list_endpoints():
                cache = self.get_endpoint_cache(endpoint.hostname)
                cache.clear(aspect_name)

    def get_cache_stats(self) -> dict:
        """Get statistics about all cached data."""
        total_endpoints = 0
        total_size_mb = 0.0
        total_events = 0
        endpoints_data: dict = {}

        for endpoint in self.registry.list_endpoints():
            cache = self.get_endpoint_cache(endpoint.hostname)
            size_mb = cache.get_cache_size_mb()
            event_count = cache.index.data.get_total_event_count()

            # Get per-aspect stats
            aspects_stats: dict = {}
            for aspect_name in cache.list_cached_aspects():
                aspect_info = cache.index.data.get_aspect_info(aspect_name)
                if aspect_info:
                    aspects_stats[aspect_name] = {
                        "event_count": aspect_info.event_count,
                        "unique_entities": aspect_info.unique_entities,
                        "last_sync": aspect_info.last_sync,
                    }

            total_endpoints += 1
            total_size_mb += size_mb
            total_events += event_count
            endpoints_data[endpoint.hostname] = {
                "alias": endpoint.alias,
                "size_mb": size_mb,
                "event_count": event_count,
                "last_sync": cache.index.data.last_sync,
                "aspects": aspects_stats,
            }

        return {
            "total_endpoints": total_endpoints,
            "total_size_mb": total_size_mb,
            "total_events": total_events,
            "endpoints": endpoints_data,
        }
