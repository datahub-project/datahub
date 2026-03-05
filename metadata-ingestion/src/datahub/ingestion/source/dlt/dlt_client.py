"""
Client for reading dlt pipeline metadata.

Primary interface: dlt Python SDK (import dlt; attach to pipeline without running it).
Fallback: parse schema YAML files directly from the filesystem when dlt is not installed.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import yaml

from datahub.ingestion.source.dlt.data_classes import (
    DLT_SYSTEM_COLUMNS,
    DltColumnInfo,
    DltLoadInfo,
    DltPipelineInfo,
    DltSchemaInfo,
    DltTableInfo,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.dlt.dlt_report import DltSourceReport

logger = logging.getLogger(__name__)

# dlt types → DataHub-friendly display names
DLT_TYPE_MAP: Dict[str, str] = {
    "text": "string",
    "bigint": "long",
    "double": "double",
    "bool": "boolean",
    "timestamp": "timestamp",
    "date": "date",
    "time": "string",
    "binary": "bytes",
    "complex": "map",
    "decimal": "decimal",
    "wei": "long",
}


def _dlt_type_to_display(dlt_type: str) -> str:
    return DLT_TYPE_MAP.get(dlt_type, dlt_type)


def _parse_columns(columns_dict: Optional[Dict[str, Any]]) -> List[DltColumnInfo]:
    """Parse a {col_name: col_def} dict into DltColumnInfo objects."""
    result: List[DltColumnInfo] = []
    for col_name, col_def in (columns_dict or {}).items():
        if col_def is None:
            continue
        result.append(
            DltColumnInfo(
                name=col_name,
                data_type=_dlt_type_to_display(col_def.get("data_type", "text")),
                nullable=bool(col_def.get("nullable", True)),
                primary_key=bool(col_def.get("primary_key", False)),
                is_dlt_system_column=col_name in DLT_SYSTEM_COLUMNS,
            )
        )
    return result


def _parse_table(table_name: str, table_def: Dict[str, Any]) -> DltTableInfo:
    """Parse a single table dict entry into a DltTableInfo."""
    return DltTableInfo(
        table_name=table_name,
        write_disposition=table_def.get("write_disposition", "append"),
        parent_table=table_def.get("parent"),
        columns=_parse_columns(table_def.get("columns")),
        resource_name=table_def.get("resource"),
    )


def _parse_schema_file(schema_path: Path) -> Optional[DltSchemaInfo]:
    """Parse a dlt schema YAML file into a DltSchemaInfo."""
    try:
        suffix = schema_path.suffix.lower()
        with schema_path.open() as f:
            raw = json.load(f) if suffix == ".json" else (yaml.safe_load(f) or {})
    except Exception as e:
        logger.warning("Failed to read schema file %s: %s", schema_path, e)
        return None

    tables = [
        _parse_table(table_name, table_def)
        for table_name, table_def in (raw.get("tables") or {}).items()
        if table_def is not None
    ]

    return DltSchemaInfo(
        schema_name=raw.get("name", schema_path.stem.replace(".schema", "")),
        version=int(raw.get("version", 0)),
        version_hash=raw.get("version_hash", ""),
        tables=tables,
    )


class DltClient:
    """
    Reads dlt pipeline metadata from the local filesystem.

    Tries the dlt Python SDK first (richer metadata including run history).
    Falls back to parsing schema YAML files directly when dlt is not installed.
    """

    def __init__(
        self,
        pipelines_dir: str,
        report: Optional["DltSourceReport"] = None,
    ) -> None:
        self.pipelines_dir = Path(pipelines_dir)
        self.report = report
        self._dlt_available = self._check_dlt()

    def _check_dlt(self) -> bool:
        try:
            import dlt  # noqa: F401

            return True
        except ImportError:
            logger.info(
                "dlt package not installed; using filesystem-only schema reading."
            )
            return False

    @property
    def dlt_available(self) -> bool:
        """Whether the dlt Python package is installed and importable."""
        return self._dlt_available

    # ------------------------------------------------------------------
    # Pipeline discovery
    # ------------------------------------------------------------------

    def list_pipeline_names(self) -> List[str]:
        """Return all pipeline names found in pipelines_dir."""
        if not self.pipelines_dir.exists():
            return []
        return sorted(
            entry.name
            for entry in self.pipelines_dir.iterdir()
            if entry.is_dir()
            and not entry.name.startswith(".")
            and (entry / "schemas").is_dir()
        )

    # ------------------------------------------------------------------
    # Pipeline info
    # ------------------------------------------------------------------

    def get_pipeline_info(self, pipeline_name: str) -> Optional[DltPipelineInfo]:
        """
        Build a DltPipelineInfo for a single pipeline.

        Uses the dlt SDK when available; falls back to filesystem parsing.
        """
        if self.dlt_available:
            return self._get_pipeline_info_via_sdk(pipeline_name)
        return self._get_pipeline_info_from_filesystem(pipeline_name)

    def _get_pipeline_info_via_sdk(
        self, pipeline_name: str
    ) -> Optional[DltPipelineInfo]:
        """Attach to an existing pipeline via the dlt SDK and read metadata."""
        try:
            import dlt

            # Attaching to an existing pipeline does NOT trigger a run — it just
            # restores state from the working directory.
            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                pipelines_dir=str(self.pipelines_dir),
            )

            destination_name = ""
            if pipeline.destination is not None:
                # dlt 1.x uses destination_name attribute; fall back to __name__ for older versions
                destination_name = (
                    getattr(pipeline.destination, "destination_name", None)
                    or getattr(pipeline.destination, "__name__", None)
                    or ""
                )

            schemas: List[DltSchemaInfo] = []
            for schema_name, schema_obj in (pipeline.schemas or {}).items():
                schema_info = self._schema_obj_to_info(schema_name, schema_obj)
                if schema_info:
                    schemas.append(schema_info)

            # Fall back to filesystem schema reading if SDK returned nothing
            if not schemas:
                schemas = self._read_schemas_from_filesystem(pipeline_name)

            return DltPipelineInfo(
                pipeline_name=pipeline_name,
                destination=destination_name,
                dataset_name=pipeline.dataset_name or "",
                working_dir=pipeline.working_dir or "",
                pipelines_dir=str(self.pipelines_dir),
                schemas=schemas,
            )
        except Exception as e:
            logger.warning(
                "dlt SDK failed for pipeline '%s', falling back to filesystem: %s",
                pipeline_name,
                e,
            )
            if self.report is not None:
                self.report.warning(
                    title="dlt SDK unavailable",
                    message="Failed to attach to pipeline via dlt SDK; using filesystem fallback.",
                    context=pipeline_name,
                )
            return self._get_pipeline_info_from_filesystem(pipeline_name)

    def _schema_obj_to_info(
        self, schema_name: str, schema_obj: Any
    ) -> Optional[DltSchemaInfo]:
        """Convert a dlt Schema object to DltSchemaInfo.

        schema_obj is typed as Any because dlt is an optional runtime dependency
        and dlt.Schema is not importable at type-check time without a hard import.
        The dlt SDK exposes schema.tables as a plain dict, so _parse_table applies
        identically to both the SDK and filesystem paths.
        """
        try:
            tables = [
                _parse_table(table_name, table_def)
                for table_name, table_def in (schema_obj.tables or {}).items()
                if table_def is not None
            ]
            return DltSchemaInfo(
                schema_name=schema_name,
                version=int(getattr(schema_obj, "version", 0) or 0),
                version_hash=getattr(schema_obj, "version_hash", "") or "",
                tables=tables,
            )
        except Exception as e:
            logger.warning("Could not convert schema '%s' from SDK: %s", schema_name, e)
            if self.report is not None:
                self.report.warning(
                    title="Failed to read schema from dlt SDK",
                    message="Schema could not be converted from the dlt SDK object. Skipping.",
                    context=schema_name,
                )
                self.report.report_schema_read_error()
            return None

    def _get_pipeline_info_from_filesystem(
        self, pipeline_name: str
    ) -> Optional[DltPipelineInfo]:
        """Read pipeline info from schema YAML files without using the dlt SDK."""
        pipeline_dir = self.pipelines_dir / pipeline_name
        if not pipeline_dir.is_dir():
            return None

        schemas = self._read_schemas_from_filesystem(pipeline_name)
        if not schemas:
            logger.warning("No schemas found for pipeline '%s'", pipeline_name)

        # Try to read destination and dataset_name from state.json
        destination = ""
        dataset_name = ""
        state_file = pipeline_dir / "state.json"
        if state_file.exists():
            try:
                with state_file.open() as f:
                    state = json.load(f)
                raw_dest = (
                    state.get("destination_type", "")
                    or state.get("destination_name", "")
                    or ""
                )
                destination = raw_dest.rsplit(".", 1)[-1] if raw_dest else ""
                dataset_name = state.get("dataset_name", "")
            except Exception as e:
                logger.warning(
                    "Failed to parse state.json for pipeline '%s': %s",
                    pipeline_name,
                    e,
                )
        else:
            # state.json is absent — normal for pipelines that haven't completed a run.
            # destination and dataset_name will be empty; outlet lineage will not be constructed.
            logger.debug(
                "No state.json found for pipeline '%s'; destination info unavailable.",
                pipeline_name,
            )

        return DltPipelineInfo(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            working_dir=str(pipeline_dir),
            pipelines_dir=str(self.pipelines_dir),
            schemas=schemas,
        )

    def _read_schemas_from_filesystem(self, pipeline_name: str) -> List[DltSchemaInfo]:
        """Read all schema YAML files for a pipeline from disk."""
        schemas_dir = self.pipelines_dir / pipeline_name / "schemas"
        if not schemas_dir.is_dir():
            return []
        schemas = []
        for yaml_file in sorted(
            list(schemas_dir.glob("*.schema.yaml"))
            + list(schemas_dir.glob("*.schema.json"))
        ):
            schema = _parse_schema_file(yaml_file)
            if schema:
                schemas.append(schema)
            else:
                # _parse_schema_file logged the specific parse error; surface to report here
                # since _read_schemas_from_filesystem has access to self.report.
                if self.report is not None:
                    self.report.warning(
                        title="Failed to read schema file",
                        message="A schema YAML file could not be parsed. Tables from this schema will be skipped.",
                        context=str(yaml_file),
                    )
                    self.report.report_schema_read_error()
        return schemas

    # ------------------------------------------------------------------
    # Run history (requires dlt SDK + destination access)
    # ------------------------------------------------------------------

    def get_run_history(
        self,
        pipeline_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[List[DltLoadInfo]]:
        """
        Query _dlt_loads from the destination to get run history.

        Requires dlt package and destination credentials in ~/.dlt/secrets.toml.

        Args:
            pipeline_name: The dlt pipeline name.
            start_time: Only return loads inserted after this time.
            end_time: Only return loads inserted before this time.

        Returns:
            List[DltLoadInfo] — successful query (may be empty if no rows in time window)
            None              — hard failure (exception during query)

        Returns [] (not None) when dlt is not installed; the caller checks
        dlt_available separately to distinguish that case.
        """
        if not self.dlt_available:
            return []
        try:
            import dlt

            pipeline = dlt.pipeline(
                pipeline_name=pipeline_name,
                pipelines_dir=str(self.pipelines_dir),
            )

            loads: List[DltLoadInfo] = []
            with pipeline.sql_client() as client:
                query = "SELECT load_id, schema_name, status, inserted_at, schema_version_hash FROM _dlt_loads ORDER BY inserted_at DESC"
                with client.execute_query(query) as cursor:
                    for row in cursor.fetchall():
                        inserted_at = row[3]
                        if isinstance(inserted_at, str):
                            inserted_at = datetime.fromisoformat(inserted_at)
                        if inserted_at.tzinfo is None:
                            inserted_at = inserted_at.replace(tzinfo=timezone.utc)
                        if start_time and inserted_at < start_time:
                            continue
                        if end_time and inserted_at > end_time:
                            continue
                        loads.append(
                            DltLoadInfo(
                                load_id=str(row[0]),
                                schema_name=str(row[1]),
                                status=int(row[2]),
                                inserted_at=inserted_at,
                                schema_version_hash=str(row[4]) if row[4] else "",
                            )
                        )
            return loads
        except Exception as e:
            logger.warning(
                "Failed to query _dlt_loads for pipeline '%s': %s", pipeline_name, e
            )
            if self.report is not None:
                self.report.warning(
                    title="Run history query failed",
                    message="Exception while querying _dlt_loads. Check destination credentials and see logs.",
                    context=pipeline_name,
                )
            return None
