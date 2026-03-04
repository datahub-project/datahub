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
    pass

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


def _parse_schema_file(schema_path: Path) -> Optional[DltSchemaInfo]:
    """Parse a dlt schema YAML file into a DltSchemaInfo."""
    try:
        suffix = schema_path.suffix.lower()
        with schema_path.open() as f:
            raw = json.load(f) if suffix == ".json" else (yaml.safe_load(f) or {})
    except Exception as e:
        logger.warning(f"Failed to read schema file {schema_path}: {e}")
        return None

    tables: List[DltTableInfo] = []
    for table_name, table_def in (raw.get("tables") or {}).items():
        if table_def is None:
            continue
        columns: List[DltColumnInfo] = []
        for col_name, col_def in (table_def.get("columns") or {}).items():
            if col_def is None:
                continue
            columns.append(
                DltColumnInfo(
                    name=col_name,
                    data_type=_dlt_type_to_display(col_def.get("data_type", "text")),
                    nullable=bool(col_def.get("nullable", True)),
                    primary_key=bool(col_def.get("primary_key", False)),
                    is_dlt_system_column=col_name in DLT_SYSTEM_COLUMNS,
                )
            )

        tables.append(
            DltTableInfo(
                table_name=table_name,
                write_disposition=table_def.get("write_disposition", "append"),
                parent_table=table_def.get("parent"),
                columns=columns,
                resource_name=table_def.get("resource"),
            )
        )

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

    def __init__(self, pipelines_dir: str) -> None:
        self.pipelines_dir = Path(pipelines_dir)
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

    # ------------------------------------------------------------------
    # Pipeline discovery
    # ------------------------------------------------------------------

    def list_pipeline_names(self) -> List[str]:
        """Return all pipeline names found in pipelines_dir."""
        if not self.pipelines_dir.exists():
            return []
        names = []
        for entry in self.pipelines_dir.iterdir():
            if entry.is_dir() and not entry.name.startswith("."):
                # A pipeline dir contains a schemas/ subdirectory
                if (entry / "schemas").is_dir():
                    names.append(entry.name)
        return sorted(names)

    # ------------------------------------------------------------------
    # Pipeline info
    # ------------------------------------------------------------------

    def get_pipeline_info(self, pipeline_name: str) -> Optional[DltPipelineInfo]:
        """
        Build a DltPipelineInfo for a single pipeline.

        Uses the dlt SDK when available; falls back to filesystem parsing.
        """
        if self._dlt_available:
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
                last_load_info=None,  # populated later via get_run_history if needed
            )
        except Exception as e:
            logger.warning(
                f"dlt SDK failed for pipeline '{pipeline_name}', falling back to filesystem: {e}"
            )
            return self._get_pipeline_info_from_filesystem(pipeline_name)

    def _schema_obj_to_info(
        self, schema_name: str, schema_obj: Any
    ) -> Optional[DltSchemaInfo]:
        """Convert a dlt Schema object to DltSchemaInfo."""
        try:
            tables: List[DltTableInfo] = []
            for table_name, table_def in (schema_obj.tables or {}).items():
                if table_def is None:
                    continue
                columns: List[DltColumnInfo] = []
                for col_name, col_def in (table_def.get("columns") or {}).items():
                    if col_def is None:
                        continue
                    columns.append(
                        DltColumnInfo(
                            name=col_name,
                            data_type=_dlt_type_to_display(
                                col_def.get("data_type", "text")
                            ),
                            nullable=bool(col_def.get("nullable", True)),
                            primary_key=bool(col_def.get("primary_key", False)),
                            is_dlt_system_column=col_name in DLT_SYSTEM_COLUMNS,
                        )
                    )
                tables.append(
                    DltTableInfo(
                        table_name=table_name,
                        write_disposition=table_def.get("write_disposition", "append"),
                        parent_table=table_def.get("parent"),
                        columns=columns,
                        resource_name=table_def.get("resource"),
                    )
                )
            return DltSchemaInfo(
                schema_name=schema_name,
                version=int(getattr(schema_obj, "version", 0) or 0),
                version_hash=getattr(schema_obj, "version_hash", "") or "",
                tables=tables,
            )
        except Exception as e:
            logger.warning(f"Could not convert schema '{schema_name}' from SDK: {e}")
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
            logger.warning(f"No schemas found for pipeline '{pipeline_name}'")

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
            except Exception:
                pass

        return DltPipelineInfo(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset_name,
            working_dir=str(pipeline_dir),
            pipelines_dir=str(self.pipelines_dir),
            schemas=schemas,
            last_load_info=None,
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
        return schemas

    # ------------------------------------------------------------------
    # Run history (requires dlt SDK + destination access)
    # ------------------------------------------------------------------

    def get_run_history(
        self, pipeline_name: str, start_time: Optional[datetime] = None
    ) -> List[DltLoadInfo]:
        """
        Query _dlt_loads from the destination to get run history.

        Requires dlt package and destination credentials in ~/.dlt/secrets.toml.
        Returns an empty list if anything fails — run history is opt-in.
        """
        if not self._dlt_available:
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
                f"Failed to query _dlt_loads for pipeline '{pipeline_name}': {e}"
            )
            return []
