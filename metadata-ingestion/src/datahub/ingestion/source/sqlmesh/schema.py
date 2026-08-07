import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.sql.sql_types import (
    DATAHUB_FIELD_TYPE,
    resolve_snowflake_modified_type,
    resolve_sql_type,
)
from datahub.ingestion.source.sqlmesh.base import SqlmeshSourceBase
from datahub.ingestion.source.sqlmesh.compat import SqlmeshModel
from datahub.ingestion.source.sqlmesh.constants import (
    PROP_AUDITS,
    PROP_CRON,
    PROP_ENVIRONMENT,
    PROP_GATEWAY,
    PROP_GRAIN,
    PROP_MODEL_KIND,
    PROP_MODEL_NAME,
    PROP_PARTITIONED_BY,
    PROP_PHYSICAL_TABLE,
    PROP_START,
    PROP_TIME_COLUMN,
    PROP_WAREHOUSE,
    PROP_WAREHOUSE_INSTANCE,
    SNOWFLAKE_PLATFORM,
    UNKNOWN_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.models import (
    _EffectiveProjectConfig,
    parse_model_audits,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import NullTypeClass

logger = logging.getLogger(__name__)


class SchemaMixin(SqlmeshSourceBase):
    def _build_custom_properties(
        self,
        fqn: str,
        physical_name: Optional[str],
        effective: _EffectiveProjectConfig,
        model: "SqlmeshModel",
    ) -> Dict[str, str]:
        props: Dict[str, str] = {
            PROP_MODEL_NAME: fqn,
            PROP_ENVIRONMENT: effective.environment,
            PROP_WAREHOUSE: effective.target_platform or UNKNOWN_PLATFORM,
        }
        if effective.gateway:
            props[PROP_GATEWAY] = effective.gateway
        if physical_name:
            props[PROP_PHYSICAL_TABLE] = physical_name
        if effective.target_platform_instance:
            props[PROP_WAREHOUSE_INSTANCE] = effective.target_platform_instance
        kind = getattr(model, "kind", None)
        if kind is not None:
            props[PROP_MODEL_KIND] = str(kind)

        cron = getattr(model, "cron", None)
        if cron:
            props[PROP_CRON] = str(cron)

        start = getattr(model, "start", None)
        if start:
            props[PROP_START] = str(start)

        time_column = getattr(model, "time_column", None)
        if time_column is not None:
            # SQLMesh wraps the time column in an object exposing ``.column``;
            # older/other shapes stringify directly. Check the attribute rather
            # than swallowing every error so a real bug still surfaces.
            column = getattr(time_column, "column", None)
            props[PROP_TIME_COLUMN] = str(column if column is not None else time_column)

        model_name = str(getattr(model, "name", "?"))
        partitioned_by = self._extract_named_list(model, "partitioned_by", model_name)
        if partitioned_by:
            props[PROP_PARTITIONED_BY] = partitioned_by
        grains = self._extract_named_list(model, "grains", model_name)
        if grains:
            props[PROP_GRAIN] = grains

        audit_names = [audit.name for audit in parse_model_audits(model)]
        if audit_names:
            props[PROP_AUDITS] = ",".join(audit_names)

        return props

    def _extract_named_list(
        self, model: "SqlmeshModel", attr: str, model_name: str
    ) -> Optional[str]:
        # partitioned_by and grains are both lists of column-like objects
        # exposing ``.name``; extract them the same way.
        values = getattr(model, attr, None)
        if not values:
            return None
        try:
            names = [str(v.name) for v in values if hasattr(v, "name")]
        except Exception:
            # Best-effort enrichment: an unexpected shape just omits the
            # property, but warn (not debug) so a new SQLMesh version dropping
            # this surfaces without raising the log level.
            logger.warning(
                "Could not extract %s for %s", attr, model_name, exc_info=True
            )
            return None
        return ",".join(names) if names else None

    def _resolve_column_type(
        self, type_str: str, platform: str
    ) -> Optional[DATAHUB_FIELD_TYPE]:
        # ``resolve_sql_type`` consults a merged cross-platform mapping where the
        # last-registered platform wins on conflicts — so ``TIMESTAMP`` resolves
        # to SQL Server's ``BytesType`` rather than Snowflake's ``TimeType``.
        # These columns describe the resolved warehouse, and the SQLMesh dataset
        # is a sibling of that warehouse entity, so a mismatched type renders
        # confusingly across the pair. Consult the platform-specific resolver
        # first for platforms where the merged mapping is known to conflict.
        if type_str and platform.lower() == SNOWFLAKE_PLATFORM:
            snowflake_type = resolve_snowflake_modified_type(type_str.upper())
            if snowflake_type is not None:
                return snowflake_type()
        return resolve_sql_type(type_str, platform.lower())

    def _build_schema_fields(
        self, model: "SqlmeshModel", effective: _EffectiveProjectConfig
    ) -> Optional[List[SchemaField]]:
        columns_to_types: Dict[str, Any] = (
            getattr(model, "columns_to_types", None) or {}
        )
        if not columns_to_types:
            logger.debug(
                "Model %s has no column type information; skipping schema",
                getattr(model, "name", "?"),
            )
            return None

        col_descriptions: Dict[str, str] = (
            getattr(model, "column_descriptions", None) or {}
        )

        fields = []
        for col_name, col_type in columns_to_types.items():
            type_str = str(col_type) if col_type is not None else ""
            resolved = self._resolve_column_type(
                type_str, effective.target_platform or ""
            )
            fields.append(
                SchemaField(
                    fieldPath=col_name,
                    type=SchemaFieldDataType(type=resolved or NullTypeClass()),
                    nativeDataType=type_str,
                    nullable=True,
                    description=col_descriptions.get(col_name) or None,
                )
            )
        return fields or None
