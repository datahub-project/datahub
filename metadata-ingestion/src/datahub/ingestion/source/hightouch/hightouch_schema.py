import json
import logging
from typing import Any, Dict, List, Optional, Set

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import HightouchSourceReport
from datahub.ingestion.source.hightouch.constants import (
    QUERY_TYPE_RAW_SQL,
    QUERY_TYPE_TABLE,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchSchemaField,
    HightouchSourceConnection,
    HightouchSync,
)
from datahub.ingestion.source.hightouch.protocols import (
    ExtractTableUrns,
    GetAggregatorForPlatform,
    GetDestination,
    GetModel,
    GetOutletUrnForSync,
    GetPlatformForSource,
    GetSource,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator

logger = logging.getLogger(__name__)


class HightouchSchemaHandler:
    def __init__(
        self,
        report: HightouchSourceReport,
        graph: Optional[DataHubGraph],
        urn_builder: HightouchUrnBuilder,
    ) -> None:
        self.report = report
        self.graph = graph
        self.urn_builder = urn_builder

    @staticmethod
    def _get_first_value(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
        """Get first non-None value from dict for any of the given keys."""
        for key in keys:
            value = d.get(key)
            if value is not None:
                return value
        return None

    def resolve_schema(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]] = None,
    ) -> Optional[List[HightouchSchemaField]]:
        schema_fields = self._parse_model_schema(model)

        if not schema_fields and source and self.graph:
            schema_fields = self._fetch_schema_from_datahub(model=model, source=source)

        if not schema_fields and referenced_columns:
            schema_fields = self._schema_from_referenced_columns(
                referenced_columns=referenced_columns, primary_key=model.primary_key
            )

        if schema_fields and model.primary_key:
            schema_fields = self._mark_primary_key_in_schema(
                schema_fields=schema_fields, primary_key=model.primary_key
            )

        return schema_fields

    def _parse_model_schema(
        self, model: HightouchModel
    ) -> Optional[List[HightouchSchemaField]]:
        if not model.query_schema:
            return None

        try:
            schema_data = model.query_schema

            if isinstance(schema_data, str):
                try:
                    schema_data = json.loads(schema_data)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Model {model.id}: query_schema is a string but not valid JSON: {e}"
                    )
                    self.report.report_model_schemas_skipped("invalid_json")
                    return None

            columns = None
            if isinstance(schema_data, list):
                columns = schema_data
            elif isinstance(schema_data, dict):
                columns = (
                    schema_data.get("columns")
                    or schema_data.get("fields")
                    or schema_data.get("schema")
                    or schema_data.get("properties")
                )
            else:
                logger.warning(
                    f"Model {model.id}: Unexpected query_schema type: {type(schema_data).__name__}"
                )
                return None

            if not columns:
                return None

            if not isinstance(columns, list):
                logger.warning(
                    f"Model {model.id}: Columns is not a list: {type(columns).__name__}"
                )
                return None

            schema_fields = []
            for col in columns:
                if not isinstance(col, dict):
                    continue

                name = self._get_first_value(
                    col,
                    ["name", "fieldName", "field_name", "columnName", "column_name"],
                )
                data_type = self._get_first_value(
                    col,
                    [
                        "type",
                        "dataType",
                        "data_type",
                        "fieldType",
                        "field_type",
                        "columnType",
                        "column_type",
                    ],
                )
                description = self._get_first_value(
                    col, ["description", "comment", "doc"]
                )

                if name and data_type:
                    schema_fields.append(
                        HightouchSchemaField(
                            name=str(name), type=str(data_type), description=description
                        )
                    )

            if schema_fields:
                self.report.report_model_schemas_emitted()
                return schema_fields
            else:
                self.report.report_model_schemas_skipped("no_valid_fields")
                return None

        except (AttributeError, TypeError, KeyError) as e:
            logger.error(
                f"Model {model.id}: Programming error parsing schema: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.warning(
                f"Model {model.id}: Could not parse schema from query_schema (malformed data): {e}",
                exc_info=True,
            )
            self.report.report_model_schemas_skipped(f"parse_error: {str(e)}")
            return None

    def _mark_primary_key_in_schema(
        self, schema_fields: List[HightouchSchemaField], primary_key: str
    ) -> List[HightouchSchemaField]:
        updated_fields = []
        for field in schema_fields:
            if field.name.upper() == primary_key.upper():
                updated_fields.append(
                    HightouchSchemaField(
                        name=field.name,
                        type=field.type,
                        description=field.description,
                        is_primary_key=True,
                    )
                )
            else:
                updated_fields.append(field)
        return updated_fields

    def _schema_from_referenced_columns(
        self, referenced_columns: List[str], primary_key: Optional[str] = None
    ) -> Optional[List[HightouchSchemaField]]:
        if not referenced_columns:
            return None

        schema_fields: List[HightouchSchemaField] = []
        for column_name in referenced_columns:
            is_pk = primary_key and column_name.upper() == primary_key.upper()

            schema_fields.append(
                HightouchSchemaField(
                    name=column_name,
                    type="STRING",
                    description=None,
                    is_primary_key=is_pk or False,
                )
            )

        self.report.schemas_from_referenced_columns += 1

        return schema_fields

    def _fetch_schema_from_datahub(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> Optional[List[HightouchSchemaField]]:
        if not self.graph:
            return None

        try:
            upstream_urn = None

            if model.query_type == QUERY_TYPE_TABLE and model.name:
                table_name = self.urn_builder.qualified_table_name(model, source)
                upstream_urn = self.urn_builder.make_upstream_table_urn(
                    table_name, source
                )

            if not upstream_urn:
                return None

            schema_metadata = self.graph.get_schema_metadata(str(upstream_urn))

            if not schema_metadata or not schema_metadata.fields:
                self.report.report_model_schema_datahub_not_found(model.slug)
                return None

            schema_fields = []
            for field in schema_metadata.fields:
                schema_fields.append(
                    HightouchSchemaField(
                        name=field.fieldPath,
                        type=field.nativeDataType or "UNKNOWN",
                        description=field.description if field.description else None,
                    )
                )

            self.report.report_model_schema_from_datahub()

            return schema_fields

        except (KeyError, AttributeError, TypeError) as e:
            logger.error(
                f"Model {model.id} ({model.slug}): Failed to fetch schema from DataHub - {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.warning(
                f"Model {model.id} ({model.slug}): Could not fetch schema from DataHub (continuing without it): {e}"
            )
            return None

    def fetch_and_register_schema(
        self,
        urn: str,
        aggregator: SqlParsingAggregator,
        registered_urns: Set[str],
    ) -> bool:
        if urn in registered_urns:
            return False

        if not self.graph:
            return False

        try:
            schema_metadata = self.graph.get_schema_metadata(urn)
            if schema_metadata and schema_metadata.fields:
                aggregator.register_schema(urn, schema_metadata)
                registered_urns.add(urn)
                return True
        except (AttributeError, TypeError) as e:
            logger.error(
                f"Programming error preloading schema for {urn}: {type(e).__name__}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.debug(
                f"Could not preload schema for {urn} (optional optimization): {e}"
            )

        return False

    def preload_model_schemas(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
        aggregator: SqlParsingAggregator,
        registered_urns: Set[str],
        extract_table_urns_fn: ExtractTableUrns,
    ) -> None:
        if model.query_type == QUERY_TYPE_TABLE and model.name:
            table_name = self.urn_builder.qualified_table_name(model, source)
            upstream_urn = str(
                self.urn_builder.make_upstream_table_urn(table_name, source)
            )
            self.fetch_and_register_schema(upstream_urn, aggregator, registered_urns)

        if model.raw_sql and model.query_type == QUERY_TYPE_RAW_SQL:
            sql_table_urns = extract_table_urns_fn(model, source)
            for table_urn in sql_table_urns:
                self.fetch_and_register_schema(table_urn, aggregator, registered_urns)

    def preload_sync_schemas(
        self,
        sync: HightouchSync,
        registered_urns: Set[str],
        get_model: GetModel,
        get_source: GetSource,
        get_destination: GetDestination,
        get_platform_for_source: GetPlatformForSource,
        get_aggregator_for_platform: GetAggregatorForPlatform,
        get_outlet_urn_for_sync: GetOutletUrnForSync,
    ) -> None:
        model = get_model(sync.model_id)
        if not model:
            return

        source = get_source(model.source_id)
        if not source:
            return

        source_platform = get_platform_for_source(source)
        if not source_platform.platform:
            return

        aggregator = get_aggregator_for_platform(source_platform)
        if not aggregator:
            return

        model_urn = str(self.urn_builder.make_model_urn(model, source))
        self.fetch_and_register_schema(model_urn, aggregator, registered_urns)

        destination = get_destination(sync.destination_id)
        if not destination:
            return

        outlet_urn = str(get_outlet_urn_for_sync(sync, destination))
        if outlet_urn:
            self.fetch_and_register_schema(outlet_urn, aggregator, registered_urns)

    def preload_schemas_for_sql_parsing(
        self,
        models: List[HightouchModel],
        syncs: List[HightouchSync],
        registered_urns: Set[str],
        get_source: GetSource,
        get_model: GetModel,
        get_destination: GetDestination,
        get_platform_for_source: GetPlatformForSource,
        get_aggregator_for_platform: GetAggregatorForPlatform,
        extract_table_urns_fn: ExtractTableUrns,
        get_outlet_urn_for_sync: GetOutletUrnForSync,
    ) -> None:
        if not self.graph:
            return

        registered_urns.clear()

        for model in models:
            source = get_source(model.source_id)
            if not source:
                continue

            source_platform = get_platform_for_source(source)
            if not source_platform.platform:
                continue

            aggregator = get_aggregator_for_platform(source_platform)
            if not aggregator:
                continue

            self.preload_model_schemas(
                model, source, aggregator, registered_urns, extract_table_urns_fn
            )

        for sync in syncs:
            self.preload_sync_schemas(
                sync,
                registered_urns,
                get_model,
                get_source,
                get_destination,
                get_platform_for_source,
                get_aggregator_for_platform,
                get_outlet_urn_for_sync,
            )
