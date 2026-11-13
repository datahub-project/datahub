import dataclasses
from typing import Dict, Iterable, List, Optional, Set

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.source import StructuredLogCategory
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
    PlatformDetail,
)
from datahub.ingestion.source.hightouch.constants import (
    HIGHTOUCH_PLATFORM,
    KNOWN_DESTINATION_PLATFORM_MAPPING,
    KNOWN_SOURCE_PLATFORM_MAPPING,
    QUERY_TYPE_RAW_SQL,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
)
from datahub.metadata.schema_classes import (
    NullTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnRef,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.urns.error import InvalidUrnError


@dataclasses.dataclass
class ParsedModelSql:
    upstream_urns: List[str]
    # Output column -> the upstream columns it is derived from.
    column_upstreams: Dict[str, List[ColumnRef]]
    output_fields: List[SchemaFieldClass]


def normalize_column_name(name: str) -> str:
    return name.lower().replace("_", "").replace("-", "")


def match_field(name: str, candidates: Iterable[str]) -> Optional[str]:
    """The candidate naming the same column as `name`: an exact match, else the
    only case-insensitive match, else the only match ignoring case, `_` and `-`.
    None when a step is ambiguous, because lineage to the wrong column is worse
    than none (`first_name` must not be wired to `firstname` by chance)."""
    candidates = list(candidates)
    if name in candidates:
        return name
    for key in (str.lower, normalize_column_name):
        matches = {c for c in candidates if key(c) == key(name)}
        if len(matches) == 1:
            return matches.pop()
        if matches:
            return None
    return None


class HightouchResolver:
    """Looks up Hightouch entities by id and turns them into DataHub URNs that join
    with the URNs emitted by the source and destination platforms' connectors.

    DataHub reads (schemas, existence) and SQL parsing are cached for the run, since
    models, syncs and lineage all ask about the same datasets.
    """

    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
        graph: Optional[DataHubGraph],
    ):
        self.config = config
        self.report = report
        self.graph = graph
        self.sources: Dict[str, HightouchSourceConnection] = {}
        self.models: Dict[str, HightouchModel] = {}
        self.destinations: Dict[str, HightouchDestination] = {}
        # The schema each emitted model was given, for syncs' column lineage.
        self.model_fields: Dict[str, List[SchemaFieldClass]] = {}
        # Models emitted as datasets this run; a sync reads from any other model's
        # warehouse tables instead, so it never points at a model that isn't there.
        self.emitted_models: Set[str] = set()
        self._source_platforms: Dict[str, PlatformDetail] = {}
        self._destination_platforms: Dict[str, PlatformDetail] = {}
        self._schemas: Dict[str, Optional[List[SchemaFieldClass]]] = {}
        self._exists: Dict[str, bool] = {}
        self._parsed_sql: Dict[str, Optional[ParsedModelSql]] = {}

    def source_platform(self, source: HightouchSourceConnection) -> PlatformDetail:
        if source.id not in self._source_platforms:
            self._source_platforms[source.id] = self._resolve_platform(
                configured=self.config.sources_to_platform_instance.get(source.id),
                hightouch_type=source.type,
                known=KNOWN_SOURCE_PLATFORM_MAPPING,
                context=f"source {source.name} (id {source.id})",
                default_database=source.configuration.get("database"),
            )
        return self._source_platforms[source.id]

    def destination_platform(self, destination: HightouchDestination) -> PlatformDetail:
        if destination.id not in self._destination_platforms:
            self._destination_platforms[destination.id] = self._resolve_platform(
                configured=self.config.destination_to_platform_instance.get(
                    destination.id
                ),
                hightouch_type=destination.type,
                known=KNOWN_DESTINATION_PLATFORM_MAPPING,
                context=f"destination {destination.name} (id {destination.id})",
                default_database=None,
            )
        return self._destination_platforms[destination.id]

    def _resolve_platform(
        self,
        configured: Optional[PlatformDetail],
        hightouch_type: str,
        known: Dict[str, str],
        context: str,
        default_database: Optional[str],
    ) -> PlatformDetail:
        # A copy, so the recipe's own PlatformDetail is never filled in.
        details = configured.model_copy() if configured else PlatformDetail()
        if details.platform is None:
            details.platform = known.get(hightouch_type.lower())
            if details.platform is None:
                details.platform = hightouch_type.lower()
                self.report.info(
                    title="Unknown Hightouch platform type",
                    message="The type is used as the DataHub platform name. Set "
                    "`platform` in sources_to_platform_instance or "
                    "destination_to_platform_instance to override it.",
                    context=f"{context}, type: {hightouch_type}",
                    log_category=StructuredLogCategory.LINEAGE,
                )
        if details.env is None:
            details.env = self.config.env
        if details.database is None:
            details.database = default_database
        return details

    def model_urn(self, model: HightouchModel) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=HIGHTOUCH_PLATFORM,
            name=model.slug,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def source_table_urn(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> Optional[str]:
        """The warehouse table a table or dbt model reads as is; None for a query."""
        table_name = model.table_name
        if not table_name:
            return None
        details = self.source_platform(source)
        database = details.database or ""
        if details.include_schema_in_urn:
            name = _qualify(
                table_name, database, source.configuration.get("schema") or ""
            )
        else:
            bare_table = table_name.split(".")[-1]
            name = f"{database}.{bare_table}" if database else bare_table
        return self._table_urn(name, details)

    def _table_urn(self, name: str, details: PlatformDetail) -> str:
        if details.lowercase_urns:
            name = name.lower()
        return make_dataset_urn_with_platform_instance(
            platform=details.platform or "",
            name=name,
            platform_instance=details.platform_instance,
            env=details.env or self.config.env,
        )

    def destination_urn(
        self, object_name: str, destination: HightouchDestination
    ) -> str:
        details = self.destination_platform(destination)
        if details.database:
            object_name = _qualify(object_name, details.database)
        return self._table_urn(object_name, details)

    def parse_model_sql(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> Optional[ParsedModelSql]:
        """Table and column lineage of a raw SQL model, parsed once per model."""
        if model.id not in self._parsed_sql:
            self._parsed_sql[model.id] = self._parse_model_sql(model, source)
        return self._parsed_sql[model.id]

    def _parse_model_sql(
        self, model: HightouchModel, source: HightouchSourceConnection
    ) -> Optional[ParsedModelSql]:
        if not (
            self.config.parse_model_sql
            and model.raw_sql
            and model.query_type == QUERY_TYPE_RAW_SQL
        ):
            return None
        details = self.source_platform(source)
        result = create_lineage_sql_parsed_result(
            query=model.raw_sql,
            # Unqualified tables resolve against the connection's database and
            # schema, as table models do. Without the schema in URNs a two-part
            # name is database.table, so the parser must not read it as
            # schema.table under the default database.
            default_db=details.database if details.include_schema_in_urn else None,
            default_schema=(
                source.configuration.get("schema") or None
                if details.include_schema_in_urn
                else None
            ),
            platform=details.platform or "",
            platform_instance=details.platform_instance,
            env=details.env or self.config.env,
            # With a graph the parser resolves the casing and columns of tables
            # already in DataHub, which column lineage needs.
            graph=self.graph,
            schema_aware=self.graph is not None,
        )
        if result.debug_info.table_error:
            self.report.sql_parsing_failures += 1
            self.report.warning(
                title="Could not parse model SQL",
                message="Lineage from the tables the model queries is missing for "
                "this model and its syncs.",
                context=f"model_slug: {model.slug}",
                exc=result.debug_info.table_error,
                log_category=StructuredLogCategory.LINEAGE,
            )
            return None
        self.report.sql_parsing_successes += 1

        normalized = {
            str(urn): self._normalize_parsed_urn(str(urn), details)
            for urn in result.in_tables
        }
        column_upstreams: Dict[str, List[ColumnRef]] = {}
        output_fields: List[SchemaFieldClass] = []
        for info in result.column_lineage or []:
            column = info.downstream.column
            column_upstreams[column] = [
                ColumnRef(
                    table=normalized.get(str(ref.table), str(ref.table)),
                    column=ref.column,
                )
                for ref in info.upstreams
            ]
            output_fields.append(
                SchemaFieldClass(
                    fieldPath=column,
                    type=info.downstream.column_type
                    or SchemaFieldDataTypeClass(type=NullTypeClass()),
                    nativeDataType=info.downstream.native_column_type or "",
                )
            )
        return ParsedModelSql(
            upstream_urns=list(dict.fromkeys(normalized.values())),
            column_upstreams=column_upstreams,
            output_fields=output_fields,
        )

    def _normalize_parsed_urn(self, urn: str, details: PlatformDetail) -> str:
        # The parser builds URNs from the query text with its own casing and
        # platform-instance prefix. Rebuild each one under the same PlatformDetail
        # rules as table models (instance, lowercasing, schema omission).
        try:
            name = DatasetUrn.from_string(urn).name
        except InvalidUrnError:
            return urn
        instance = details.platform_instance
        if instance and name.lower().startswith(f"{instance.lower()}."):
            name = name[len(instance) + 1 :]
        if not details.include_schema_in_urn:
            # Parsed without a default database, so the parts are as written:
            # table, database.table or database.schema.table.
            parts = name.split(".")
            if len(parts) == 1 and details.database:
                name = f"{details.database}.{name}"
            elif len(parts) >= 3:
                name = f"{parts[0]}.{parts[-1]}"
        return self._table_urn(name, details)

    def schema_fields(self, urn: str) -> Optional[List[SchemaFieldClass]]:
        """The schema of a dataset already in DataHub, or None."""
        if urn in self._schemas:
            return self._schemas[urn]
        fields = None
        if self.graph:
            try:
                metadata = self.graph.get_schema_metadata(urn)
                fields = list(metadata.fields) if metadata and metadata.fields else None
            except Exception as e:
                self.report.report_datahub_lookup_failure("dataset schema", urn, e)
        self._schemas[urn] = fields
        return fields

    def exists(self, urn: str) -> bool:
        if urn not in self._exists:
            exists = False
            if self.graph:
                try:
                    exists = self.graph.exists(urn)
                except Exception as e:
                    self.report.report_datahub_lookup_failure(
                        "dataset existence", urn, e
                    )
            self._exists[urn] = exists
        return self._exists[urn]


def _qualify(name: str, database: str, schema: str = "") -> str:
    # Add only the missing qualifiers. A three-part name is complete, and a two-part
    # name already carries its schema (or its database, when it starts with it);
    # qualifying either again gives a four-part name that never joins.
    parts = name.split(".")
    if len(parts) >= 3 or (
        database and name.lower().startswith(f"{database.lower()}.")
    ):
        return name
    if len(parts) == 2:
        return f"{database}.{name}" if database else name
    return ".".join(part for part in (database, schema, name) if part)
