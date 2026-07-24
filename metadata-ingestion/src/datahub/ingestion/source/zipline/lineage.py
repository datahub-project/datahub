import re
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.zipline.config import ZiplineConfig
from datahub.ingestion.source.zipline.models import Source
from datahub.ingestion.source.zipline.report import ZiplineSourceReport
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import create_lineage_from_sql_statements

# Chronon SQL embeds Jinja macros (`{{ start_date }}`) that aren't valid SQL.
# Replace them with a bare, unquoted token: templates are often already quoted
# (`'{{ ds }}'`), and adding quotes here would produce an invalid `''...''`.
_TEMPLATE_RE = re.compile(r"\{\{.*?\}\}")
_TEMPLATE_REPLACEMENT = "__zipline_template__"


def strip_sql_templates(query: str) -> str:
    return _TEMPLATE_RE.sub(_TEMPLATE_REPLACEMENT, query)


class SourceResolver:
    """Resolves Chronon `Source` structs to backing Dataset URNs.

    The compiled config never records a table's platform, so the user supplies
    `source_platform_map` (per-namespace) plus a default.
    """

    def __init__(self, config: ZiplineConfig, report: ZiplineSourceReport) -> None:
        self.config = config
        self.report = report
        # Chronon's namespace and a native connector's database may differ only
        # in case, so match the platform map case-insensitively.
        self._platform_map_lower: Dict[str, str] = {
            key.lower(): value for key, value in config.source_platform_map.items()
        }

    def resolve_source_urns(self, source: Source) -> List[str]:
        urns: List[str] = []

        batch_table = source.batch_table
        if batch_table:
            urns.append(self.resolve_table_urn(batch_table))

        topic = source.topic
        if topic:
            urns.append(self._resolve_topic_urn(topic))

        return urns

    def resolve_table_urn(self, table: str) -> str:
        namespace = table.split(".", 1)[0] if "." in table else None
        platform = self.config.default_source_platform
        if namespace is not None and namespace.lower() in self._platform_map_lower:
            platform = self._platform_map_lower[namespace.lower()]
        elif namespace is not None:
            self.report.report_unmapped_namespace(namespace)

        name = table.lower() if self.config.convert_urns_to_lowercase else table
        return make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name,
            platform_instance=self.config.source_platform_instance,
            env=self.config.env,
        )

    def _resolve_topic_urn(self, topic: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.config.stream_platform,
            name=topic,
            platform_instance=self.config.source_platform_instance,
            env=self.config.env,
        )


class StagingQueryLineageExtractor:
    """Best-effort table-level lineage from a StagingQuery's Spark SQL."""

    def __init__(
        self,
        config: ZiplineConfig,
        report: ZiplineSourceReport,
        graph: Optional[DataHubGraph],
        source_resolver: SourceResolver,
    ) -> None:
        self.config = config
        self.report = report
        self.graph = graph
        self.source_resolver = source_resolver

    def extract_input_urns(self, query: str, name: Optional[str] = None) -> List[str]:
        cleaned = strip_sql_templates(query)
        try:
            result = create_lineage_from_sql_statements(
                queries=cleaned,
                default_db=None,
                platform=self.config.default_source_platform,
                platform_instance=self.config.source_platform_instance,
                env=self.config.env,
                graph=self.graph,
            )
        except Exception as exc:
            self.report.sql_lineage_failures += 1
            self.report.warning(
                title="StagingQuery SQL parse failure",
                message="Could not parse StagingQuery SQL for lineage",
                context=name,
                exc=exc,
            )
            return []

        # sqlglot's common failure mode is a result object carrying an error
        # rather than a raised exception; surface it or the DataJob silently
        # loses all input lineage.
        if result.debug_info and result.debug_info.error:
            self.report.sql_lineage_failures += 1
            self.report.warning(
                title="StagingQuery SQL parse failure",
                message="sqlglot could not derive lineage from StagingQuery SQL",
                context=(
                    f"{name}: {result.debug_info.error}"
                    if name
                    else str(result.debug_info.error)
                ),
            )
            return []

        self.report.sql_lineage_parsed += 1
        # sqlglot attributes every derived table to default_source_platform. Re-map
        # each through the resolver so source_platform_map, lowercasing and the
        # unmapped-namespace warning apply here too — otherwise a table in a mapped
        # platform (e.g. Snowflake) is mis-attributed and its lineage never stitches.
        resolved = {
            self.source_resolver.resolve_table_urn(DatasetUrn.from_string(urn).name)
            for urn in result.in_tables
        }
        return sorted(resolved)
