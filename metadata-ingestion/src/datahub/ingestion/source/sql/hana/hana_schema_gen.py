"""Generator that emits work units and lineage for SAP HANA calculation views."""

import logging
from typing import Callable, Iterable, List, Optional

from sqlalchemy.engine import Engine

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.hana.hana_calculation_view_parser import (
    SAPCalculationViewParser,
)
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaCalcViewColumn,
)
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    QueryLanguageClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)

logger = logging.getLogger(__name__)

# Mapping from SAP HANA native type names (as they appear in
# ``SYS.VIEW_COLUMNS.DATA_TYPE_NAME``) to DataHub schema type classes.
# Unmapped types fall through to ``NullTypeClass`` rather than guessing string
# so downstream consumers can distinguish "we don't know" from "actually text".
_HANA_TYPE_MAP: dict = {
    "BOOLEAN": BooleanTypeClass,
    "TINYINT": NumberTypeClass,
    "SMALLINT": NumberTypeClass,
    "INTEGER": NumberTypeClass,
    "BIGINT": NumberTypeClass,
    "SMALLDECIMAL": NumberTypeClass,
    "DECIMAL": NumberTypeClass,
    "REAL": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "VARCHAR": StringTypeClass,
    "NVARCHAR": StringTypeClass,
    "ALPHANUM": StringTypeClass,
    "SHORTTEXT": StringTypeClass,
    "CLOB": StringTypeClass,
    "NCLOB": StringTypeClass,
    "TEXT": StringTypeClass,
    "VARBINARY": BytesTypeClass,
    "BLOB": BytesTypeClass,
    "ARRAY": ArrayTypeClass,
    "ST_GEOMETRY": RecordTypeClass,
    "ST_POINT": RecordTypeClass,
    "DATE": DateTypeClass,
    "TIME": TimeTypeClass,
    "SECONDDATE": TimeTypeClass,
    "TIMESTAMP": TimeTypeClass,
}


class HanaCalculationViewExtractor:
    """Emit dataset work units and column-level lineage for HANA calc views.

    The extractor is invoked from
    :meth:`HanaSource.get_workunits_internal` after
    :class:`SQLAlchemySource` has finished populating the parsing
    aggregator's schema resolver with every regular table and view we already
    know about. That ordering matters because :meth:`add_known_query_lineage`
    enriches column-level lineage from the resolver's schemas.
    """

    PLATFORM = "hana"

    def __init__(
        self,
        *,
        config: HanaConfig,
        report: SQLSourceReport,
        identifiers: HanaIdentifierBuilder,
        engine_factory: Callable[[], Engine],
        aggregator: SqlParsingAggregator,
    ):
        self.config = config
        self.report = report
        self.identifiers = identifiers
        self.engine_factory = engine_factory
        self.aggregator = aggregator
        self.parser = SAPCalculationViewParser()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        engine = self.engine_factory()
        try:
            with engine.connect() as conn:
                data_dict = HanaDataDictionary(conn, self.report)
                for calc_view in data_dict.get_calculation_views():
                    qualified = (
                        f"_sys_bic.{calc_view.package_id}.{calc_view.name}".lower()
                    )
                    if not self.config.view_pattern.allowed(qualified):
                        self.report.report_dropped(qualified)
                        continue
                    calc_view.columns = data_dict.get_columns_for_calculation_view(
                        calc_view
                    )
                    yield from self._emit_workunits(calc_view)
                    self._populate_lineage(calc_view)
        finally:
            engine.dispose()

    def _emit_workunits(
        self, calc_view: HanaCalculationView
    ) -> Iterable[MetadataWorkUnit]:
        urn = self.identifiers.calc_view_urn(calc_view)

        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=self._build_schema_metadata(calc_view),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetPropertiesClass(
                name=calc_view.name,
                description=None,
                customProperties={
                    "view_type": "CALCULATION_VIEW",
                    "package_id": calc_view.package_id,
                    "runtime_view_name": calc_view.runtime_view_name,
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ViewPropertiesClass(
                materialized=False,
                viewLanguage=QueryLanguageClass.UNKNOWN,
                viewLogic=calc_view.definition,
            ),
        ).as_workunit()

    def _build_schema_metadata(
        self, calc_view: HanaCalculationView
    ) -> SchemaMetadataClass:
        fields = [
            SchemaFieldClass(
                fieldPath=column.name,
                type=_schema_field_type(column),
                nativeDataType=column.get_precise_native_type(),
                description=column.comment or None,
                nullable=column.nullable,
            )
            for column in calc_view.columns
        ]
        return SchemaMetadataClass(
            schemaName=f"_sys_bic.{calc_view.runtime_view_name}",
            platform=make_data_platform_urn(self.PLATFORM),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    def _populate_lineage(self, calc_view: HanaCalculationView) -> None:
        """Translate parser output into ``KnownQueryLineageInfo`` for the aggregator.

        We hand the aggregator a ``KnownQueryLineageInfo`` rather than a plain
        ``KnownLineageMapping`` because we have explicit column-to-column
        mappings (from the calc-view XML) and want them surfaced as
        ``FineGrainedLineage`` rather than identity-only column lineage.
        """
        downstream_urn = self.identifiers.calc_view_urn(calc_view)
        column_lineage: List[ColumnLineageInfo] = []
        upstreams: set[str] = set()

        try:
            parser_output = self.parser.column_lineage(
                calc_view.runtime_view_name, calc_view.definition
            )
        except Exception as e:
            logger.warning(
                "Failed to parse calculation view %s for lineage: %s",
                calc_view.runtime_view_name,
                e,
            )
            self.report.report_warning(
                "calc-view-parse",
                f"Could not parse {calc_view.runtime_view_name}: {e}",
            )
            return

        for entry in parser_output:
            column_refs: List[ColumnRef] = []
            for upstream in entry.get("upstreams") or []:
                upstream_urn = self.identifiers.upstream_urn_for_calc_view_source(
                    source_type=upstream.get("source_type", ""),
                    source_name=upstream.get("source_name", ""),
                    source_path=upstream.get("source_path") or None,
                )
                upstream_col = upstream.get("column") or ""
                if not upstream_urn or not upstream_col:
                    continue
                upstreams.add(upstream_urn)
                column_refs.append(ColumnRef(table=upstream_urn, column=upstream_col))
            if column_refs:
                column_lineage.append(
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=downstream_urn,
                            column=entry["downstream_column"],
                        ),
                        upstreams=column_refs,
                    )
                )

        if not upstreams:
            # No usable upstream references; the parser may have returned
            # rows with empty source names (e.g. unresolved formula
            # references). Skip lineage rather than emitting a downstream
            # with no upstreams attached.
            return

        # ``query_text`` is required by KnownQueryLineageInfo; the XML is the
        # closest thing to a "query" we have, and storing it lets reviewers
        # trace lineage back to the source artefact.
        self.aggregator.add_known_query_lineage(
            KnownQueryLineageInfo(
                query_text=calc_view.definition,
                downstream=downstream_urn,
                upstreams=sorted(upstreams),
                column_lineage=column_lineage,
            )
        )


def _schema_field_type(column: HanaCalcViewColumn) -> SchemaFieldDataTypeClass:
    type_cls: Optional[type] = _HANA_TYPE_MAP.get(column.data_type.upper())
    if type_cls is None:
        return SchemaFieldDataTypeClass(type=NullTypeClass())
    return SchemaFieldDataTypeClass(type=type_cls())
