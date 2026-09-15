from typing import Callable, Iterable, List, Mapping, Set, Type, Union

from sqlalchemy.engine import Engine

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.hana.constants import (
    CALCULATION_VIEW_TYPE_TAG,
    SYS_BIC_SCHEMA,
    CalcViewProperty,
    HanaSourceType,
)
from datahub.ingestion.source.sql.hana.hana_calculation_view_parser import (
    SAPCalculationViewParser,
)
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaCalcViewColumn,
)
from datahub.ingestion.source.sql.hana.hana_script_lineage import (
    extract_table_references,
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

# Mapping from SAP HANA native type names (as they appear in
# ``SYS.VIEW_COLUMNS.DATA_TYPE_NAME``) to DataHub schema type classes.
# Unmapped types fall through to ``NullTypeClass`` rather than guessing string
# so downstream consumers can distinguish "we don't know" from "actually text".
_HANA_TYPE_MAP: Mapping[
    str,
    Type[
        Union[
            ArrayTypeClass,
            BooleanTypeClass,
            BytesTypeClass,
            DateTypeClass,
            NumberTypeClass,
            RecordTypeClass,
            StringTypeClass,
            TimeTypeClass,
        ]
    ],
] = {
    "BOOLEAN": BooleanTypeClass,
    "TINYINT": NumberTypeClass,
    "SMALLINT": NumberTypeClass,
    "INTEGER": NumberTypeClass,
    "BIGINT": NumberTypeClass,
    "SMALLDECIMAL": NumberTypeClass,
    "DECIMAL": NumberTypeClass,
    "REAL": NumberTypeClass,
    "DOUBLE": NumberTypeClass,
    "FLOAT": NumberTypeClass,
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

    Invoked after :class:`SQLAlchemySource` has populated the aggregator's
    schema resolver with regular tables and views — ordering matters because
    :meth:`add_known_query_lineage` enriches column lineage from the resolver.
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
        # Surface unmapped HANA types once at end-of-run so users notice
        # when SAP adds new types we silently emit as ``NullTypeClass``.
        self._unmapped_hana_types: Set[str] = set()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        engine = self.engine_factory()
        try:
            with engine.connect() as conn:
                data_dict = HanaDataDictionary(conn, self.report)
                for calc_view in data_dict.get_calculation_views():
                    # Match against the natural package-prefixed form
                    # users see in HANA tooling (e.g.
                    # ``acme.analytics.SalesOverview``) rather than the
                    # URN-safe lower-cased qualified identifier, so
                    # config patterns can be copied directly from HANA
                    # Studio / Web IDE without case juggling.
                    pattern_key = f"{calc_view.package_id}.{calc_view.name}"
                    if not self.config.calculation_view_pattern.allowed(pattern_key):
                        self.report.report_dropped(calc_view.qualified_identifier)
                        continue
                    calc_view.columns = data_dict.get_columns_for_calculation_view(
                        calc_view
                    )
                    yield from self._emit_workunits(calc_view)
                    self._populate_lineage(calc_view)
        finally:
            engine.dispose()
            if self._unmapped_hana_types:
                self.report.info(
                    title="Unmapped HANA column types",
                    message=(
                        "HANA native types not in _HANA_TYPE_MAP were "
                        "emitted as NullTypeClass."
                    ),
                    context=", ".join(sorted(self._unmapped_hana_types)),
                )

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
                    CalcViewProperty.VIEW_TYPE: CALCULATION_VIEW_TYPE_TAG,
                    CalcViewProperty.PACKAGE_ID: calc_view.package_id,
                    CalcViewProperty.RUNTIME_VIEW_NAME: calc_view.runtime_view_name,
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.SAP_HANA_CALCULATION_VIEW]),
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
                type=self._schema_field_type(column),
                nativeDataType=column.get_precise_native_type(),
                description=column.comment or None,
                nullable=column.nullable,
            )
            for column in calc_view.columns
        ]
        return SchemaMetadataClass(
            schemaName=f"{SYS_BIC_SCHEMA.lower()}.{calc_view.runtime_view_name}",
            platform=make_data_platform_urn(self.PLATFORM),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    def _populate_lineage(self, calc_view: HanaCalculationView) -> None:
        downstream_urn = self.identifiers.calc_view_urn(calc_view)
        column_lineage: List[ColumnLineageInfo] = []
        upstreams: set[str] = set()

        try:
            xml_lineage = self.parser.column_lineage(
                calc_view.runtime_view_name, calc_view.definition
            )
            script_defs = self.parser.script_view_definitions(
                calc_view.runtime_view_name, calc_view.definition
            )
        except Exception as e:
            self.report.warning(
                title="Calculation view lineage parse failed",
                message="Could not parse a calculation view XML for lineage. "
                "The dataset will still be emitted, but without upstream lineage.",
                context=calc_view.runtime_view_name,
                exc=e,
            )
            return

        for entry in xml_lineage:
            column_refs: List[ColumnRef] = []
            for upstream in entry.upstreams:
                upstream_urn = self.identifiers.upstream_urn_for_calc_view_source(
                    source_type=upstream.source_type,
                    source_name=upstream.source_name,
                    source_path=upstream.source_path,
                )
                if not upstream_urn or not upstream.column:
                    continue
                upstreams.add(upstream_urn)
                column_refs.append(
                    ColumnRef(table=upstream_urn, column=upstream.column)
                )
            if column_refs:
                column_lineage.append(
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=downstream_urn,
                            column=entry.downstream_column,
                        ),
                        upstreams=column_refs,
                    )
                )

        for script in script_defs:
            for ref in extract_table_references(script.definition):
                upstream_urn = self.identifiers.upstream_urn_for_calc_view_source(
                    source_type=HanaSourceType.DATA_BASE_TABLE,
                    source_name=ref.name,
                    source_path=ref.schema_name,
                )
                if upstream_urn:
                    upstreams.add(upstream_urn)

        if not upstreams:
            # No usable upstream refs (e.g. SqlScriptView that only CALLs
            # another procedure). Skip rather than emit an empty downstream.
            return

        # KnownQueryLineageInfo requires a ``query_text``; the XML
        # definition is preserved on the dataset via ``ViewProperties``,
        # so here we emit a synthetic SQL-shaped placeholder that
        # surfaces the calc view's identifier in UI / debugging without
        # stuffing an XML blob into a SQL field.
        query_text = (
            f"-- SAP HANA calculation view: {calc_view.qualified_identifier}\n"
            f"-- See dataset ViewProperties.viewLogic for the XML definition."
        )
        self.aggregator.add_known_query_lineage(
            KnownQueryLineageInfo(
                query_text=query_text,
                downstream=downstream_urn,
                upstreams=sorted(upstreams),
                column_lineage=column_lineage,
            )
        )

    def _schema_field_type(
        self, column: HanaCalcViewColumn
    ) -> SchemaFieldDataTypeClass:
        type_key = column.data_type.upper()
        type_cls = _HANA_TYPE_MAP.get(type_key)
        if type_cls is None:
            self._unmapped_hana_types.add(type_key)
            return SchemaFieldDataTypeClass(type=NullTypeClass())
        return SchemaFieldDataTypeClass(type=type_cls())
