"""Unit tests for the SAP HANA ingestion source."""

from typing import List, cast
from unittest.mock import MagicMock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.hana import HanaConfig, HanaSource
from datahub.ingestion.source.sql.hana.hana_calculation_view_parser import (
    SAPCalculationViewParser,
)
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaCalcViewColumn,
)
from datahub.ingestion.source.sql.hana.hana_schema_gen import (
    HanaCalculationViewExtractor,
)
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    ViewPropertiesClass,
)

# The sqlalchemy-hana driver is unavailable on aarch64 / arm64 wheels, but
# none of the tests below actually open a connection — they only construct
# config objects and source objects and exercise pure-Python helpers, all of
# which work without the driver. We only need to skip if a future test
# starts calling get_sql_alchemy_url() in a context that requires dialect
# registration.


# ---------------------------------------------------------------------------
# Config / source wiring
# ---------------------------------------------------------------------------


def test_platform_correctly_set_hana():
    source = HanaSource(
        ctx=PipelineContext(run_id="hana-source-test"),
        config=HanaConfig(),
    )
    assert source.platform == "hana"


def test_hana_uri_native():
    config = HanaConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:39041",
            "scheme": "hana+hdbcli",
        }
    )
    assert config.get_sql_alchemy_url() == "hana+hdbcli://user:password@host:39041"


def test_hana_uri_native_db():
    config = HanaConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:39041",
            "scheme": "hana+hdbcli",
            "database": "database",
        }
    )
    assert (
        config.get_sql_alchemy_url()
        == "hana+hdbcli://user:password@host:39041/database"
    )


def test_default_schema_pattern_denies_system_schemas():
    config = HanaConfig()
    # System schemas should be denied by default.
    assert not config.schema_pattern.allowed("SYS")
    assert not config.schema_pattern.allowed("_SYS_STATISTICS")
    assert not config.schema_pattern.allowed("_SYS_REPO")
    # _SYS_BIC is the runtime schema for activated calculation views and
    # must remain accessible so the calc-view extractor can discover them.
    assert config.schema_pattern.allowed("_SYS_BIC")
    # User schemas pass through.
    assert config.schema_pattern.allowed("REPORTING")


def test_include_calculation_views_is_off_by_default():
    """Opt-in: the calc-view extractor must not run on a vanilla config."""
    source = HanaSource(
        ctx=PipelineContext(run_id="hana-default-config"),
        config=HanaConfig(),
    )
    assert source.calc_view_extractor is None


def test_calc_view_extractor_constructed_when_enabled():
    config = HanaConfig.model_validate({"include_calculation_views": True})
    source = HanaSource(
        ctx=PipelineContext(run_id="hana-calc-views"),
        config=config,
    )
    assert source.calc_view_extractor is not None


# ---------------------------------------------------------------------------
# HanaCalcViewColumn precise native type
# ---------------------------------------------------------------------------


def test_calc_view_column_precise_decimal():
    column = HanaCalcViewColumn(
        name="AMOUNT",
        data_type="DECIMAL",
        nullable=True,
        ordinal_position=1,
        length=15,
        scale=2,
    )
    assert column.get_precise_native_type() == "DECIMAL(15,2)"


def test_calc_view_column_precise_varchar():
    column = HanaCalcViewColumn(
        name="NAME",
        data_type="NVARCHAR",
        nullable=False,
        ordinal_position=1,
        length=100,
    )
    assert column.get_precise_native_type() == "NVARCHAR(100)"


def test_calc_view_column_basic_type_unchanged():
    column = HanaCalcViewColumn(
        name="ID", data_type="INTEGER", nullable=False, ordinal_position=1
    )
    assert column.get_precise_native_type() == "INTEGER"


# ---------------------------------------------------------------------------
# HanaIdentifierBuilder
# ---------------------------------------------------------------------------


def test_calc_view_urn_lower_cased_and_uses_sys_bic_prefix():
    config = HanaConfig(host_port="localhost:39041")
    builder = HanaIdentifierBuilder(config)
    calc_view = HanaCalculationView(
        package_id="Acme.Analytics", name="SalesOverview", definition="<x/>"
    )
    urn = builder.calc_view_urn(calc_view)
    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:hana,"
        "_sys_bic.acme.analytics.salesoverview,PROD)"
    )


def test_upstream_urn_for_data_base_table():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    urn = builder.upstream_urn_for_calc_view_source(
        source_type="DATA_BASE_TABLE",
        source_name="CUSTOMERS",
        source_path="REPORTING",
    )
    assert urn is not None
    assert "reporting.customers" in urn


def test_upstream_urn_for_calc_view_source_returns_none_for_unknown_type():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    assert (
        builder.upstream_urn_for_calc_view_source(
            source_type="WHO_KNOWS", source_name="X", source_path="Y"
        )
        is None
    )


def test_upstream_urn_for_calc_view_drops_empty_source_name():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    assert (
        builder.upstream_urn_for_calc_view_source(
            source_type="DATA_BASE_TABLE", source_name="", source_path="REPORTING"
        )
        is None
    )


def test_upstream_urn_for_nested_calculation_view():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    urn = builder.upstream_urn_for_calc_view_source(
        source_type="CALCULATION_VIEW",
        source_name="ProductRollup",
        source_path="acme.analytics.products",
    )
    assert urn is not None
    assert "_sys_bic.acme.analytics.products.productrollup" in urn


# ---------------------------------------------------------------------------
# SAPCalculationViewParser — XML fixtures
# ---------------------------------------------------------------------------


_SIMPLE_CALC_VIEW_XML = """
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="CUSTOMERS" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="CUSTOMERS"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:ProjectionView" id="Projection_1">
      <input node="#CUSTOMERS">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="ID" target="CUST_ID"/>
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="NAME" target="CUST_NAME"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <attributes>
      <attribute id="CUSTOMER_ID">
        <keyMapping columnObjectName="Projection_1" columnName="CUST_ID"/>
      </attribute>
      <attribute id="CUSTOMER_NAME">
        <keyMapping columnObjectName="Projection_1" columnName="CUST_NAME"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""


def test_parser_extracts_simple_projection_lineage():
    parser = SAPCalculationViewParser()
    lineage = parser.column_lineage("SalesOverview", _SIMPLE_CALC_VIEW_XML)

    by_column = {entry["downstream_column"]: entry for entry in lineage}
    assert set(by_column) == {"CUSTOMER_ID", "CUSTOMER_NAME"}

    cust_id = by_column["CUSTOMER_ID"]
    assert len(cust_id["upstreams"]) == 1
    upstream = cust_id["upstreams"][0]
    assert upstream["column"] == "ID"
    assert upstream["source_name"] == "CUSTOMERS"
    assert upstream["source_path"] == "REPORTING"
    assert upstream["source_type"] == "DATA_BASE_TABLE"


_UNION_CALC_VIEW_XML = """
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="ONLINE_SALES" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="ONLINE_SALES"/>
    </DataSource>
    <DataSource id="STORE_SALES" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="STORE_SALES"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:UnionView" id="Union_1">
      <input node="#ONLINE_SALES">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="AMOUNT" target="REVENUE"/>
      </input>
      <input node="#STORE_SALES">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="TOTAL" target="REVENUE"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <baseMeasures>
      <measure id="TOTAL_REVENUE">
        <measureMapping columnObjectName="Union_1" columnName="REVENUE"/>
      </measure>
    </baseMeasures>
  </logicalModel>
</Calculation:scenario>
"""


def test_parser_extracts_union_lineage_from_both_branches():
    parser = SAPCalculationViewParser()
    lineage = parser.column_lineage("RevenueRollup", _UNION_CALC_VIEW_XML)
    assert len(lineage) == 1
    upstream_pairs = {
        (upstream["source_name"], upstream["column"])
        for upstream in lineage[0]["upstreams"]
    }
    assert upstream_pairs == {
        ("ONLINE_SALES", "AMOUNT"),
        ("STORE_SALES", "TOTAL"),
    }


def test_parser_returns_empty_list_for_invalid_xml():
    parser = SAPCalculationViewParser()
    assert parser.column_lineage("Broken", "<not valid xml") == []


def test_parser_extracts_columns_from_formula():
    parser = SAPCalculationViewParser()
    columns = parser._extract_columns_from_formula(
        'if("SALES_AMOUNT" > 1000, "HIGH", "LOW")'
    )
    assert "SALES_AMOUNT" in columns


# ---------------------------------------------------------------------------
# HanaCalculationViewExtractor — workunit shape and aggregator wiring
# ---------------------------------------------------------------------------


def _build_fake_engine(calc_view_rows: list, column_rows: list) -> MagicMock:
    """Wire a MagicMock SQLAlchemy engine that returns canned rows.

    We hand-roll this rather than reaching for a sqlite in-memory engine
    because the extractor's queries reference SAP HANA-specific catalog
    objects (``_SYS_REPO.ACTIVE_OBJECT``, ``SYS.VIEW_COLUMNS``) that don't
    exist in any portable engine.
    """

    def make_row(mapping: dict) -> MagicMock:
        row = MagicMock()
        row._mapping = mapping
        return row

    def execute(stmt, params=None):
        text = str(stmt).lower()
        if "_sys_repo.active_object" in text:
            result = MagicMock()
            result.__iter__ = lambda self: iter([make_row(r) for r in calc_view_rows])
            result.all.return_value = [make_row(r) for r in calc_view_rows]
            return result
        if "sys.view_columns" in text:
            result = MagicMock()
            result.all.return_value = [make_row(r) for r in column_rows]
            return result
        raise AssertionError(f"Unexpected query: {stmt}")

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn.__enter__ = lambda self: conn
    conn.__exit__ = lambda self, *args: None

    engine = MagicMock()
    engine.connect.return_value = conn
    engine.dispose = MagicMock()
    return engine


def test_calc_view_extractor_emits_expected_aspects_and_lineage():
    """Verify the extractor emits one full set of aspects per calc view and
    feeds column-level lineage into the SQL parsing aggregator."""

    config = HanaConfig.model_validate({"include_calculation_views": True})
    identifiers = HanaIdentifierBuilder(config)
    report = MagicMock()
    aggregator = MagicMock()

    calc_view_row = {
        "PACKAGE_ID": "acme.analytics",
        "OBJECT_NAME": "SalesOverview",
        "CDATA": _SIMPLE_CALC_VIEW_XML,
    }
    column_rows = [
        {
            "COLUMN_NAME": "CUSTOMER_ID",
            "COMMENTS": None,
            "DATA_TYPE_NAME": "INTEGER",
            "IS_NULLABLE": "TRUE",
            "POSITION": 1,
            "LENGTH": None,
            "SCALE": None,
        },
        {
            "COLUMN_NAME": "CUSTOMER_NAME",
            "COMMENTS": "Customer display name",
            "DATA_TYPE_NAME": "NVARCHAR",
            "IS_NULLABLE": "TRUE",
            "POSITION": 2,
            "LENGTH": 100,
            "SCALE": None,
        },
    ]
    engine = _build_fake_engine([calc_view_row], column_rows)

    extractor = HanaCalculationViewExtractor(
        config=config,
        report=report,
        identifiers=identifiers,
        engine_factory=lambda: engine,
        aggregator=aggregator,
    )

    workunits: List[MetadataWorkUnit] = list(extractor.get_workunits_internal())

    # Five aspects per calc view: status, schemaMetadata, datasetProperties,
    # subType, viewProperties. They share the same entityUrn.
    assert len(workunits) == 5
    mcps = [cast(MetadataChangeProposalWrapper, wu.metadata) for wu in workunits]
    aspects_by_type = {type(mcp.aspect).__name__: mcp.aspect for mcp in mcps}
    assert set(aspects_by_type) == {
        StatusClass.__name__,
        SchemaMetadataClass.__name__,
        DatasetPropertiesClass.__name__,
        SubTypesClass.__name__,
        ViewPropertiesClass.__name__,
    }

    expected_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:hana,"
        "_sys_bic.acme.analytics.salesoverview,PROD)"
    )
    for mcp in mcps:
        assert mcp.entityUrn == expected_urn

    # Custom properties expose the SAP-side identifiers so consumers can
    # cross-reference DataHub URNs with HANA repository content.
    dataset_props = aspects_by_type[DatasetPropertiesClass.__name__]
    assert isinstance(dataset_props, DatasetPropertiesClass)
    assert dataset_props.customProperties["package_id"] == "acme.analytics"
    assert (
        dataset_props.customProperties["runtime_view_name"]
        == "acme.analytics/SalesOverview"
    )

    schema_metadata = aspects_by_type[SchemaMetadataClass.__name__]
    assert isinstance(schema_metadata, SchemaMetadataClass)
    assert [field.fieldPath for field in schema_metadata.fields] == [
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
    ]
    # NVARCHAR(100) should be reflected in the native type spelling.
    name_field = next(
        f for f in schema_metadata.fields if f.fieldPath == "CUSTOMER_NAME"
    )
    assert name_field.nativeDataType == "NVARCHAR(100)"

    # Aggregator must have been fed the column-level lineage we parsed out
    # of the calc view's XML. The exact ColumnLineageInfo shape is unit-
    # tested via the parser; here we only verify the wiring.
    assert aggregator.add_known_query_lineage.call_count == 1
    known_lineage = aggregator.add_known_query_lineage.call_args.args[0]
    assert known_lineage.downstream == expected_urn
    assert known_lineage.upstreams == [
        "urn:li:dataset:(urn:li:dataPlatform:hana,reporting.customers,PROD)"
    ]
    assert len(known_lineage.column_lineage) == 2

    # The extractor must dispose of the engine it opened, even on the
    # happy path — verifies the try/finally around the inner ``with``.
    engine.dispose.assert_called_once()
