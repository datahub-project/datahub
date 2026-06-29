import datetime
from typing import Any, Dict, List, cast
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.sql.hana import hana_calculation_view_parser
from datahub.ingestion.source.sql.hana.constants import HanaSourceType
from datahub.ingestion.source.sql.hana.hana import HanaSource
from datahub.ingestion.source.sql.hana.hana_calculation_view_parser import (
    SAPCalculationViewParser,
)
from datahub.ingestion.source.sql.hana.hana_config import HanaConfig
from datahub.ingestion.source.sql.hana.hana_data_dictionary import HanaDataDictionary
from datahub.ingestion.source.sql.hana.hana_schema import (
    HanaCalculationView,
    HanaCalcViewColumn,
)
from datahub.ingestion.source.sql.hana.hana_schema_gen import (
    HanaCalculationViewExtractor,
)
from datahub.ingestion.source.sql.hana.hana_script_lineage import (
    extract_table_references,
)
from datahub.ingestion.source.sql.hana.hana_utils import HanaIdentifierBuilder
from datahub.ingestion.source.sql.hana.models import (
    ColumnLineage,
    HanaObservedQueryRow,
    ScriptViewDefinition,
    UpstreamColumnRef,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from tests.integration.hana._xml_fixtures import (
    PROJECTION_VIEW_XML as _SIMPLE_CALC_VIEW_XML,
    SQL_SCRIPT_VIEW_XML as _SQL_SCRIPT_VIEW_XML,
)

# ---------------------------------------------------------------------------
# Config / source wiring
# ---------------------------------------------------------------------------


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
    assert not config.schema_pattern.allowed("SYS")
    assert not config.schema_pattern.allowed("_SYS_STATISTICS")
    assert not config.schema_pattern.allowed("_SYS_REPO")
    # _SYS_BIC must stay accessible — it exposes activated calc views.
    assert config.schema_pattern.allowed("_SYS_BIC")
    assert config.schema_pattern.allowed("REPORTING")


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"data_type": "DECIMAL", "length": 15, "scale": 2}, "DECIMAL(15,2)"),
        ({"data_type": "NVARCHAR", "length": 100}, "NVARCHAR(100)"),
        ({"data_type": "INTEGER"}, "INTEGER"),
        ({"data_type": "FLOAT", "length": 24}, "FLOAT(24)"),
        ({"data_type": "BIGINT", "length": 8}, "BIGINT"),
    ],
)
def test_calc_view_column_precise_native_type(
    kwargs: Dict[str, Any], expected: str
) -> None:
    column = HanaCalcViewColumn(name="C", nullable=True, ordinal_position=1, **kwargs)
    assert column.get_precise_native_type() == expected


def test_include_usage_stats_without_query_usage_rejected():
    with pytest.raises(ValueError, match="include_query_usage"):
        HanaConfig.model_validate(
            {"include_usage_stats": True, "include_query_usage": False}
        )


def test_calc_view_urn_lower_cased_and_uses_sys_bic_prefix():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="localhost:39041"))
    urn = builder.calc_view_urn(
        HanaCalculationView(
            package_id="Acme.Analytics", name="SalesOverview", definition="<x/>"
        )
    )
    assert urn == (
        "urn:li:dataset:(urn:li:dataPlatform:hana,"
        "_sys_bic.acme.analytics.salesoverview,PROD)"
    )


def test_upstream_urn_for_data_base_table():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    urn = builder.upstream_urn_for_calc_view_source(
        source_type=HanaSourceType.DATA_BASE_TABLE,
        source_name="CUSTOMERS",
        source_path="REPORTING",
    )
    assert urn is not None
    assert "reporting.customers" in urn


def test_parser_drops_data_source_with_unknown_type():
    xml = """
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="MYSTERY" type="HDI_FUNCTION_FROM_THE_FUTURE">
      <columnObject schemaName="REPORTING" columnObjectName="MYSTERY"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:ProjectionView" id="Projection_1">
      <input node="#MYSTERY">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="X" target="X"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <attributes>
      <attribute id="X">
        <keyMapping columnObjectName="Projection_1" columnName="X"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""
    lineage = SAPCalculationViewParser().column_lineage("Mystery", xml)
    assert lineage and lineage[0].upstreams == []


def test_upstream_urn_for_calc_view_drops_empty_source_name():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    assert (
        builder.upstream_urn_for_calc_view_source(
            source_type=HanaSourceType.DATA_BASE_TABLE,
            source_name="",
            source_path="REPORTING",
        )
        is None
    )


def test_upstream_urn_for_nested_calculation_view():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    urn = builder.upstream_urn_for_calc_view_source(
        source_type=HanaSourceType.CALCULATION_VIEW,
        source_name="ProductRollup",
        source_path="acme.analytics.products",
    )
    assert urn is not None
    assert "_sys_bic.acme.analytics.products.productrollup" in urn


def test_upstream_urn_for_table_function_flattens_namespace():
    builder = HanaIdentifierBuilder(HanaConfig(host_port="h:1"))
    urn = builder.upstream_urn_for_calc_view_source(
        source_type=HanaSourceType.TABLE_FUNCTION,
        source_name="acme.analytics::tf_sales",
        source_path=None,
    )
    assert urn is not None
    assert "_sys_bic.acme.analytics.tf_sales" in urn


def _lineage_by_column(
    lineage: List[ColumnLineage],
) -> Dict[str, ColumnLineage]:
    return {entry.downstream_column: entry for entry in lineage}


def test_parser_extracts_simple_projection_lineage():
    lineage = SAPCalculationViewParser().column_lineage(
        "SalesOverview", _SIMPLE_CALC_VIEW_XML
    )
    by_col = _lineage_by_column(lineage)
    assert set(by_col) == {"CUSTOMER_ID", "CUSTOMER_NAME"}

    cust_id = by_col["CUSTOMER_ID"]
    assert len(cust_id.upstreams) == 1
    assert cust_id.upstreams[0] == UpstreamColumnRef(
        column="ID",
        source_name="CUSTOMERS",
        source_path="REPORTING",
        source_type=HanaSourceType.DATA_BASE_TABLE,
    )


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
    lineage = SAPCalculationViewParser().column_lineage(
        "RevenueRollup", _UNION_CALC_VIEW_XML
    )
    assert len(lineage) == 1
    pairs = {(u.source_name, u.column) for u in lineage[0].upstreams}
    assert pairs == {("ONLINE_SALES", "AMOUNT"), ("STORE_SALES", "TOTAL")}


# JoinView and AggregationView share the non-Union code path with
# ProjectionView, so a single shared fixture covers them. They are the
# most common non-projection node types in real HANA calc views, so we
# keep an explicit test to lock the behaviour even though the underlying
# code path is the same.


def _calc_view_xml_for_node_type(node_xsi_type: str) -> str:
    return f"""
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="ORDERS" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="ORDERS"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="{node_xsi_type}" id="Node_1">
      <input node="#ORDERS">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="AMOUNT" target="TOTAL"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <baseMeasures>
      <measure id="TOTAL_AMOUNT">
        <measureMapping columnObjectName="Node_1" columnName="TOTAL"/>
      </measure>
    </baseMeasures>
  </logicalModel>
</Calculation:scenario>
"""


@pytest.mark.parametrize(
    "node_xsi_type",
    ["Calculation:JoinView", "Calculation:AggregationView"],
)
def test_parser_traces_lineage_through_join_and_aggregation_nodes(
    node_xsi_type: str,
) -> None:
    xml = _calc_view_xml_for_node_type(node_xsi_type)
    lineage = SAPCalculationViewParser().column_lineage("OrdersRollup", xml)
    assert len(lineage) == 1
    assert lineage[0].downstream_column == "TOTAL_AMOUNT"
    assert lineage[0].upstreams == [
        UpstreamColumnRef(
            column="AMOUNT",
            source_name="ORDERS",
            source_path="REPORTING",
            source_type=HanaSourceType.DATA_BASE_TABLE,
        ),
    ]


def test_parser_returns_empty_list_for_invalid_xml():
    assert SAPCalculationViewParser().column_lineage("Broken", "<not valid xml") == []


def test_parser_rejects_xml_bomb():
    """defusedxml must reject XML with billion-laughs / entity expansion."""
    xml_bomb = (
        '<?xml version="1.0"?>\n'
        "<!DOCTYPE lolz [\n"
        '  <!ENTITY lol "lol">\n'
        '  <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">\n'
        "]>\n"
        '<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore">\n'
        "  <name>&lol2;</name>\n"
        "</Calculation:scenario>"
    )
    assert SAPCalculationViewParser().column_lineage("Bomb", xml_bomb) == []


def test_parser_returns_empty_lineage_when_collector_raises(monkeypatch):
    """A collector raising mid-parse must yield an empty model, not a partial one."""

    def explode(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(hana_calculation_view_parser, "_collect_outputs", explode)

    assert (
        SAPCalculationViewParser().column_lineage("PartialParse", _SIMPLE_CALC_VIEW_XML)
        == []
    )


def test_parser_extracts_columns_from_formula():
    columns = SAPCalculationViewParser._extract_columns_from_formula(
        'if("SALES_AMOUNT" > 1000, "HIGH", "LOW")'
    )
    assert "SALES_AMOUNT" in columns


# TREE_BASED dimension views expose a DataSource directly through the
# logicalModel; keyMappings reference it by columnObjectName, not by id.
_TREE_BASED_NO_INNER_NODE_XML = """
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                      calculationScenarioType="TREE_BASED" dataCategory="DIMENSION">
  <dataSources>
    <DataSource id="DIM_SRC" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="DIM_CUSTOMERS"/>
    </DataSource>
  </dataSources>
  <calculationViews/>
  <logicalModel id="DIM_CUSTOMERS">
    <attributes>
      <attribute id="CUST_ID">
        <keyMapping columnObjectName="DIM_CUSTOMERS" columnName="ID"/>
      </attribute>
      <attribute id="CUST_NAME">
        <keyMapping columnObjectName="DIM_CUSTOMERS" columnName="NAME"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""


def test_parser_resolves_data_source_by_column_object_name_alias():
    lineage = SAPCalculationViewParser().column_lineage(
        "DimCustomers", _TREE_BASED_NO_INNER_NODE_XML
    )
    by_col = _lineage_by_column(lineage)
    assert set(by_col) == {"CUST_ID", "CUST_NAME"}
    upstream = by_col["CUST_ID"].upstreams[0]
    assert upstream.column == "ID"
    assert upstream.source_name == "DIM_CUSTOMERS"
    assert upstream.source_path == "REPORTING"


_FORMULA_IN_SAME_NODE_XML = """
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="CUSTOMERS" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="CUSTOMERS"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:ProjectionView" id="Projection_1">
      <calculatedViewAttributes>
        <calculatedViewAttribute id="UPPER_NAME">
          <formula>upper("ORIG_NAME")</formula>
        </calculatedViewAttribute>
      </calculatedViewAttributes>
      <input node="#CUSTOMERS">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="NAME" target="ORIG_NAME"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <attributes>
      <attribute id="UPPER_CUSTOMER_NAME">
        <keyMapping columnObjectName="Projection_1" columnName="UPPER_NAME"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""


def test_parser_traces_formula_referencing_input_column_in_same_node():
    lineage = SAPCalculationViewParser().column_lineage(
        "Upper", _FORMULA_IN_SAME_NODE_XML
    )
    assert len(lineage) == 1
    upstream = lineage[0].upstreams[0]
    assert upstream.source_name == "CUSTOMERS"
    # Formula referenced ORIG_NAME, mapped from CUSTOMERS.NAME.
    assert upstream.column == "NAME"


# Calc views in the wild often map a column literally named ``type``; the
# previous formula-vs-branch discriminator collided on this name and dropped
# every column in the affected node.
_COLUMN_NAMED_TYPE_XML = """
<Calculation:scenario xmlns:Calculation="http://www.sap.com/ndb/BiModelCalculation.ecore"
                      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dataSources>
    <DataSource id="EVENTS" type="DATA_BASE_TABLE">
      <columnObject schemaName="REPORTING" columnObjectName="EVENTS"/>
    </DataSource>
  </dataSources>
  <calculationViews>
    <calculationView xsi:type="Calculation:ProjectionView" id="Projection_1">
      <input node="#EVENTS">
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="EVENT_TYPE" target="type"/>
        <mapping xsi:type="Calculation:AttributeMapping"
                 source="EVENT_TS"   target="ts"/>
      </input>
    </calculationView>
  </calculationViews>
  <logicalModel>
    <attributes>
      <attribute id="event_kind">
        <keyMapping columnObjectName="Projection_1" columnName="type"/>
      </attribute>
      <attribute id="event_time">
        <keyMapping columnObjectName="Projection_1" columnName="ts"/>
      </attribute>
    </attributes>
  </logicalModel>
</Calculation:scenario>
"""


def test_parser_handles_column_literally_named_type():
    lineage = SAPCalculationViewParser().column_lineage(
        "Events", _COLUMN_NAMED_TYPE_XML
    )
    by_col = _lineage_by_column(lineage)
    assert by_col["event_kind"].upstreams[0].column == "EVENT_TYPE"
    assert by_col["event_time"].upstreams[0].column == "EVENT_TS"


def test_parser_captures_sql_script_view_definitions():
    scripts = SAPCalculationViewParser().script_view_definitions(
        "SalesScript", _SQL_SCRIPT_VIEW_XML
    )
    assert len(scripts) == 1
    assert isinstance(scripts[0], ScriptViewDefinition)
    assert scripts[0].node_id == "Script_View"
    assert 'FROM "REPORTING"."SALES"' in scripts[0].definition


def test_parser_does_not_walk_sql_script_view_as_xml_node():
    lineage = SAPCalculationViewParser().column_lineage(
        "SalesScript", _SQL_SCRIPT_VIEW_XML
    )
    # SqlScriptView columns have no resolvable XML-DAG upstreams.
    for entry in lineage:
        assert entry.upstreams == []


def test_extract_table_references_finds_from_and_join_refs():
    sql = """
    BEGIN
        T_X = SELECT * FROM "REPORTING"."SALES"
              JOIN "REPORTING"."CUSTOMERS" ON "SALES"."ID" = "CUSTOMERS"."ID";
        T_Y = SELECT * FROM "REPORTING"."ORDERS";
    END
    """
    refs = extract_table_references(sql)
    assert {(r.schema_name, r.name) for r in refs} == {
        ("REPORTING", "SALES"),
        ("REPORTING", "CUSTOMERS"),
        ("REPORTING", "ORDERS"),
    }


def test_extract_table_references_skips_dummy():
    assert extract_table_references('SELECT 1 FROM "DUMMY"; SELECT 1 FROM DUMMY;') == []


def test_extract_table_references_dedupes_repeated_references():
    sql = (
        'SELECT * FROM "X"."A" JOIN "X"."B" ON "A"."k" = "B"."k" '
        'UNION ALL SELECT * FROM "X"."A";'
    )
    refs = extract_table_references(sql)
    assert [(r.schema_name, r.name) for r in refs] == [("X", "A"), ("X", "B")]


def test_extract_table_references_empty_for_call_only_script():
    sql = 'BEGIN call "UIS"."sap.hana.uis.db/GET_NAVIGATION_URL"(IN_TAG, var_out); END'
    assert extract_table_references(sql) == []


def _build_fake_engine(calc_view_rows: list, column_rows: list) -> MagicMock:
    """MagicMock engine that returns canned _SYS_REPO / SYS.VIEW_COLUMNS rows."""

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
    config = HanaConfig.model_validate({"include_calculation_views": True})
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
        report=MagicMock(),
        identifiers=HanaIdentifierBuilder(config),
        engine_factory=lambda: engine,
        aggregator=aggregator,
    )

    workunits: List[MetadataWorkUnit] = list(extractor.get_workunits_internal())

    # Five aspects per calc view: status, schemaMetadata, datasetProperties,
    # subType, viewProperties.
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
    assert all(mcp.entityUrn == expected_urn for mcp in mcps)

    subtypes = aspects_by_type[SubTypesClass.__name__]
    assert isinstance(subtypes, SubTypesClass)
    assert subtypes.typeNames == [DatasetSubTypes.SAP_HANA_CALCULATION_VIEW]

    dataset_props = aspects_by_type[DatasetPropertiesClass.__name__]
    assert isinstance(dataset_props, DatasetPropertiesClass)
    assert dataset_props.customProperties["package_id"] == "acme.analytics"
    assert (
        dataset_props.customProperties["runtime_view_name"]
        == "acme.analytics/SalesOverview"
    )

    schema_metadata = aspects_by_type[SchemaMetadataClass.__name__]
    assert isinstance(schema_metadata, SchemaMetadataClass)
    assert [f.fieldPath for f in schema_metadata.fields] == [
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
    ]
    name_field = next(
        f for f in schema_metadata.fields if f.fieldPath == "CUSTOMER_NAME"
    )
    assert name_field.nativeDataType == "NVARCHAR(100)"

    assert aggregator.add_known_query_lineage.call_count == 1
    known = aggregator.add_known_query_lineage.call_args.args[0]
    assert known.downstream == expected_urn
    assert known.upstreams == [
        "urn:li:dataset:(urn:li:dataPlatform:hana,reporting.customers,PROD)"
    ]
    assert len(known.column_lineage) == 2
    engine.dispose.assert_called_once()


def test_calc_view_extractor_emits_table_lineage_from_sql_script_view():
    config = HanaConfig.model_validate({"include_calculation_views": True})
    aggregator = MagicMock()
    calc_view_row = {
        "PACKAGE_ID": "acme.scripts",
        "OBJECT_NAME": "SalesScript",
        "CDATA": _SQL_SCRIPT_VIEW_XML,
    }
    column_rows = [
        {
            "COLUMN_NAME": "CUST_ID",
            "COMMENTS": None,
            "DATA_TYPE_NAME": "INTEGER",
            "IS_NULLABLE": "TRUE",
            "POSITION": 1,
            "LENGTH": None,
            "SCALE": None,
        },
        {
            "COLUMN_NAME": "REVENUE",
            "COMMENTS": None,
            "DATA_TYPE_NAME": "DECIMAL",
            "IS_NULLABLE": "TRUE",
            "POSITION": 2,
            "LENGTH": 15,
            "SCALE": 2,
        },
    ]
    engine = _build_fake_engine([calc_view_row], column_rows)
    extractor = HanaCalculationViewExtractor(
        config=config,
        report=MagicMock(),
        identifiers=HanaIdentifierBuilder(config),
        engine_factory=lambda: engine,
        aggregator=aggregator,
    )
    list(extractor.get_workunits_internal())

    assert aggregator.add_known_query_lineage.call_count == 1
    known = aggregator.add_known_query_lineage.call_args.args[0]
    # SqlScriptView has no XML column lineage, only table-level upstreams.
    assert known.column_lineage == []
    assert sorted(known.upstreams) == [
        "urn:li:dataset:(urn:li:dataPlatform:hana,reporting.customers,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hana,reporting.sales,PROD)",
    ]


def test_calc_view_extractor_reports_unmapped_hana_types():
    config = HanaConfig.model_validate({"include_calculation_views": True})
    aggregator = MagicMock()
    report = MagicMock()

    calc_view_row = {
        "PACKAGE_ID": "acme.analytics",
        "OBJECT_NAME": "SalesOverview",
        "CDATA": _SIMPLE_CALC_VIEW_XML,
    }
    column_rows = [
        {
            "COLUMN_NAME": "FUTURE_COL",
            "COMMENTS": None,
            "DATA_TYPE_NAME": "FUTURE_HANA_TYPE_FROM_HEAVEN",
            "IS_NULLABLE": "TRUE",
            "POSITION": 1,
            "LENGTH": None,
            "SCALE": None,
        },
    ]
    engine = _build_fake_engine([calc_view_row], column_rows)
    extractor = HanaCalculationViewExtractor(
        config=config,
        report=report,
        identifiers=HanaIdentifierBuilder(config),
        engine_factory=lambda: engine,
        aggregator=aggregator,
    )

    list(extractor.get_workunits_internal())

    report.info.assert_called_once()
    info_kwargs = report.info.call_args.kwargs
    assert "FUTURE_HANA_TYPE_FROM_HEAVEN" in info_kwargs["context"]


def test_calculation_view_pattern_deny_drops_matching_views():
    config = HanaConfig.model_validate(
        {
            "include_calculation_views": True,
            "calculation_view_pattern": {"deny": [r"^acme\.sandbox\..*"]},
        }
    )
    aggregator = MagicMock()
    report = MagicMock()

    calc_view_rows = [
        {
            "PACKAGE_ID": "acme.analytics",
            "OBJECT_NAME": "SalesOverview",
            "CDATA": _SIMPLE_CALC_VIEW_XML,
        },
        {
            "PACKAGE_ID": "acme.sandbox",
            "OBJECT_NAME": "Experimental",
            "CDATA": _SIMPLE_CALC_VIEW_XML,
        },
    ]
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
    ]
    engine = _build_fake_engine(calc_view_rows, column_rows)
    extractor = HanaCalculationViewExtractor(
        config=config,
        report=report,
        identifiers=HanaIdentifierBuilder(config),
        engine_factory=lambda: engine,
        aggregator=aggregator,
    )

    workunits: List[MetadataWorkUnit] = list(extractor.get_workunits_internal())

    emitted_urns = {
        cast(MetadataChangeProposalWrapper, wu.metadata).entityUrn for wu in workunits
    }
    assert emitted_urns == {
        "urn:li:dataset:(urn:li:dataPlatform:hana,"
        "_sys_bic.acme.analytics.salesoverview,PROD)"
    }
    report.report_dropped.assert_called_once_with("_sys_bic.acme.sandbox.experimental")
    assert aggregator.add_known_query_lineage.call_count == 1


def test_get_procedures_for_schema_yields_base_procedures():
    proc_row = {
        "SCHEMA_NAME": "REPORTING",
        "PROCEDURE_NAME": "REFRESH_SALES",
        "DEFINITION": (
            'BEGIN\n  INSERT INTO "REPORTING"."SALES_AGG" '
            'SELECT * FROM "REPORTING"."SALES";\nEND'
        ),
        "PROCEDURE_TYPE": "PROCEDURE",
        "CREATE_TIME": None,
        "LANGUAGE": "SQLSCRIPT",
        "ARGUMENT_SIGNATURE": "()",
    }

    def make_row(mapping):
        row = MagicMock()
        row._mapping = mapping
        return row

    result = MagicMock()
    result.all.return_value = [make_row(proc_row)]
    conn = MagicMock()
    conn.execute.return_value = result

    data_dict = HanaDataDictionary(conn, MagicMock())
    procs = list(data_dict.get_stored_procedures("REPORTING"))
    assert len(procs) == 1
    assert procs[0].name == "REFRESH_SALES"
    assert procs[0].default_schema == "REPORTING"
    assert procs[0].language == "SQLSCRIPT"
    assert procs[0].procedure_definition is not None
    assert 'INSERT INTO "REPORTING"."SALES_AGG"' in procs[0].procedure_definition


def test_iter_observed_queries_yields_typed_rows_and_skips_empty_text():
    ts = datetime.datetime(2025, 1, 1, 12, 30, 0, tzinfo=datetime.timezone.utc)
    valid_row = {
        "STATEMENT_HASH": "abc123",
        "STATEMENT_STRING": 'SELECT * FROM "REPORTING"."CUSTOMERS"',
        "USER_NAME": "ALICE",
        "SCHEMA_NAME": "REPORTING",
        "APPLICATION_NAME": "HANA_STUDIO",
        "LAST_EXECUTION_TIMESTAMP": ts,
    }
    empty_text_row = {
        "STATEMENT_HASH": "def456",
        "STATEMENT_STRING": None,
        "USER_NAME": "BOB",
        "SCHEMA_NAME": "REPORTING",
        "APPLICATION_NAME": None,
        "LAST_EXECUTION_TIMESTAMP": ts,
    }

    def make_row(mapping):
        row = MagicMock()
        row._mapping = mapping
        return row

    result = MagicMock()
    result.all.return_value = [make_row(valid_row), make_row(empty_text_row)]
    conn = MagicMock()
    conn.execute.return_value = result

    data_dict = HanaDataDictionary(conn, MagicMock())
    rows = list(
        data_dict.iter_observed_queries(
            start_time=ts - datetime.timedelta(hours=1),
            end_time=ts + datetime.timedelta(hours=1),
            top_n=100,
        )
    )
    assert len(rows) == 1
    row = rows[0]
    assert isinstance(row, HanaObservedQueryRow)
    assert row.statement_hash == "abc123"
    assert row.user_name == "ALICE"
    assert row.schema_name == "REPORTING"
    assert row.last_execution_timestamp == ts


def test_iter_observed_queries_reports_warning_on_failure():
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("statistics service not running")
    report = MagicMock()

    data_dict = HanaDataDictionary(conn, report)
    rows = list(
        data_dict.iter_observed_queries(
            start_time=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            end_time=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
            top_n=100,
        )
    )
    assert rows == []
    report.warning.assert_called_once()


def test_hana_source_aggregator_has_usage_hooks_when_enabled():
    config = HanaConfig.model_validate(
        {
            "include_query_usage": True,
            "include_usage_stats": True,
        }
    )
    source = HanaSource(ctx=PipelineContext(run_id="hana-usage-test"), config=config)
    assert source.aggregator.generate_usage_statistics is True
    assert source.aggregator.usage_config is config


def test_hana_source_aggregator_disables_usage_when_query_usage_off():
    source = HanaSource(
        ctx=PipelineContext(run_id="hana-usage-off-test"),
        config=HanaConfig(),
    )
    assert source.aggregator.generate_usage_statistics is False
    assert source.aggregator.usage_config is None
