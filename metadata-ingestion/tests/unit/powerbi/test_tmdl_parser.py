from typing import List

from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    DefinitionPart,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.tmdl_parser import (
    dax_table_references,
    parse_relationships,
    parse_semantic_model,
    parse_shared_expressions,
    parse_table,
)

IMPORT_TABLE = """/// One row per policy.
table Policies
\tlineageTag: 0000000f-1111-4222-8333-00000000000f

\t/// Surrogate key.
\tcolumn PolicyId
\t\tdataType: int64
\t\tisHidden
\t\tisKey
\t\tsummarizeBy: none
\t\tsourceColumn: POLICY_ID

\tcolumn EffectiveDate
\t\tdataType: dateTime
\t\tformatString: yyyy-MM-dd
\t\tsourceColumn: EFFECTIVE_DATE

\tcolumn PremiumBand = IF('Policies'[WrittenPremium] >= 5000, "High", "Low")
\t\tdataType: string

\tmeasure 'Loss Ratio' =
\t\t\tVAR Losses = SUM('Claims'[PaidAmount])
\t\t\tRETURN DIVIDE(Losses, [Total Premium], 0)
\t\tformatString: 0.0%

\tpartition Policies = m
\t\tmode: import
\t\tsource =
\t\t\t\tlet
\t\t\t\t    Source = Snowflake.Databases("my-account.snowflakecomputing.com","WH"),
\t\t\t\t    Tbl = Source{[Name="POLICIES",Kind="Table"]}[Data]
\t\t\t\tin
\t\t\t\t    Tbl

\tannotation PBI_ResultType = Table
"""

CALCULATED_TABLE = """table 'Claims By State'

\tcolumn State
\t\tdataType: string
\t\tisNameInferred
\t\tsourceColumn: [State]

\tpartition 'Claims By State' = calculated
\t\tmode: import
\t\tsource = SELECTCOLUMNS(SUMMARIZE('Policies', 'Policies'[State]), "State", 'Policies'[State], "N", CALCULATE(COUNTROWS('Claims')))
"""

DIRECTLAKE_TABLE = """table claims
\tsourceLineageTag: [dbo].[claims]

\tcolumn claim_id
\t\tdataType: int64
\t\tsourceColumn: claim_id

\tpartition claims = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: claims
\t\t\tschemaName: dbo
\t\t\texpressionSource: DatabaseQuery
"""

QUERY_TABLE = """table Dates

\tcolumn Date
\t\tdataType: dateTime
\t\tsourceColumn: FullDate

\tpartition Dates = query
\t\tdataView: full
\t\tsource
\t\t\tquery =
\t\t\t\tSELECT [Dates].*
\t\t\t\tFROM [Dates]
\t\t\tdataSource: 00000012-1111-4222-8333-000000000012
"""

RELATIONSHIPS = """relationship 00000011-1111-4222-8333-000000000011
\tfromColumn: Claims.PolicyId
\ttoColumn: Policies.PolicyId

relationship 00000016-1111-4222-8333-000000000016
\tisActive: false
\tfromColumn: Policies.EffectiveDate
\ttoColumn: 'Claims By State'.State

relationship broken
\tfromColumn: NoDot
\ttoColumn: Policies.PolicyId
"""

EXPRESSIONS = """expression DatabaseQuery =
\t\tlet
\t\t    database = Sql.Database("my-endpoint.datawarehouse.fabric.microsoft.com", "6F2E7D8C-9B0A-4C1D-8E2F-3A4B5C6D7E8F")
\t\tin
\t\t    database
\tlineageTag: 00000010-1111-4222-8333-000000000010

expression Param = "value" meta [IsParameterQuery=true]
"""


def test_parse_import_table_columns_measures_and_m_partition() -> None:
    table = parse_table(IMPORT_TABLE)
    assert table is not None
    assert table.name == "Policies"
    assert table.description == "One row per policy."

    columns = {c.name: c for c in table.columns}
    assert set(columns) == {"PolicyId", "EffectiveDate", "PremiumBand"}
    assert columns["PolicyId"].data_type == "Int64"
    assert columns["PolicyId"].source_column == "POLICY_ID"
    assert columns["PolicyId"].is_hidden
    assert columns["PolicyId"].description == "Surrogate key."
    assert columns["EffectiveDate"].data_type == "DateTime"
    assert not columns["EffectiveDate"].is_hidden
    assert columns["PremiumBand"].expression is not None
    assert columns["PremiumBand"].expression.startswith("IF('Policies'[WrittenPremium]")

    assert len(table.measures) == 1
    measure = table.measures[0]
    assert measure.name == "Loss Ratio"
    assert measure.expression.splitlines() == [
        "VAR Losses = SUM('Claims'[PaidAmount])",
        "RETURN DIVIDE(Losses, [Total Premium], 0)",
    ]
    assert [(r.table, r.field) for r in measure.references] == [
        ("Claims", "PaidAmount"),
        (None, "Total Premium"),
    ]

    assert table.partition is not None
    assert table.partition.kind == "m"
    assert table.partition.mode == "import"
    assert table.partition.source is not None
    assert table.partition.source.startswith("let\n")
    assert 'Snowflake.Databases("my-account.snowflakecomputing.com"' in (
        table.partition.source
    )
    assert table.partition.source.rstrip().endswith("Tbl")


def test_parse_calculated_table_and_dax_table_references() -> None:
    table = parse_table(CALCULATED_TABLE)
    assert table is not None
    assert table.partition is not None
    assert table.partition.kind == "calculated"
    assert table.partition.source is not None
    assert table.partition.source.startswith("SELECTCOLUMNS(")
    assert dax_table_references(table.partition.source) == ["Policies", "Claims"]


def test_parse_directlake_entity_partition() -> None:
    table = parse_table(DIRECTLAKE_TABLE)
    assert table is not None
    partition = table.partition
    assert partition is not None
    assert partition.kind == "entity"
    assert partition.mode == "directLake"
    assert partition.entity_name == "claims"
    assert partition.schema_name == "dbo"
    assert partition.expression_source == "DatabaseQuery"
    assert partition.source is None


def test_parse_query_partition_keeps_sql_and_following_properties() -> None:
    table = parse_table(QUERY_TABLE)
    assert table is not None
    assert table.partition is not None
    assert table.partition.kind == "query"
    assert table.partition.source == "SELECT [Dates].*\nFROM [Dates]"
    assert [c.name for c in table.columns] == ["Date"]


def test_parse_relationships_including_inactive_and_quoted_tables() -> None:
    relationships = parse_relationships(RELATIONSHIPS)
    assert [
        (r.from_table, r.from_column, r.to_table, r.to_column, r.is_active)
        for r in relationships
    ] == [
        ("Claims", "PolicyId", "Policies", "PolicyId", True),
        ("Policies", "EffectiveDate", "Claims By State", "State", False),
    ]


def test_parse_relationships_unquotes_column_names() -> None:
    # TMDL quotes any identifier with a space; both sides of the reference
    # must come back as the bare schema field name.
    skipped: List[str] = []
    relationships = parse_relationships(
        "relationship r1\n"
        "\tfromColumn: Claims.'Paid Amount'\n"
        "\ttoColumn: 'Claims By State'.'Loss Ratio'\n"
        "\n"
        "relationship broken\n"
        "\tfromColumn: NoDot\n"
        "\ttoColumn: Policies.PolicyId\n",
        skipped,
    )
    assert [(r.from_column, r.to_table, r.to_column) for r in relationships] == [
        ("Paid Amount", "Claims By State", "Loss Ratio")
    ]
    assert skipped == ["broken"]


def test_parse_shared_expressions_extracts_sql_endpoint() -> None:
    expressions = parse_shared_expressions(EXPRESSIONS)
    assert set(expressions) == {"DatabaseQuery", "Param"}
    assert expressions["DatabaseQuery"].sql_endpoint_id == (
        "6F2E7D8C-9B0A-4C1D-8E2F-3A4B5C6D7E8F"
    )
    assert expressions["DatabaseQuery"].sql_endpoint_server == (
        "my-endpoint.datawarehouse.fabric.microsoft.com"
    )
    assert expressions["Param"].sql_endpoint_id is None


def test_parse_semantic_model_isolates_bad_parts() -> None:
    model = parse_semantic_model(
        [
            DefinitionPart("definition.pbism", "{}"),
            DefinitionPart("definition/tables/Policies.tmdl", IMPORT_TABLE),
            DefinitionPart("definition/tables/Empty.tmdl", "annotation Only = 1\n"),
            DefinitionPart("definition/relationships.tmdl", RELATIONSHIPS),
            DefinitionPart("definition/expressions.tmdl", EXPRESSIONS),
        ]
    )
    assert [t.name for t in model.tables] == ["Policies"]
    assert model.failed_parts == ["definition/tables/Empty.tmdl"]
    assert len(model.relationships) == 2
    assert "DatabaseQuery" in model.shared_expressions
