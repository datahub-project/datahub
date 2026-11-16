"""Mapper behaviour added for metadata_api_mode: workspace_apis."""

from typing import Any, List, Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataPlatformTable,
    Lineage,
)
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    DatasetRefresh,
    Measure,
    MeasureReference,
    Page,
    PageFieldReference,
    PowerBIDataset,
    Relationship,
    Report,
    ReportType,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import (
    InputFieldsClass,
    NumberTypeClass,
    OperationClass,
    OperationTypeClass,
    SchemaMetadataClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)

SNOWFLAKE_A = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.a,PROD)"
SNOWFLAKE_B = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.b,PROD)"


def _config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    base = {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "convert_lineage_urns_to_lowercase": False,
        "metadata_api_mode": "workspace_apis",
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.model_validate(base)


def _mapper(config: Optional[PowerBiDashboardSourceConfig] = None) -> Mapper:
    config = config or _config()
    return Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )


def _workspace() -> Workspace:
    return Workspace(
        id="ws-1",
        name="demo-workspace",
        type="Workspace",
        webUrl=None,
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def _column(name: str) -> Column:
    return Column(
        name=name, dataType="Int64", isHidden=False, datahubDataType=NumberTypeClass()
    )


def _dataset(tables: List[Table], **overrides: Any) -> PowerBIDataset:
    dataset = PowerBIDataset(
        id="ds-1",
        name="Model",
        description="",
        webUrl=None,
        workspace_id="ws-1",
        workspace_name="demo-workspace",
        parameters={},
        tables=tables,
        tags=[],
        **overrides,
    )
    for table in tables:
        table.dataset = dataset
    return dataset


def _urn(mapper: Mapper, table: Table) -> str:
    return mapper.make_dataset_urn_for_table(table)


def _lineage_aspect(mcps: List[Any]) -> Optional[UpstreamLineageClass]:
    for mcp in mcps:
        if isinstance(mcp.aspect, UpstreamLineageClass):
            return mcp.aspect
    return None


def test_fine_grained_lineage_emitted_once_per_lineage_not_per_upstream() -> None:
    """Regression: column lineage used to be appended once per upstream table."""
    mapper = _mapper()
    table = Table(name="Orders", full_name="Model.Orders", expression="let ...")
    dataset = _dataset([table])
    lineage = Lineage(
        upstreams=[
            DataPlatformTable(SupportedDataPlatform.SNOWFLAKE.value, SNOWFLAKE_A),
            DataPlatformTable(SupportedDataPlatform.SNOWFLAKE.value, SNOWFLAKE_B),
        ],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="id"),
                upstreams=[ColumnRef(table=SNOWFLAKE_A, column="id")],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(table=None, column="amount"),
                upstreams=[ColumnRef(table=SNOWFLAKE_B, column="amount")],
            ),
        ],
    )
    with patch(
        "datahub.ingestion.source.powerbi.powerbi.parser.get_upstream_tables",
        return_value=[lineage],
    ):
        aspect = _lineage_aspect(
            mapper.extract_lineage(table, _urn(mapper, table), _workspace())
        )

    assert aspect is not None
    assert len(aspect.upstreams) == 2
    assert aspect.fineGrainedLineages is not None
    assert len(aspect.fineGrainedLineages) == 2
    assert dataset.tables == [table]


def test_measure_references_become_cross_table_fine_grained_lineage() -> None:
    mapper = _mapper()
    policies = Table(
        name="Policies", full_name="Model.Policies", columns=[_column("Premium")]
    )
    kpis = Table(
        name="KPIs",
        full_name="Model.KPIs",
        columns=[_column("Value")],
        measures=[
            Measure(
                name="Total Premium",
                expression="SUM('Policies'[Premium])",
                isHidden=False,
                references=[MeasureReference("Policies", "Premium")],
            ),
            Measure(
                name="Self Ref",
                expression="SUM('KPIs'[Value])",
                isHidden=False,
                references=[MeasureReference("KPIs", "Value")],
            ),
            Measure(
                name="Ghost",
                expression="SUM('Policies'[Renamed])",
                isHidden=False,
                references=[MeasureReference("Policies", "Renamed")],
            ),
            Measure(
                name="Ratio",
                expression="[Total Premium] / 2",
                isHidden=False,
                references=[MeasureReference(None, "Total Premium")],
            ),
        ],
    )
    _dataset([policies, kpis])
    with patch(
        "datahub.ingestion.source.powerbi.powerbi.parser.get_upstream_tables",
        return_value=[],
    ):
        aspect = _lineage_aspect(
            mapper.extract_lineage(kpis, _urn(mapper, kpis), _workspace())
        )

    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [_urn(mapper, policies)]
    assert aspect.fineGrainedLineages is not None
    # Only the resolvable cross-table reference produces an edge: the
    # same-table reference, the bare measure reference and the reference to a
    # column Policies does not expose are skipped.
    assert len(aspect.fineGrainedLineages) == 1
    edge = aspect.fineGrainedLineages[0]
    assert edge.downstreams == [
        f"urn:li:schemaField:({_urn(mapper, kpis)},Total Premium)"
    ]
    assert edge.upstreams == [f"urn:li:schemaField:({_urn(mapper, policies)},Premium)"]
    assert mapper._Mapper__reporter.measure_references_unresolved == 1  # type: ignore[attr-defined]


def test_calculated_table_depends_on_referenced_model_tables() -> None:
    mapper = _mapper()
    policies = Table(name="Policies", full_name="Model.Policies")
    claims = Table(name="Claims", full_name="Model.Claims")
    by_state = Table(
        name="By State",
        full_name="Model.By_State",
        calculated_expression="SUMMARIZE('Policies', 'Policies'[State], \"N\", COUNTROWS('Claims'))",
    )
    _dataset([policies, claims, by_state])
    with patch(
        "datahub.ingestion.source.powerbi.powerbi.parser.get_upstream_tables",
        return_value=[],
    ):
        aspect = _lineage_aspect(
            mapper.extract_lineage(by_state, _urn(mapper, by_state), _workspace())
        )
    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [
        _urn(mapper, policies),
        _urn(mapper, claims),
    ]
    assert aspect.fineGrainedLineages is None


def test_relationships_become_foreign_keys_with_datahub_field_semantics() -> None:
    mapper = _mapper(_config(extract_model_relationships=True))
    policies = Table(
        name="Policies", full_name="Model.Policies", columns=[_column("PolicyId")]
    )
    claims = Table(
        name="Claims", full_name="Model.Claims", columns=[_column("PolicyId")]
    )
    _dataset(
        [policies, claims],
        relationships=[
            Relationship("r1", "Claims", "PolicyId", "Policies", "PolicyId"),
            Relationship("r2", "Claims", "PolicyId", "Missing", "Id", is_active=False),
            # A renamed column: no key to a schema field that does not exist.
            Relationship("r3", "Claims", "OldPolicyId", "Policies", "PolicyId"),
        ],
    )
    claims_urn = _urn(mapper, claims)
    schema = mapper.extract_dataset_schema(claims, claims_urn)[0].aspect
    assert isinstance(schema, SchemaMetadataClass)
    assert schema.foreignKeys is not None
    assert len(schema.foreignKeys) == 1
    fk = schema.foreignKeys[0]
    assert fk.name == "Claims.PolicyId -> Policies.PolicyId"
    # sourceFields are on this dataset, foreignFields on the referenced one.
    assert fk.sourceFields == [f"urn:li:schemaField:({claims_urn},PolicyId)"]
    assert fk.foreignDataset == _urn(mapper, policies)
    assert fk.foreignFields == [
        f"urn:li:schemaField:({_urn(mapper, policies)},PolicyId)"
    ]
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.relationships_unresolved == 2
    # Same-title warnings are aggregated into one entry by the reporter.
    assert [w.title for w in reporter.warnings] == ["Relationship Not Mapped"]

    # The `from` side owns the constraint; the `to` table gets none.
    policies_schema = mapper.extract_dataset_schema(policies, _urn(mapper, policies))[
        0
    ].aspect
    assert isinstance(policies_schema, SchemaMetadataClass)
    assert not policies_schema.foreignKeys


def test_refresh_operation_only_for_completed_refresh_with_parseable_time() -> None:
    mapper = _mapper(_config(extract_refresh_history=True))
    table = Table(name="T", full_name="Model.T")

    completed = _dataset(
        [table],
        last_refresh=DatasetRefresh(
            "req",
            "Scheduled",
            "Completed",
            "2022-02-01T20:56:55.927Z",
            "2022-02-01T20:57:05.053Z",
        ),
    )
    mcp = mapper._refresh_operation_mcp(completed, _urn(mapper, table))
    assert mcp is not None
    assert isinstance(mcp.aspect, OperationClass)
    assert mcp.aspect.operationType == OperationTypeClass.CUSTOM
    assert mcp.aspect.customOperationType == "REFRESH"
    assert mcp.aspect.lastUpdatedTimestamp == 1643749025053
    assert mcp.aspect.customProperties == {
        "refreshType": "Scheduled",
        "status": "Completed",
        "requestId": "req",
        "startTime": "2022-02-01T20:56:55.927Z",
    }

    failed = _dataset(
        [table],
        last_refresh=DatasetRefresh(
            "req", "Scheduled", "Failed", None, "2022-02-01T20:57:05Z"
        ),
    )
    assert mapper._refresh_operation_mcp(failed, _urn(mapper, table)) is None

    unparseable = _dataset(
        [table],
        last_refresh=DatasetRefresh("req", "Scheduled", "Completed", None, "yesterday"),
    )
    assert mapper._refresh_operation_mcp(unparseable, _urn(mapper, table)) is None
    assert mapper._Mapper__reporter.refresh_history_unparseable == 1  # type: ignore[attr-defined]

    assert _mapper()._refresh_operation_mcp(completed, _urn(mapper, table)) is None


def test_page_input_fields_resolve_against_dataset_schema() -> None:
    mapper = _mapper(_config(extract_report_field_usage=True))
    policies = Table(
        name="Policies",
        full_name="Model.Policies",
        columns=[_column("State")],
        measures=[Measure(name="Total", expression="1", isHidden=False)],
    )
    dataset = _dataset([policies])
    page = Page(
        id="r1.p1",
        displayName="Overview",
        name="p1",
        order=0,
        field_references=[
            PageFieldReference("Policies", "State", "column"),
            PageFieldReference("Policies", "Total", "measure"),
            PageFieldReference("Policies", "Gone", "column"),
            PageFieldReference("Other", "X", "column"),
        ],
        visual_count=2,
        visual_types=["card"],
    )
    report = Report(
        id="r1",
        name="Report",
        type=ReportType.PowerBIReport,
        webUrl=None,
        embedUrl=None,
        description="",
        dataset_id=dataset.id,
        dataset=dataset,
        pages=[page],
        users=[],
        tags=[],
    )
    mcp = mapper._page_input_fields_mcp(
        page, report, "urn:li:chart:(powerbi,pages.r1.p1)"
    )
    assert mcp is not None
    assert isinstance(mcp.aspect, InputFieldsClass)
    policies_urn = _urn(mapper, policies)
    assert [f.schemaFieldUrn for f in mcp.aspect.fields] == [
        f"urn:li:schemaField:({policies_urn},State)",
        f"urn:li:schemaField:({policies_urn},Total)",
    ]
    assert [f.schemaField.fieldPath for f in mcp.aspect.fields if f.schemaField] == [
        "State",
        "Total",
    ]
    assert mapper._Mapper__reporter.report_field_references_unresolved == 2  # type: ignore[attr-defined]
