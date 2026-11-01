"""Mapper behaviour added for metadata_api_mode: workspace_apis."""

import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.powerbi.config import (
    Constant,
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
    DaxFieldReference,
    Measure,
    Page,
    PageFieldReference,
    PowerBIDataset,
    RefreshPolicy,
    Relationship,
    Report,
    ReportType,
    RlsTableFilter,
    Table,
    Workspace,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
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
        parameters=overrides.pop("parameters", {}),
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
                references=[DaxFieldReference("Policies", "Premium")],
            ),
            Measure(
                name="Self Ref",
                expression="SUM('KPIs'[Value])",
                isHidden=False,
                references=[DaxFieldReference("KPIs", "Value")],
            ),
            Measure(
                name="Ghost",
                expression="SUM('Policies'[Renamed])",
                isHidden=False,
                references=[DaxFieldReference("Policies", "Renamed")],
            ),
            Measure(
                name="Ratio",
                expression="[Total Premium] / 2",
                isHidden=False,
                references=[DaxFieldReference(None, "Total Premium")],
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
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.dax_references_unresolved == 1
    assert [w.title for w in reporter.warnings] == [Constant.DAX_REFERENCE_NOT_RESOLVED]


def _dax_model(mapper: Mapper) -> List[Table]:
    """Policies (import) with a calculated column, KPIs with measures that
    reference each other and a column of Policies."""
    policies = Table(
        name="Policies",
        full_name="Model.Policies",
        columns=[
            _column("Premium"),
            Column(
                name="Band",
                dataType="String",
                isHidden=False,
                datahubDataType=NumberTypeClass(),
                columnType="Calculated",
                expression="IF([Premium] > 1, [Total Premium], RELATED('KPIs'[Value]))",
                references=[
                    DaxFieldReference(None, "Premium"),
                    DaxFieldReference(None, "Total Premium"),
                    DaxFieldReference("KPIs", "Value"),
                    DaxFieldReference("KPIs", "Missing"),
                ],
            ),
        ],
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
                references=[DaxFieldReference("Policies", "Premium")],
            ),
            Measure(
                name="Ratio",
                expression="[Total Premium] / 'KPIs'[Total Premium] + [Nowhere]",
                isHidden=False,
                references=[
                    DaxFieldReference(None, "Total Premium"),
                    DaxFieldReference("KPIs", "Total Premium"),
                    DaxFieldReference(None, "Nowhere"),
                ],
            ),
        ],
    )
    _dataset([policies, kpis])
    return [policies, kpis]


def _extract(mapper: Mapper, table: Table) -> Optional[UpstreamLineageClass]:
    with patch(
        "datahub.ingestion.source.powerbi.powerbi.parser.get_upstream_tables",
        return_value=[],
    ):
        return _lineage_aspect(
            mapper.extract_lineage(table, _urn(mapper, table), _workspace())
        )


def test_dax_reference_lineage_is_off_by_default() -> None:
    mapper = _mapper()
    policies, kpis = _dax_model(mapper)
    # A calculated column has no upstream and no DAX edges without the flag.
    assert _extract(mapper, policies) is None
    aspect = _extract(mapper, kpis)
    assert aspect is not None
    assert aspect.fineGrainedLineages is not None
    assert [e.downstreams for e in aspect.fineGrainedLineages] == [
        [f"urn:li:schemaField:({_urn(mapper, kpis)},Total Premium)"]
    ]
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.dax_references_unresolved == 0


def test_dax_reference_lineage_for_calculated_columns_and_measures() -> None:
    mapper = _mapper(_config(extract_dax_reference_lineage=True))
    policies, kpis = _dax_model(mapper)
    policies_urn, kpis_urn = _urn(mapper, policies), _urn(mapper, kpis)

    aspect = _extract(mapper, policies)
    assert aspect is not None
    # The calculated column reads KPIs, which becomes a coarse upstream; the
    # same-table column is a field of this dataset and adds no self-upstream.
    assert [u.dataset for u in aspect.upstreams] == [kpis_urn]
    assert aspect.fineGrainedLineages is not None
    assert len(aspect.fineGrainedLineages) == 1
    band = aspect.fineGrainedLineages[0]
    assert band.downstreams == [f"urn:li:schemaField:({policies_urn},Band)"]
    assert band.upstreams == [
        f"urn:li:schemaField:({policies_urn},Premium)",
        f"urn:li:schemaField:({kpis_urn},Total Premium)",
        f"urn:li:schemaField:({kpis_urn},Value)",
    ]

    aspect = _extract(mapper, kpis)
    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [policies_urn]
    assert aspect.fineGrainedLineages is not None
    edges = {
        (e.downstreams or [""])[0]: e.upstreams for e in aspect.fineGrainedLineages
    }
    # Bare and table-qualified references to the same measure collapse into
    # one edge from that measure's field.
    assert edges[f"urn:li:schemaField:({kpis_urn},Ratio)"] == [
        f"urn:li:schemaField:({kpis_urn},Total Premium)"
    ]

    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    # KPIs[Missing] on the column and [Nowhere] on the measure.
    assert reporter.dax_references_unresolved == 2
    assert reporter.dax_references_resolved == 6
    assert {w.title for w in reporter.warnings} == {Constant.DAX_REFERENCE_NOT_RESOLVED}


def test_self_only_dax_edges_still_emit_the_lineage_aspect() -> None:
    mapper = _mapper(_config(extract_dax_reference_lineage=True))
    kpis = Table(
        name="KPIs",
        full_name="Model.KPIs",
        measures=[
            Measure(name="Base", expression="1", isHidden=False),
            Measure(
                name="Derived",
                expression="[Base] * 2",
                isHidden=False,
                references=[DaxFieldReference(None, "Base")],
            ),
        ],
    )
    _dataset([kpis])
    aspect = _extract(mapper, kpis)
    assert aspect is not None
    assert aspect.upstreams == []
    assert aspect.fineGrainedLineages is not None
    assert aspect.fineGrainedLineages[0].upstreams == [
        f"urn:li:schemaField:({_urn(mapper, kpis)},Base)"
    ]


def _dax_mapper() -> Mapper:
    return _mapper(_config(extract_dax_reference_lineage=True))


def _warnings(mapper: Mapper) -> List[str]:
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    return [
        str(context)
        for warning in reporter.warnings
        if warning.title == Constant.DAX_REFERENCE_NOT_RESOLVED
        for context in warning.context
    ]


def _calculated_column(
    name: str,
    references: List[DaxFieldReference],
    has_row_context_iterators: bool = False,
) -> Column:
    return Column(
        name=name,
        dataType="String",
        isHidden=False,
        datahubDataType=NumberTypeClass(),
        columnType="Calculated",
        references=references,
        has_row_context_iterators=has_row_context_iterators,
    )


def test_dax_identifiers_resolve_case_insensitively() -> None:
    mapper = _dax_mapper()
    kpis = Table(name="KPIs", full_name="Model.KPIs", columns=[_column("Value")])
    policies = Table(
        name="Policies",
        full_name="Model.Policies",
        columns=[
            _column("Premium"),
            _calculated_column(
                "Band",
                [
                    DaxFieldReference("kpis", "value"),
                    DaxFieldReference(None, "PREMIUM"),
                ],
            ),
        ],
    )
    _dataset([policies, kpis])
    aspect = _extract(mapper, policies)
    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [_urn(mapper, kpis)]
    assert aspect.fineGrainedLineages is not None
    assert aspect.fineGrainedLineages[0].upstreams == [
        f"urn:li:schemaField:({_urn(mapper, kpis)},Value)",
        f"urn:li:schemaField:({_urn(mapper, policies)},Premium)",
    ]
    assert _warnings(mapper) == []


def test_bare_column_inside_an_iterator_is_not_this_tables_column() -> None:
    mapper = _dax_mapper()
    policies = Table(
        name="Policies", full_name="Model.Policies", columns=[_column("Premium")]
    )
    claims = Table(
        name="Claims",
        full_name="Model.Claims",
        columns=[
            _column("Premium"),
            _calculated_column(
                "Policy Premium",
                [DaxFieldReference(None, "Premium")],
                has_row_context_iterators=True,
            ),
        ],
    )
    _dataset([policies, claims])
    # SUMX(RELATEDTABLE(Policies), [Premium]) reads Policies[Premium], which
    # cannot be told from Claims[Premium] without evaluating the DAX: no edge,
    # and no warning either since the column does exist.
    assert _extract(mapper, claims) is None
    assert _warnings(mapper) == []
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.dax_references_unattributed == 1
    assert reporter.dax_references_unresolved == 0


def test_bare_column_inside_a_measure_iterator_is_counted_not_warned() -> None:
    mapper = _dax_mapper()
    claims = Table(
        name="Claims", full_name="Model.Claims", columns=[_column("PaidAmount")]
    )
    kpis = Table(
        name="KPIs",
        full_name="Model.KPIs",
        measures=[
            Measure(
                name="Paid",
                expression="SUMX('Claims', [PaidAmount])",
                isHidden=False,
                references=[DaxFieldReference(None, "PaidAmount")],
                has_row_context_iterators=True,
            )
        ],
    )
    _dataset([claims, kpis])
    assert _extract(mapper, kpis) is None
    assert _warnings(mapper) == []
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.dax_references_unattributed == 1
    assert reporter.dax_references_unresolved == 0


def test_same_table_reference_to_nothing_is_reported() -> None:
    mapper = _dax_mapper()
    kpis = Table(
        name="KPIs",
        full_name="Model.KPIs",
        columns=[_column("Value")],
        measures=[
            Measure(
                name="Ratio",
                expression="'KPIs'[Typo] + 'KPIs'[Value]",
                isHidden=False,
                references=[
                    DaxFieldReference("KPIs", "Typo"),
                    DaxFieldReference("KPIs", "Value"),
                ],
            )
        ],
    )
    _dataset([kpis])
    assert _extract(mapper, kpis) is None
    # The same-table column stays a design skip; the typo is a real miss.
    assert _warnings(mapper) == ["Model.KPIs.Ratio -> 'KPIs'[Typo]"]


def test_calculated_table_reading_an_unknown_table_is_reported() -> None:
    mapper = _mapper()
    policies = Table(name="Policies", full_name="Model.Policies")
    by_state = Table(
        name="By State",
        full_name="Model.By_State",
        calculated_expression="UNION('Policies', 'Archive')",
    )
    _dataset([policies, by_state])
    aspect = _extract(mapper, by_state)
    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [_urn(mapper, policies)]
    assert _warnings(mapper) == ["Model.By_State.By State -> 'Archive'"]


def test_dax_references_are_counted_once_per_reference() -> None:
    mapper = _dax_mapper()
    policies, kpis = _dax_model(mapper)
    for _ in range(2):
        _extract(mapper, policies)
        _extract(mapper, kpis)
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.dax_references_unresolved == 2
    assert reporter.dax_references_resolved == 6
    assert len(_warnings(mapper)) == 2


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


def _properties_table() -> Table:
    return Table(
        name="Claims",
        full_name="Model.Claims",
        columns=[
            Column(
                name="ClaimId",
                dataType="Int64",
                isHidden=True,
                datahubDataType=NumberTypeClass(),
                is_key=True,
                is_nullable=False,
                summarize_by="none",
            ),
            Column(
                name="PaidAmount",
                dataType="Double",
                isHidden=False,
                datahubDataType=NumberTypeClass(),
                format_string="$#,##0.00",
                display_folder="Financials",
                data_category="Currency",
                sort_by_column="ClaimId",
            ),
            _column("Plain"),
        ],
        measures=[
            Measure(
                name="Loss Ratio",
                expression="DIVIDE([Paid], [Premium])",
                isHidden=False,
                format_string="0.0%",
                display_folder="Ratios",
                has_kpi=True,
            ),
            Measure(name="Paid", expression="SUM(Claims[PaidAmount])", isHidden=True),
        ],
        storage_mode="Import",
        partition_count=2,
        refresh_policy=RefreshPolicy(
            rolling_window_granularity="year",
            rolling_window_periods="3",
            incremental_granularity="month",
            incremental_periods="12",
        ),
        rls_filters=[
            RlsTableFilter(role="Regional Managers", filter="'Claims'[State] = \"CA\""),
            RlsTableFilter(role="Auditors", filter="TRUE()"),
        ],
    )


def _dataset_properties(mapper: Mapper, table: Table) -> Dict[str, str]:
    dataset = table.dataset
    assert dataset is not None
    for mcp in mapper.to_datahub_dataset(dataset, _workspace()):
        if isinstance(mcp.aspect, DatasetPropertiesClass):
            return mcp.aspect.customProperties
    raise AssertionError("no datasetProperties emitted")


def test_model_properties_on_schema_fields_and_table_properties() -> None:
    mapper = _mapper(_config(extract_model_properties=True))
    table = _properties_table()
    _dataset(
        [table],
        parameters={
            "Environment": "prod",
            "Api Token": "abc",
            "WarehouseKey": "k1",
            "Secret Path": "x",
        },
    )

    schema = mapper.to_datahub_schema(table)
    fields = {f.fieldPath: f for f in schema.fields}
    claim_id = fields["ClaimId"]
    assert claim_id.isPartOfKey
    assert not claim_id.nullable
    assert json.loads(claim_id.jsonProps or "{}") == {
        "isHidden": True,
        "summarizeBy": "none",
    }
    paid = fields["PaidAmount"]
    assert not paid.isPartOfKey
    assert json.loads(paid.jsonProps or "{}") == {
        "dataCategory": "Currency",
        "displayFolder": "Financials",
        "formatString": "$#,##0.00",
        "sortByColumn": "ClaimId",
    }
    assert fields["Plain"].jsonProps is None
    assert json.loads(fields["Loss Ratio"].jsonProps or "{}") == {
        "displayFolder": "Ratios",
        "formatString": "0.0%",
        "hasKpi": True,
    }
    assert json.loads(fields["Paid"].jsonProps or "{}") == {"isHidden": True}
    # The measure's DAX still rides in the description, as before.
    assert fields["Paid"].description == "SUM(Claims[PaidAmount])"

    properties = _dataset_properties(mapper, table)
    assert properties["datasetId"] == "ds-1"
    assert properties["storageMode"] == "Import"
    assert properties["partitionCount"] == "2"
    assert properties["incrementalRefresh"] == "true"
    assert properties["refreshPolicy.rollingWindowGranularity"] == "year"
    assert properties["refreshPolicy.rollingWindowPeriods"] == "3"
    assert properties["refreshPolicy.incrementalGranularity"] == "month"
    assert properties["refreshPolicy.incrementalPeriods"] == "12"
    # Roles are sorted by name for a stable payload.
    assert json.loads(properties["rlsRoles"]) == [
        {"role": "Auditors", "filter": "TRUE()"},
        {"role": "Regional Managers", "filter": "'Claims'[State] = \"CA\""},
    ]
    assert properties["rlsRoleCount"] == "2"
    assert properties["parameter.Environment"] == "prod"
    # Any name containing a secret marker is masked, case-insensitively.
    assert properties["parameter.Api Token"] == "***"
    assert properties["parameter.WarehouseKey"] == "***"
    assert properties["parameter.Secret Path"] == "***"


def test_model_properties_absent_without_the_flag() -> None:
    mapper = _mapper()
    table = _properties_table()
    _dataset([table], parameters={"Environment": "prod"})

    schema = mapper.to_datahub_schema(table)
    for field in schema.fields:
        assert field.jsonProps is None
        assert not field.isPartOfKey
        assert not field.nullable
    assert _dataset_properties(mapper, table) == {"datasetId": "ds-1"}


def test_table_without_model_properties_only_carries_dataset_id() -> None:
    mapper = _mapper(_config(extract_model_properties=True))
    table = Table(name="T", full_name="Model.T", columns=[_column("A")])
    _dataset([table])
    assert _dataset_properties(mapper, table) == {"datasetId": "ds-1"}
    assert mapper.to_datahub_schema(table).fields[0].jsonProps is None
