"""Mapper behaviour added for metadata_api_mode: workspace_apis."""

import json
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
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
from datahub.ingestion.source.powerbi.powerbi import MAX_FILTERS_IN_PROPERTY, Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FABRIC_ARTIFACT_LAKEHOUSE,
    Column,
    Dashboard,
    DatasetRefresh,
    DatasetRefreshSchedule,
    DaxFieldReference,
    FabricArtifact,
    FieldModelProperties,
    Measure,
    Page,
    PageFieldReference,
    PowerBIDataset,
    RefreshPolicy,
    Relationship,
    RemoteModelMatch,
    RemoteModelReference,
    Report,
    ReportFilter,
    ReportType,
    RlsTableFilter,
    Table,
    Tile,
    Visual,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.ingestion.source.powerbi.rest_api_wrapper.tmdl_parser import parse_table
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
    DatasetPropertiesClass,
    InputFieldsClass,
    NumberTypeClass,
    OperationClass,
    OperationTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
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


def _mapper(
    config: Optional[PowerBiDashboardSourceConfig] = None,
    remote_models: Optional[Dict[str, Optional[PowerBIDataset]]] = None,
) -> Mapper:
    """`remote_models` maps "workspace/model" to the dataset a composite
    model's connection resolves to (None: the name is ambiguous), standing
    in for PowerBiAPI's index."""
    config = config or _config()
    index = remote_models or {}

    def lookup(reference: RemoteModelReference) -> RemoteModelMatch:
        key = f"{reference.workspace_name}/{reference.model_name}"
        if key not in index:
            return RemoteModelMatch()
        if index[key] is None:
            return RemoteModelMatch(ambiguous=True)
        return RemoteModelMatch(dataset=index[key])

    return Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
        remote_model_lookup=lookup,
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


COMPLETED_REFRESH = DatasetRefresh(
    "req",
    "Scheduled",
    "Completed",
    "2022-02-01T20:56:55.927Z",
    "2022-02-01T20:57:05.053Z",
)
COMPLETED_END_MS = 1643749025053
NEWER_FAILED_REFRESH = DatasetRefresh(
    "req-failed",
    "Scheduled",
    "Failed",
    "2022-02-02T20:56:55Z",
    "2022-02-02T20:57:05Z",
    service_exception_json='{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified",'
    '"errorDescription":"' + "x" * 2000 + '"}',
)
NEWER_FAILED_END_MS = 1643835425000
OLDER_FAILED_REFRESH = DatasetRefresh(
    "req-failed-old",
    "Scheduled",
    "Failed",
    "2022-01-31T20:56:55Z",
    "2022-01-31T20:57:05Z",
    service_exception_json="not json at all " + "y" * 2000,
)
OLDER_FAILED_END_MS = 1643662625000


def _refresh_operations(
    mapper: Mapper, dataset: PowerBIDataset, table: Table
) -> List[OperationClass]:
    operations = []
    for mcp in mapper._refresh_operation_mcps(dataset, _urn(mapper, table)):
        assert isinstance(mcp.aspect, OperationClass)
        operations.append(mcp.aspect)
    return operations


def test_completed_refresh_operation() -> None:
    mapper = _mapper(_config(extract_refresh_history=True))
    table = Table(name="T", full_name="Model.T")
    dataset = _dataset([table], last_refresh=COMPLETED_REFRESH)

    operations = _refresh_operations(mapper, dataset, table)

    assert len(operations) == 1
    assert operations[0].operationType == OperationTypeClass.CUSTOM
    assert operations[0].customOperationType == "REFRESH"
    assert operations[0].timestampMillis == COMPLETED_END_MS
    assert operations[0].lastUpdatedTimestamp == COMPLETED_END_MS
    assert operations[0].customProperties == {
        "refreshType": "Scheduled",
        "status": "Completed",
        "requestId": "req",
        "startTime": "2022-02-01T20:56:55.927Z",
    }


def test_failed_refresh_newer_than_completed_points_last_updated_at_completed() -> None:
    # The data was last updated by the completed refresh, so the newer
    # failure's lastUpdatedTimestamp (which feeds the dataset's last-modified
    # time) keeps pointing at it. The error payload is parsed and truncated.
    mapper = _mapper(_config(extract_refresh_history=True))
    table = Table(name="T", full_name="Model.T")
    dataset = _dataset(
        [table],
        last_refresh=COMPLETED_REFRESH,
        last_failed_refresh=NEWER_FAILED_REFRESH,
    )

    operations = _refresh_operations(mapper, dataset, table)

    assert [op.customProperties["status"] for op in operations] == [  # type: ignore[index]
        "Completed",
        "Failed",
    ]
    failed = operations[1]
    assert failed.timestampMillis == NEWER_FAILED_END_MS
    assert failed.lastUpdatedTimestamp == COMPLETED_END_MS
    assert failed.customProperties is not None
    assert failed.customProperties["requestId"] == "req-failed"
    assert (
        failed.customProperties["errorCode"]
        == "ModelRefreshFailed_CredentialsNotSpecified"
    )
    description = failed.customProperties["errorDescription"]
    assert len(description) == 1000
    assert description.endswith("...")
    assert "serviceException" not in failed.customProperties
    assert mapper._Mapper__reporter.refresh_failed_without_completed_baseline == 0  # type: ignore[attr-defined]


def test_failed_refresh_older_than_completed_keeps_its_own_time() -> None:
    # An older failure is not the newest Operation, so it must not be stamped
    # with the newer completed time; a non-JSON payload is kept raw.
    mapper = _mapper(_config(extract_refresh_history=True))
    table = Table(name="T", full_name="Model.T")
    dataset = _dataset(
        [table],
        last_refresh=COMPLETED_REFRESH,
        last_failed_refresh=OLDER_FAILED_REFRESH,
    )

    operations = _refresh_operations(mapper, dataset, table)

    failed = operations[1]
    assert failed.timestampMillis == OLDER_FAILED_END_MS
    assert failed.lastUpdatedTimestamp == OLDER_FAILED_END_MS
    assert failed.customProperties is not None
    raw = failed.customProperties["serviceException"]
    assert raw.startswith("not json at all ")
    assert len(raw) == 1000
    assert "errorCode" not in failed.customProperties
    assert mapper._Mapper__reporter.refresh_failed_without_completed_baseline == 0  # type: ignore[attr-defined]


def test_failed_refresh_without_completed_baseline_is_counted_and_warned() -> None:
    mapper = _mapper(_config(extract_refresh_history=True))
    table = Table(name="T", full_name="Model.T")
    dataset = _dataset([table], last_failed_refresh=NEWER_FAILED_REFRESH)

    operations = _refresh_operations(mapper, dataset, table)

    assert len(operations) == 1
    assert operations[0].timestampMillis == NEWER_FAILED_END_MS
    # 0 is "unknown" to auto_patch_last_modified and to the UI.
    assert operations[0].lastUpdatedTimestamp == 0
    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.refresh_failed_without_completed_baseline == 1
    assert [w.title for w in reporter.warnings] == ["Dataset Last Updated Unknown"]


def test_refresh_counters_and_warnings_fire_once_per_dataset() -> None:
    # Operations are emitted on every table, but the baseline decision and
    # the unparseable-time count belong to the dataset.
    mapper = _mapper(_config(extract_refresh_history=True))
    tables = [
        Table(name="A", full_name="Model.A"),
        Table(name="B", full_name="Model.B"),
    ]
    dataset = _dataset(
        tables,
        last_refresh=DatasetRefresh("req", "Scheduled", "Completed", None, "yesterday"),
        last_failed_refresh=NEWER_FAILED_REFRESH,
    )

    for table in tables:
        operations = _refresh_operations(mapper, dataset, table)
        assert len(operations) == 1
        assert operations[0].lastUpdatedTimestamp == 0

    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.refresh_history_unparseable == 1
    assert reporter.refresh_failed_without_completed_baseline == 1
    assert [w.title for w in reporter.warnings] == ["Dataset Last Updated Unknown"]


def test_refresh_without_usable_end_time_is_skipped() -> None:
    mapper = _mapper(_config(extract_refresh_history=True))
    table = Table(name="T", full_name="Model.T")

    unfinished = _dataset(
        [table],
        last_failed_refresh=DatasetRefresh("req", "Scheduled", "Failed", None, None),
    )
    assert _refresh_operations(mapper, unfinished, table) == []

    # Plans are memoised per dataset id, so a second scenario needs its own
    # mapper.
    mapper = _mapper(_config(extract_refresh_history=True))
    unparseable = _dataset(
        [table],
        last_refresh=DatasetRefresh("req", "Scheduled", "Completed", None, "yesterday"),
    )
    assert _refresh_operations(mapper, unparseable, table) == []
    assert mapper._Mapper__reporter.refresh_history_unparseable == 1  # type: ignore[attr-defined]


def test_refresh_operations_need_the_flag() -> None:
    mapper = _mapper()
    table = Table(name="T", full_name="Model.T")
    dataset = _dataset(
        [table],
        last_refresh=COMPLETED_REFRESH,
        last_failed_refresh=NEWER_FAILED_REFRESH,
    )
    assert _refresh_operations(mapper, dataset, table) == []


def test_refresh_schedule_becomes_dataset_custom_properties() -> None:
    schedule = DatasetRefreshSchedule(
        days=["Monday", "Friday"],
        times=["07:00", "18:30"],
        local_time_zone_id="UTC",
        enabled=True,
    )
    assert Mapper._refresh_schedule_properties(schedule) == {
        "refreshSchedule.days": "Monday,Friday",
        "refreshSchedule.times": "07:00,18:30",
        "refreshSchedule.localTimeZoneId": "UTC",
        "refreshSchedule.enabled": "true",
    }

    disabled = DatasetRefreshSchedule(
        days=[], times=[], local_time_zone_id=None, enabled=False
    )
    assert Mapper._refresh_schedule_properties(disabled) == {
        "refreshSchedule.days": "",
        "refreshSchedule.times": "",
        "refreshSchedule.enabled": "false",
    }
    assert Mapper._refresh_schedule_properties(None) == {}


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
    mcp = mapper._page_input_fields(
        page, report, "urn:li:chart:(powerbi,pages.r1.p1)"
    ).mcp
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
                model_properties=FieldModelProperties(
                    is_key=True, is_nullable=False, summarize_by="none"
                ),
            ),
            Column(
                name="PaidAmount",
                dataType="Double",
                isHidden=False,
                datahubDataType=NumberTypeClass(),
                model_properties=FieldModelProperties(
                    format_string="$#,##0.00",
                    display_folder="Financials",
                    data_category="Currency",
                    sort_by_column="ClaimId",
                    summarize_by="sum",
                ),
            ),
            Column(
                name="Notes",
                dataType="String",
                isHidden=False,
                datahubDataType=StringTypeClass(),
                model_properties=FieldModelProperties(is_nullable=True),
            ),
            _column("Plain"),
        ],
        measures=[
            Measure(
                name="Loss Ratio",
                expression="DIVIDE([Paid], [Premium])",
                isHidden=False,
                model_properties=FieldModelProperties(
                    format_string="0.0%", display_folder="Ratios", has_kpi=True
                ),
            ),
            Measure(
                name="Paid",
                expression="SUM(Claims[PaidAmount])",
                isHidden=True,
                model_properties=FieldModelProperties(),
            ),
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
            "DbPass": "p",
            "ConnectionString": "c",
            "Retries": 3,  # type: ignore[dict-item]
            "Long": "x" * 2000,
        },
    )

    schema = mapper.to_datahub_schema(table)
    fields = {f.fieldPath: f for f in schema.fields}
    claim_id = fields["ClaimId"]
    assert claim_id.isPartOfKey
    assert not claim_id.nullable
    # The Desktop default `summarizeBy: none` is not written.
    assert json.loads(claim_id.jsonProps or "{}") == {"isHidden": True}
    paid = fields["PaidAmount"]
    assert not paid.isPartOfKey
    assert json.loads(paid.jsonProps or "{}") == {
        "dataCategory": "Currency",
        "displayFolder": "Financials",
        "formatString": "$#,##0.00",
        "sortByColumn": "ClaimId",
        "summarizeBy": "sum",
    }
    assert fields["Notes"].nullable is True
    assert fields["Notes"].jsonProps is None
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
    assert properties["parameter.DbPass"] == "***"
    assert properties["parameter.ConnectionString"] == "***"
    # Values are stringified and cut so they cannot break the aspect.
    assert properties["parameter.Retries"] == "3"
    assert properties["parameter.Long"] == "x" * 1000


def test_parsed_table_properties_reach_the_schema() -> None:
    """TMDL -> ParsedColumn -> Column -> SchemaFieldClass, end to end."""
    mapper = _mapper(_config(extract_model_properties=True))
    parsed = parse_table(
        "table T\n\n\tcolumn Notes\n\t\tdataType: string\n\t\tisNullable\n\n"
        "\tcolumn Id\n\t\tdataType: int64\n\t\tisKey\n\t\tisNullable: false\n"
    )
    assert parsed is not None
    table = Table(
        name="T",
        full_name="Model.T",
        columns=[PowerBiAPI._column_from_parsed(c) for c in parsed.columns],
    )
    _dataset([table])
    fields = {f.fieldPath: f for f in mapper.to_datahub_schema(table).fields}
    assert fields["Notes"].nullable is True
    assert fields["Id"].nullable is False
    assert fields["Id"].isPartOfKey


def test_model_properties_absent_without_the_flag_or_properties() -> None:
    off = _mapper()
    table = _properties_table()
    _dataset([table], parameters={"Environment": "prod"})
    for field in off.to_datahub_schema(table).fields:
        assert field.jsonProps is None
        assert not field.isPartOfKey
        assert not field.nullable
    assert _dataset_properties(off, table) == {"datasetId": "ds-1"}

    on = _mapper(_config(extract_model_properties=True))
    plain = Table(name="T", full_name="Model.T", columns=[_column("A")])
    _dataset([plain])
    assert _dataset_properties(on, plain) == {"datasetId": "ds-1"}
    assert on.to_datahub_schema(plain).fields[0].jsonProps is None


# --- composite models ---------------------------------------------------------

SOURCE_WORKSPACE = "demo-workspace"
SOURCE_MODEL = "Insurance Model"
SOURCE_KEY = f"{SOURCE_WORKSPACE}/{SOURCE_MODEL}"


def _source_model(column_names: Optional[List[str]] = None) -> PowerBIDataset:
    policies = Table(
        name="Policies",
        full_name="Insurance_Model.Policies",
        expression="let ...",
        columns=[_column(name) for name in column_names or ["PolicyId", "Premium"]],
    )
    dataset = PowerBIDataset(
        id="ds-source",
        name=SOURCE_MODEL,
        description="",
        webUrl=None,
        workspace_id="ws-source",
        workspace_name=SOURCE_WORKSPACE,
        parameters={},
        tables=[policies],
        tags=[],
    )
    policies.dataset = dataset
    return dataset


def _remote_column(name: str, source_column: Optional[str] = None) -> Column:
    column = _column(name)
    column.columnType = Constant.COLUMN_TYPE_DATA
    column.source_column = source_column
    return column


def _remote_table(
    name: str = "Policies",
    remote_name: str = "Policies",
    columns: Optional[List[Column]] = None,
) -> Table:
    calculated = _column("Band")
    calculated.columnType = Constant.COLUMN_TYPE_CALCULATED
    return Table(
        name=name,
        full_name=f"Exec_Model.{name}",
        # A column calculated on the remote table is local DAX, not mirrored.
        columns=(columns if columns is not None else [_remote_column("PolicyId")])
        + [calculated],
        storage_mode=Constant.DIRECT_QUERY,
        remote_model=RemoteModelReference(SOURCE_WORKSPACE, SOURCE_MODEL),
        remote_table_name=remote_name,
    )


def _composite_mapper(
    remote_models: Optional[Dict[str, Optional[PowerBIDataset]]] = None,
) -> Mapper:
    return _mapper(_config(extract_composite_model_lineage=True), remote_models)


def _reporter(mapper: Mapper) -> PowerBiDashboardSourceReport:
    return mapper._Mapper__reporter  # type: ignore[attr-defined]


def test_composite_model_lineage_is_off_by_default() -> None:
    mapper = _mapper(remote_models={SOURCE_KEY: _source_model()})
    table = _remote_table()
    _dataset([table])

    assert _extract(mapper, table) is None
    assert _reporter(mapper).composite_model_tables_linked == 0
    assert _reporter(mapper).warnings == []


def test_flag_off_remote_table_next_to_a_directlake_table_gets_no_lakehouse_edge() -> (
    None
):
    # Before composite models were recognised, a remote AS table was
    # classified DirectLake and could inherit the model's lakehouse edge.
    mapper = _mapper()
    workspace = _workspace()
    workspace.fabric_artifacts = {
        "lh-1": FabricArtifact(
            id="lh-1",
            name="Claims_Lakehouse",
            artifact_type=FABRIC_ARTIFACT_LAKEHOUSE,
            workspace_id="ws-1",
        )
    }
    directlake = Table(
        name="Claims",
        full_name="Model.Claims",
        columns=[],
        storage_mode=Constant.DIRECT_LAKE,
        source_schema="dbo",
        source_expression="claims",
    )
    remote = _remote_table()
    _dataset([directlake, remote], dependent_on_artifact_id="lh-1")

    with patch(
        "datahub.ingestion.source.powerbi.powerbi.parser.get_upstream_tables",
        return_value=[],
    ):
        lakehouse = _lineage_aspect(
            mapper.extract_lineage(directlake, _urn(mapper, directlake), workspace)
        )
        remote_aspect = _lineage_aspect(
            mapper.extract_lineage(remote, _urn(mapper, remote), workspace)
        )
    assert lakehouse is not None
    assert any("lh-1" in u.dataset for u in lakehouse.upstreams)
    assert remote_aspect is None
    assert _reporter(mapper).warnings == []


def test_composite_model_remote_table_links_to_the_ingested_source_table() -> None:
    source = _source_model()
    mapper = _composite_mapper({SOURCE_KEY: source})
    table = _remote_table()
    _dataset([table])
    table_urn = _urn(mapper, table)
    source_urn = _urn(mapper, source.tables[0])

    aspect = _extract(mapper, table)
    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [source_urn]
    assert aspect.fineGrainedLineages is not None
    # Only the mirrored data column gets an edge; the calculated one does not.
    assert [(e.downstreams, e.upstreams) for e in aspect.fineGrainedLineages] == [
        (
            [f"urn:li:schemaField:({table_urn},PolicyId)"],
            [f"urn:li:schemaField:({source_urn},PolicyId)"],
        )
    ]

    # Mapping the dataset twice (workspace, then report) counts once.
    _extract(mapper, table)
    reporter = _reporter(mapper)
    assert reporter.composite_model_sources_resolved == 1
    assert reporter.composite_model_tables_linked == 1
    assert reporter.composite_model_sources_not_ingested == 0
    assert reporter.composite_model_columns_unresolved == 0
    assert reporter.warnings == []


def test_composite_columns_match_on_source_column_not_display_name() -> None:
    # The source model names its columns in lowercase; the composite model
    # renames `policyid` to `Policy Number` and reuses the display name
    # `PolicyId` for the premium, which must not be matched by that name.
    source = _source_model(["policyid", "writtenpremium"])
    mapper = _composite_mapper({SOURCE_KEY: source})
    table = _remote_table(
        columns=[
            _remote_column("Policy Number", "policyid"),
            _remote_column("PolicyId", "writtenpremium"),
            _remote_column("Ghost", "ghost"),
        ]
    )
    _dataset([table])
    table_urn = _urn(mapper, table)
    source_urn = _urn(mapper, source.tables[0])

    aspect = _extract(mapper, table)
    assert aspect is not None
    assert aspect.fineGrainedLineages is not None
    assert {
        ((e.downstreams or [""])[0], (e.upstreams or [""])[0])
        for e in aspect.fineGrainedLineages
    } == {
        (
            f"urn:li:schemaField:({table_urn},Policy Number)",
            f"urn:li:schemaField:({source_urn},policyid)",
        ),
        (
            f"urn:li:schemaField:({table_urn},PolicyId)",
            f"urn:li:schemaField:({source_urn},writtenpremium)",
        ),
    }
    _extract(mapper, table)
    reporter = _reporter(mapper)
    assert reporter.composite_model_columns_unresolved == 1
    assert [w.title for w in reporter.warnings] == [
        Constant.COMPOSITE_MODEL_COLUMN_NOT_RESOLVED
    ]
    assert any("columns=['Ghost']" in c for c in reporter.warnings[0].context or [])


def test_composite_model_source_not_ingested_warns_and_emits_no_edge() -> None:
    mapper = _composite_mapper()
    table = _remote_table()
    _dataset([table])

    assert _extract(mapper, table) is None
    _extract(mapper, table)
    reporter = _reporter(mapper)
    assert reporter.composite_model_sources_not_ingested == 1
    assert reporter.composite_model_sources_resolved == 0
    assert reporter.composite_model_tables_linked == 0
    assert [w.title for w in reporter.warnings] == [
        Constant.COMPOSITE_MODEL_SOURCE_NOT_INGESTED
    ]
    assert any(
        f"model={SOURCE_MODEL}, table=Policies" in context
        for context in reporter.warnings[0].context or []
    )


def test_composite_model_source_without_definition_is_not_ingested() -> None:
    # The source model was listed (so it is in the index) but its
    # getDefinition failed: no tables, so nothing to link.
    source = _source_model()
    source.tables = []
    mapper = _composite_mapper({SOURCE_KEY: source})
    table = _remote_table()
    _dataset([table])

    assert _extract(mapper, table) is None
    reporter = _reporter(mapper)
    assert reporter.composite_model_sources_not_ingested == 1
    assert reporter.composite_model_sources_resolved == 0
    assert [w.title for w in reporter.warnings] == [
        Constant.COMPOSITE_MODEL_SOURCE_NOT_INGESTED
    ]
    assert "definition was not ingested" in reporter.warnings[0].message


def test_composite_model_ambiguous_source_name_warns_and_emits_no_edge() -> None:
    mapper = _composite_mapper({SOURCE_KEY: None})
    table = _remote_table()
    _dataset([table])

    assert _extract(mapper, table) is None
    _extract(mapper, table)
    reporter = _reporter(mapper)
    assert reporter.composite_model_sources_ambiguous == 1
    assert reporter.composite_model_sources_not_ingested == 0
    assert [w.title for w in reporter.warnings] == [
        Constant.COMPOSITE_MODEL_SOURCE_AMBIGUOUS
    ]


def test_composite_model_source_table_missing_warns_and_emits_no_edge() -> None:
    mapper = _composite_mapper({SOURCE_KEY: _source_model()})
    table = _remote_table(name="Regions", remote_name="Regions")
    _dataset([table])

    assert _extract(mapper, table) is None
    reporter = _reporter(mapper)
    assert reporter.composite_model_sources_resolved == 1
    assert reporter.composite_model_source_tables_not_found == 1
    assert reporter.composite_model_sources_not_ingested == 0
    assert reporter.composite_model_tables_linked == 0
    assert [w.title for w in reporter.warnings] == [
        Constant.COMPOSITE_MODEL_SOURCE_TABLE_NOT_FOUND
    ]


def test_composite_model_source_resolving_to_itself_is_ambiguous() -> None:
    # Without the workspace in the URN two models named alike collapse into
    # one URN; an edge from a table to itself is never emitted.
    source = _source_model()
    mapper = _composite_mapper({SOURCE_KEY: source})
    table = _remote_table()
    table.full_name = source.tables[0].full_name
    _dataset([table])

    assert _extract(mapper, table) is None
    reporter = _reporter(mapper)
    assert reporter.composite_model_sources_ambiguous == 1
    assert reporter.composite_model_tables_linked == 0
    assert [w.title for w in reporter.warnings] == [
        Constant.COMPOSITE_MODEL_SOURCE_AMBIGUOUS
    ]


def test_measure_on_a_remote_table_chains_through_the_composite_lineage() -> None:
    # A local measure reading the remote table's mirrored column keeps its
    # model-internal edge next to the composite edge on the same aspect.
    mapper = _composite_mapper({SOURCE_KEY: _source_model()})
    table = _remote_table()
    kpis = Table(
        name="KPIs",
        full_name="Exec_Model.KPIs",
        columns=[],
        measures=[
            Measure(
                name="Policies Count",
                expression="COUNTROWS('Policies')",
                isHidden=False,
                references=[DaxFieldReference("Policies", "PolicyId")],
            )
        ],
    )
    _dataset([table, kpis])

    aspect = _extract(mapper, kpis)
    assert aspect is not None
    assert [u.dataset for u in aspect.upstreams] == [_urn(mapper, table)]


def test_filters_become_custom_properties_on_page_chart_and_report_dashboard() -> None:
    mapper = _mapper(_config(extract_report_field_usage=True))
    policies = Table(
        name="Policies",
        full_name="Model.Policies",
        columns=[_column("State"), _column("WrittenPremium")],
        measures=[],
    )
    dataset = _dataset([policies])
    state = PageFieldReference("Policies", "State", "column")
    premium = PageFieldReference("Policies", "WrittenPremium", "column")
    page_filter = ReportFilter(
        scope="page",
        name="f_state",
        filter_type="Categorical",
        target=state,
        fields=[state],
        condition='In ["CA", "NY"]',
    )
    report_filter = ReportFilter(
        scope="report",
        name="f_premium",
        filter_type="Advanced",
        target=premium,
        fields=[premium],
        condition=">= 1000",
    )
    unresolved_filter = ReportFilter(
        scope="report",
        name="f_untargeted",
        filter_type="Categorical",
        target=None,
        fields=[],
        condition='In ["x"]',
    )
    page = Page(
        id="r1.p1",
        displayName="Overview",
        name="p1",
        order=0,
        # The API layer already merged the filter targets into the page's fields.
        field_references=[
            PageFieldReference("Policies", "State", "column"),
            PageFieldReference("Policies", "WrittenPremium", "column"),
        ],
        visual_count=1,
        visual_types=["card"],
        filters=[page_filter],
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
        filters=[report_filter, unresolved_filter],
    )

    chart_mcps = mapper.pages_to_chart(
        [page], workspace=_workspace(), ds_mcps=[], report=report
    )
    chart_info = next(
        mcp.aspect for mcp in chart_mcps if isinstance(mcp.aspect, ChartInfoClass)
    )
    assert chart_info.customProperties["order"] == "0"
    assert chart_info.customProperties["visualCount"] == "1"
    assert chart_info.customProperties["visualTypes"] == "card"
    assert chart_info.customProperties["filterCount"] == "1"
    assert "filtersTruncated" not in chart_info.customProperties
    assert json.loads(chart_info.customProperties["filters"]) == [
        {
            "scope": "page",
            "name": "f_state",
            "type": "Categorical",
            "target": "Policies[State]",
            "condition": 'In ["CA", "NY"]',
        }
    ]
    input_fields = next(
        mcp.aspect for mcp in chart_mcps if isinstance(mcp.aspect, InputFieldsClass)
    )
    policies_urn = _urn(mapper, policies)
    assert [f.schemaFieldUrn for f in input_fields.fields] == [
        f"urn:li:schemaField:({policies_urn},State)",
        f"urn:li:schemaField:({policies_urn},WrittenPremium)",
    ]

    dashboard_mcps = mapper.report_to_dashboard(
        workspace=_workspace(),
        report=report,
        chart_mcps=chart_mcps,
        user_mcps=[],
        dataset_edges=[],
    )
    dashboard_info = next(
        mcp.aspect
        for mcp in dashboard_mcps
        if isinstance(mcp.aspect, DashboardInfoClass)
    )
    assert dashboard_info.customProperties["filterCount"] == "2"
    assert json.loads(dashboard_info.customProperties["filters"]) == [
        {
            "scope": "report",
            "name": "f_premium",
            "type": "Advanced",
            "target": "Policies[WrittenPremium]",
            "condition": ">= 1000",
        },
        {
            "scope": "report",
            "name": "f_untargeted",
            "type": "Categorical",
            "condition": 'In ["x"]',
        },
    ]

    # A filter-heavy report is capped; the count still reports all of them.
    many = [report_filter] * (MAX_FILTERS_IN_PROPERTY + 3)
    capped = mapper._filter_properties(many)
    assert capped["filterCount"] == str(MAX_FILTERS_IN_PROPERTY + 3)
    assert capped["filtersTruncated"] == "true"
    assert len(json.loads(capped["filters"])) == MAX_FILTERS_IN_PROPERTY
    assert mapper._Mapper__reporter.report_filters_truncated == 3  # type: ignore[attr-defined]


def _report_with_visuals(dataset: PowerBIDataset, with_visuals: bool = True) -> Report:
    state = PageFieldReference("Policies", "State", "column")
    status = PageFieldReference("Claims", "Status", "column")
    visual_filter = ReportFilter(
        scope="visual",
        name="f_open",
        filter_type="Categorical",
        target=status,
        fields=[status],
        condition='In ["Open"]',
    )
    visuals = [
        Visual(
            name="v-bar",
            page_id="r1.p1",
            title="Policies by State",
            visual_type="clusteredBarChart",
            field_references=[state, status],
            filters=[visual_filter],
        ),
        Visual(
            name="v-card",
            page_id="r1.p1",
            visual_type="card",
            field_references=[PageFieldReference("Other", "Gone", "column")],
        ),
        Visual(name="v-text", page_id="r1.p1", visual_type="textbox"),
        Visual(name="v-custom", page_id="r1.p1"),
    ]
    page = Page(
        id="r1.p1",
        displayName="Overview",
        name="p1",
        order=0,
        field_references=[state, status],
        visual_count=4,
        visual_types=["card", "clusteredBarChart", "textbox"],
        visuals=visuals if with_visuals else [],
    )
    return Report(
        id="r1",
        name="Report",
        type=ReportType.PowerBIReport,
        webUrl="https://app.powerbi.com/groups/ws-1/reports/r1",
        embedUrl=None,
        description="",
        dataset_id=dataset.id,
        dataset=dataset,
        pages=[page],
        users=[],
        tags=[],
    )


def _aspects_of(mcps: List[Any], urn: str) -> Dict[str, Any]:
    return {
        type(mcp.aspect).__name__: mcp.aspect for mcp in mcps if mcp.entityUrn == urn
    }


PAGE_URN = "urn:li:chart:(powerbi,prod.pages.r1.p1)"
BAR_URN = "urn:li:chart:(powerbi,prod.visuals.r1.p1.v-bar)"
CARD_URN = "urn:li:chart:(powerbi,prod.visuals.r1.p1.v-card)"
TEXT_URN = "urn:li:chart:(powerbi,prod.visuals.r1.p1.v-text)"
CUSTOM_URN = "urn:li:chart:(powerbi,prod.visuals.r1.p1.v-custom)"


def _visual_charts(
    extract_report_visuals: bool = True,
) -> Tuple[Mapper, Report, Workspace, List[Any]]:
    config = _config(
        extract_report_field_usage=True,
        extract_report_visuals=extract_report_visuals,
        platform_instance="prod",
    )
    mapper = _mapper(config)
    workspace = _workspace()
    mapper.workspace_key = mapper.make_container_key_for_workspace(workspace)
    policies = Table(
        name="Policies", full_name="Model.Policies", columns=[_column("State")]
    )
    claims = Table(name="Claims", full_name="Model.Claims", columns=[_column("Status")])
    report = _report_with_visuals(_dataset([policies, claims]))
    chart_mcps = mapper.pages_to_chart(
        report.pages, workspace=workspace, ds_mcps=[], report=report
    )
    return mapper, report, workspace, chart_mcps


def test_visual_chart_urns_and_page_chart_are_emitted_together() -> None:
    mapper, _, _, chart_mcps = _visual_charts()
    assert {mcp.entityUrn for mcp in chart_mcps} == {
        PAGE_URN,
        BAR_URN,
        CARD_URN,
        TEXT_URN,
        CUSTOM_URN,
    }
    # The page chart keeps its own shape.
    page_info = _aspects_of(chart_mcps, PAGE_URN)["ChartInfoClass"]
    assert page_info.customProperties["visualCount"] == "4"
    assert "visualName" not in page_info.customProperties
    assert [
        e.urn for e in _aspects_of(chart_mcps, PAGE_URN)["BrowsePathsV2Class"].path
    ][-1] == mapper.make_dashboard_urn_for_report(_report_with_visuals(_dataset([])))


def test_visual_chart_info_title_url_type_and_filters() -> None:
    _, _, _, chart_mcps = _visual_charts()
    info = _aspects_of(chart_mcps, BAR_URN)["ChartInfoClass"]
    assert info.title == "Policies by State"
    assert info.chartUrl == "https://app.powerbi.com/groups/ws-1/reports/r1/p1"
    # A query string on the report URL stays after the page segment.
    report = _report_with_visuals(_dataset([]))
    report.webUrl = "https://app.powerbi.com/groups/ws-1/reports/r1?ctid=t1"
    assert (
        _mapper()._page_url(report, report.pages[0])
        == "https://app.powerbi.com/groups/ws-1/reports/r1/p1?ctid=t1"
    )
    assert info.type == ChartTypeClass.BAR
    assert info.customProperties["visualType"] == "clusteredBarChart"
    assert info.customProperties["visualName"] == "v-bar"
    assert info.customProperties["pageName"] == "p1"
    assert info.customProperties["filterCount"] == "1"
    assert json.loads(info.customProperties["filters"]) == [
        {
            "scope": "visual",
            "name": "f_open",
            "type": "Categorical",
            "target": "Claims[Status]",
            "condition": 'In ["Open"]',
        }
    ]
    # No title: the fallback names the visual type and page; no mapped chart
    # type leaves `type` unset but keeps the raw visualType.
    card = _aspects_of(chart_mcps, CARD_URN)["ChartInfoClass"]
    assert card.title == "card on Overview"
    assert card.type is None
    assert card.customProperties["visualType"] == "card"
    custom = _aspects_of(chart_mcps, CUSTOM_URN)["ChartInfoClass"]
    assert custom.title == "Visual on Overview"
    assert "visualType" not in custom.customProperties


def test_visual_chart_subtype_container_and_browse_path_under_page() -> None:
    mapper, report, workspace, chart_mcps = _visual_charts()
    bar = _aspects_of(chart_mcps, BAR_URN)
    assert bar["SubTypesClass"].typeNames == [BIAssetSubTypes.POWERBI_VISUAL]
    assert mapper.workspace_key is not None
    assert bar["ContainerClass"].container == mapper.workspace_key.as_urn()
    assert [(entry.id, entry.urn) for entry in bar["BrowsePathsV2Class"].path] == [
        (workspace.name, mapper.make_container_urn_for_workspace(workspace)),
        (report.name, mapper.make_dashboard_urn_for_report(report)),
        ("Overview", PAGE_URN),
    ]


def test_visual_chart_input_fields_and_counters() -> None:
    mapper, report, _, chart_mcps = _visual_charts()
    assert report.dataset is not None
    policies, claims = report.dataset.tables
    bar = _aspects_of(chart_mcps, BAR_URN)
    assert [f.schemaFieldUrn for f in bar["InputFieldsClass"].fields] == [
        f"urn:li:schemaField:({_urn(mapper, policies)},State)",
        f"urn:li:schemaField:({_urn(mapper, claims)},Status)",
    ]
    # Table-level inputs are exactly the tables the resolved fields belong to,
    # not the page's whole model.
    assert bar["ChartInfoClass"].inputs == [
        _urn(mapper, policies),
        _urn(mapper, claims),
    ]
    # An unresolved field leaves the chart without inputFields but still
    # emitted; a text box reads nothing at all. Neither claims table inputs.
    assert "InputFieldsClass" not in _aspects_of(chart_mcps, CARD_URN)
    assert "InputFieldsClass" not in _aspects_of(chart_mcps, TEXT_URN)
    assert _aspects_of(chart_mcps, CARD_URN)["ChartInfoClass"].inputs == []
    assert _aspects_of(chart_mcps, TEXT_URN)["ChartInfoClass"].inputs == []

    reporter = mapper._Mapper__reporter  # type: ignore[attr-defined]
    assert reporter.report_visuals_emitted == 4
    assert reporter.report_visuals_without_fields == 2
    assert reporter.report_visuals_fields_unresolved == 1
    assert reporter.report_visual_field_references_resolved == 2
    assert reporter.report_visual_field_references_unresolved == 1
    # The page pass keeps its own counters, no double counting.
    assert reporter.report_field_references_resolved == 2
    assert reporter.report_field_references_unresolved == 0


def test_visual_charts_are_listed_on_the_dashboard_and_dropped_when_flag_off() -> None:
    def dashboard_charts(extract_report_visuals: bool) -> List[str]:
        mapper, report, workspace, chart_mcps = _visual_charts(extract_report_visuals)
        dashboard_mcps = mapper.report_to_dashboard(
            workspace=workspace,
            report=report,
            chart_mcps=chart_mcps,
            user_mcps=[],
            dataset_edges=[],
        )
        info = next(
            mcp.aspect
            for mcp in dashboard_mcps
            if isinstance(mcp.aspect, DashboardInfoClass)
        )
        return info.charts

    assert dashboard_charts(True) == [
        PAGE_URN,
        BAR_URN,
        CARD_URN,
        TEXT_URN,
        CUSTOM_URN,
    ]
    # A later run with the flag off rewrites the (non-patched) chart list.
    assert dashboard_charts(False) == [PAGE_URN]


def test_visuals_are_not_emitted_when_flag_is_off() -> None:
    mapper, _, _, chart_mcps = _visual_charts(extract_report_visuals=False)
    assert {mcp.entityUrn for mcp in chart_mcps} == {PAGE_URN}
    assert mapper._Mapper__reporter.report_visuals_emitted == 0  # type: ignore[attr-defined]


def _three_table_report(with_field_references: bool = True) -> Report:
    """A report over three tables whose single page reads only the first
    two; without field references it is what the API gives when
    `extract_report_field_usage` is off."""
    policies = Table(
        name="Policies", full_name="Model.Policies", columns=[_column("State")]
    )
    claims = Table(name="Claims", full_name="Model.Claims", columns=[_column("Status")])
    unused = Table(name="Unused", full_name="Model.Unused", columns=[_column("X")])
    report = _report_with_visuals(
        _dataset([policies, claims, unused]), with_visuals=False
    )
    if not with_field_references:
        report.pages[0].field_references = []
    return report


def _model_mcps(mapper: Mapper, report: Report) -> List[MetadataChangeProposalWrapper]:
    assert report.dataset is not None
    return [
        MetadataChangeProposalWrapper(
            entityUrn=_urn(mapper, table), aspect=StatusClass(removed=False)
        )
        for table in report.dataset.tables
    ]


def _page_chart_info(
    mapper: Mapper, report: Report
) -> Tuple[ChartInfoClass, PowerBiDashboardSourceReport]:
    workspace = _workspace()
    mapper.workspace_key = mapper.make_container_key_for_workspace(workspace)
    chart_mcps = mapper.pages_to_chart(
        report.pages,
        workspace=workspace,
        ds_mcps=_model_mcps(mapper, report),
        report=report,
    )
    return _aspects_of(chart_mcps, PAGE_URN)["ChartInfoClass"], _reporter(mapper)


def test_page_inputs_are_the_tables_its_fields_read_with_field_usage() -> None:
    mapper = _mapper(_config(extract_report_field_usage=True, platform_instance="prod"))
    report = _three_table_report()
    info, reporter = _page_chart_info(mapper, report)
    assert report.dataset is not None
    policies, claims, _ = report.dataset.tables
    assert info.inputs == [_urn(mapper, policies), _urn(mapper, claims)]
    assert info.chartUrl == "https://app.powerbi.com/groups/ws-1/reports/r1/p1"
    assert reporter.report_field_references_resolved == 2
    assert reporter.report_field_references_unresolved == 0


def test_page_inputs_stay_the_whole_model_without_field_usage() -> None:
    mapper = _mapper(_config(platform_instance="prod"))
    report = _three_table_report(with_field_references=False)
    info, reporter = _page_chart_info(mapper, report)
    assert report.dataset is not None
    assert info.inputs == [_urn(mapper, t) for t in report.dataset.tables]
    # The page URL does not depend on field usage.
    assert info.chartUrl == "https://app.powerbi.com/groups/ws-1/reports/r1/p1"
    assert reporter.report_field_references_resolved == 0


def test_page_inputs_stay_the_whole_model_when_nothing_resolves() -> None:
    mapper = _mapper(_config(extract_report_field_usage=True, platform_instance="prod"))
    report = _three_table_report()
    report.pages[0].field_references = [PageFieldReference("Other", "Gone", "column")]
    info, reporter = _page_chart_info(mapper, report)
    assert report.dataset is not None
    assert info.inputs == [_urn(mapper, t) for t in report.dataset.tables]
    assert reporter.report_field_references_unresolved == 1


def test_page_inputs_keep_a_table_whose_field_is_missing_from_the_schema() -> None:
    """A renamed column proves the page reads the table even though the
    field itself cannot be matched: the table-level edge stays, the field
    is counted as unresolved and gets no inputField."""
    mapper = _mapper(_config(extract_report_field_usage=True, platform_instance="prod"))
    report = _three_table_report()
    report.pages[0].field_references = [
        PageFieldReference("Policies", "State", "column"),
        PageFieldReference("Claims", "Renamed", "column"),
    ]
    info, reporter = _page_chart_info(mapper, report)
    assert report.dataset is not None
    policies, claims, _ = report.dataset.tables
    assert info.inputs == [_urn(mapper, policies), _urn(mapper, claims)]
    assert reporter.report_field_references_resolved == 1
    assert reporter.report_field_references_unresolved == 1


def _tile(
    tile_id: str,
    dataset: Optional[PowerBIDataset],
    report: Optional[Report],
    title: Optional[str],
) -> Tile:
    """A tile pinned from `report`, or straight from `dataset` when there
    is no report."""
    return Tile(
        id=tile_id,
        title=title,
        embedUrl=None,
        dataset_id=dataset.id if dataset is not None else None,
        report_id=report.id if report is not None else None,
        createdFrom=(
            Tile.CreatedFrom.REPORT if report is not None else Tile.CreatedFrom.DATASET
        ),
        dataset=dataset,
        report=report,
    )


def _tile_chart_infos(mapper: Mapper, tiles: List[Tile]) -> Dict[str, ChartInfoClass]:
    workspace = _workspace()
    mapper.workspace_key = mapper.make_container_key_for_workspace(workspace)
    dashboard = Dashboard(
        id="d1",
        displayName="Board",
        description="",
        embedUrl=None,
        isReadOnly=False,
        workspace_id=workspace.id,
        workspace_name=workspace.name,
        tiles=tiles,
        users=[],
        tags=[],
        webUrl=None,
    )
    _, chart_mcps = mapper.to_datahub_chart(tiles, workspace, dashboard)
    infos: Dict[str, ChartInfoClass] = {}
    for mcp in chart_mcps:
        if isinstance(mcp.aspect, ChartInfoClass):
            assert mcp.entityUrn is not None
            infos[mcp.entityUrn] = mcp.aspect
    return infos


def test_report_tile_inputs_are_the_tables_the_report_pages_read() -> None:
    mapper = _mapper(_config(extract_report_field_usage=True, platform_instance="prod"))
    report = _three_table_report()
    assert report.dataset is not None
    policies, claims, _ = report.dataset.tables
    whole_model = [_urn(mapper, t) for t in report.dataset.tables]
    infos = _tile_chart_infos(
        mapper,
        [
            _tile("t1", report.dataset, report, "From report"),
            _tile("t2", report.dataset, report, "Also from report"),
            _tile("t3", report.dataset, None, "Pinned from the model"),
        ],
    )
    narrowed = [_urn(mapper, policies), _urn(mapper, claims)]
    assert infos["urn:li:chart:(powerbi,prod.charts.t1)"].inputs == narrowed
    assert infos["urn:li:chart:(powerbi,prod.charts.t2)"].inputs == narrowed
    # A tile pinned straight from the model has no report pages to narrow by.
    assert infos["urn:li:chart:(powerbi,prod.charts.t3)"].inputs == whole_model
    # Tiles never feed the page counters; the page pass does that once.
    reporter = _reporter(mapper)
    assert reporter.report_field_references_resolved == 0
    assert reporter.report_field_references_unresolved == 0
    # Resolved once per report, then served from the memo.
    with patch.object(
        mapper, "_resolve_input_fields", wraps=mapper._resolve_input_fields
    ) as resolve:
        assert mapper._report_used_dataset_urns(report) == narrowed
        resolve.assert_not_called()


def test_report_tile_inputs_stay_the_whole_model_without_field_usage() -> None:
    mapper = _mapper(_config(platform_instance="prod"))
    report = _three_table_report(with_field_references=False)
    assert report.dataset is not None
    infos = _tile_chart_infos(
        mapper, [_tile("t1", report.dataset, report, "From report")]
    )
    assert infos["urn:li:chart:(powerbi,prod.charts.t1)"].inputs == [
        _urn(mapper, t) for t in report.dataset.tables
    ]


def test_untitled_tile_is_named_by_its_position_on_the_dashboard() -> None:
    mapper = _mapper(_config(platform_instance="prod"))
    report = _three_table_report(with_field_references=False)
    infos = _tile_chart_infos(
        mapper,
        [
            _tile("t1", report.dataset, report, "Titled"),
            _tile("t2", report.dataset, None, None),
        ],
    )
    assert infos["urn:li:chart:(powerbi,prod.charts.t1)"].title == "Titled"
    untitled = infos["urn:li:chart:(powerbi,prod.charts.t2)"]
    assert untitled.title == "Tile 2"
    assert untitled.description == "Tile 2"
