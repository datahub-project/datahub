import json as _json
import os
import pathlib
import subprocess
import sys
import textwrap
import time
import types
from pathlib import Path
from typing import Any, Iterable, List, Type, TypeVar
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sqlmesh.compat import (
    _TOBIKO_CONVERT_PATCH_SENTINEL,
    _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL,
    _install_enterprise_config_compat_patches,
    _install_tobiko_local_state_fallback_shim,
    _scoped_tobiko_cloud_env,
)
from datahub.ingestion.source.sqlmesh.constants import (
    PROP_GRAIN,
    PROP_PARTITIONED_BY,
    PROP_TIME_COLUMN,
    SQLMESH_PLATFORM,
)
from datahub.ingestion.source.sqlmesh.models import (
    _build_count_query,
    _CapabilityProbes,
    _EffectiveProjectConfig,
)
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    SqlmeshSourceConfig,
    _get_tobiko_token_file_cache,
    _read_tobiko_cloud_token_file,
)
from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
    SqlmeshSource,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionRunEventClass,
    AssertionTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    IncidentInfoClass,
    OperationClass,
    SiblingsClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import TagUrn

WAREHOUSE_PLATFORM = "snowflake"

_AspectT = TypeVar("_AspectT")


def _aspects_of_type(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> List[_AspectT]:
    # wu.metadata is a union (MCE/MCP/MCPW); only MCP/MCPW carry `.aspect`.
    # Narrowing by isinstance here keeps call sites both runtime-safe and
    # mypy-clean (a bare wu.metadata.aspect trips union-attr).
    out: List[_AspectT] = []
    for wu in workunits:
        aspect = getattr(wu.metadata, "aspect", None)
        if isinstance(aspect, aspect_type):
            out.append(aspect)
    return out


def _make_source(extra_config: dict | None = None) -> SqlmeshSource:
    config_dict = {
        "project_path": "/fake/project",
        "target_platform": WAREHOUSE_PLATFORM,
        "env": "PROD",
        **(extra_config or {}),
    }
    config = SqlmeshSourceConfig.model_validate(config_dict)
    return SqlmeshSource(config, PipelineContext(run_id="test"))


def _make_mock_model(
    name: str = "star.dim_developer",
    columns: dict | None = None,
    depends_on: set | None = None,
    description: str | None = None,
    kind_name: str = "FULL",
    tags: list | None = None,
    owner: str | None = None,
    is_embedded: bool = False,
) -> MagicMock:
    model = MagicMock()
    model.name = name
    model.columns_to_types = (
        {"id": MagicMock(__str__=lambda s: "BIGINT")} if columns is None else columns
    )
    model.depends_on = depends_on or set()
    model.description = description
    model.tags = tags or []
    model.owner = owner
    kind = MagicMock()
    kind.__str__ = lambda s: kind_name  # type: ignore[method-assign, misc, assignment]
    kind.model_kind_name = kind_name
    kind.is_embedded = is_embedded
    model.kind = kind
    # Physical table name attributes (used by _build_physical_name_map)
    model.catalog = "db"
    model.physical_schema = "sqlmesh__star"
    model.schema_name = "star"
    model.view_name = name.split(".")[-1]
    model.data_hash = "4235172200"
    model.column_descriptions = {}
    model.audits = []
    model.cron = None
    model.start = None
    model.time_column = None
    model.partitioned_by = []
    model.grains = []
    return model


def _make_mock_snapshot(
    model_name: str = "star.dim_developer",
    physical_name: str = "db.sqlmesh__star.star__dim_developer__4235172200",
) -> MagicMock:
    snapshot = MagicMock()
    snapshot.name = model_name
    physical_table = MagicMock()
    physical_table.__str__ = lambda s: physical_name  # type: ignore[method-assign, misc, assignment]
    snapshot.table_name = MagicMock(return_value=physical_table)
    return snapshot


def _make_mock_context(
    models: dict,
    snapshots: dict,
    connection_type: str = WAREHOUSE_PLATFORM,
    extra_models: dict | None = None,
) -> MagicMock:
    """Build a mock SqlmeshContext.

    extra_models: additional models returned by get_model() but NOT in ctx.models
                  (simulates declared-external or other separately resolvable models).
    """
    all_resolvable = {**models, **(extra_models or {})}
    mock_ctx = MagicMock()
    mock_ctx.models = models
    mock_ctx.snapshots = snapshots
    mock_ctx.connection_config.type_ = connection_type
    mock_ctx.get_model = lambda name, **kw: all_resolvable.get(name)
    return mock_ctx


def _run_project(
    source: SqlmeshSource,
    models: dict,
    snapshots: dict,
    connection_type: str = WAREHOUSE_PLATFORM,
    extra_models: dict | None = None,
) -> list:
    mock_ctx = _make_mock_context(models, snapshots, connection_type, extra_models)
    with patch(
        "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
        return_value=mock_ctx,
    ):
        return list(source._ingest_project())


def _make_multi_gateway_context(
    models: dict,
    *,
    gateway_dialects: dict[str, str],
    selected_gateway: str,
    default_catalog_per_gateway: dict[str, str] | None = None,
    snapshots: dict | None = None,
) -> MagicMock:
    """Build a mock SqlmeshContext with multiple gateways visible.

    gateway_dialects: gateway_name → dialect string (e.g. "snowflake", "bigquery").
                      Drives auto-detection of target_platform per gateway.
    selected_gateway: which gateway is the default (used when model.gateway is None).
    """
    mock_ctx = _make_mock_context(
        models,
        snapshots or {},
        connection_type=gateway_dialects[selected_gateway],
    )
    mock_ctx.selected_gateway = selected_gateway
    mock_ctx.engine_adapters = {
        gw: MagicMock(dialect=dialect) for gw, dialect in gateway_dialects.items()
    }
    mock_ctx.default_catalog_per_gateway = default_catalog_per_gateway or {}
    return mock_ctx


def _run_multi_gateway_project(
    source: SqlmeshSource,
    models: dict,
    *,
    gateway_dialects: dict[str, str],
    selected_gateway: str,
    default_catalog_per_gateway: dict[str, str] | None = None,
) -> list:
    mock_ctx = _make_multi_gateway_context(
        models,
        gateway_dialects=gateway_dialects,
        selected_gateway=selected_gateway,
        default_catalog_per_gateway=default_catalog_per_gateway,
    )
    with patch(
        "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
        return_value=mock_ctx,
    ):
        return list(source._ingest_project())


class TestMultiGateway:
    """Multi-gateway: different models targeting different warehouses.

    The connector reads ctx.engine_adapters to discover all gateways and
    builds per-gateway _EffectiveProjectConfig. _effective_for_model(model)
    resolves model.gateway → the right config; URN construction picks up
    the per-gateway platform / instance / catalog.
    """

    def test_single_gateway_project_unchanged(self):
        """Existing single-gateway tests should keep passing — the dict has
        one entry and every model resolves to the default. This is a
        sanity test that _effective_for_model doesn't perturb anything
        when no multi-gateway machinery is set up by the mock."""
        source = _make_source()
        model = _make_mock_model()
        workunits = _run_project(source, {"star.dim_developer": model}, {})

        siblings = next(
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        )
        # Sibling points at the warehouse URN — same as single-gateway today.
        assert WAREHOUSE_PLATFORM in siblings.siblings[0]

    def test_per_model_warehouse_urn_from_gateway(self):
        """Two models on different gateways get sibling URNs on different
        warehouse platforms. The default-gateway model uses the auto-detected
        Snowflake platform; the bigquery-gateway model uses bigquery."""

        source = _make_source({"target_platform": "snowflake"})
        model_snow = _make_mock_model("star.dim_developer")
        model_snow.gateway = None  # uses default
        model_bq = _make_mock_model("star.fct_orders")
        model_bq.gateway = "bigquery_lake"

        workunits = _run_multi_gateway_project(
            source,
            {"star.dim_developer": model_snow, "star.fct_orders": model_bq},
            gateway_dialects={
                "snowflake_prod": "snowflake",
                "bigquery_lake": "bigquery",
            },
            selected_gateway="snowflake_prod",
        )

        siblings_by_sqlmesh_urn = {
            wu.metadata.entityUrn: wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        }
        # Find the sibling for each model — sibling[0] is the warehouse URN
        snow_sibling = next(
            s for urn, s in siblings_by_sqlmesh_urn.items() if "dim_developer" in urn
        )
        bq_sibling = next(
            s for urn, s in siblings_by_sqlmesh_urn.items() if "fct_orders" in urn
        )
        assert "snowflake" in snow_sibling.siblings[0]
        assert "bigquery" in bq_sibling.siblings[0]

    def test_gateway_overrides_apply_platform_instance(self):
        """User-supplied target_platform_instance on a non-default gateway
        flows through to the warehouse URN."""

        source = _make_source(
            {
                "target_platform": "snowflake",
                "gateway_overrides": {
                    "bigquery_lake": {
                        "target_platform": "bigquery",
                        "target_platform_instance": "prod_bigquery",
                    }
                },
            }
        )
        model_bq = _make_mock_model("star.fct_orders")
        model_bq.gateway = "bigquery_lake"

        workunits = _run_multi_gateway_project(
            source,
            {"star.fct_orders": model_bq},
            gateway_dialects={
                "snowflake_prod": "snowflake",
                "bigquery_lake": "bigquery",
            },
            selected_gateway="snowflake_prod",
        )

        bq_sibling = next(
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        )
        assert "prod_bigquery" in bq_sibling.siblings[0]

    def test_default_catalog_per_gateway_used_when_no_override(self):
        """When the user doesn't override default_catalog for a non-default
        gateway, we fall back to ctx.default_catalog_per_gateway."""

        source = _make_source({"target_platform": "snowflake"})
        # Bare two-part name — needs catalog prepending to be 3-part.
        model_bq = _make_mock_model("star.fct_orders")
        model_bq.gateway = "bigquery_lake"

        workunits = _run_multi_gateway_project(
            source,
            {"star.fct_orders": model_bq},
            gateway_dialects={
                "snowflake_prod": "snowflake",
                "bigquery_lake": "bigquery",
            },
            selected_gateway="snowflake_prod",
            default_catalog_per_gateway={"bigquery_lake": "lake-prod"},
        )

        bq_sibling = next(
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        )
        # Warehouse URN prepends the gateway's auto-discovered default_catalog
        assert "lake-prod" in bq_sibling.siblings[0]


class TestSiblingEmission:
    def test_siblings_always_emitted_for_each_model(self):
        """SQLMesh entity gets a SiblingsClass write; warehouse gets a patch."""
        source = _make_source()
        model = _make_mock_model()

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        # Only the sqlmesh entity is written as a full SiblingsClass aspect.
        assert len(sibling_aspects) == 1
        assert sibling_aspects[0].primary is True
        assert WAREHOUSE_PLATFORM in sibling_aspects[0].siblings[0]

        # Warehouse sibling is a PATCH workunit (is_primary_source=False), not an overwrite.
        warehouse_patches = [
            wu
            for wu in workunits
            if wu.is_primary_source is False
            and WAREHOUSE_PLATFORM in str(getattr(wu.metadata, "entityUrn", ""))
        ]
        assert warehouse_patches

    def test_sqlmesh_entity_is_primary_by_default(self):
        """SQLMesh entity is primary sibling (owns model definition), same as dbt."""
        source = _make_source()
        model = _make_mock_model()

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_aspects = [
            (wu.metadata.entityUrn, wu.metadata.aspect)
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]

        primary_urn = next(urn for urn, a in sibling_aspects if a.primary)
        assert SQLMESH_PLATFORM in primary_urn
        assert "dim_developer" in primary_urn

    def test_warehouse_entity_is_patched_as_secondary(self):
        """Warehouse view sibling is secondary via DatasetPatchBuilder."""
        source = _make_source()
        model = _make_mock_model()

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        warehouse_patches = [
            wu
            for wu in workunits
            if wu.is_primary_source is False
            and WAREHOUSE_PLATFORM in str(getattr(wu.metadata, "entityUrn", ""))
        ]
        assert warehouse_patches
        warehouse_urn = warehouse_patches[0].metadata.entityUrn
        assert "dim_developer" in warehouse_urn
        assert "sqlmesh__" not in warehouse_urn

    def test_warehouse_can_be_primary(self):
        source = _make_source({"sqlmesh_is_primary_sibling": False})
        model = _make_mock_model()

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_aspects = [
            (wu.metadata.entityUrn, wu.metadata.aspect)
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]

        # SQLMesh entity still gets the full aspect write, but primary=False.
        assert len(sibling_aspects) == 1
        assert sibling_aspects[0][1].primary is False
        assert SQLMESH_PLATFORM in sibling_aspects[0][0]

    def test_physical_table_not_a_sibling(self):
        """Physical fingerprint table never appears as a sibling."""
        source = _make_source()
        model = _make_mock_model()
        snapshot = _make_mock_snapshot()  # has physical name

        workunits = _run_project(source, {"star.dim_developer": model}, {1: snapshot})

        sibling_urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        # Physical fingerprint table must not appear
        assert not any("sqlmesh__" in u for u in sibling_urns)

    def test_physical_table_in_custom_properties(self):
        """Physical table name stored as custom property, not as an entity."""

        source = _make_source()
        model = _make_mock_model()
        snapshot = _make_mock_snapshot(
            physical_name="db.sqlmesh__star.star__dim_developer__4235172200"
        )

        workunits = _run_project(source, {"star.dim_developer": model}, {1: snapshot})

        props_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
        ]
        assert len(props_aspects) == 1
        assert "sqlmesh__" in props_aspects[0].customProperties.get(
            "sqlmesh.physical_table", ""
        )


class TestAssertionTarget:
    """Regression: sqlmesh audit-derived assertions attach to the SQLMesh
    (logical) URN, not the warehouse URN. Audits are properties of the model
    definition; the warehouse counterpart in SQLMesh is a virtual view over
    a rotating fingerprint table, which has no stable physical equivalent to
    dbt's model→table mapping. Siblings bridge logical → physical in the UI.
    """

    def _assertion_target_urn(self, info: AssertionInfoClass) -> str:
        """Return the CUSTOM assertion's entity URN."""
        assert info.customAssertion is not None
        return info.customAssertion.entity

    def test_assertion_dataset_matches_sqlmesh_urn(self):
        source = _make_source()
        model = _make_mock_model()
        model.audits = [("some_custom_audit", {})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        assertion_infos = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
        ]
        assert len(assertion_infos) >= 1
        target = self._assertion_target_urn(assertion_infos[0])
        assert SQLMESH_PLATFORM in target
        assert WAREHOUSE_PLATFORM not in target

    def test_embedded_model_assertion_targets_sqlmesh_urn(self):
        """Embedded models still attach assertions to the SQLMesh URN."""
        source = _make_source()
        model = _make_mock_model(kind_name="EMBEDDED", is_embedded=True)
        model.audits = [("some_custom_audit", {})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        assertion_infos = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
        ]
        assert len(assertion_infos) >= 1
        target = self._assertion_target_urn(assertion_infos[0])
        assert SQLMESH_PLATFORM in target

    def test_unknown_audit_uses_custom_assertion_type(self):
        """Unknown audits land as AssertionTypeClass.CUSTOM with CustomAssertionInfo,
        not DATASET / SQL. SQLMesh executes them; DataHub only records the definition."""
        source = _make_source()
        model = _make_mock_model()
        model.audits = [("custom_drift_check", {"threshold": 0.05})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        assertion_infos = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
            and wu.metadata.aspect.type == AssertionTypeClass.CUSTOM
        ]
        assert len(assertion_infos) >= 1
        info = assertion_infos[0]
        assert info.customAssertion is not None
        assert info.customAssertion.type == "SQLMesh"
        # Provenance stays in customProperties; the audit name and its kwargs are
        # structured on customAssertion (nativeType / nativeParameters).
        assert info.customProperties.get("sqlmesh.audit") == "custom_drift_check"
        assert info.customAssertion.nativeType == "custom_drift_check"
        assert info.customAssertion.nativeParameters == {"threshold": "0.05"}


class TestFreshnessAndVolumeSignals:
    """Connector no longer emits FRESHNESS / VOLUME assertion definitions.
    It still emits OperationAspect (fingerprint rebuild) and DatasetProfile
    (row count) when state / warehouse are reachable — users create monitors
    against those timeseries.
    """

    def _assertion_types(self, workunits):
        return {
            wu.metadata.aspect.type
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
        }

    def test_no_freshness_or_volume_assertions_emitted(self):
        source = _make_source()
        model = _make_mock_model()
        model.interval_unit = MagicMock(value="hour")
        model.audits = [("not_null", {"columns": MagicMock(expressions=[])})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})
        types = self._assertion_types(workunits)
        assert AssertionTypeClass.FRESHNESS not in types
        assert AssertionTypeClass.VOLUME not in types
        assert AssertionTypeClass.DATASET not in types
        assert AssertionTypeClass.SQL not in types

    def test_known_audit_is_custom(self):
        source = _make_source()
        model = _make_mock_model()
        col = MagicMock()
        col.name = "id"
        model.audits = [("not_null", {"columns": MagicMock(expressions=[col])})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})
        infos = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
        ]
        assert len(infos) == 1
        assert infos[0].type == AssertionTypeClass.CUSTOM
        assert infos[0].customAssertion.type == "SQLMesh"
        # Operator/scope/aggregation are structured on customAssertion (dbt
        # parity), not stringified into customProperties.
        assert infos[0].customAssertion.operator == "NOT_NULL"
        field_urn = (
            "urn:li:schemaField:(urn:li:dataset:"
            "(urn:li:dataPlatform:sqlmesh,star.dim_developer,PROD),id)"
        )
        assert infos[0].customAssertion.field == field_urn
        assert infos[0].customAssertion.fields == [field_urn]

    def test_builtin_audit_carries_std_parameters(self):
        """A built-in with bounds (accepted_range) emits structured
        AssertionStdParameters on customAssertion, not string customProperties."""
        source = _make_source()
        model = _make_mock_model()
        col = MagicMock()
        col.name = "amount"
        model.audits = [
            (
                "accepted_range",
                {
                    "columns": MagicMock(expressions=[col]),
                    "min_v": MagicMock(this="0"),
                    "max_v": MagicMock(this="100"),
                },
            )
        ]

        workunits = _run_project(source, {"star.dim_developer": model}, {})
        infos = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
        ]
        assert len(infos) == 1
        ca = infos[0].customAssertion
        assert ca.operator == "BETWEEN"
        assert ca.parameters is not None
        assert ca.parameters.minValue.value == "0"
        assert ca.parameters.maxValue.value == "100"

    def test_pipeline_operation_emitted_when_state_available(self):
        source = _make_source()
        model = _make_mock_model()
        model.fqn = "star.dim_developer"
        snapshot = _make_mock_snapshot()
        snapshot.updated_ts = 1_700_000_000_000

        # Pin the state capability so the assertion is deterministic rather than
        # dependent on how the MagicMock probe happens to resolve.
        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source._probe_capabilities",
            return_value=_CapabilityProbes(
                has_state=True, has_warehouse_query=False, has_graph=False
            ),
        ):
            workunits = _run_project(
                source,
                {"star.dim_developer": model},
                {"star.dim_developer": snapshot},
            )

        ops = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), OperationClass)
        ]
        # Exactly one CUSTOM fingerprint-rebuild operation for the one model.
        assert len(ops) == 1
        assert ops[0].operationType == "CUSTOM"
        assert ops[0].customOperationType == "SQLMESH_FINGERPRINT_REBUILD"
        assert ops[0].lastUpdatedTimestamp == 1_700_000_000_000


class TestRowCountProfile:
    """`_emit_row_count_profile` must run its COUNT(*) on the adapter for the
    model's OWN gateway, not the default one. Multi-gateway projects route
    different models to different warehouses; querying the default adapter
    would count rows in the wrong warehouse (the bug fixed by
    `_engine_adapter_for_model`)."""

    def test_row_count_uses_model_gateway_adapter(self):
        source = _make_source()
        source._capabilities = _CapabilityProbes(
            has_state=False, has_warehouse_query=True, has_graph=False
        )
        source._selected_gateway = None

        model = _make_mock_model("star.dim_developer")
        # Model lives on the "bq" gateway, not the project default.
        model.gateway = "bq"

        default_adapter = MagicMock()
        default_adapter.fetchone.return_value = (999,)  # wrong warehouse
        bq_adapter = MagicMock(DIALECT="bigquery")
        bq_adapter.fetchone.return_value = (42,)

        ctx = MagicMock()
        ctx.engine_adapter = default_adapter
        ctx.engine_adapters = {"bq": bq_adapter}

        workunits = list(
            source._emit_row_count_profile(
                model=model,
                sqlmesh_urn="urn:li:dataset:(urn:li:dataPlatform:sqlmesh,star.dim_developer,PROD)",
                physical_name="db.sqlmesh__star.star__dim_developer__4235172200",
                sqlmesh_ctx=ctx,
            )
        )

        # The count came from the model's gateway adapter, never the default.
        bq_adapter.fetchone.assert_called_once()
        default_adapter.fetchone.assert_not_called()

        profiles = _aspects_of_type(workunits, DatasetProfileClass)
        assert len(profiles) == 1
        assert profiles[0].rowCount == 42

    def test_no_profile_without_warehouse_access(self):
        source = _make_source()
        source._capabilities = _CapabilityProbes(
            has_state=False, has_warehouse_query=False, has_graph=False
        )
        source._selected_gateway = None

        model = _make_mock_model("star.dim_developer")
        # Single-gateway model: no explicit gateway, so it resolves to the
        # default adapter — the one the has_warehouse_query probe covers.
        model.gateway = None
        ctx = MagicMock()

        workunits = list(
            source._emit_row_count_profile(
                model=model,
                sqlmesh_urn="urn:li:dataset:(urn:li:dataPlatform:sqlmesh,star.dim_developer,PROD)",
                physical_name="db.sqlmesh__star.star__dim_developer__4235172200",
                sqlmesh_ctx=ctx,
            )
        )
        assert workunits == []


class TestIncidentOnFailure:
    """When emit_incidents_on_failure is True (default), every 'fail' entry
    in the audit_results_path JSON also produces an Incident pointing at
    the failing dataset, sourced from the corresponding assertion URN.
    """

    def _write_results(self, tmp_path: Path, results: list) -> str:

        f = Path(tmp_path) / "audit_results.json"
        f.write_text(
            _json.dumps(
                {
                    "metadata": {"generated_at": "2026-05-28T00:00:00"},
                    "results": results,
                }
            ),
            encoding="utf-8",
        )
        return str(f)

    def _run_project_then_audit(
        self,
        source: SqlmeshSource,
        model_dict: dict,
        results: list,
        tmp_path: Path,
    ) -> list:
        # _emit_audit_run_events requires _resolved_effective populated, which
        # _ingest_project does as a side effect. Consume the workunits but
        # discard them — we only care about the audit-run-event call after.
        list(_run_project(source, model_dict, {}))
        path = self._write_results(tmp_path, results)
        return list(source._emit_audit_run_events(path))

    def test_incident_emitted_for_failing_audit(self, tmp_path):

        source = _make_source()
        model = _make_mock_model("star.dim_developer")

        workunits = self._run_project_then_audit(
            source,
            {"star.dim_developer": model},
            [
                {
                    "model": "star.dim_developer",
                    "audit": "not_null",
                    "columns": ["id"],
                    "status": "fail",
                    "failing_rows": 7,
                },
            ],
            tmp_path,
        )

        incidents = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), IncidentInfoClass)
        ]
        assert len(incidents) == 1
        info = incidents[0]
        assert info.type == "CUSTOM"
        assert info.customType == "SQLMESH_AUDIT/not_null"
        assert SQLMESH_PLATFORM in info.entities[0]
        # Source links back to the assertion that fired
        assert info.source is not None
        assert info.source.type == "ASSERTION_FAILURE"
        assert info.source.sourceUrn.startswith("urn:li:assertion:")
        # The exact failing-row count is surfaced in both the title and the
        # description, so a regression that drops it is caught (a bare "7 in
        # title" would also pass on an unrelated 7 elsewhere in the string).
        assert "(7 failing rows)" in info.title
        assert "7 failing rows" in info.description

    def test_no_incident_for_passing_audit(self, tmp_path):

        source = _make_source()
        model = _make_mock_model("star.dim_developer")

        workunits = self._run_project_then_audit(
            source,
            {"star.dim_developer": model},
            [
                {
                    "model": "star.dim_developer",
                    "audit": "not_null",
                    "columns": ["id"],
                    "status": "pass",
                    "failing_rows": 0,
                },
            ],
            tmp_path,
        )

        incidents = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), IncidentInfoClass)
        ]
        assert incidents == []

    def test_disable_via_config(self, tmp_path):

        source = _make_source({"emit_incidents_on_failure": False})
        model = _make_mock_model("star.dim_developer")

        workunits = self._run_project_then_audit(
            source,
            {"star.dim_developer": model},
            [
                {
                    "model": "star.dim_developer",
                    "audit": "not_null",
                    "columns": ["id"],
                    "status": "fail",
                    "failing_rows": 3,
                },
            ],
            tmp_path,
        )

        # Assertion run event is still emitted; just no incident
        incidents = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), IncidentInfoClass)
        ]
        assert incidents == []

    def test_incident_urn_is_idempotent(self, tmp_path):
        """Two ingest passes against the same audit-results JSON produce
        the same Incident URN — re-ingest updates the incident in place
        rather than creating duplicates."""
        source = _make_source()
        model = _make_mock_model("star.dim_developer")
        results = [
            {
                "model": "star.dim_developer",
                "audit": "not_null",
                "columns": ["id"],
                "status": "fail",
                "failing_rows": 7,
            }
        ]

        wu1 = self._run_project_then_audit(
            source, {"star.dim_developer": model}, results, tmp_path
        )
        wu2 = self._run_project_then_audit(
            source, {"star.dim_developer": model}, results, tmp_path
        )

        urn1 = next(
            wu.metadata.entityUrn
            for wu in wu1
            if str(wu.metadata.entityUrn).startswith("urn:li:incident:")
        )
        urn2 = next(
            wu.metadata.entityUrn
            for wu in wu2
            if str(wu.metadata.entityUrn).startswith("urn:li:incident:")
        )
        assert urn1 == urn2


class TestAuditRunEventUrnMatching:
    """Run events must land on the exact assertion URN the definition used.

    The suffix is computed by a single shared helper so the definition side
    (_emit_single_audit) and run-event side (_audit_run_events_for_entry) can't
    drift. These tests pin that both hash to the same URN — including the
    unknown/custom-audit case, which previously diverged (definition used ""
    while run events joined the columns).
    """

    def _write_results(self, tmp_path: Path, results: list) -> str:
        f = Path(tmp_path) / "audit_results.json"
        f.write_text(
            _json.dumps(
                {
                    "metadata": {"generated_at": "2026-05-28T00:00:00"},
                    "results": results,
                }
            ),
            encoding="utf-8",
        )
        return str(f)

    def _definition_urn(self, workunits: list, audit_substr: str) -> str:
        for wu in workunits:
            aspect = getattr(wu.metadata, "aspect", None)
            if (
                isinstance(aspect, AssertionInfoClass)
                and aspect.customProperties.get("sqlmesh.audit") == audit_substr
            ):
                return wu.metadata.entityUrn
        raise AssertionError(f"no assertion definition for {audit_substr}")

    def _run_event_urns(
        self, source: SqlmeshSource, tmp_path: Path, results: list
    ) -> List[str]:
        path = self._write_results(tmp_path, results)
        return [
            event.assertionUrn
            for event in _aspects_of_type(
                source._emit_audit_run_events(path), AssertionRunEventClass
            )
        ]

    def test_column_audit_run_event_matches_definition(self, tmp_path):
        source = _make_source()
        col = MagicMock()
        col.name = "id"
        model = _make_mock_model("star.dim_developer")
        model.audits = [("not_null", {"columns": MagicMock(expressions=[col])})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})
        definition_urn = self._definition_urn(workunits, "not_null")

        run_urns = self._run_event_urns(
            source,
            tmp_path,
            [
                {
                    "model": "star.dim_developer",
                    "audit": "not_null",
                    "columns": ["id"],
                    "status": "pass",
                    "failing_rows": 0,
                }
            ],
        )
        assert definition_urn in run_urns

    def test_unknown_audit_with_columns_run_event_matches_definition(self, tmp_path):
        """Regression: an unknown audit's definition uses an empty suffix, so a
        run event whose results list columns must still resolve to that same
        URN — not one built by joining the columns."""
        source = _make_source()
        model = _make_mock_model("star.dim_developer")
        model.audits = [("custom_drift_check", {})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})
        definition_urn = self._definition_urn(workunits, "custom_drift_check")

        run_urns = self._run_event_urns(
            source,
            tmp_path,
            [
                {
                    "model": "star.dim_developer",
                    "audit": "custom_drift_check",
                    "columns": ["a", "b"],
                    "status": "fail",
                    "failing_rows": 4,
                }
            ],
        )
        assert definition_urn in run_urns

    def test_malformed_entry_skipped_and_valid_one_still_emitted(self, tmp_path):
        """One malformed entry must not abort the file: it is skipped with a
        warning while a valid entry still yields its AssertionRunEvent."""
        source = _make_source()
        col = MagicMock()
        col.name = "id"
        model = _make_mock_model("star.dim_developer")
        model.audits = [("not_null", {"columns": MagicMock(expressions=[col])})]
        _run_project(source, {"star.dim_developer": model}, {})

        path = self._write_results(
            tmp_path,
            [
                "totally-not-an-entry",
                {
                    "model": "star.dim_developer",
                    "audit": "not_null",
                    "columns": ["id"],
                    "status": "fail",
                    "failing_rows": 2,
                },
            ],
        )
        before = len(list(source.report.warnings))
        run_events = _aspects_of_type(
            source._emit_audit_run_events(path), AssertionRunEventClass
        )
        assert len(run_events) == 1
        assert len(list(source.report.warnings)) > before


class TestAuditResultUrnResolution:
    """_sqlmesh_urn_for_audit_result: cached hit, cache-miss fallback, and the
    no-model-ingested guard — the gateway-aware cache this rework added, whose
    fallback path had no coverage."""

    def test_cache_hit_returns_ingested_urn(self):
        source = _make_source()
        model = _make_mock_model("star.dim_developer")
        _run_project(source, {"star.dim_developer": model}, {})

        urn = source._sqlmesh_urn_for_audit_result("star.dim_developer")
        assert urn is not None
        assert SQLMESH_PLATFORM in urn
        assert "dim_developer" in urn

    def test_cache_miss_for_un_ingested_model_skips_and_warns(self):
        source = _make_source()
        model = _make_mock_model("star.dim_developer")
        _run_project(source, {"star.dim_developer": model}, {})

        before = len(list(source.report.warnings))
        urn = source._sqlmesh_urn_for_audit_result("star.not_ingested")
        # No assertion definition was emitted for this model, so fabricating a
        # URN would attach orphaned run events/incidents to a non-existent
        # assertion. Skip (return None) and surface a warning instead.
        assert urn is None
        assert len(list(source.report.warnings)) > before

    def test_no_model_ingested_returns_none_and_warns(self):
        source = _make_source()
        # _resolved_effective is None because no project was ingested.
        before = len(list(source.report.warnings))
        urn = source._sqlmesh_urn_for_audit_result("star.dim_developer")
        assert urn is None
        assert len(list(source.report.warnings)) > before


class TestLineageEmission:
    def test_lineage_points_to_sqlmesh_urns(self):
        """Lineage edges for managed deps target sqlmesh URNs, not warehouse URNs."""
        source = _make_source()
        upstream = _make_mock_model("star.base_developer")
        model = _make_mock_model(depends_on={"star.base_developer"})

        # Both models in context → Category 1 (managed) → sqlmesh URN
        workunits = _run_project(
            source,
            {"star.dim_developer": model, "star.base_developer": upstream},
            {},
        )

        lineage_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage_aspects) == 1
        assert len(lineage_aspects[0].upstreams) == 1
        upstream_urn = lineage_aspects[0].upstreams[0].dataset
        assert SQLMESH_PLATFORM in upstream_urn
        assert "base_developer" in upstream_urn
        assert WAREHOUSE_PLATFORM not in upstream_urn

    def test_no_lineage_when_disabled(self):
        source = _make_source({"include_lineage": False})
        model = _make_mock_model(depends_on={"star.base_developer"})

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        lineage_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage_aspects) == 0

    def test_denied_deps_excluded_from_lineage(self):
        source = _make_source({"model_name_pattern": {"deny": ["star.raw_.*"]}})
        model = _make_mock_model(depends_on={"star.base_developer", "star.raw_source"})

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        lineage_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage_aspects) == 1
        upstream_urns = [u.dataset for u in lineage_aspects[0].upstreams]
        assert all("raw_source" not in u for u in upstream_urns)
        assert any("base_developer" in u for u in upstream_urns)


class TestColumnLineage:
    """Tests for column-level lineage. Patches _build_column_lineage directly
    since sqlmesh is not installed in the test venv."""

    def test_column_lineage_emitted_when_enabled(self):
        """FineGrainedLineage from _build_column_lineage appears in output."""

        source = _make_source()
        upstream = _make_mock_model("star.base_developer")
        model = _make_mock_model(
            "star.dim_developer",
            columns={"developer_id": MagicMock(__str__=lambda s: "BIGINT")},
            depends_on={"star.base_developer"},
        )

        fake_cll = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:sqlmesh,star.base_developer,PROD),id)"
                ],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:sqlmesh,star.dim_developer,PROD),developer_id)"
                ],
            )
        ]

        with patch.object(source, "_build_column_lineage", return_value=fake_cll):
            workunits = _run_project(
                source,
                # Only dim_developer in context.models — base_developer accessible via
                # get_model() but not iterated itself to avoid duplicate CLL aspects
                {"star.dim_developer": model},
                {},
                extra_models={"star.base_developer": upstream},
            )

        cll_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
            and getattr(wu.metadata.aspect, "fineGrainedLineages", None)
        ]
        assert len(cll_aspects) == 1
        fg = cll_aspects[0].fineGrainedLineages[0]
        assert "developer_id" in fg.downstreams[0]
        assert "id" in fg.upstreams[0]

    def test_no_column_lineage_when_disabled(self):
        source = _make_source({"include_column_lineage": False})
        model = _make_mock_model(depends_on={"star.base_developer"})
        upstream = _make_mock_model("star.base_developer")

        with patch.object(source, "_build_column_lineage", return_value=[]) as mock_cll:
            _run_project(
                source,
                {"star.dim_developer": model, "star.base_developer": upstream},
                {},
            )
            mock_cll.assert_not_called()

    def test_no_cll_mcp_when_build_returns_empty(self):
        """When _build_column_lineage returns [] no extra MCP is emitted."""
        source = _make_source()
        model = _make_mock_model(depends_on={"star.base_developer"})
        upstream = _make_mock_model("star.base_developer")

        with patch.object(source, "_build_column_lineage", return_value=[]):
            workunits = _run_project(
                source,
                {"star.dim_developer": model, "star.base_developer": upstream},
                {},
            )

        cll_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
            and getattr(wu.metadata.aspect, "fineGrainedLineages", None)
        ]
        assert len(cll_aspects) == 0


def _install_fake_column_dependencies(
    monkeypatch: pytest.MonkeyPatch, deps_by_column: dict
) -> None:
    """Wire a fake ``sqlmesh.core.lineage.column_dependencies`` into sys.modules.

    sqlmesh isn't installed in the test venv, so ``_build_column_lineage``'s
    ``from sqlmesh.core.lineage import column_dependencies`` would otherwise
    short-circuit to []. Injecting a stand-in lets us regression-test the real
    (un-mocked) body — in particular the model_name_pattern filter on upstream
    columns.
    """

    def _column_dependencies(ctx: Any, model_name: str, col: str) -> dict:
        return deps_by_column.get(col, {})

    lineage_mod = types.ModuleType("sqlmesh.core.lineage")
    lineage_mod.column_dependencies = _column_dependencies  # type: ignore[attr-defined]
    for name, mod in [
        ("sqlmesh", types.ModuleType("sqlmesh")),
        ("sqlmesh.core", types.ModuleType("sqlmesh.core")),
        ("sqlmesh.core.lineage", lineage_mod),
    ]:
        # Don't clobber a real sqlmesh if it's importable in this venv.
        if name not in sys.modules:
            monkeypatch.setitem(sys.modules, name, mod)
    if "sqlmesh.core.lineage" not in sys.modules or not hasattr(
        sys.modules["sqlmesh.core.lineage"], "column_dependencies"
    ):
        monkeypatch.setitem(sys.modules, "sqlmesh.core.lineage", lineage_mod)


class TestColumnLineageFilter:
    """Regression: _build_column_lineage's model_name_pattern filter must drop
    denied upstreams from column-level edges too, not just table-level ones.
    Runs the real _build_column_lineage (column_dependencies faked in), so the
    filter is actually exercised rather than mocked away."""

    def test_denied_upstream_excluded_from_column_lineage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_column_dependencies(
            monkeypatch,
            {
                "developer_id": {
                    "star.base_developer": {"id"},
                    "raw.denied_table": {"secret"},
                }
            },
        )
        source = _make_source({"model_name_pattern": {"deny": ["raw\\..*"]}})
        upstream = _make_mock_model("star.base_developer")
        model = _make_mock_model(
            "star.dim_developer",
            columns={"developer_id": MagicMock(__str__=lambda s: "BIGINT")},
            depends_on={"star.base_developer"},
        )

        workunits = _run_project(
            source,
            {"star.dim_developer": model},
            {},
            extra_models={"star.base_developer": upstream},
        )

        cll = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
            and getattr(wu.metadata.aspect, "fineGrainedLineages", None)
        ]
        assert len(cll) == 1
        upstream_field_urns = cll[0].fineGrainedLineages[0].upstreams
        # The allowed upstream is present; the denied one is filtered out of the
        # column-level edge entirely.
        assert any("base_developer" in u for u in upstream_field_urns)
        assert not any("denied_table" in u for u in upstream_field_urns)


class TestLineageCategories:
    """Tests for the 3-category lineage handling."""

    def test_cat1_managed_model_uses_sqlmesh_urn(self):
        """Category 1: managed deps → sqlmesh URN."""
        source = _make_source()
        upstream = _make_mock_model("star.base_developer")
        model = _make_mock_model(depends_on={"star.base_developer"})

        workunits = _run_project(
            source,
            {"star.dim_developer": model, "star.base_developer": upstream},
            {},
        )

        lineage = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage) == 1
        upstream_urn = lineage[0].upstreams[0].dataset
        assert SQLMESH_PLATFORM in upstream_urn

    def test_cat3_undeclared_implicit_uses_warehouse_urn(self):
        """Category 3: dep not in context.models → warehouse URN directly."""
        source = _make_source()
        model = _make_mock_model(depends_on={"raw.source_table"})

        # raw.source_table is NOT in context.models → get_model returns None
        workunits = _run_project(source, {"star.dim_developer": model}, {})

        lineage = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage) == 1
        upstream_urn = lineage[0].upstreams[0].dataset
        assert WAREHOUSE_PLATFORM in upstream_urn
        assert SQLMESH_PLATFORM not in upstream_urn

    def test_cat2_declared_external_uses_sqlmesh_urn_by_default(self):
        """Category 2 default: declared external → sqlmesh Source entity."""
        source = _make_source()
        external_model = _make_mock_model("raw.source_table", kind_name="EXTERNAL")
        model = _make_mock_model(depends_on={"raw.source_table"})

        workunits = _run_project(
            source,
            {"star.dim_developer": model},
            {},
            extra_models={"raw.source_table": external_model},
        )

        lineage = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage) == 1
        upstream_urn = lineage[0].upstreams[0].dataset
        assert SQLMESH_PLATFORM in upstream_urn

    def test_cat2_skip_external_uses_warehouse_urn(self):
        """Category 2 with skip_external_models_in_lineage → warehouse URN."""
        source = _make_source({"skip_external_models_in_lineage": True})
        external_model = _make_mock_model("raw.source_table", kind_name="EXTERNAL")
        model = _make_mock_model(depends_on={"raw.source_table"})

        workunits = _run_project(
            source,
            {"star.dim_developer": model},
            {},
            extra_models={"raw.source_table": external_model},
        )

        lineage = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage) == 1
        upstream_urn = lineage[0].upstreams[0].dataset
        assert WAREHOUSE_PLATFORM in upstream_urn
        assert SQLMESH_PLATFORM not in upstream_urn

    def test_dep_filtered_by_model_kind_uses_warehouse_urn(self):
        """A managed dep excluded by model_kind_filter has no sqlmesh entity, so
        lineage points at its warehouse URN instead of a dangling sqlmesh URN."""
        source = _make_source({"model_kind_filter": ["VIEW"]})
        # Upstream is a FULL model (filtered out); the downstream VIEW is emitted.
        upstream = _make_mock_model("star.base_developer", kind_name="FULL")
        model = _make_mock_model(
            "star.dim_developer",
            depends_on={"star.base_developer"},
            kind_name="VIEW",
        )

        workunits = _run_project(
            source,
            {"star.dim_developer": model, "star.base_developer": upstream},
            {},
        )

        lineage = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), UpstreamLineageClass)
        ]
        assert len(lineage) == 1
        upstream_urn = lineage[0].upstreams[0].dataset
        assert WAREHOUSE_PLATFORM in upstream_urn
        assert SQLMESH_PLATFORM not in upstream_urn

    def test_include_database_name_false_strips_catalog(self):
        """include_database_name=False drops catalog from warehouse sibling URN."""
        source = _make_source(
            {"default_catalog": "analytics", "include_database_name": False}
        )
        model = _make_mock_model("star.dim_developer")

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_targets = [
            aspect.siblings[0] for aspect in _aspects_of_type(workunits, SiblingsClass)
        ]
        warehouse_urn = next(u for u in sibling_targets if WAREHOUSE_PLATFORM in u)
        # With include_database_name=False, catalog 'analytics' is stripped
        assert "analytics" not in warehouse_urn
        assert "dim_developer" in warehouse_urn


class TestPlatformDetection:
    def test_target_platform_auto_detected_from_connection(self):
        """target_platform detected from gateway connection type when not configured."""
        source = _make_source({"target_platform": None})
        model = _make_mock_model()

        workunits = _run_project(
            source, {"star.dim_developer": model}, {}, connection_type="bigquery"
        )

        sibling_targets = [
            aspect.siblings[0] for aspect in _aspects_of_type(workunits, SiblingsClass)
        ]
        assert any("bigquery" in u for u in sibling_targets)

    def test_explicit_target_platform_overrides_auto_detection(self):
        source = _make_source({"target_platform": "redshift"})
        model = _make_mock_model()

        # connection_type says databricks, but explicit config says redshift
        workunits = _run_project(
            source, {"star.dim_developer": model}, {}, connection_type="databricks"
        )

        sibling_targets = [
            aspect.siblings[0] for aspect in _aspects_of_type(workunits, SiblingsClass)
        ]
        assert any("redshift" in u for u in sibling_targets)
        assert not any("databricks" in u for u in sibling_targets)


def _effective(source: SqlmeshSource) -> _EffectiveProjectConfig:
    """Return the resolved effective config."""

    return _EffectiveProjectConfig(
        project_path=source.config.project_path,
        gateway=source.config.gateway,
        environment=source.config.environment,
        target_platform=source.config.target_platform,
        target_platform_instance=source.config.target_platform_instance,
        sqlmesh_platform_instance=source.config.sqlmesh_platform_instance,
        default_catalog=source.config.default_catalog,
        convert_urns_to_lowercase=source.config.convert_urns_to_lowercase,
    )


class TestNormalization:
    def test_quoted_names_are_normalized(self):
        source = _make_source({"target_platform": "databricks"})
        eff = _effective(source)
        assert (
            source._normalize_name('"STAR"."DIM_DEVELOPER"', eff)
            == "STAR.DIM_DEVELOPER"
        )
        assert (
            source._normalize_name("`star`.`dim_developer`", eff)
            == "star.dim_developer"
        )

    def test_lowercase_applied_when_configured(self):
        source = _make_source({"convert_urns_to_lowercase": True})
        eff = _effective(source)
        assert source._normalize_name("STAR.DIM_DEVELOPER", eff) == "star.dim_developer"

    def test_no_lowercase_for_non_snowflake_platforms(self):
        source = _make_source({"target_platform": "databricks"})
        eff = _effective(source)
        assert source._normalize_name("STAR.DIM_DEVELOPER", eff) == "STAR.DIM_DEVELOPER"

    def test_snowflake_auto_lowercases(self):
        source = _make_source({"target_platform": "snowflake"})
        eff = _effective(source)
        assert source._normalize_name("STAR.DIM_DEVELOPER", eff) == "star.dim_developer"

    def test_qualify_fqn_prepends_catalog_for_two_part_names(self):
        source = _make_source({"default_catalog": "analytics"})
        eff = _effective(source)
        assert (
            source._qualify_fqn("star.dim_developer", eff)
            == "analytics.star.dim_developer"
        )

    def test_qualify_fqn_leaves_three_part_names_unchanged(self):
        source = _make_source({"default_catalog": "analytics"})
        eff = _effective(source)
        assert (
            source._qualify_fqn("mydb.star.dim_developer", eff)
            == "mydb.star.dim_developer"
        )

    def test_qualify_fqn_no_op_when_catalog_not_set(self):
        source = _make_source()
        eff = _effective(source)
        assert source._qualify_fqn("star.dim_developer", eff) == "star.dim_developer"

    def test_qualify_fqn_lowercases_catalog(self):
        source = _make_source(
            {"default_catalog": "Analytics", "convert_urns_to_lowercase": True}
        )
        eff = _effective(source)
        assert (
            source._qualify_fqn("star.dim_developer", eff)
            == "analytics.star.dim_developer"
        )

    def test_target_platform_flows_to_effective_config(self):
        source = _make_source(
            {"target_platform": "bigquery", "default_catalog": "my-gcp-project"}
        )
        eff = _effective(source)
        assert eff.target_platform == "bigquery"
        assert eff.default_catalog == "my-gcp-project"

    def test_default_catalog_flows_to_effective_config(self):
        source = _make_source({"default_catalog": "analytics"})
        eff = _effective(source)
        assert eff.target_platform == WAREHOUSE_PLATFORM
        assert eff.default_catalog == "analytics"

    def test_sqlmesh_platform_instance_flows_to_effective_config(self):
        source = _make_source({"sqlmesh_platform_instance": "project_a"})
        eff = _effective(source)
        assert eff.sqlmesh_platform_instance == "project_a"


class TestBuildCountQuery:
    """Identifier quoting in _build_count_query — SQLGlot has to render the
    three-part name with dialect-correct quoting so hyphens, reserved words,
    and other identifier-significant characters don't break the query.
    """

    def test_three_part_name_hyphenated_catalog_duckdb(self):

        # 'sushi-example' is the canonical pathological case — bare splice
        # gives DuckDB `sushi - example` (a subtraction).
        sql = _build_count_query(
            "sushi-example.sqlmesh__sushimoderate.sushimoderate__top_waiters__abc",
            dialect="duckdb",
        )
        assert '"sushi-example"' in sql
        assert '"sqlmesh__sushimoderate"' in sql
        assert '"sushimoderate__top_waiters__abc"' in sql
        assert sql.startswith("SELECT COUNT(*)")

    def test_three_part_name_snowflake_uppercase(self):

        sql = _build_count_query(
            "ANALYTICS.SQLMESH__STAR.STAR__DIM_DEVELOPER__123",
            dialect="snowflake",
        )
        # Snowflake double-quotes preserve case; identifiers are kept.
        assert '"ANALYTICS"' in sql
        assert '"SQLMESH__STAR"' in sql
        assert '"STAR__DIM_DEVELOPER__123"' in sql

    def test_three_part_name_bigquery_uses_backticks(self):

        sql = _build_count_query(
            "my-gcp-project.sqlmesh__schema.dim_user__abc",
            dialect="bigquery",
        )
        # BigQuery quotes identifiers with backticks.
        assert "`my-gcp-project`" in sql
        assert "`sqlmesh__schema`" in sql
        assert "`dim_user__abc`" in sql

    def test_two_part_name(self):

        sql = _build_count_query("schema.table", dialect="duckdb")
        assert '"schema"' in sql
        assert '"table"' in sql
        assert "FROM " in sql

    def test_one_part_name(self):

        sql = _build_count_query("standalone_table", dialect="duckdb")
        assert '"standalone_table"' in sql

    def test_reserved_word_table_name_quoted(self):
        """SQL reserved words in identifiers must be quoted to be usable."""

        sql = _build_count_query("catalog.schema.order", dialect="duckdb")
        # "order" is a SQL reserved word; quoting prevents parser confusion
        assert '"order"' in sql


class TestSchemaEmission:
    def test_no_schema_when_disabled(self):
        source = _make_source({"include_schema": False})
        model = _make_mock_model()
        workunits = _run_project(source, {"star.dim_developer": model}, {})

        schema_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SchemaMetadata)
        ]
        assert len(schema_aspects) == 0

    def test_no_schema_when_model_has_no_columns(self):
        source = _make_source()
        model = _make_mock_model(columns={})
        workunits = _run_project(source, {"star.dim_developer": model}, {})

        schema_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SchemaMetadata)
        ]
        assert len(schema_aspects) == 0

    def test_snowflake_timestamp_resolves_to_time_not_bytes(self):
        # The shared cross-platform type map is last-writer-wins, so SQL
        # Server's TIMESTAMP -> BytesType shadows Snowflake's TIMESTAMP ->
        # TimeType. For a Snowflake target we must consult Snowflake's own
        # resolver so a sibling of a Snowflake table doesn't render a
        # timestamp column as an opaque bytes blob.
        from datahub.metadata.schema_classes import BytesTypeClass, TimeTypeClass

        source = _make_source()
        resolved = source._resolve_column_type("TIMESTAMP", "snowflake")
        assert isinstance(resolved, TimeTypeClass)

        # A platform without the conflict still flows through resolve_sql_type.
        other = source._resolve_column_type("TIMESTAMP", "sqlserver")
        assert isinstance(other, BytesTypeClass)


class TestErrorHandling:
    def test_failing_model_is_recorded_and_others_continue(self):
        source = _make_source()
        good_model = _make_mock_model("star.good_model")
        bad_model = _make_mock_model("star.bad_model")

        mock_ctx = _make_mock_context(
            {"star.bad_model": bad_model, "star.good_model": good_model}, {}
        )

        with (
            patch(
                "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
                return_value=mock_ctx,
            ),
            patch.object(
                source,
                "_emit_model",
                side_effect=[RuntimeError("boom"), iter([])],
            ),
        ):
            list(source._ingest_project())

        assert source.report.models_scanned == 2
        assert any(m == "star.bad_model" for m in source.report.models_failed)

    def test_context_init_failure_is_fatal_and_yields_nothing(self):
        source = _make_source()
        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            side_effect=Exception("connection refused"),
        ):
            workunits = list(source._ingest_project())

        assert workunits == []
        assert len(source.report.failures) > 0


class TestEnvironmentSuffix:
    """Tests for environment suffix auto-detection (REQ-12)."""

    def _make_effective(
        self, env: str, suffix_target: str, catalog_mapping: dict | None = None
    ) -> _EffectiveProjectConfig:

        return _EffectiveProjectConfig(
            project_path="/proj",
            gateway=None,
            environment=env,
            target_platform="snowflake",
            target_platform_instance=None,
            sqlmesh_platform_instance=None,
            default_catalog=None,
            convert_urns_to_lowercase=False,
            env_suffix_target=suffix_target,
            env_catalog_mapping=catalog_mapping or {},
        )

    def test_prod_no_suffix(self):
        source = _make_source()
        eff = self._make_effective("prod", "schema")
        assert (
            source._apply_env_suffix("analytics.star.dim_developer", eff)
            == "analytics.star.dim_developer"
        )

    def test_schema_mode_suffixes_schema(self):
        source = _make_source()
        eff = self._make_effective("dev", "schema")
        result = source._apply_env_suffix("analytics.star.dim_developer", eff)
        assert result == "analytics.star__dev.dim_developer"

    def test_table_mode_suffixes_table(self):
        source = _make_source()
        eff = self._make_effective("dev", "table")
        result = source._apply_env_suffix("analytics.star.dim_developer", eff)
        assert result == "analytics.star.dim_developer__dev"

    def test_catalog_mode_suffixes_catalog(self):
        source = _make_source()
        eff = self._make_effective("dev", "catalog")
        result = source._apply_env_suffix("analytics.star.dim_developer", eff)
        assert result == "analytics__dev.star.dim_developer"

    def test_catalog_mapping_overrides_suffix(self):
        source = _make_source()
        eff = self._make_effective(
            "dev", "schema", catalog_mapping={"dev": "dev_catalog"}
        )
        result = source._apply_env_suffix("analytics.star.dim_developer", eff)
        assert result == "dev_catalog.star.dim_developer"

    def test_suffix_applied_in_warehouse_urn(self):
        """Environment suffix flows through to the warehouse sibling URN."""
        model = _make_mock_model()

        # Mock context with env_suffix_target = "schema"
        mock_ctx = _make_mock_context({"star.dim_developer": model}, {})
        mock_ctx.config.environment_suffix_target = "schema"
        mock_ctx.config.environment_catalog_mapping = {}

        config = SqlmeshSourceConfig.model_validate(
            {
                "project_path": "/proj",
                "environment": "dev",
                "target_platform": "snowflake",
                "env": "DEV",
            }
        )
        source2 = SqlmeshSource(config, PipelineContext(run_id="test"))

        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            return_value=mock_ctx,
        ):
            workunits = list(source2._ingest_project())

        sibling_targets = [
            aspect.siblings[0] for aspect in _aspects_of_type(workunits, SiblingsClass)
        ]
        warehouse_urn = next(u for u in sibling_targets if "snowflake" in u)
        # In dev with schema mode: star__dev schema, not star
        assert "star__dev" in warehouse_urn
        assert "dim_developer" in warehouse_urn


class TestModelFiltering:
    def test_denied_model_emits_no_workunits_and_is_not_scanned(self):
        source = _make_source({"model_name_pattern": {"deny": ["star\\.raw_.*"]}})
        raw_model = _make_mock_model("star.raw_source")

        workunits = _run_project(source, {"star.raw_source": raw_model}, {})

        assert workunits == []
        assert source.report.models_scanned == 0

    def test_model_kind_filter_accepts_valid_kinds(self):
        source = _make_source(
            {"model_kind_filter": ["FULL", "INCREMENTAL_BY_TIME_RANGE"]}
        )
        assert source.config.model_kind_filter == [
            "FULL",
            "INCREMENTAL_BY_TIME_RANGE",
        ]

    def test_model_kind_filter_rejects_unknown_kind(self):
        # A typo would otherwise silently ingest nothing; the validator fails fast.
        with pytest.raises(ValueError, match="unknown model kind"):
            SqlmeshSourceConfig.model_validate(
                {
                    "project_path": "/p",
                    "target_platform": "snowflake",
                    "model_kind_filter": ["FULL", "INCREMENTAL"],
                }
            )


class TestTagAndOwnerExtraction:
    def test_tags_get_configured_prefix(self):
        source = _make_source()  # default tag_prefix = "sqlmesh:"
        model = _make_mock_model(tags=["pii", "gold"])
        assert source._get_tags(model) == [
            str(TagUrn("sqlmesh:pii")),
            str(TagUrn("sqlmesh:gold")),
        ]

    def test_empty_tag_prefix_uses_tags_as_is(self):
        source = _make_source({"tag_prefix": ""})
        assert source._get_tags(_make_mock_model(tags=["pii"])) == [str(TagUrn("pii"))]

    def test_no_tags_returns_empty(self):
        source = _make_source()
        assert source._get_tags(_make_mock_model(tags=[])) == []

    def test_owner_none_when_absent(self):
        source = _make_source()
        assert source._get_owner_urn(_make_mock_model(owner=None)) is None

    def test_owner_used_as_is_without_pattern(self):
        source = _make_source()
        assert source._get_owner_urn(_make_mock_model(owner="jdoe")) == make_user_urn(
            "jdoe"
        )

    def test_owner_extraction_pattern_uses_capture_group(self):
        # (.*)@.* strips the email domain, matching the config field's documented example.
        source = _make_source({"owner_extraction_pattern": r"(.*)@.*"})
        assert source._get_owner_urn(
            _make_mock_model(owner="jane@corp.com")
        ) == make_user_urn("jane")


class TestStaleFingerprintDetection:
    def _custom_props(self, source: SqlmeshSource, snapshot: MagicMock) -> dict:
        model = _make_mock_model()
        workunits = _run_project(
            source, {"star.dim_developer": model}, {"star.dim_developer": snapshot}
        )
        props_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), DatasetPropertiesClass)
        ]
        assert len(props_aspects) == 1
        return dict(props_aspects[0].customProperties or {})

    def test_stale_fingerprint_flagged_when_snapshot_old(self):
        snapshot = _make_mock_snapshot()
        snapshot.updated_ts = 1_000  # epoch millis: 1970 — ancient
        source = _make_source({"detect_stale_fingerprints": True})
        props = self._custom_props(source, snapshot)
        assert props.get("sqlmesh.fingerprint_stale") == "true"

    def test_fresh_fingerprint_not_flagged(self):
        snapshot = _make_mock_snapshot()
        snapshot.updated_ts = int(time.time() * 1000)
        source = _make_source({"detect_stale_fingerprints": True})
        props = self._custom_props(source, snapshot)
        assert "sqlmesh.fingerprint_stale" not in props

    def test_stale_fingerprint_not_flagged_when_disabled(self):
        snapshot = _make_mock_snapshot()
        snapshot.updated_ts = 1_000
        source = _make_source({})  # detect_stale_fingerprints defaults to False
        props = self._custom_props(source, snapshot)
        assert "sqlmesh.fingerprint_stale" not in props

    def test_missing_snapshot_timestamp_treated_as_unknown_not_stale(self):
        snapshot = _make_mock_snapshot()
        snapshot.updated_ts = 0
        source = _make_source({"detect_stale_fingerprints": True})
        props = self._custom_props(source, snapshot)
        assert "sqlmesh.fingerprint_stale" not in props


class TestConfigValidation:
    def test_target_platform_sqlmesh_rejected(self):
        # Guards the common misconfiguration of pointing the warehouse platform
        # back at sqlmesh itself, which would break sibling URN stitching.
        with pytest.raises(ValueError, match="cannot be 'sqlmesh'"):
            SqlmeshSourceConfig.model_validate(
                {"project_path": "/p", "target_platform": "sqlmesh"}
            )

    def test_gateway_override_target_platform_sqlmesh_rejected(self):
        # Same guard must apply per-gateway: a gateway override of "sqlmesh"
        # would silently corrupt that gateway's sibling URNs.
        with pytest.raises(ValueError, match="cannot be 'sqlmesh'"):
            SqlmeshSourceConfig.model_validate(
                {
                    "project_path": "/p",
                    "gateway_overrides": {"gw": {"target_platform": "sqlmesh"}},
                }
            )


# ---------------------------------------------------------------------------
# Tobiko Cloud token config + state-store fallback
#
# These tests cover the no-creds path described by enterprise Tobiko Cloud
# patches: an EnterpriseConfig project whose RemoteCloudSchedulerConfig would
# normally crash Context init when there's no Tobiko Cloud token. We can't
# install the real tobikodata package (it's gated behind a cloud account), so
# the shim is exercised against a fake module tree wired into sys.modules.
# ---------------------------------------------------------------------------


def _install_fake_tobikodata(
    monkeypatch: pytest.MonkeyPatch, error_message: str
) -> Any:
    """Insert a tobikodata.sqlmesh_enterprise.config.scheduler stand-in into
    sys.modules whose RemoteCloudSchedulerConfig raises ConfigError on every
    state-sync call. The shim's contract is independent of tobikodata's real
    internals — it only needs the class to exist and to raise."""
    # Skip when sqlmesh's import chain is broken in this venv (e.g. an
    # sqlglot/sqlmesh version mismatch). The shim's contract is the same in
    # CI where the deps line up.
    pytest.importorskip("sqlmesh.utils.errors")
    from sqlmesh.utils.errors import ConfigError

    class RemoteCloudSchedulerConfig:
        def create_state_sync(self, context):
            raise ConfigError(error_message)

        def state_sync_fingerprint(self, context):
            raise ConfigError(error_message)

    scheduler_mod = types.ModuleType("tobikodata.sqlmesh_enterprise.config.scheduler")
    scheduler_mod.RemoteCloudSchedulerConfig = RemoteCloudSchedulerConfig  # type: ignore[attr-defined]
    for name, mod in [
        ("tobikodata", types.ModuleType("tobikodata")),
        (
            "tobikodata.sqlmesh_enterprise",
            types.ModuleType("tobikodata.sqlmesh_enterprise"),
        ),
        (
            "tobikodata.sqlmesh_enterprise.config",
            types.ModuleType("tobikodata.sqlmesh_enterprise.config"),
        ),
        ("tobikodata.sqlmesh_enterprise.config.scheduler", scheduler_mod),
    ]:
        monkeypatch.setitem(sys.modules, name, mod)
    return RemoteCloudSchedulerConfig


class TestTobikoCloudConfig:
    def test_token_and_file_both_set_is_rejected(self, tmp_path):
        token_file = tmp_path / "tok"
        token_file.write_text("x")
        with pytest.raises(ValueError, match="at most one"):
            SqlmeshSourceConfig.model_validate(
                {
                    "project_path": "/p",
                    "gateway": "gw",
                    "tobiko_cloud_token": "v",
                    "tobiko_cloud_token_file": str(token_file),
                }
            )

    def test_token_without_gateway_is_rejected(self):
        with pytest.raises(ValueError, match="gateway is required"):
            SqlmeshSourceConfig.model_validate(
                {"project_path": "/p", "tobiko_cloud_token": "v"}
            )

    def test_resolve_inline_token(self):
        cfg = SqlmeshSourceConfig.model_validate(
            {"project_path": "/p", "gateway": "gw", "tobiko_cloud_token": "value"}
        )
        assert cfg.resolve_tobiko_cloud_token() == "value"

    def test_resolve_no_token_returns_none(self):
        cfg = SqlmeshSourceConfig.model_validate({"project_path": "/p"})
        assert cfg.resolve_tobiko_cloud_token() is None

    def test_resolve_file_token_caches_then_picks_up_rotation(self, tmp_path):
        """Mirrors the k8s projected secret rotation pattern: the file is
        re-read only after the TTL cache is invalidated."""

        cache = _get_tobiko_token_file_cache()
        assert cache is not None  # cachetools is installed in the test env
        cache.clear()
        token_file = tmp_path / "tok"
        token_file.write_text("first\n")

        cfg = SqlmeshSourceConfig.model_validate(
            {
                "project_path": "/p",
                "gateway": "gw",
                "tobiko_cloud_token_file": str(token_file),
            }
        )
        assert cfg.resolve_tobiko_cloud_token() == "first"

        # Simulating a secret rotation: file content changes, cache hasn't expired.
        token_file.write_text("second\n")
        assert cfg.resolve_tobiko_cloud_token() == "first"

        # After TTL expiry (simulated by clearing the cache), the next resolve
        # observes the new content.
        cache = _get_tobiko_token_file_cache()
        assert cache is not None
        cache.clear()
        assert _read_tobiko_cloud_token_file(str(token_file)) == "second"

    def test_read_token_file_without_cachetools_falls_back_uncached(self, tmp_path):
        # When cachetools isn't installed the getter returns None; the read must
        # still work (uncached) rather than raising NameError on cachetools.
        token_file = tmp_path / "tok"
        token_file.write_text("tok-value\n")
        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_config._get_tobiko_token_file_cache",
            return_value=None,
        ):
            assert _read_tobiko_cloud_token_file(str(token_file)) == "tok-value"


class TestTobikoCloudStateFallback:
    """Contract tests for _install_tobiko_local_state_fallback_shim.

    We can't install the real tobikodata package, so we exercise the shim
    against a fake module tree. The shim's contract is precise: catch one
    specific ConfigError message and substitute an in-memory DuckDB state
    sync. Everything else surfaces.
    """

    def test_specific_no_creds_error_falls_back_to_local_state(self, monkeypatch):
        """The user's primary requirement: with no token configured and a
        project folder, an EnterpriseConfig project that would normally fail
        on RemoteCloudSchedulerConfig.create_state_sync can still init.
        """

        scheduler_cls = _install_fake_tobikodata(
            monkeypatch, "Cloud scheduler requires a cloud state connection"
        )
        _install_tobiko_local_state_fallback_shim()

        context = MagicMock()
        context.gateway = "gw"
        context.config.get_state_schema.return_value = "sqlmesh_state"
        context.cache_dir = pathlib.Path("/tmp/state-cache")  # sqlmesh joins via `/`
        context.console = MagicMock()

        result = scheduler_cls().create_state_sync(context)
        assert result is not None

    def test_unrelated_config_errors_propagate(self, monkeypatch):
        """If a token IS configured (or some other failure occurs), we want
        the real error — never silently swallow."""

        scheduler_cls = _install_fake_tobikodata(
            monkeypatch, "Some entirely different problem"
        )
        from sqlmesh.utils.errors import (
            ConfigError,  # safe: helper above skipped if sqlmesh broken
        )

        _install_tobiko_local_state_fallback_shim()

        with pytest.raises(ConfigError, match="entirely different"):
            scheduler_cls().create_state_sync(MagicMock())

    def test_fingerprint_also_falls_back(self, monkeypatch):

        scheduler_cls = _install_fake_tobikodata(
            monkeypatch, "Cloud scheduler requires a cloud state connection"
        )
        _install_tobiko_local_state_fallback_shim()

        # state_sync_fingerprint is called alongside create_state_sync; both
        # need the same fallback or Context init still blows up.
        fingerprint = scheduler_cls().state_sync_fingerprint(MagicMock())
        assert fingerprint

    def test_shim_is_idempotent(self, monkeypatch):

        scheduler_cls = _install_fake_tobikodata(
            monkeypatch, "Cloud scheduler requires a cloud state connection"
        )
        _install_tobiko_local_state_fallback_shim()
        wrapped_once = scheduler_cls.create_state_sync
        _install_tobiko_local_state_fallback_shim()
        assert scheduler_cls.create_state_sync is wrapped_once

    def test_noop_when_tobikodata_not_installed(self, monkeypatch):
        for key in list(sys.modules):
            if key.startswith("tobikodata"):
                monkeypatch.delitem(sys.modules, key, raising=False)

        _install_tobiko_local_state_fallback_shim()  # must not raise


@pytest.fixture
def _enterprise_compat_patches_isolated(monkeypatch):
    """Save/restore the global state mutated by the enterprise compat patches
    so tests don't pollute one another or the rest of the suite."""
    pytest.importorskip("sqlmesh.core.config.loader")
    import sqlmesh.core.config.loader as loader_mod
    from sqlmesh.core.config.connection import SnowflakeConnectionConfig

    # Save current state
    saved_convert = loader_mod.convert_config_type
    saved_app_field = SnowflakeConnectionConfig.model_fields["application"]
    saved_app_annotation = saved_app_field.annotation
    saved_convert_sentinel = getattr(
        loader_mod.convert_config_type, _TOBIKO_CONVERT_PATCH_SENTINEL, False
    )
    saved_snowflake_sentinel = getattr(
        SnowflakeConnectionConfig, _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL, False
    )

    yield monkeypatch

    # Restore
    loader_mod.convert_config_type = saved_convert
    saved_app_field.annotation = saved_app_annotation
    SnowflakeConnectionConfig.model_rebuild(force=True)
    if not saved_convert_sentinel and hasattr(
        loader_mod.convert_config_type, _TOBIKO_CONVERT_PATCH_SENTINEL
    ):
        delattr(loader_mod.convert_config_type, _TOBIKO_CONVERT_PATCH_SENTINEL)
    if not saved_snowflake_sentinel and hasattr(
        SnowflakeConnectionConfig, _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL
    ):
        delattr(SnowflakeConnectionConfig, _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL)


class TestEnterpriseConfigCompatPatches:
    """Contract tests for _install_enterprise_config_compat_patches.

    Patches 1 and 2 from enterprise Tobiko Cloud projects:
    - Patch 1: relax SnowflakeConnectionConfig.application Literal so the
      enterprise value "Tobiko_TobikoCloud" validates.
    - Patch 2: convert_config_type short-circuits on isinstance so an
      EnterpriseConfig subclass isn't re-instantiated as plain Config and
      stripped of its enterprise-only fields.
    """

    @staticmethod
    def _stub_tobikodata(monkeypatch):
        """Both patches gate on `import tobikodata` succeeding. We can't
        install the real package, so stub it into sys.modules."""
        monkeypatch.setitem(sys.modules, "tobikodata", types.ModuleType("tobikodata"))

    def test_patch1_relaxes_snowflake_application_literal(
        self, _enterprise_compat_patches_isolated
    ):
        from sqlmesh.core.config.connection import SnowflakeConnectionConfig

        self._stub_tobikodata(_enterprise_compat_patches_isolated)

        _install_enterprise_config_compat_patches()

        # After the patch, the application field is no longer a strict Literal —
        # the enterprise value "Tobiko_TobikoCloud" would validate alongside
        # the OSS default. We assert on the annotation rather than constructing
        # the model (Snowflake engine library may not be installed in CI).
        field = SnowflakeConnectionConfig.model_fields["application"]
        assert field.annotation is str

    def test_patch2_convert_config_type_returns_subclass_unchanged(
        self, _enterprise_compat_patches_isolated
    ):
        import sqlmesh.core.config.loader as loader_mod
        from sqlmesh.core.config import Config

        self._stub_tobikodata(_enterprise_compat_patches_isolated)

        _install_enterprise_config_compat_patches()

        class FakeEnterpriseConfig(Config):  # subclass, mirrors EnterpriseConfig
            pass

        instance = FakeEnterpriseConfig()
        # The OSS loader's strict check would treat this as needing
        # conversion; the patched function returns the subclass instance as-is.
        assert loader_mod.convert_config_type(instance, Config) is instance

    def test_patches_are_noop_when_tobikodata_absent(
        self, _enterprise_compat_patches_isolated
    ):
        monkeypatch = _enterprise_compat_patches_isolated
        for key in list(sys.modules):
            if key.startswith("tobikodata"):
                monkeypatch.delitem(sys.modules, key, raising=False)

        _install_enterprise_config_compat_patches()  # must not raise

    def test_patches_are_idempotent(self, _enterprise_compat_patches_isolated):
        import sqlmesh.core.config.loader as loader_mod

        self._stub_tobikodata(_enterprise_compat_patches_isolated)

        _install_enterprise_config_compat_patches()
        wrapped_once = loader_mod.convert_config_type
        _install_enterprise_config_compat_patches()
        assert loader_mod.convert_config_type is wrapped_once


@pytest.fixture
def _process_pool_patch_isolated():
    """Save/restore the patch sentinel and the SQLMesh factory attributes the
    patch swaps, so installing it in a test doesn't leak into the rest of the suite."""
    pytest.importorskip("sqlmesh.core.loader")
    import sqlmesh.core.loader as loader_mod
    import sqlmesh.core.model.cache as cache_mod

    import datahub.ingestion.source.sqlmesh.compat as compat_mod

    saved_flag = compat_mod._process_pool_patched
    saved_loader = loader_mod.create_process_pool_executor
    saved_cache = cache_mod.create_process_pool_executor
    compat_mod._process_pool_patched = False
    try:
        yield compat_mod, loader_mod, cache_mod
    finally:
        compat_mod._process_pool_patched = saved_flag
        loader_mod.create_process_pool_executor = saved_loader
        cache_mod.create_process_pool_executor = saved_cache


class TestProcessPoolPatch:
    """Contract tests for _install_process_pool_patch (the fork-deadlock guard)."""

    def test_redirects_loader_and_cache_factories(self, _process_pool_patch_isolated):
        compat_mod, loader_mod, cache_mod = _process_pool_patch_isolated
        from sqlmesh.utils.process import SynchronousPoolExecutor

        compat_mod._install_process_pool_patch()

        assert compat_mod._process_pool_patched is True
        # Both call sites that captured the factory by name now build a synchronous
        # in-process pool instead of forking a worker.
        assert isinstance(
            loader_mod.create_process_pool_executor(), SynchronousPoolExecutor
        )
        assert isinstance(
            cache_mod.create_process_pool_executor(), SynchronousPoolExecutor
        )

    def test_is_idempotent(self, _process_pool_patch_isolated):
        compat_mod, loader_mod, _ = _process_pool_patch_isolated

        compat_mod._install_process_pool_patch()
        first = loader_mod.create_process_pool_executor
        compat_mod._install_process_pool_patch()

        # Second call is a no-op: the same replacement is left in place, not re-wrapped.
        assert loader_mod.create_process_pool_executor is first

    def test_import_error_leaves_unpatched_without_raising(
        self, _process_pool_patch_isolated, monkeypatch
    ):
        compat_mod, _, _ = _process_pool_patch_isolated
        # Simulate the private SynchronousPoolExecutor symbol moving in a future
        # sqlmesh: the install must log and carry on rather than raise, and must
        # not publish the sentinel (so a half-applied state is never claimed done).
        monkeypatch.setitem(
            sys.modules,
            "sqlmesh.utils.process",
            types.ModuleType("sqlmesh.utils.process"),
        )

        compat_mod._install_process_pool_patch()

        assert compat_mod._process_pool_patched is False


class TestScopedTobikoCloudEnv:
    """The credential injection channel is sqlmesh's documented
    SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__* env-var override (the same
    one tcloud uses in tcloud/installer.py). URL/TYPE are injected whenever a
    cloud URL is configured; TOKEN is injected only for a static token, so
    SSO users (no token) still get pointed at the cloud state store and fall
    back to ~/.tcloud/auth.yaml. Values are saved/restored around the block.
    """

    def test_noop_when_neither_url_nor_token(self):

        before = dict(os.environ)
        with _scoped_tobiko_cloud_env(token=None, gateway="gw", url=None):
            assert dict(os.environ) == before

    def test_noop_when_gateway_missing(self):

        # Without a gateway there is no gateway-scoped env-var prefix to set,
        # so even a configured url/token must be a no-op.
        before = dict(os.environ)
        with _scoped_tobiko_cloud_env(
            token="secret", gateway=None, url="https://example"
        ):
            assert dict(os.environ) == before

    def test_sso_path_injects_url_without_token(self):

        # SSO users configure a url but no static token. TYPE + URL must be
        # set (so tobikodata targets the cloud state store), while TOKEN must
        # be left untouched so tobikodata falls back to tcloud SSO auth.
        snapshot = dict(os.environ)
        token_key = "SQLMESH__GATEWAYS__GW__STATE_CONNECTION__TOKEN"
        assert token_key not in os.environ
        with _scoped_tobiko_cloud_env(token=None, gateway="gw", url="https://example"):
            assert (
                os.environ["SQLMESH__GATEWAYS__GW__STATE_CONNECTION__TYPE"] == "cloud"
            )
            assert (
                os.environ["SQLMESH__GATEWAYS__GW__STATE_CONNECTION__URL"]
                == "https://example"
            )
            assert token_key not in os.environ
            assert os.environ["SQLMESH__DEFAULT_GATEWAY"] == "gw"
        assert dict(os.environ) == snapshot

    def test_sets_and_restores_env_vars(self):

        snapshot = dict(os.environ)
        with _scoped_tobiko_cloud_env(
            token="secret", gateway="gw", url="https://example"
        ):
            assert (
                os.environ["SQLMESH__GATEWAYS__GW__STATE_CONNECTION__TYPE"] == "cloud"
            )
            assert (
                os.environ["SQLMESH__GATEWAYS__GW__STATE_CONNECTION__TOKEN"] == "secret"
            )
            assert (
                os.environ["SQLMESH__GATEWAYS__GW__STATE_CONNECTION__URL"]
                == "https://example"
            )
            assert os.environ["SQLMESH__DEFAULT_GATEWAY"] == "gw"
        assert dict(os.environ) == snapshot

    def test_restores_env_on_exception(self):

        snapshot = dict(os.environ)
        with (
            pytest.raises(RuntimeError, match="boom"),
            _scoped_tobiko_cloud_env(token="t", gateway="gw", url=None),
        ):
            raise RuntimeError("boom")
        assert dict(os.environ) == snapshot

    def test_ambient_token_cleared_during_sso_and_restored(self):

        # A stale TOKEN already in the environment must not silently authenticate
        # an SSO (token=None) run: it is removed inside the block and restored on
        # exit.
        token_key = "SQLMESH__GATEWAYS__GW__STATE_CONNECTION__TOKEN"
        os.environ[token_key] = "stale-ambient-token"
        try:
            with _scoped_tobiko_cloud_env(
                token=None, gateway="gw", url="https://example"
            ):
                assert token_key not in os.environ
            assert os.environ[token_key] == "stale-ambient-token"
        finally:
            os.environ.pop(token_key, None)


class TestBuildCustomProperties:
    """Covers time_column / partitioned_by / grains extraction in
    _build_custom_properties, including the fallback and warning paths."""

    @staticmethod
    def _model(**attrs: object) -> MagicMock:
        # Only the fields _build_custom_properties reads matter; the rest are
        # stable no-ops so the extracted property values are unambiguous.
        model = MagicMock()
        model.name = "myschema.orders"
        model.kind = None
        model.cron = None
        model.start = None
        model.time_column = None
        model.partitioned_by = []
        model.grains = []
        model.audits = []
        for key, value in attrs.items():
            setattr(model, key, value)
        return model

    def test_time_column_with_column_attr(self) -> None:
        source = _make_source()
        eff = _effective(source)

        class _TimeCol:
            column = "event_ts"

        props = source._build_custom_properties(
            "myschema.orders", None, eff, self._model(time_column=_TimeCol())
        )
        assert props[PROP_TIME_COLUMN] == "event_ts"

    def test_time_column_without_column_attr_falls_back_to_str(self) -> None:
        # A shape with no ``.column`` stringifies directly rather than being dropped.
        source = _make_source()
        eff = _effective(source)
        props = source._build_custom_properties(
            "myschema.orders", None, eff, self._model(time_column="ts_raw")
        )
        assert props[PROP_TIME_COLUMN] == "ts_raw"

    def test_partitioned_by_and_grains_named_items(self) -> None:
        source = _make_source()
        eff = _effective(source)

        class _Named:
            def __init__(self, name: str) -> None:
                self.name = name

        props = source._build_custom_properties(
            "myschema.orders",
            None,
            eff,
            self._model(
                partitioned_by=[_Named("ds"), _Named("region")],
                grains=[_Named("order_id")],
            ),
        )
        assert props[PROP_PARTITIONED_BY] == "ds,region"
        assert props[PROP_GRAIN] == "order_id"

    def test_malformed_partitioned_by_hits_warning_path_and_omits_property(
        self,
    ) -> None:
        source = _make_source()
        eff = _effective(source)

        class _RaisingName:
            @property
            def name(self) -> str:
                raise ValueError("unexpected shape")

        # The extraction fails soft: the property is omitted rather than
        # crashing the model's emission.
        props = source._build_custom_properties(
            "myschema.orders", None, eff, self._model(partitioned_by=[_RaisingName()])
        )
        assert PROP_PARTITIONED_BY not in props


class TestBaseDepImportability:
    """The ``[sqlmesh]`` extra (sqlmesh, cachetools, boto3, GitPython) is excluded
    from pyproject, so ``datahub check plugins`` and the CI plugin-import validation
    load the source class with *base* deps only. This guards against a new top-level
    import of an extra-only package silently breaking that (which would flip sqlmesh
    from a cleanly-disabled plugin into a hard CI import failure)."""

    def test_source_module_imports_without_optional_deps(self) -> None:
        code = textwrap.dedent(
            """
            import builtins
            _blocked = {"boto3", "botocore", "cachetools", "sqlmesh", "git", "gitdb"}
            _real_import = builtins.__import__

            def _guard(name, *args, **kwargs):
                if name.split(".")[0] in _blocked:
                    raise ModuleNotFoundError(name.split(".")[0])
                return _real_import(name, *args, **kwargs)

            builtins.__import__ = _guard
            import datahub.ingestion.source.sqlmesh.sqlmesh_source  # noqa: F401
            import datahub.ingestion.source.sqlmesh.project_location  # noqa: F401
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
