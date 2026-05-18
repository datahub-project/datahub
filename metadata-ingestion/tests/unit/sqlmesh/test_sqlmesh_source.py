import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sqlmesh.sqlmesh_config import SqlmeshSourceConfig
from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
    SQLMESH_PLATFORM,
    SqlmeshSource,
)
from datahub.metadata.schema_classes import SiblingsClass, UpstreamLineageClass

WAREHOUSE_PLATFORM = "snowflake"


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
    kind.__str__ = lambda s: kind_name
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
    physical_table.__str__ = lambda s: physical_name
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


class TestSiblingEmission:
    def test_siblings_always_emitted_for_each_model(self):
        """Siblings link sqlmesh entity to warehouse view — no snapshot needed."""
        source = _make_source()
        model = _make_mock_model()

        # No snapshots — siblings still emitted (physical table is just a custom property)
        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_aspects = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        assert len(sibling_aspects) == 2

        primaries = [a for a in sibling_aspects if a.primary]
        secondaries = [a for a in sibling_aspects if not a.primary]
        assert len(primaries) == 1
        assert len(secondaries) == 1

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

    def test_warehouse_entity_is_secondary(self):
        """Warehouse view sibling is secondary."""
        source = _make_source()
        model = _make_mock_model()

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_aspects = [
            (wu.metadata.entityUrn, wu.metadata.aspect)
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]

        secondary_urn = next(urn for urn, a in sibling_aspects if not a.primary)
        assert WAREHOUSE_PLATFORM in secondary_urn
        assert "dim_developer" in secondary_urn
        # Warehouse view has the same name as the model — not the physical fingerprint table
        assert "sqlmesh__" not in secondary_urn

    def test_warehouse_can_be_primary(self):
        source = _make_source({"sqlmesh_is_primary_sibling": False})
        model = _make_mock_model()

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_aspects = [
            (wu.metadata.entityUrn, wu.metadata.aspect)
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]

        primary_urn = next(urn for urn, a in sibling_aspects if a.primary)
        assert WAREHOUSE_PLATFORM in primary_urn
        assert SQLMESH_PLATFORM not in primary_urn

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
        from datahub.metadata.schema_classes import DatasetPropertiesClass

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
    """Regression: sqlmesh audit-derived assertions attach to the warehouse
    URN (matching dbt's convention) so they surface on the same Data Quality
    tab as UI-created Monitor assertions and so a follow-on warehouse
    connector ingestion stitches them naturally.
    """

    def test_assertion_dataset_matches_warehouse_sibling_urn(self):
        from datahub.metadata.schema_classes import (
            AssertionInfoClass,
            SiblingsClass,
        )

        source = _make_source()
        model = _make_mock_model()
        # Unknown audit name → dataset-level assertion (no column resolution
        # needed), exercising the URN-attachment path without sqlglot mocks.
        model.audits = [("some_custom_audit", {})]

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        # Pull the warehouse URN from the sibling aspect emitted on the
        # sqlmesh entity (the sqlmesh URN's Siblings.siblings[0] is the
        # warehouse URN). This is the URN our assertion MUST match.
        sqlmesh_sibling_aspect = next(
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
            and SQLMESH_PLATFORM in wu.metadata.entityUrn
        )
        warehouse_urn = sqlmesh_sibling_aspect.siblings[0]
        assert WAREHOUSE_PLATFORM in warehouse_urn

        assertion_infos = [
            wu.metadata.aspect
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), AssertionInfoClass)
        ]
        assert len(assertion_infos) >= 1
        info = assertion_infos[0]
        assert info.datasetAssertion is not None
        # The key invariant — assertion.dataset must match the warehouse
        # URN we emit as a sibling, so the two never drift.
        assert info.datasetAssertion.dataset == warehouse_urn

    def test_embedded_model_assertion_falls_back_to_sqlmesh_urn(self):
        """Embedded sqlmesh models don't materialize to the warehouse, so
        there's no warehouse URN to target. Assertions stay on the sqlmesh
        URN — the only place they have somewhere to live."""
        from datahub.metadata.schema_classes import AssertionInfoClass

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
        # Embedded → no warehouse URN exists; assertion targets sqlmesh URN.
        assert SQLMESH_PLATFORM in assertion_infos[0].datasetAssertion.dataset


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
        from datahub.metadata.schema_classes import (
            FineGrainedLineageClass,
            FineGrainedLineageDownstreamTypeClass,
            FineGrainedLineageUpstreamTypeClass,
        )

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

    def test_include_database_name_false_strips_catalog(self):
        """include_database_name=False drops catalog from warehouse sibling URN."""
        source = _make_source(
            {"default_catalog": "analytics", "include_database_name": False}
        )
        model = _make_mock_model("star.dim_developer")

        workunits = _run_project(source, {"star.dim_developer": model}, {})

        sibling_urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        warehouse_urn = next(u for u in sibling_urns if WAREHOUSE_PLATFORM in u)
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

        sibling_urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        assert any("bigquery" in u for u in sibling_urns)

    def test_explicit_target_platform_overrides_auto_detection(self):
        source = _make_source({"target_platform": "redshift"})
        model = _make_mock_model()

        # connection_type says databricks, but explicit config says redshift
        workunits = _run_project(
            source, {"star.dim_developer": model}, {}, connection_type="databricks"
        )

        sibling_urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        assert any("redshift" in u for u in sibling_urns)
        assert not any("databricks" in u for u in sibling_urns)


def _effective(source: SqlmeshSource) -> object:
    """Return the resolved effective config."""
    from datahub.ingestion.source.sqlmesh.sqlmesh_source import _EffectiveProjectConfig

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

    def test_snapshot_physical_name_fallback(self):
        source = _make_source()
        eff = _effective(source)

        snapshot = MagicMock()
        physical = MagicMock()
        physical.__str__ = lambda s: "db.sqlmesh__star.star__model__123"

        def table_name_side_effect(**kwargs):
            if "ignore_mapping" in kwargs:
                raise TypeError("unexpected keyword argument")
            return physical

        snapshot.table_name = MagicMock(side_effect=table_name_side_effect)

        result = source._snapshot_physical_name(snapshot, eff)
        assert result == "db.sqlmesh__star.star__model__123"

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


class TestSchemaEmission:
    def test_no_schema_when_disabled(self):
        source = _make_source({"include_schema": False})
        model = _make_mock_model()
        workunits = _run_project(source, {"star.dim_developer": model}, {})
        from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata

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
        from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata

        schema_aspects = [
            wu
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SchemaMetadata)
        ]
        assert len(schema_aspects) == 0


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
    ):
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _EffectiveProjectConfig,
        )

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

        sibling_urns = [
            wu.metadata.entityUrn
            for wu in workunits
            if isinstance(getattr(wu.metadata, "aspect", None), SiblingsClass)
        ]
        warehouse_urn = next(u for u in sibling_urns if "snowflake" in u)
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


# ---------------------------------------------------------------------------
# Tobiko Cloud token config + state-store fallback
#
# These tests cover the no-creds path described by Gen Digital's customer
# patches: an EnterpriseConfig project whose RemoteCloudSchedulerConfig would
# normally crash Context init when there's no Tobiko Cloud token. We can't
# install the real tobikodata package (it's gated behind a cloud account), so
# the shim is exercised against a fake module tree wired into sys.modules.
# ---------------------------------------------------------------------------


def _install_fake_tobikodata(monkeypatch, error_message: str):
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
    scheduler_mod.RemoteCloudSchedulerConfig = RemoteCloudSchedulerConfig
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
        from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
            _read_tobiko_cloud_token_file,
            _tobiko_token_file_cache,
        )

        _tobiko_token_file_cache.clear()
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
        _tobiko_token_file_cache.clear()
        assert _read_tobiko_cloud_token_file(str(token_file)) == "second"


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
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_tobiko_local_state_fallback_shim,
        )

        scheduler_cls = _install_fake_tobikodata(
            monkeypatch, "Cloud scheduler requires a cloud state connection"
        )
        _install_tobiko_local_state_fallback_shim()

        import pathlib

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
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_tobiko_local_state_fallback_shim,
        )

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
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_tobiko_local_state_fallback_shim,
        )

        scheduler_cls = _install_fake_tobikodata(
            monkeypatch, "Cloud scheduler requires a cloud state connection"
        )
        _install_tobiko_local_state_fallback_shim()

        # state_sync_fingerprint is called alongside create_state_sync; both
        # need the same fallback or Context init still blows up.
        fingerprint = scheduler_cls().state_sync_fingerprint(MagicMock())
        assert fingerprint

    def test_shim_is_idempotent(self, monkeypatch):
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_tobiko_local_state_fallback_shim,
        )

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
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_tobiko_local_state_fallback_shim,
        )

        _install_tobiko_local_state_fallback_shim()  # must not raise


@pytest.fixture
def _enterprise_compat_patches_isolated(monkeypatch):
    """Save/restore the global state mutated by the enterprise compat patches
    so tests don't pollute one another or the rest of the suite."""
    pytest.importorskip("sqlmesh.core.config.loader")
    import sqlmesh.core.config.loader as loader_mod
    from sqlmesh.core.config.connection import SnowflakeConnectionConfig

    from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
        _TOBIKO_CONVERT_PATCH_SENTINEL,
        _TOBIKO_SNOWFLAKE_APP_PATCH_SENTINEL,
    )

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

    Patches 1 and 2 from Gen Digital's customer description:
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

        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_enterprise_config_compat_patches,
        )

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

        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_enterprise_config_compat_patches,
        )

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

        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_enterprise_config_compat_patches,
        )

        _install_enterprise_config_compat_patches()  # must not raise

    def test_patches_are_idempotent(self, _enterprise_compat_patches_isolated):
        import sqlmesh.core.config.loader as loader_mod

        self._stub_tobikodata(_enterprise_compat_patches_isolated)

        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _install_enterprise_config_compat_patches,
        )

        _install_enterprise_config_compat_patches()
        wrapped_once = loader_mod.convert_config_type
        _install_enterprise_config_compat_patches()
        assert loader_mod.convert_config_type is wrapped_once


class TestScopedTobikoCloudEnv:
    """The token injection channel is sqlmesh's documented
    SQLMESH__GATEWAYS__<gw>__STATE_CONNECTION__* env-var override (the same
    one tcloud uses in tcloud/installer.py). We narrow the exposure window
    to a single Context.__init__ by saving/restoring around the block.
    """

    def test_noop_when_token_is_none(self):
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _scoped_tobiko_cloud_env,
        )

        before = dict(os.environ)
        with _scoped_tobiko_cloud_env(token=None, gateway="gw", url=None):
            assert dict(os.environ) == before

    def test_sets_and_restores_env_vars(self):
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _scoped_tobiko_cloud_env,
        )

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
        from datahub.ingestion.source.sqlmesh.sqlmesh_source import (
            _scoped_tobiko_cloud_env,
        )

        snapshot = dict(os.environ)
        with (
            pytest.raises(RuntimeError, match="boom"),
            _scoped_tobiko_cloud_env(token="t", gateway="gw", url=None),
        ):
            raise RuntimeError("boom")
        assert dict(os.environ) == snapshot
