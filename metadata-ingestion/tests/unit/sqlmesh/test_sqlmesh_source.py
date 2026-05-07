from unittest.mock import MagicMock, patch

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
        "projects": [{"project_path": "/fake/project"}],
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
        return list(source._ingest_project(source.config.projects[0]))


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


class TestMultiProject:
    def test_multiple_projects_ingested(self):
        config = SqlmeshSourceConfig.model_validate(
            {
                "projects": [
                    {"project_path": "/project/a", "gateway": "snowflake"},
                    {"project_path": "/project/b", "gateway": "bigquery"},
                ],
                "target_platform": "snowflake",
                "env": "PROD",
            }
        )
        source = SqlmeshSource(config, PipelineContext(run_id="test"))

        model_a = _make_mock_model("schema_a.model_a")
        model_b = _make_mock_model("schema_b.model_b")

        call_count = 0

        def fake_context(**kwargs):
            nonlocal call_count
            mock_ctx = MagicMock()
            if call_count == 0:
                mock_ctx.models = {"schema_a.model_a": model_a}
            else:
                mock_ctx.models = {"schema_b.model_b": model_b}
            mock_ctx.snapshots = {}
            mock_ctx.connection_config.type_ = "snowflake"
            call_count += 1
            return mock_ctx

        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            side_effect=fake_context,
        ):
            list(source.get_workunits_internal())

        assert source.report.models_scanned == 2


def _effective(source: SqlmeshSource) -> object:
    """Return the resolved effective config for the first project."""
    return source._resolve_project_config(source.config.projects[0])


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

    def test_per_project_platform_override(self):
        config = SqlmeshSourceConfig.model_validate(
            {
                "projects": [
                    {
                        "project_path": "/proj",
                        "target_platform": "bigquery",
                        "default_catalog": "my-gcp-project",
                    }
                ],
                "target_platform": "snowflake",
                "env": "PROD",
            }
        )
        source = SqlmeshSource(config, PipelineContext(run_id="test"))
        eff = source._resolve_project_config(config.projects[0])
        assert eff.target_platform == "bigquery"
        assert eff.default_catalog == "my-gcp-project"

    def test_global_platform_used_when_no_project_override(self):
        config = SqlmeshSourceConfig.model_validate(
            {
                "projects": [{"project_path": "/proj"}],
                "target_platform": "snowflake",
                "default_catalog": "analytics",
                "env": "PROD",
            }
        )
        source = SqlmeshSource(config, PipelineContext(run_id="test"))
        eff = source._resolve_project_config(config.projects[0])
        assert eff.target_platform == "snowflake"
        assert eff.default_catalog == "analytics"

    def test_sqlmesh_platform_instance_resolved(self):
        config = SqlmeshSourceConfig.model_validate(
            {
                "projects": [
                    {"project_path": "/proj", "sqlmesh_platform_instance": "project_a"}
                ],
                "target_platform": "snowflake",
                "env": "PROD",
            }
        )
        source = SqlmeshSource(config, PipelineContext(run_id="test"))
        eff = source._resolve_project_config(config.projects[0])
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
            list(source._ingest_project(source.config.projects[0]))

        assert source.report.models_scanned == 2
        assert any(m == "star.bad_model" for m in source.report.models_failed)

    def test_context_init_failure_is_fatal_and_yields_nothing(self):
        source = _make_source()
        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            side_effect=Exception("connection refused"),
        ):
            workunits = list(source._ingest_project(source.config.projects[0]))

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
                "projects": [
                    {"project_path": "/proj", "gateway": None, "environment": "dev"}
                ],
                "target_platform": "snowflake",
                "env": "DEV",
            }
        )
        source2 = SqlmeshSource(config, PipelineContext(run_id="test"))

        with patch(
            "datahub.ingestion.source.sqlmesh.sqlmesh_source.SqlmeshContext",
            return_value=mock_ctx,
        ):
            workunits = list(source2._ingest_project(config.projects[0]))

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
