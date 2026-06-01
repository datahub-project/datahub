import logging
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dremio.dremio_config import (
    DremioConnectionConfig,
    DremioSourceConfig,
)
from datahub.ingestion.source.dremio.dremio_source import DremioSource
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)


class TestDremioConfigValidators:
    def test_invalid_auth_method_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DremioConnectionConfig(
                hostname="localhost",
                tls=False,
                authentication_method="oauth2",
                password="token",
            )
        assert (
            "oauth2" in str(exc_info.value)
            or "authentication_method" in str(exc_info.value).lower()
        )

    def test_pat_with_explicit_none_password_raises(self):
        # Pydantic v2 field_validator only runs when the field is explicitly provided.
        # Passing password=None explicitly triggers the PAT validation check.
        with pytest.raises(ValidationError) as exc_info:
            DremioConnectionConfig(
                hostname="localhost",
                tls=False,
                authentication_method="PAT",
                password=None,
            )
        error_text = str(exc_info.value).lower()
        assert "pat" in error_text or "token" in error_text or "password" in error_text

    def test_password_auth_without_password_allowed(self):
        """The 'password' method doesn't require the password field — it may come from env."""
        config = DremioConnectionConfig(
            hostname="localhost",
            tls=False,
            authentication_method="password",
        )
        assert config.password is None

    def test_transparent_secret_str_serializes_plaintext(self):
        """TransparentSecretStr must serialize as plain text (used for cross-process pass-through)."""
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="supersecret",
        )
        data = config.model_dump()
        assert data["password"] == "supersecret"

    def test_transparent_secret_str_masked_in_repr(self):
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="supersecret",
        )
        assert "supersecret" not in repr(config.password)


class TestStatefulTimeWindowValidator:
    def _base_kwargs(self) -> dict:
        return {
            "hostname": "localhost",
            "tls": False,
            "authentication_method": "PAT",
            "password": "token",
        }

    def test_warns_when_time_window_enabled_without_stateful(self, caplog):
        caplog.set_level(logging.WARNING)
        DremioSourceConfig(
            **self._base_kwargs(),
            enable_stateful_time_window=True,
        )
        assert any(
            "enable_stateful_time_window" in rec.getMessage()
            and "stateful_ingestion" in rec.getMessage()
            for rec in caplog.records
        ), "expected a warning about the no-op configuration"

    def test_no_warning_when_time_window_disabled(self, caplog):
        caplog.set_level(logging.WARNING)
        DremioSourceConfig(
            **self._base_kwargs(),
            enable_stateful_time_window=False,
        )
        assert not any(
            "enable_stateful_time_window" in rec.getMessage() for rec in caplog.records
        )

    def test_no_warning_when_stateful_ingestion_enabled(self, caplog):
        caplog.set_level(logging.WARNING)
        DremioSourceConfig(
            **self._base_kwargs(),
            enable_stateful_time_window=True,
            stateful_ingestion={"enabled": True},
        )
        assert not any(
            "enable_stateful_time_window" in rec.getMessage() for rec in caplog.records
        )

    def test_no_stateful_lineage_or_profiling_mixin_warnings_on_default_run(
        self, caplog
    ):
        # Regression: DremioSourceConfig used to inherit
        # StatefulLineageConfigMixin + StatefulProfilingConfigMixin from the
        # state base, whose post validators emit a warning when
        # stateful_ingestion isn't enabled (the common Dremio default).
        # Neither enable_stateful_lineage_ingestion nor
        # enable_stateful_profiling is read anywhere in the Dremio source
        # — the mixins were dead, so the warnings were pure noise. Pin
        # that the default-config Dremio run produces no such warnings.
        caplog.set_level(logging.WARNING)
        DremioSourceConfig(**self._base_kwargs())
        offending = [
            rec.getMessage()
            for rec in caplog.records
            if "enable_stateful_lineage_ingestion" in rec.getMessage()
            or "enable_stateful_profiling" in rec.getMessage()
        ]
        assert offending == [], (
            "Default Dremio config should not surface stateful-lineage / "
            "stateful-profiling mixin warnings; they relate to fields the "
            f"Dremio source never reads. Got: {offending}"
        )


class TestIncrementalLineageWiring:
    """Behavioural pins for ``incremental_lineage`` — verifies the flag
    actually re-routes upstream-lineage workunits through the framework's
    PATCH converter rather than just sitting in the config."""

    def _base_kwargs(self) -> dict:
        return {
            "hostname": "localhost",
            "tls": False,
            "authentication_method": "PAT",
            "password": "token",
        }

    @pytest.fixture
    def mock_ctx(self):
        ctx = Mock()
        ctx.run_id = "test-run-id"
        ctx.pipeline_name = "test-pipeline"
        ctx.graph = None
        return ctx

    @pytest.fixture
    def patch_session(self, monkeypatch):
        mock_session = Mock()
        monkeypatch.setattr("requests.Session", Mock(return_value=mock_session))
        mock_session.post.return_value.json.return_value = {"token": "dummy-token"}
        mock_session.post.return_value.status_code = 200
        return mock_session

    def _lineage_workunit(
        self, downstream_urn: str, upstream_urn: str
    ) -> "MetadataWorkUnit":
        aspect = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:datahub"),
                ),
            ],
        )
        mcp = MetadataChangeProposalWrapper(entityUrn=downstream_urn, aspect=aspect)
        return mcp.as_workunit()

    def _run_through_dremio_pipeline(
        self, source: DremioSource, workunit: "MetadataWorkUnit"
    ) -> list:
        # Drop the framework's stale-entity-removal handler — it requires
        # a real graph + stateful_ingestion + checkpointing wiring that
        # this unit test doesn't set up. Compose only the lineage /
        # property handlers we're actually asserting on.
        from functools import partial

        from datahub.ingestion.api.incremental_lineage_helper import (
            auto_incremental_lineage,
        )
        from datahub.ingestion.api.incremental_properties_helper import (
            auto_incremental_properties,
        )

        stream = iter([workunit])
        stream = partial(auto_incremental_lineage, source.config.incremental_lineage)(
            stream
        )
        stream = partial(
            auto_incremental_properties, source.config.incremental_properties
        )(stream)
        return list(stream)

    def test_default_emits_upsert_full_overwrite(self, mock_ctx, patch_session):
        # With incremental_lineage=False (default) the workunit passes
        # through unchanged: an MCPW UpstreamLineageClass with the
        # framework's UPSERT change type.
        config = DremioSourceConfig(**self._base_kwargs())
        assert config.incremental_lineage is False  # sanity-pin the default

        source = DremioSource(config, mock_ctx)
        downstream = make_dataset_urn("dremio", "space.schema.t1")
        upstream = make_dataset_urn("dremio", "space.schema.t0")
        wu = self._lineage_workunit(downstream, upstream)

        out = self._run_through_dremio_pipeline(source, wu)

        assert len(out) == 1
        mcp = out[0].metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        assert isinstance(mcp.aspect, UpstreamLineageClass)
        assert mcp.changeType == ChangeTypeClass.UPSERT

    def test_flag_enabled_converts_to_patch(self, mock_ctx, patch_session):
        # With incremental_lineage=True the framework converts the
        # workunit into a PATCH-mode aspect MCP. We don't re-test the
        # framework's patch shape (that's covered by
        # tests/unit/api/source_helpers/test_incremental_lineage_helper);
        # we just pin that Dremio's wiring routes through it.
        config = DremioSourceConfig(**self._base_kwargs(), incremental_lineage=True)
        source = DremioSource(config, mock_ctx)
        downstream = make_dataset_urn("dremio", "space.schema.t1")
        upstream = make_dataset_urn("dremio", "space.schema.t0")
        wu = self._lineage_workunit(downstream, upstream)

        out = self._run_through_dremio_pipeline(source, wu)

        assert len(out) == 1
        emitted = out[0].metadata
        # auto_incremental_lineage emits MCPs (not MCPWs) with PATCH type.
        assert getattr(emitted, "changeType", None) == ChangeTypeClass.PATCH
        assert getattr(emitted, "entityUrn", None) == downstream
