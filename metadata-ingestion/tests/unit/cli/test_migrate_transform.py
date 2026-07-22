"""Tests for the converter-driven generic migration (migrate transform)."""

from unittest.mock import MagicMock, patch

import datahub.cli.migration_utils as migration_utils
from datahub.cli.migrate import _migrate_single_entity
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    StatusClass,
    SystemMetadataClass,
)

MIXED = "urn:li:dataset:(urn:li:dataPlatform:snowflake,MyDb.MySchema.MyTable,PROD)"
LOWER = "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.myschema.mytable,PROD)"


class TestLowercaseConverter:
    def test_convert_and_should_convert(self):
        c = migration_utils.LowercaseConverter()
        assert c.name == "lowercase"
        assert c.convert_urn(MIXED) == LOWER
        assert c.should_convert(MIXED) is True
        assert c.should_convert(LOWER) is False


class TestMigrateWithoutInstance:
    """A non-instance migration (e.g. lowercase) must not emit dataPlatformInstance."""

    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    def test_no_platform_instance_emitted_when_platform_is_none(
        self, _mock_rels: MagicMock
    ) -> None:
        graph = MagicMock()
        graph.exists.return_value = False
        emitted = []
        graph.emit_mcp.side_effect = lambda mcp: emitted.append(mcp)

        cloned = MetadataChangeProposalWrapper(
            entityUrn=LOWER, aspect=StatusClass(removed=False)
        )
        with patch(
            "datahub.cli.migrate.migration_utils.clone_aspect", return_value=[cloned]
        ):
            _migrate_single_entity(
                src_entity_urn=MIXED,
                make_new_urn=lambda _: LOWER,
                platform=None,
                target_instance=None,
                dry_run=False,
                hard=False,
                keep=True,
                run_id="t",
                graph=graph,
                on_conflict=None,
                system_metadata=SystemMetadataClass(runId="t"),
                migration_report=MagicMock(),
            )

        assert emitted, "expected the cloned aspect to be emitted"
        assert not any(isinstance(m.aspect, DataPlatformInstanceClass) for m in emitted)


class TestRunTransform:
    @patch("datahub.cli.migrate._migrate_entities")
    def test_filters_by_should_convert_and_drops_instance(
        self, mock_migrate: MagicMock
    ) -> None:
        from datahub.cli.migrate import run_transform
        from datahub.cli.migration_utils import LowercaseConverter

        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [MIXED, LOWER]  # LOWER already lower
        run_transform(graph, LowercaseConverter(), "snowflake")

        _, kwargs = mock_migrate.call_args
        assert kwargs["urns_to_migrate"] == [MIXED]
        assert kwargs["platform"] is None
        assert kwargs["target_instance"] is None

    @patch("datahub.cli.migrate._migrate_entities")
    def test_noop_when_nothing_to_convert(self, mock_migrate: MagicMock) -> None:
        from datahub.cli.migrate import run_transform
        from datahub.cli.migration_utils import LowercaseConverter

        graph = MagicMock()
        graph.get_urns_by_filter.return_value = [LOWER]
        run_transform(graph, LowercaseConverter(), "snowflake")
        mock_migrate.assert_not_called()
