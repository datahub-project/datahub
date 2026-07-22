"""Tests for registry-driven aspect discovery and generic URN rewriting in migration."""

from typing import Dict
from unittest.mock import MagicMock, patch

from avrogen.dict_wrapper import DictWrapper

import datahub.cli.migration_utils as migration_utils
from datahub.cli.migrate import _migrate_single_entity
from datahub.cli.migration_utils import ConflictStrategy, merge_entity
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SubTypesClass,
    SystemMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.urn_iter import transform_urns

OLD_DS = (
    "urn:li:dataset:(urn:li:dataPlatform:mysql,old_inst.my_db.my_schema.events,PROD)"
)
NEW_DS = (
    "urn:li:dataset:(urn:li:dataPlatform:mysql,new_inst.my_db.my_schema.events,PROD)"
)
OTHER_DS = (
    "urn:li:dataset:(urn:li:dataPlatform:mysql,old_inst.my_db.my_schema.other,PROD)"
)


def _sf(dataset_urn: str, col: str) -> str:
    return f"urn:li:schemaField:({dataset_urn},{col})"


class TestGetMigratableAspectNames:
    def test_includes_user_authored_aspects_from_registry(self):
        result = migration_utils.get_migratable_aspect_names("dataset")
        assert "upstreamLineage" in result
        assert "structuredProperties" in result
        assert "forms" in result
        # siblings is a relationship aspect (array[Urn], @Relationship), not a
        # computed rollup — it must be migrated, not skipped.
        assert "siblings" in result

    def test_excludes_timeseries_aspects(self):
        result = migration_utils.get_migratable_aspect_names("dataset")
        assert "datasetProfile" not in result
        assert "datasetUsageStatistics" not in result

    def test_excludes_system_managed_aspects(self):
        result = migration_utils.get_migratable_aspect_names("dataset")
        assert "dataPlatformInstance" not in result
        assert "browsePaths" not in result
        assert "browsePathsV2" not in result

    def test_excludes_derived_summary_aspects(self):
        # incidentsSummary is a platform-computed rollup, not user intent;
        # cloning it verbatim would carry a stale summary to the new URN.
        result = migration_utils.get_migratable_aspect_names("dataset")
        assert "incidentsSummary" not in result

    def test_unknown_entity_type_returns_empty(self):
        assert migration_utils.get_migratable_aspect_names("nonsense") == []


class TestMakeSelfUrnRewriter:
    def test_rewrites_exact_entity_urn(self):
        rewrite = migration_utils.make_self_urn_rewriter(OLD_DS, NEW_DS)
        assert rewrite(OLD_DS) == NEW_DS

    def test_rewrites_embedded_schema_field_urn(self):
        rewrite = migration_utils.make_self_urn_rewriter(OLD_DS, NEW_DS)
        assert rewrite(_sf(OLD_DS, "col_a")) == _sf(NEW_DS, "col_a")

    def test_leaves_other_dataset_urn_untouched(self):
        rewrite = migration_utils.make_self_urn_rewriter(OLD_DS, NEW_DS)
        assert rewrite(OTHER_DS) == OTHER_DS

    def test_leaves_unrelated_schema_field_untouched(self):
        rewrite = migration_utils.make_self_urn_rewriter(OLD_DS, NEW_DS)
        assert rewrite(_sf(OTHER_DS, "col_a")) == _sf(OTHER_DS, "col_a")


class TestFineGrainedLineageRewrite:
    """The headline correctness fix: column-level lineage URNs must be rewritten."""

    def test_transform_urns_rewrites_fine_grained_lineage(self):
        # A downstream's upstreamLineage pointing at OLD_DS both table- and
        # column-level (the column-level edge is what the old hand-written
        # modifier missed).
        aspect = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=OLD_DS, type=DatasetLineageTypeClass.TRANSFORMED)
            ],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[_sf(OLD_DS, "col_a")],
                    downstreams=[_sf(OTHER_DS, "col_a")],
                )
            ],
        )

        transform_urns(aspect, migration_utils.make_self_urn_rewriter(OLD_DS, NEW_DS))

        assert aspect.upstreams[0].dataset == NEW_DS
        fgl = aspect.fineGrainedLineages
        assert fgl is not None
        assert fgl[0].upstreams == [_sf(NEW_DS, "col_a")]
        # Downstream references a different dataset — must be left untouched.
        assert fgl[0].downstreams == [_sf(OTHER_DS, "col_a")]


class TestClonePathRewritesSelfReferences:
    """The clone path must rewrite the migrated entity's own aspect URNs."""

    @patch(
        "datahub.cli.migrate.migration_utils.get_incoming_relationships",
        return_value=[],
    )
    def test_clone_path_rewrites_own_fine_grained_lineage(
        self, _mock_rels: MagicMock
    ) -> None:
        # clone_aspect yields the entity's own upstreamLineage, whose CLL
        # downstreams reference the entity being migrated (OLD_DS).
        cloned = MetadataChangeProposalWrapper(
            entityUrn=NEW_DS,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=OTHER_DS, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[_sf(OTHER_DS, "col_a")],
                        downstreams=[_sf(OLD_DS, "col_a")],
                    )
                ],
            ),
        )
        graph = MagicMock()
        graph.exists.return_value = False
        emitted = []
        graph.emit_mcp.side_effect = lambda mcp: emitted.append(mcp)

        with patch(
            "datahub.cli.migrate.migration_utils.clone_aspect",
            return_value=[cloned],
        ):
            _migrate_single_entity(
                src_entity_urn=OLD_DS,
                make_new_urn=lambda _: NEW_DS,
                platform="mysql",
                target_instance="new_inst",
                dry_run=False,
                hard=False,
                keep=True,
                run_id="test-run",
                graph=graph,
                on_conflict=None,
                system_metadata=SystemMetadataClass(runId="test-run"),
                migration_report=MagicMock(),
            )

        lineage = [
            m.aspect for m in emitted if isinstance(m.aspect, UpstreamLineageClass)
        ]
        assert lineage, "expected an upstreamLineage MCP to be emitted"
        fgl = lineage[0].fineGrainedLineages
        assert fgl is not None
        # The entity's own column-level downstream must point at the new URN.
        assert fgl[0].downstreams == [_sf(NEW_DS, "col_a")]


class TestMergeEntityDefaultBucket:
    """Registry aspects not in a known classification set must still migrate."""

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_unclassified_aspect_is_migrated_not_dropped(
        self, mock_get: MagicMock
    ) -> None:
        # subTypes is a real dataset aspect that is in none of the additive /
        # mixed / non-additive / always-overwrite buckets.
        def side_effect(session, server, urn, aspects=None, typed=False):
            if urn == OLD_DS:
                return {"subTypes": SubTypesClass(typeNames=["View"])}
            return {}  # target has nothing

        mock_get.side_effect = side_effect
        graph = MagicMock()

        merge_entity(OLD_DS, NEW_DS, ConflictStrategy.PATCH, graph, dry_run=False)

        emitted = [c.args[0].aspect for c in graph.emit_mcp.call_args_list]
        assert any(isinstance(a, SubTypesClass) for a in emitted), (
            "subTypes was silently dropped during merge"
        )


class TestProcessContainerRelationships:
    """Referencing entities must have their container URN rewritten (via transform_urns)."""

    OLD_CONTAINER = "urn:li:container:old-guid"
    NEW_CONTAINER = "urn:li:container:new-guid"
    CHILD_DS = (
        "urn:li:dataset:(urn:li:dataPlatform:mysql,inst.my_db.my_schema.child,PROD)"
    )

    @patch("datahub.cli.migrate.get_default_graph")
    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    @patch("datahub.cli.migrate.migration_utils.get_incoming_relationships")
    def test_child_container_reference_is_rewritten(
        self,
        mock_rels: MagicMock,
        mock_get_aspects: MagicMock,
        mock_graph: MagicMock,
    ) -> None:
        from datahub.cli.migrate import _process_container_relationships
        from datahub.ingestion.graph.openapi import RelatedEntity
        from datahub.metadata.schema_classes import ContainerClass

        mock_rels.return_value = [
            RelatedEntity(urn=self.CHILD_DS, relationship_type="IsPartOf")
        ]
        mock_get_aspects.return_value = {
            "container": ContainerClass(container=self.OLD_CONTAINER)
        }
        emitter = MagicMock()
        emitted = []
        emitter.emit_mcp.side_effect = lambda mcp: emitted.append(mcp)

        _process_container_relationships(
            container_id_map={self.OLD_CONTAINER: self.NEW_CONTAINER},
            dry_run=False,
            src_urn=self.OLD_CONTAINER,
            dst_urn=self.NEW_CONTAINER,
            migration_report=MagicMock(),
            rest_emitter=emitter,
        )

        containers = [m.aspect for m in emitted if isinstance(m.aspect, ContainerClass)]
        assert containers, "expected a container MCP to be emitted"
        assert containers[0].container == self.NEW_CONTAINER


class TestRewriteIncomingReferences:
    """Rewrite the migrated URN across ALL aspects of a referencing entity."""

    DOWNSTREAM = (
        "urn:li:dataset:(urn:li:dataPlatform:mysql,inst.my_db.my_schema.down,PROD)"
    )

    @patch("datahub.cli.migration_utils.cli_utils.get_aspects_for_entity")
    def test_rewrites_only_aspects_that_reference_migrated_urn(
        self, mock_get: MagicMock
    ) -> None:
        from datahub.metadata.schema_classes import StatusClass

        mock_get.return_value = {
            "upstreamLineage": UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=OLD_DS, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ]
            ),
            "status": StatusClass(removed=False),  # no reference — must be skipped
        }
        graph = MagicMock()

        changed = migration_utils.rewrite_incoming_references(
            graph,
            self.DOWNSTREAM,
            migration_utils.make_self_urn_rewriter(OLD_DS, NEW_DS),
        )

        # Only the aspect that referenced the migrated URN comes back.
        aspects = [m.aspect for m in changed]
        assert len(aspects) == 1
        assert isinstance(aspects[0], UpstreamLineageClass)
        assert aspects[0].upstreams[0].dataset == NEW_DS
        assert changed[0].entityUrn == self.DOWNSTREAM


class TestMergeCarriesFineGrainedLineage:
    """Merge mode must carry column-level lineage, not just table-level."""

    @patch("datahub.cli.migration_utils.DatasetPatchBuilder")
    def test_merge_additive_patches_fine_grained_lineage(
        self, mock_pb_cls: MagicMock
    ) -> None:
        pb = mock_pb_cls.return_value
        pb.build.return_value = []
        src: Dict[str, DictWrapper] = {
            "upstreamLineage": UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=OTHER_DS, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[_sf(OTHER_DS, "a")],
                        downstreams=[_sf(NEW_DS, "b")],
                    )
                ],
            )
        }
        migration_utils.merge_additive_aspects(src, NEW_DS, MagicMock(), dry_run=True)

        assert pb.add_upstream_lineage.call_count == 1
        assert pb.add_fine_grained_lineage.call_count == 1
