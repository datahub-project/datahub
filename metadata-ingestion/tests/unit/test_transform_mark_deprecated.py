"""Tests for the MarkDeprecated transformer."""

import time
from typing import List, Optional
from unittest import mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.mark_deprecated import MarkDeprecated
from datahub.metadata.schema_classes import DeprecationClass


def _make_mcp(
    entity_urn: str,
    aspect: Optional[DeprecationClass] = None,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect)


def _run_transformer(
    config: dict,
    entity_urn: str,
    existing_aspect: Optional[DeprecationClass] = None,
    graph: Optional[mock.MagicMock] = None,
) -> List[RecordEnvelope]:
    ctx = PipelineContext(run_id="test_mark_deprecated")
    if graph is not None:
        ctx.graph = graph
    transformer = MarkDeprecated.create(config, ctx)

    mcp = _make_mcp(entity_urn, existing_aspect)
    return list(
        transformer.transform(
            [RecordEnvelope(r, metadata={}) for r in [mcp, EndOfStream()]]
        )
    )


def _get_deprecation_aspect(
    output: List[RecordEnvelope], entity_urn: str
) -> Optional[DeprecationClass]:
    for envelope in output:
        if isinstance(envelope.record, MetadataChangeProposalWrapper):
            if envelope.record.entityUrn == entity_urn and isinstance(
                envelope.record.aspect, DeprecationClass
            ):
                return envelope.record.aspect
    return None


class TestMarkDeprecatedBasic:
    DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

    def test_marks_entity_deprecated(self) -> None:
        output = _run_transformer(
            {"deprecated": True, "note": "Replaced by new_table"},
            self.DATASET_URN,
            DeprecationClass(
                deprecated=False, note="", actor="urn:li:corpuser:someone"
            ),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is True
        assert aspect.note == "Replaced by new_table"
        assert aspect.actor == "urn:li:corpuser:datahub"

    def test_marks_entity_undeprecated(self) -> None:
        output = _run_transformer(
            {"deprecated": False, "note": "No longer deprecated"},
            self.DATASET_URN,
            DeprecationClass(deprecated=True, note="old", actor="urn:li:corpuser:x"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is False

    def test_creates_aspect_when_none_exists(self) -> None:
        """When no deprecation aspect exists, the transformer should create one."""
        output = _run_transformer(
            {"deprecated": True, "note": "Legacy"},
            self.DATASET_URN,
            None,
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is True
        assert aspect.note == "Legacy"

    def test_custom_actor(self) -> None:
        output = _run_transformer(
            {"deprecated": True, "actor": "urn:li:corpuser:admin"},
            self.DATASET_URN,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:old"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.actor == "urn:li:corpuser:admin"

    def test_replacement_urn(self) -> None:
        replacement = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.new_table,PROD)"
        )
        output = _run_transformer(
            {"deprecated": True, "replacement": replacement},
            self.DATASET_URN,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.replacement == replacement

    def test_decommission_time_defaults_to_now(self) -> None:
        """When not specified, decommission_time defaults to current time."""
        before = int(time.time() * 1000)
        output = _run_transformer(
            {"deprecated": True},
            self.DATASET_URN,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
        )
        after = int(time.time() * 1000)
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.decommissionTime is not None
        assert before <= aspect.decommissionTime <= after

    def test_decommission_time_explicit_override(self) -> None:
        output = _run_transformer(
            {"deprecated": True, "decommission_time": 1750000000000},
            self.DATASET_URN,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.decommissionTime == 1750000000000

    def test_overwrites_already_deprecated_entity(self) -> None:
        """OVERWRITE semantics: always stamps config values regardless of existing state."""
        output = _run_transformer(
            {"deprecated": True, "note": "New reason", "actor": "urn:li:corpuser:bot"},
            self.DATASET_URN,
            DeprecationClass(
                deprecated=True, note="Old reason", actor="urn:li:corpuser:human"
            ),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is True
        assert aspect.note == "New reason"
        assert aspect.actor == "urn:li:corpuser:bot"


class TestMarkDeprecatedUrnFilter:
    DATASET_A = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table_a,PROD)"
    DATASET_B = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table_b,PROD)"

    def test_only_affects_listed_urns(self) -> None:
        output = _run_transformer(
            {"deprecated": True, "note": "Gone", "urns": [self.DATASET_A]},
            self.DATASET_A,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_A)
        assert aspect is not None
        assert aspect.deprecated is True

    def test_skips_unlisted_urns(self) -> None:
        output = _run_transformer(
            {"deprecated": True, "note": "Gone", "urns": [self.DATASET_A]},
            self.DATASET_B,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_B)
        assert aspect is not None
        # Should remain unchanged
        assert aspect.deprecated is False

    def test_empty_urns_affects_all(self) -> None:
        output = _run_transformer(
            {"deprecated": True, "note": "All deprecated", "urns": []},
            self.DATASET_B,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_B)
        assert aspect is not None
        assert aspect.deprecated is True


class TestMarkDeprecatedPatchSemantics:
    """PATCH semantics: merge with server, preserving existing values."""

    DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"

    def test_patch_preserves_existing_note_and_actor(self) -> None:
        """When server already has a note and actor, PATCH keeps them."""
        graph = mock.MagicMock()
        graph.get_aspect.return_value = DeprecationClass(
            deprecated=True,
            note="Original note from human",
            actor="urn:li:corpuser:alice",
            decommissionTime=1700000000000,
        )

        output = _run_transformer(
            {
                "deprecated": True,
                "note": "Pipeline note",
                "actor": "urn:li:corpuser:bot",
                "semantics": "PATCH",
            },
            self.DATASET_URN,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
            graph=graph,
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is True
        # Server values preserved
        assert aspect.note == "Original note from human"
        assert aspect.actor == "urn:li:corpuser:alice"
        assert aspect.decommissionTime == 1700000000000

    def test_patch_fills_empty_fields_from_config(self) -> None:
        """When server has empty/None fields, PATCH fills them from config."""
        graph = mock.MagicMock()
        graph.get_aspect.return_value = DeprecationClass(
            deprecated=False,
            note="",
            actor="",
            decommissionTime=None,
        )

        output = _run_transformer(
            {
                "deprecated": True,
                "note": "Replaced by v2",
                "actor": "urn:li:corpuser:bot",
                "decommission_time": 1800000000000,
                "semantics": "PATCH",
            },
            self.DATASET_URN,
            DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
            graph=graph,
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is True
        assert aspect.note == "Replaced by v2"
        assert aspect.actor == "urn:li:corpuser:bot"
        assert aspect.decommissionTime == 1800000000000

    def test_patch_no_server_aspect_creates_new(self) -> None:
        """When server has no deprecation at all, PATCH creates from config."""
        graph = mock.MagicMock()
        graph.get_aspect.return_value = None

        output = _run_transformer(
            {
                "deprecated": True,
                "note": "First deprecation",
                "semantics": "PATCH",
            },
            self.DATASET_URN,
            None,
            graph=graph,
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is True
        assert aspect.note == "First deprecation"

    def test_patch_always_updates_deprecated_flag(self) -> None:
        """The deprecated boolean is always driven by config, even in PATCH mode."""
        graph = mock.MagicMock()
        graph.get_aspect.return_value = DeprecationClass(
            deprecated=True,
            note="Was deprecated",
            actor="urn:li:corpuser:alice",
        )

        output = _run_transformer(
            {
                "deprecated": False,
                "note": "Un-deprecating",
                "semantics": "PATCH",
            },
            self.DATASET_URN,
            DeprecationClass(deprecated=True, note="", actor="urn:li:corpuser:x"),
            graph=graph,
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        assert aspect.deprecated is False
        # Existing note preserved since server already had one
        assert aspect.note == "Was deprecated"

    def test_patch_sets_replacement_only_if_not_already_set(self) -> None:
        """PATCH should not overwrite an existing replacement URN."""
        existing_replacement = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.old_replacement,PROD)"
        )
        new_replacement = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.new_replacement,PROD)"
        )

        graph = mock.MagicMock()
        graph.get_aspect.return_value = DeprecationClass(
            deprecated=True,
            note="Already deprecated",
            actor="urn:li:corpuser:alice",
            replacement=existing_replacement,
        )

        output = _run_transformer(
            {
                "deprecated": True,
                "replacement": new_replacement,
                "semantics": "PATCH",
            },
            self.DATASET_URN,
            DeprecationClass(deprecated=True, note="", actor="urn:li:corpuser:x"),
            graph=graph,
        )
        aspect = _get_deprecation_aspect(output, self.DATASET_URN)
        assert aspect is not None
        # Existing replacement preserved
        assert aspect.replacement == existing_replacement


class TestMarkDeprecatedEntityTypes:
    def test_supports_all_entity_types(self) -> None:
        urns = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
            "urn:li:chart:(looker,chart1)",
            "urn:li:dashboard:(looker,dash1)",
            "urn:li:dataFlow:(airflow,flow1,PROD)",
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow1,PROD),job1)",
            "urn:li:container:my_container",
        ]
        for urn in urns:
            output = _run_transformer(
                {"deprecated": True},
                urn,
                DeprecationClass(deprecated=False, note="", actor="urn:li:corpuser:x"),
            )
            aspect = _get_deprecation_aspect(output, urn)
            assert aspect is not None, f"Failed for {urn}"
            assert aspect.deprecated is True
