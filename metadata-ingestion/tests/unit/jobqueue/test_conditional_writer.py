from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import SystemMetadataClass
from datahub.sdk.patterns._shared.conditional_writer import (
    CASResult,
    ConditionalWriter,
    VersionedAspect,
    _extract_version,
    _is_precondition_failure,
)


def _make_graph() -> MagicMock:
    return MagicMock(spec=["get_entity_as_mcps", "get_aspect", "emit"])


def _make_mcpw(
    aspect_name: str, version: str, entity_urn: str = "urn:li:dataset:test"
) -> MetadataChangeProposalWrapper:
    aspect = MagicMock()
    aspect.get_aspect_name.return_value = aspect_name
    aspect.ASPECT_NAME = aspect_name
    sm = SystemMetadataClass(version=version)
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=aspect,
        systemMetadata=sm,
    )


# ---------------------------------------------------------------------------
# _extract_version
# ---------------------------------------------------------------------------


class TestExtractVersion:
    def test_with_version_field(self) -> None:
        sm = SystemMetadataClass(version="42")
        assert _extract_version(sm) == "42"

    def test_without_version_field(self) -> None:
        sm = SystemMetadataClass()
        assert _extract_version(sm) == "-1"

    def test_with_none_version(self) -> None:
        sm = SystemMetadataClass(version=None)
        assert _extract_version(sm) == "-1"

    def test_integer_version_converted_to_string(self) -> None:
        sm = SystemMetadataClass(version=7)  # type: ignore[arg-type]
        assert _extract_version(sm) == "7"


# ---------------------------------------------------------------------------
# _is_precondition_failure
# ---------------------------------------------------------------------------


class TestIsPreconditionFailure:
    def test_412_status_in_info(self) -> None:
        exc = OperationalError("fail", {"status": 412})
        assert _is_precondition_failure(exc) is True

    def test_precondition_in_message(self) -> None:
        exc = OperationalError("Precondition Failed: expected version 1, actual 2")
        assert _is_precondition_failure(exc) is True

    def test_expected_version_in_message(self) -> None:
        exc = OperationalError("Expected version -1, actual version 3")
        assert _is_precondition_failure(exc) is True

    def test_unrelated_error(self) -> None:
        exc = OperationalError("Connection refused", {"status": 500})
        assert _is_precondition_failure(exc) is False

    def test_no_info_dict(self) -> None:
        exc = OperationalError("some error")
        assert _is_precondition_failure(exc) is False

    def test_none_message(self) -> None:
        exc = OperationalError.__new__(OperationalError)
        exc.message = None  # type: ignore[assignment]
        exc.info = {}
        assert _is_precondition_failure(exc) is False


# ---------------------------------------------------------------------------
# CASResult
# ---------------------------------------------------------------------------


class TestCASResult:
    def test_success_result(self) -> None:
        result = CASResult(success=True, new_version="5")
        assert result.success is True
        assert result.new_version == "5"
        assert result.reason is None

    def test_failure_result(self) -> None:
        result = CASResult(success=False, reason="conflict")
        assert result.success is False
        assert result.new_version is None
        assert result.reason == "conflict"


# ---------------------------------------------------------------------------
# ConditionalWriter.read_version
# ---------------------------------------------------------------------------


class TestConditionalWriterReadVersion:
    def test_returns_version_from_system_metadata(self) -> None:
        graph = _make_graph()
        mcpw = _make_mcpw("myAspect", "7")
        graph.get_entity_as_mcps.return_value = [mcpw]

        writer = ConditionalWriter(graph)
        version = writer.read_version("urn:li:dataset:test", "myAspect")

        assert version == "7"
        graph.get_entity_as_mcps.assert_called_once_with(
            "urn:li:dataset:test", aspects=["myAspect"]
        )

    def test_returns_minus_one_when_aspect_not_found(self) -> None:
        graph = _make_graph()
        graph.get_entity_as_mcps.return_value = []

        writer = ConditionalWriter(graph)
        version = writer.read_version("urn:li:dataset:test", "myAspect")

        assert version == "-1"

    def test_returns_minus_one_when_system_metadata_is_none(self) -> None:
        graph = _make_graph()
        aspect = MagicMock()
        aspect.get_aspect_name.return_value = "myAspect"
        aspect.ASPECT_NAME = "myAspect"
        mcpw = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:test",
            aspect=aspect,
            systemMetadata=None,
        )
        graph.get_entity_as_mcps.return_value = [mcpw]

        writer = ConditionalWriter(graph)
        version = writer.read_version("urn:li:dataset:test", "myAspect")

        assert version == "-1"

    def test_returns_minus_one_when_aspect_value_is_none(self) -> None:
        """If the MCP has aspectName matching but aspect is None, return -1."""
        graph = _make_graph()
        mcpw = MagicMock()
        mcpw.aspect = None
        mcpw.aspectName = "myAspect"
        mcpw.systemMetadata = SystemMetadataClass()
        graph.get_entity_as_mcps.return_value = [mcpw]

        writer = ConditionalWriter(graph)
        version = writer.read_version("urn:li:dataset:test", "myAspect")

        assert version == "-1"

    def test_skips_non_matching_aspect_names(self) -> None:
        """Only considers MCPs where aspectName matches the requested aspect."""
        graph = _make_graph()
        mcpw_other = _make_mcpw("otherAspect", "99")
        mcpw_match = _make_mcpw("myAspect", "3")
        graph.get_entity_as_mcps.return_value = [mcpw_other, mcpw_match]

        writer = ConditionalWriter(graph)
        version = writer.read_version("urn:li:dataset:test", "myAspect")

        assert version == "3"

    def test_propagates_exception_from_graph(self) -> None:
        graph = _make_graph()
        graph.get_entity_as_mcps.side_effect = RuntimeError("network error")

        writer = ConditionalWriter(graph)
        with pytest.raises(RuntimeError, match="network error"):
            writer.read_version("urn:li:dataset:test", "myAspect")


# ---------------------------------------------------------------------------
# ConditionalWriter.read_aspect
# ---------------------------------------------------------------------------


class TestConditionalWriterReadAspect:
    def test_delegates_to_graph_get_aspect(self) -> None:
        graph = _make_graph()
        fake_aspect = MagicMock()
        graph.get_aspect.return_value = fake_aspect
        aspect_class = MagicMock()

        writer = ConditionalWriter(graph)
        result = writer.read_aspect("urn:li:dataset:test", "myAspect", aspect_class)

        assert result is fake_aspect
        graph.get_aspect.assert_called_once_with(
            entity_urn="urn:li:dataset:test",
            aspect_type=aspect_class,
        )

    def test_returns_none_when_aspect_absent(self) -> None:
        graph = _make_graph()
        graph.get_aspect.return_value = None
        aspect_class = MagicMock()

        writer = ConditionalWriter(graph)
        result = writer.read_aspect("urn:li:dataset:test", "myAspect", aspect_class)

        assert result is None


# ---------------------------------------------------------------------------
# ConditionalWriter.write_if_version
# ---------------------------------------------------------------------------


class TestConditionalWriterWriteIfVersion:
    def test_success_returns_new_version(self) -> None:
        graph = _make_graph()
        graph.emit.return_value = None

        mcpw_after = _make_mcpw("myAspect", "2")
        graph.get_entity_as_mcps.return_value = [mcpw_after]

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        result = writer.write_if_version(
            "urn:li:dataset:test", "myAspect", aspect_value, "-1"
        )

        assert result.success is True
        assert result.new_version == "2"
        graph.emit.assert_called_once()

        emitted_mcpw = graph.emit.call_args[0][0]
        assert isinstance(emitted_mcpw, MetadataChangeProposalWrapper)
        assert emitted_mcpw.headers == {"If-Version-Match": "-1"}

    def test_conflict_on_412_returns_failure(self) -> None:
        graph = _make_graph()
        graph.emit.side_effect = OperationalError(
            "Expected version -1, actual version 3", {"status": 412}
        )

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        result = writer.write_if_version(
            "urn:li:dataset:test", "myAspect", aspect_value, "-1"
        )

        assert result.success is False
        assert result.reason == "conflict"

    def test_conflict_on_precondition_message(self) -> None:
        """Conflict detected via message text when status is not 412."""
        graph = _make_graph()
        graph.emit.side_effect = OperationalError(
            "Precondition Failed: version mismatch"
        )

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        result = writer.write_if_version(
            "urn:li:dataset:test", "myAspect", aspect_value, "5"
        )

        assert result.success is False
        assert result.reason == "conflict"

    def test_non_precondition_error_propagates(self) -> None:
        graph = _make_graph()
        graph.emit.side_effect = OperationalError("Connection refused", {"status": 500})

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        with pytest.raises(OperationalError, match="Connection refused"):
            writer.write_if_version(
                "urn:li:dataset:test", "myAspect", aspect_value, "-1"
            )

    def test_emitted_mcpw_contains_correct_entity_and_aspect(self) -> None:
        graph = _make_graph()
        graph.emit.return_value = None
        mcpw_after = _make_mcpw("myAspect", "10")
        graph.get_entity_as_mcps.return_value = [mcpw_after]

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        writer.write_if_version("urn:li:dataset:custom", "myAspect", aspect_value, "9")

        emitted_mcpw = graph.emit.call_args[0][0]
        assert emitted_mcpw.entityUrn == "urn:li:dataset:custom"
        assert emitted_mcpw.aspect is aspect_value
        assert emitted_mcpw.headers == {"If-Version-Match": "9"}

    def test_track_new_version_false_skips_read(self) -> None:
        graph = _make_graph()
        graph.emit.return_value = None

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        result = writer.write_if_version(
            "urn:li:dataset:test",
            "myAspect",
            aspect_value,
            "-1",
            track_new_version=False,
        )

        assert result.success is True
        assert result.new_version is None
        graph.emit.assert_called_once()
        graph.get_entity_as_mcps.assert_not_called()

    def test_track_new_version_true_reads_version(self) -> None:
        graph = _make_graph()
        graph.emit.return_value = None
        mcpw_after = _make_mcpw("myAspect", "5")
        graph.get_entity_as_mcps.return_value = [mcpw_after]

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        result = writer.write_if_version(
            "urn:li:dataset:test",
            "myAspect",
            aspect_value,
            "-1",
            track_new_version=True,
        )

        assert result.success is True
        assert result.new_version == "5"
        graph.get_entity_as_mcps.assert_called_once()

    def test_track_new_version_ignored_on_conflict(self) -> None:
        graph = _make_graph()
        graph.emit.side_effect = OperationalError(
            "Precondition Failed", {"status": 412}
        )

        writer = ConditionalWriter(graph)
        aspect_value = MagicMock()
        aspect_value.get_aspect_name.return_value = "myAspect"
        aspect_value.ASPECT_NAME = "myAspect"

        result = writer.write_if_version(
            "urn:li:dataset:test",
            "myAspect",
            aspect_value,
            "-1",
            track_new_version=False,
        )

        assert result.success is False
        assert result.reason == "conflict"


# ---------------------------------------------------------------------------
# ConditionalWriter.read_versioned_aspect
# ---------------------------------------------------------------------------


class TestReadVersionedAspect:
    def test_returns_version_and_aspect(self) -> None:
        """When aspect matches aspect_class, both version and aspect are returned."""
        from datahub.metadata.schema_classes import DatasetPropertiesClass

        graph = _make_graph()
        aspect = DatasetPropertiesClass()
        sm = SystemMetadataClass(version="42")
        mcpw = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:test",
            aspect=aspect,
            systemMetadata=sm,
        )
        graph.get_entity_as_mcps.return_value = [mcpw]

        writer = ConditionalWriter(graph)
        result = writer.read_versioned_aspect(
            "urn:li:dataset:test", "datasetProperties", DatasetPropertiesClass
        )

        assert isinstance(result, VersionedAspect)
        assert result.version == "42"
        assert isinstance(result.aspect, DatasetPropertiesClass)

    def test_returns_unversioned_when_absent(self) -> None:
        graph = _make_graph()
        graph.get_entity_as_mcps.return_value = []

        writer = ConditionalWriter(graph)
        result = writer.read_versioned_aspect(
            "urn:li:dataset:test", "myAspect", MagicMock
        )

        assert result.version == "-1"
        assert result.aspect is None

    def test_aspect_none_on_type_mismatch(self) -> None:
        """When the aspect doesn't match the requested class, aspect is None."""

        class WrongType:
            pass

        graph = _make_graph()
        # Use a mock MCP to control aspectName independently of aspect type
        mcpw = MagicMock()
        mcpw.aspect = MagicMock()  # not an instance of WrongType
        mcpw.aspectName = "myAspect"
        mcpw.systemMetadata = SystemMetadataClass(version="10")
        graph.get_entity_as_mcps.return_value = [mcpw]

        writer = ConditionalWriter(graph)
        result = writer.read_versioned_aspect(
            "urn:li:dataset:test",
            "myAspect",
            WrongType,  # type: ignore[arg-type]
        )

        assert result.version == "10"
        assert result.aspect is None

    def test_skips_non_matching_aspect_names(self) -> None:
        graph = _make_graph()
        mcpw = _make_mcpw("otherAspect", "99")
        graph.get_entity_as_mcps.return_value = [mcpw]

        writer = ConditionalWriter(graph)
        result = writer.read_versioned_aspect(
            "urn:li:dataset:test", "myAspect", MagicMock
        )

        assert result.version == "-1"
        assert result.aspect is None
