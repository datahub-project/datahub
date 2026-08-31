import json
from typing import Any, cast

import pytest

from datahub.emitter.mcp_patch_builder import (
    UNIT_SEPARATOR,
    GenericJsonPatch,
    _Patch,
    parse_patch_path,
)
from datahub.metadata.schema_classes import GenericAspectClass

TAGS_ARRAY_PRIMARY_KEYS = {"tags": [f"attribution{UNIT_SEPARATOR}source", "tag"]}


def test_patch_to_obj():
    """Test _Patch serialization to dictionary."""
    op = _Patch(op="add", path=("tags", "test"), value={"tag": "urn:li:tag:test"})
    result = op.to_obj()
    assert result == {
        "op": "add",
        "path": "/tags/test",
        "value": {"tag": "urn:li:tag:test"},
    }

    op_no_value = _Patch(op="remove", path=("tags", "test"), value={})
    result = op_no_value.to_obj()
    assert result == {"op": "remove", "path": "/tags/test", "value": {}}


@pytest.mark.parametrize(
    "value,quoted",
    [
        ("test", "test"),
        ("test/path", "test~1path"),
        ("test~tilde", "test~0tilde"),
        ("test~1/path", "test~01~1path"),
    ],
)
def test_patch_quote_unquote_path_component(value, quoted):
    """Test _Patch quote/unquote path component methods."""
    assert _Patch.quote_path_component(value) == quoted
    assert _Patch.unquote_path_component(quoted) == value


@pytest.mark.parametrize(
    "value", ["simple", "with/slash", "with~tilde", "complex~1/path"]
)
def test_patch_quote_unquote_round_trip(value):
    """Round-trip: value -> quoted -> unquoted == value."""
    quoted = _Patch.quote_path_component(value)
    unquoted = _Patch.unquote_path_component(quoted)
    assert unquoted == value


@pytest.mark.parametrize(
    "path_str,expected",
    [
        ("/tags/test", ("tags", "test")),
        ("/tags/urn:li:tag:test", ("tags", "urn:li:tag:test")),
        ("/tags/test~1path", ("tags", "test/path")),
        ("/tags/test~0tilde", ("tags", "test~tilde")),
        (
            "/tags/urn:li:tag:test/attribution",
            ("tags", "urn:li:tag:test", "attribution"),
        ),
    ],
)
def test_parse_patch_path(path_str, expected):
    """Test parsing JSON Patch path strings to PatchPath tuples."""
    assert parse_patch_path(path_str) == expected


def test_parse_patch_path_invalid():
    """Invalid path (no leading slash) raises ValueError."""
    with pytest.raises(ValueError, match="must start with"):
        parse_patch_path("tags/test")


@pytest.mark.parametrize(
    "path_str",
    [
        "/tags//attribution",
        "/tags/",
        "//tags",
        "/a//b",
        "/a//b//c",
    ],
)
def test_parse_patch_path_empty_components(path_str: str) -> None:
    """Paths with empty components (e.g. // or trailing /) raise ValueError."""
    with pytest.raises(ValueError, match="empty component"):
        parse_patch_path(path_str)


@pytest.mark.parametrize(
    "force_generic_patch,expected_force", [(False, False), (True, True)]
)
def test_generic_json_patch_to_dict(force_generic_patch, expected_force):
    """Test GenericJsonPatch serialization."""
    patch_ops = [
        _Patch(op="add", path=("tags", "test1"), value={"tag": "urn:li:tag:test1"}),
        _Patch(op="add", path=("tags", "test2"), value={"tag": "urn:li:tag:test2"}),
    ]
    patch = GenericJsonPatch(
        array_primary_keys=TAGS_ARRAY_PRIMARY_KEYS,
        patch=patch_ops,
        force_generic_patch=force_generic_patch,
    )

    result = patch.to_dict()
    assert result["arrayPrimaryKeys"] == TAGS_ARRAY_PRIMARY_KEYS
    assert len(result["patch"]) == 2
    assert result["patch"][0]["op"] == "add"
    assert result["patch"][0]["path"] == "/tags/test1"
    assert result["patch"][1]["path"] == "/tags/test2"
    assert result["forceGenericPatch"] is expected_force


def test_generic_json_patch_to_generic_aspect():
    """Test GenericJsonPatch conversion to GenericAspectClass."""
    patch_ops = [
        _Patch(op="add", path=("tags", "test"), value={"tag": "urn:li:tag:test"})
    ]
    patch = GenericJsonPatch(
        array_primary_keys=TAGS_ARRAY_PRIMARY_KEYS,
        patch=patch_ops,
    )

    aspect = patch.to_generic_aspect()
    assert isinstance(aspect, GenericAspectClass)
    assert aspect.contentType == "application/json-patch+json"
    # Verify JSON is valid
    decoded = json.loads(aspect.value.decode())
    assert "arrayPrimaryKeys" in decoded
    assert "patch" in decoded
    assert len(decoded["patch"]) == 1
    assert decoded["patch"][0]["op"] == "add"
    assert decoded["patch"][0]["path"] == "/tags/test"


def test_generic_json_patch_to_generic_aspect_serialization_error() -> None:
    """Non-JSON-serializable patch value raises ValueError with clear context."""
    patch_ops = [
        _Patch(
            op="add",
            path=("tags", "test"),
            value={"tag": "urn:li:tag:test", "nested": object()},
        )
    ]
    patch = GenericJsonPatch(
        array_primary_keys=TAGS_ARRAY_PRIMARY_KEYS,
        patch=patch_ops,
    )
    with pytest.raises(
        ValueError, match="Failed to serialize GenericJsonPatch to JSON"
    ):
        patch.to_generic_aspect()


def test_generic_json_patch_from_dict():
    """Test creating GenericJsonPatch from dictionary."""
    patch_dict = {
        "arrayPrimaryKeys": TAGS_ARRAY_PRIMARY_KEYS,
        "patch": [
            {"op": "add", "path": "/tags/test1", "value": {"tag": "urn:li:tag:test1"}},
            {"op": "add", "path": "/tags/test2", "value": {"tag": "urn:li:tag:test2"}},
        ],
        "forceGenericPatch": True,
    }

    patch = GenericJsonPatch.from_dict(patch_dict)

    assert patch.array_primary_keys == TAGS_ARRAY_PRIMARY_KEYS
    assert len(patch.patch) == 2
    assert patch.patch[0].op == "add"
    assert patch.patch[0].path == ("tags", "test1")
    assert patch.patch[0].value == {"tag": "urn:li:tag:test1"}
    assert patch.patch[1].path == ("tags", "test2")
    assert patch.force_generic_patch is True


def test_generic_json_patch_from_dict_with_quoted_paths():
    """Test GenericJsonPatch.from_dict with quoted paths."""
    patch_dict = {
        "arrayPrimaryKeys": {"tags": ["tag"]},
        "patch": [
            {
                "op": "add",
                "path": "/tags/test~1path",
                "value": {"tag": "urn:li:tag:test"},
            }
        ],
    }

    patch = GenericJsonPatch.from_dict(patch_dict)
    assert patch.patch[0].path == ("tags", "test/path")


def test_generic_json_patch_from_dict_invalid_op():
    """Test GenericJsonPatch.from_dict with invalid operation."""
    patch_dict = {
        "arrayPrimaryKeys": {"tags": ["tag"]},
        "patch": [{"op": "invalid", "path": "/tags/test", "value": {}}],
    }

    with pytest.raises(ValueError, match="Unsupported patch operation"):
        GenericJsonPatch.from_dict(patch_dict)


def test_generic_json_patch_from_dict_empty_path_component() -> None:
    """GenericJsonPatch.from_dict raises when a patch path has empty components."""
    patch_dict = {
        "arrayPrimaryKeys": {"tags": ["tag"]},
        "patch": [{"op": "add", "path": "/tags//attribution", "value": {}}],
    }
    with pytest.raises(ValueError, match="empty component"):
        GenericJsonPatch.from_dict(patch_dict)


def test_generic_json_patch_round_trip():
    """Test round-trip conversion: dict -> GenericJsonPatch -> dict."""
    original_dict = {
        "arrayPrimaryKeys": TAGS_ARRAY_PRIMARY_KEYS,
        "patch": [
            {"op": "add", "path": "/tags/test1", "value": {"tag": "urn:li:tag:test1"}},
            {"op": "remove", "path": "/tags/test2"},
        ],
        "forceGenericPatch": False,
    }

    patch = GenericJsonPatch.from_dict(original_dict)
    result_dict = patch.to_dict()

    # Compare structure (order may differ)
    assert result_dict["arrayPrimaryKeys"] == original_dict["arrayPrimaryKeys"]
    assert result_dict["forceGenericPatch"] == original_dict["forceGenericPatch"]
    patch_list = cast(list[dict[str, Any]], result_dict["patch"])
    original_patch_list = cast(list[dict[str, Any]], original_dict["patch"])
    assert len(patch_list) == len(original_patch_list)

    # Check operations (normalize None values)
    ops_by_path: dict[str, dict[str, Any]] = {
        op["path"]: {k: v for k, v in op.items() if v is not None} for op in patch_list
    }
    original_ops_by_path: dict[str, dict[str, Any]] = {
        op["path"]: {k: v for k, v in op.items() if v is not None}
        for op in original_patch_list
    }
    assert ops_by_path == original_ops_by_path


def test_generic_json_patch_with_unit_separator():
    """Test GenericJsonPatch with UNIT_SEPARATOR in arrayPrimaryKeys."""
    patch_ops = [
        _Patch(
            op="add",
            path=("tags", "urn:li:platformResource:ingestion", "urn:li:tag:test"),
            value={"tag": "urn:li:tag:test", "attribution": {"source": "test"}},
        )
    ]
    patch = GenericJsonPatch(
        array_primary_keys=TAGS_ARRAY_PRIMARY_KEYS,
        patch=patch_ops,
    )

    result = patch.to_dict()
    assert result["arrayPrimaryKeys"] == TAGS_ARRAY_PRIMARY_KEYS
    # Verify UNIT_SEPARATOR is preserved in JSON (may be escaped as \u241f)
    json_str = json.dumps(result)
    assert UNIT_SEPARATOR in json_str or "\\u241f" in json_str or "\u241f" in json_str


def test_generic_json_patch_empty_patch():
    """Test GenericJsonPatch with empty patch list."""
    patch = GenericJsonPatch(
        array_primary_keys={"tags": ["tag"]},
        patch=[],
        force_generic_patch=False,
    )

    result = patch.to_dict()
    assert result["arrayPrimaryKeys"] == {"tags": ["tag"]}
    assert result["patch"] == []
    assert result["forceGenericPatch"] is False
