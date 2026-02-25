import pathlib

import pytest

from datahub.metadata.urns import CorpUserUrn, TagUrn
from datahub.sdk.tag import Tag
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "tag_golden"


def test_tag_basic() -> None:
    t = Tag(name="test-tag")

    assert Tag.get_urn_type() == TagUrn
    assert isinstance(t.urn, TagUrn)
    assert str(t.urn) == "urn:li:tag:test-tag"
    assert str(t.urn) in repr(t)

    assert t.name == "test-tag"
    assert t.display_name == "test-tag"  # name is the fallback value
    assert t.description is None
    assert t.color is None
    assert t.owners is None

    with pytest.raises(AttributeError):
        assert t.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        t.extra_attribute = "slots should reject extra fields"  # type: ignore
    with pytest.raises(AttributeError):
        # This should fail. Eventually we should make it suggest calling set_owners instead.
        t.owners = []  # type: ignore

    assert_entity_golden(t, GOLDEN_DIR / "test_tag_basic_golden.json")


def test_tag_complex() -> None:
    t = Tag(
        name="complex-tag",
        display_name="Complex Tag",
        description="A complex tag for testing purposes",
        color="#FF5733",
        owners=[
            CorpUserUrn("admin@datahubproject.io"),
        ],
    )

    assert isinstance(t.urn, TagUrn)
    assert str(t.urn) == "urn:li:tag:complex-tag"

    assert t.name == "complex-tag"
    assert t.display_name == "Complex Tag"
    assert t.description == "A complex tag for testing purposes"
    assert t.color == "#FF5733"
    assert t.owners is not None
    assert len(t.owners) == 1

    t.set_display_name("Updated Tag")
    assert t.display_name == "Updated Tag"

    t.set_description("Updated description")
    assert t.description == "Updated description"

    t.set_color("#00FF00")
    assert t.color == "#00FF00"

    t.add_owner(CorpUserUrn("user@datahubproject.io"))

    assert_entity_golden(t, GOLDEN_DIR / "test_tag_complex_golden.json")
