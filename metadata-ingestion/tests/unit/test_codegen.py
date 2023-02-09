import pytest

from datahub.metadata.schema_classes import (
    ASPECT_CLASSES,
    KEY_ASPECTS,
    OwnershipClass,
    _Aspect,
)


def test_class_filter() -> None:
    # The codegen should only generate classes for aspects and a few extra classes.
    # As such, stuff like lineage search results should not appear.

    with pytest.raises(ImportError):
        from datahub.metadata.schema_classes import (  # type: ignore[attr-defined] # noqa: F401
            LineageSearchResultClass,
        )


def test_codegen_aspect_name():
    assert issubclass(OwnershipClass, _Aspect)

    assert OwnershipClass.ASPECT_NAME == "ownership"
    assert OwnershipClass.get_aspect_name() == "ownership"


def test_codegen_aspects():
    # These bounds are extremely loose, and mainly verify that the lists aren't empty.
    assert len(ASPECT_CLASSES) > 30
    assert len(KEY_ASPECTS) > 10


def test_cannot_instantiated_codegen_aspect():
    with pytest.raises(TypeError, match="instantiate"):
        _Aspect()
