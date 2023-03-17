import pytest

from datahub.metadata.schema_classes import (
    ASPECT_CLASSES,
    KEY_ASPECTS,
    FineGrainedLineageClass,
    OwnershipClass,
    TelemetryKeyClass,
    UpstreamClass,
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


def test_key_aspect_info():
    expected = {
        "keyForEntity": "telemetry",
        "entityCategory": "internal",
        "entityAspects": ["telemetryClientId"],
    }
    assert TelemetryKeyClass.ASPECT_INFO == expected
    assert TelemetryKeyClass.get_aspect_info() == expected


def test_cannot_instantiate_codegen_aspect():
    with pytest.raises(TypeError, match="instantiate"):
        _Aspect()


def test_urn_annotation():
    # We rely on these annotations elsewhere, so we want to make sure they show up.

    assert (
        UpstreamClass.RECORD_SCHEMA.fields_dict["dataset"].get_prop("Urn")
        == "DatasetUrn"
    )
    assert not UpstreamClass.RECORD_SCHEMA.fields_dict["dataset"].get_prop(
        "urn_is_array"
    )

    assert (
        FineGrainedLineageClass.RECORD_SCHEMA.fields_dict["upstreams"].get_prop("Urn")
        == "Urn"
    )
    assert FineGrainedLineageClass.RECORD_SCHEMA.fields_dict["upstreams"].get_prop(
        "urn_is_array"
    )
