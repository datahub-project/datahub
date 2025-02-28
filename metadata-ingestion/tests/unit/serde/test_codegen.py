import os
import pathlib
import typing
from typing import List, Type

import pytest
import typing_inspect

from datahub.emitter.mce_builder import ALL_ENV_TYPES
from datahub.metadata.schema_classes import (
    ASPECT_CLASSES,
    KEY_ASPECTS,
    FineGrainedLineageClass,
    MetadataChangeEventClass,
    OwnershipClass,
    TelemetryKeyClass,
    UpstreamClass,
    _Aspect,
)
from datahub.utilities.urns._urn_base import URN_TYPES

_UPDATE_ENTITY_REGISTRY = os.getenv("UPDATE_ENTITY_REGISTRY", "false").lower() == "true"
ENTITY_REGISTRY_PATH = pathlib.Path(
    "../metadata-models/src/main/resources/entity-registry.yml"
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


def _add_to_registry(entity: str, aspect: str) -> None:
    from ruamel.yaml import YAML

    yaml = YAML()

    doc = yaml.load(ENTITY_REGISTRY_PATH)

    for entry in doc["entities"]:
        if entry["name"] == entity:
            entry["aspects"].append(aspect)
            break
    else:
        raise ValueError(
            f'could not find entity "{entity}" in entity registry at {ENTITY_REGISTRY_PATH}'
        )

    # Prevent line wrapping + preserve indentation.
    yaml.width = 2**20  # type: ignore[assignment]
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.dump(doc, ENTITY_REGISTRY_PATH)


def test_entity_registry_completeness():
    # The snapshot classes can have aspects that the entity registry doesn't know about.
    # This ensures that we don't have any of those cases.

    errors: List[str] = []

    def _err(msg: str) -> None:
        print(msg)
        errors.append(msg)

    snapshot_classes: List[Type] = typing_inspect.get_args(
        typing.get_type_hints(MetadataChangeEventClass.__init__)["proposedSnapshot"]
    )

    lowercase_entity_type_map = {name.lower(): name for name in KEY_ASPECTS}

    for snapshot_class in snapshot_classes:
        lowercase_entity_type: str = snapshot_class.__name__.replace(
            "SnapshotClass", ""
        ).lower()
        entity_type = lowercase_entity_type_map.get(lowercase_entity_type)
        if entity_type is None:
            _err(f"entity {entity_type}: missing from the entity registry entirely")
            continue

        key_aspect = KEY_ASPECTS[entity_type]
        supported_aspect_names = set(key_aspect.get_aspect_info()["entityAspects"])

        snapshot_aspect_types: List[Type[_Aspect]] = typing_inspect.get_args(
            typing_inspect.get_args(
                typing.get_type_hints(snapshot_class.__init__)["aspects"]
            )[0]
        )

        # print(f"Entity type: {entity_type}")
        # print(f"Supported aspects: {supported_aspect_names}")
        # print(f"Snapshot aspects: {snapshot_aspect_types}")

        for aspect_type in snapshot_aspect_types:
            if aspect_type == key_aspect:
                continue

            aspect_name = aspect_type.ASPECT_NAME
            if aspect_name not in supported_aspect_names:
                if _UPDATE_ENTITY_REGISTRY:
                    _add_to_registry(entity_type, aspect_name)
                else:
                    _err(
                        f"entity {entity_type}: aspect {aspect_name} is missing from the entity registry"
                    )

    assert not errors, (
        f'To fix these errors, run "UPDATE_ENTITY_REGISTRY=true pytest {__file__}"'
    )


def test_enum_options():
    # This is mainly a sanity check to ensure that it doesn't do anything too crazy.
    assert "PROD" in ALL_ENV_TYPES


def test_urn_types() -> None:
    assert len(URN_TYPES) > 10
    for checked_type in ["dataset", "dashboard", "dataFlow", "schemaField"]:
        assert checked_type in URN_TYPES
