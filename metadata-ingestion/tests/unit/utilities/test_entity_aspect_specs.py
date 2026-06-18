"""Unit tests for the EntityAspectSpecs value type."""

import pytest

from datahub.utilities.entity_aspect_specs import EntityAspectSpecs


def test_from_registry_elements_collects_key_and_aspect_specs() -> None:
    specs = EntityAspectSpecs.from_registry_elements(
        [
            {
                "name": "dataset",
                "keyAspectName": "datasetKey",
                "aspectSpecs": [
                    {"aspectAnnotation": {"name": "status"}},
                    {
                        "aspectAnnotation": {
                            "name": "datasetProperties",
                            "schemaVersion": 2,
                        }
                    },
                ],
            }
        ]
    )
    assert specs.supports("dataset", "datasetKey")
    assert specs.supports("dataset", "status")
    assert not specs.supports("dataset", "ownership")
    assert specs.schema_version("datasetProperties") == 2
    # Registered but no advertised version (older server).
    assert specs.schema_version("status") is None


def test_supports_unregistered_entity_raises() -> None:
    specs = EntityAspectSpecs(entity_aspects={"dataset": {"status"}})
    with pytest.raises(ValueError):
        specs.supports("chart", "chartInfo")


def test_schema_version_unregistered_aspect_raises() -> None:
    specs = EntityAspectSpecs(entity_aspects={"dataset": {"status"}})
    with pytest.raises(ValueError):
        specs.schema_version("chartInfo")
