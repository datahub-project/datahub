import json
from pathlib import Path
from typing import Set
from unittest.mock import patch

from datahub.ingestion.autogen_ui.generator import (
    generate_bundle,
    generate_form,
    write_ui_form_bundle,
)

HOST_PORT_FIELD_PATH = "source.config.host_port"


def test_generate_form_for_real_connector() -> None:
    form = generate_form("mysql")

    assert form is not None
    assert form.sections[0].key == "connection"

    field_paths: Set[str] = {
        field.field_path for section in form.sections for field in section.fields
    }
    assert HOST_PORT_FIELD_PATH in field_paths


def test_generate_form_for_unknown_connector_returns_none() -> None:
    assert generate_form("not_a_real_connector_xyz") is None


def test_generate_bundle_skips_unresolvable_connectors() -> None:
    bundle = generate_bundle(["mysql", "postgres"])

    assert bundle["version"] == 1
    forms = bundle["forms"]
    assert isinstance(forms, list)
    assert {form["connector"] for form in forms} == {"mysql", "postgres"}


def test_write_ui_form_bundle_writes_valid_json(tmp_path: Path) -> None:
    output_path = tmp_path / "bundle.json"

    write_ui_form_bundle(str(output_path), ["mysql"])

    data = json.loads(output_path.read_text())
    assert data["version"] == 1
    assert data["forms"][0]["connector"] == "mysql"


def test_generate_form_is_crash_isolated_from_build_form() -> None:
    with patch(
        "datahub.ingestion.autogen_ui.generator.build_form",
        side_effect=RuntimeError("boom"),
    ):
        assert generate_form("mysql") is None

        bundle = generate_bundle(["mysql"])
        assert bundle == {"version": 1, "forms": []}
