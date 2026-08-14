import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import docgen


def _generate(tmp_path: Path, catalog: Dict[str, Any]) -> Dict[str, Any]:
    """Run the generator over a catalog with no registry platforms."""
    catalog_path = tmp_path / "integrations_catalog.json"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
    output_path = tmp_path / "integrations.json"

    docgen.generate_filter_tag_indexes(
        platforms={},
        catalog_path=str(catalog_path),
        output_path=str(output_path),
    )

    return json.loads(output_path.read_text(encoding="utf-8"))


def _entry(tmp_path: Path, meta: Dict[str, Any]) -> Dict[str, Any]:
    result = _generate(tmp_path, {"my-platform": {"title": "My Platform", **meta}})
    entries = result["ingestionSources"]
    assert len(entries) == 1
    return entries[0]


def test_api_entry_gets_the_default_request_url(tmp_path: Path) -> None:
    entry = _entry(tmp_path, {"api_connector": True})

    assert entry["isApiConnector"] is True
    assert entry["requestNativeUrl"] == docgen.DEFAULT_REQUEST_CONNECTOR_URL
    assert entry["tags"]["Connection Type"] == "API"


def test_explicit_request_url_overrides_the_default(tmp_path: Path) -> None:
    entry = _entry(
        tmp_path, {"api_connector": True, "requestNativeUrl": "docs/custom-request"}
    )

    assert entry["requestNativeUrl"] == "docs/custom-request"


@pytest.mark.parametrize("value", ["", None])
def test_empty_request_url_falls_back_to_the_default(
    tmp_path: Path, value: Optional[str]
) -> None:
    """An empty value must not leave the card without a link to request one."""
    entry = _entry(tmp_path, {"api_connector": True, "requestNativeUrl": value})

    assert entry["requestNativeUrl"] == docgen.DEFAULT_REQUEST_CONNECTOR_URL


def test_non_api_entry_has_no_request_url(tmp_path: Path) -> None:
    entry = _entry(tmp_path, {"external": True})

    assert "requestNativeUrl" not in entry
    assert "isApiConnector" not in entry
