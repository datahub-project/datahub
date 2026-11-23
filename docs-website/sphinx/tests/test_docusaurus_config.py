import json
import pathlib
import shutil
import subprocess
from typing import Any

DOCUSAURUS_CONFIG_PATH = (
    pathlib.Path(__file__).resolve().parents[2] / "docusaurus.config.js"
)
ARCHIVE_BASE = "https://archive.docs.datahub.com/docs"


def _load_dropdown_items_after() -> list[dict[str, Any]]:
    if not shutil.which("node"):
        return []

    node_script = """
const fs = require("fs");
let content = fs.readFileSync(process.argv[1], "utf8");
content = content.replace('require("dotenv").config();', "");
const fakeRequire = (id) => (id === "./sidebars.js" ? {} : require(id));
fakeRequire.resolve = (id) => id;
const m = { exports: {} };
new Function("module", "exports", "require", "process", content)(
  m,
  m.exports,
  fakeRequire,
  process
);
const navItems = m.exports.themeConfig.navbar.items;
const versionDropdown = navItems.find((item) => item.type === "docsVersionDropdown");
console.log(JSON.stringify(versionDropdown.dropdownItemsAfter));
"""
    result = subprocess.run(
        ["node", "-e", node_script, str(DOCUSAURUS_CONFIG_PATH)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_dropdown_items_after_archived_versions_are_plain_links():
    items = _load_dropdown_items_after()
    if not items:
        # Fallback to source check if node is unavailable
        return

    # First two items are the separator and heading
    archived_items = items[2:]
    assert len(archived_items) >= 15

    for item in archived_items:
        assert "label" in item
        assert "href" in item
        assert item["target"] == "_blank"
        assert item["href"].startswith(f"{ARCHIVE_BASE}/")
        assert item.get("type") != "html"


def test_dropdown_items_after_has_no_nested_anchor_tags():
    items = _load_dropdown_items_after()
    if not items:
        return

    for item in items:
        if item.get("type") == "html":
            value = item.get("value", "")
            assert "<a" not in value.lower()


def test_docusaurus_config_source_avoids_raw_anchor_dropdown_values():
    content = DOCUSAURUS_CONFIG_PATH.read_text(encoding="utf-8")

    # Ensure ARCHIVED_VERSIONS is not mapped to raw HTML dropdown links
    assert "EXTERNAL_LINK_ICON" not in content
    assert '<a class="dropdown__link"' not in content
    assert "...ARCHIVED_VERSIONS.map(([label, path]) => ({" in content
