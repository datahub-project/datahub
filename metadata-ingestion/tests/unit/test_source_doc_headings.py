"""docGen's heading contract for docs/sources, run in the unit suite.

The contract is enforced only when docGen runs, which in CI is the docs-website
build. A connector author learns about a stray `### Probe support` there, long
after their PR is otherwise green. These tests call the very function docGen
calls, so the rule has one definition.
"""

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import List

import pytest

_INGESTION_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _INGESTION_ROOT / "scripts"
_SOURCE_DOCS = _INGESTION_ROOT / "docs" / "sources"

_VALID_POST = """\
### Capabilities

#### Probe support

`datahub recipe probe` lists what this source exposes.

### Limitations

None known.

### Troubleshooting

```yaml
### a comment inside a fence is not a heading
source:
  type: example
```
"""


@pytest.fixture(scope="module")
def docgen() -> ModuleType:
    # Imported late, through sys.path: scripts/ is not a package, and docgen
    # imports its siblings (docgen_types, docs_config_table) by bare name.
    # The path entry is removed again so no other test resolves against it.
    sys.path.insert(0, str(_SCRIPTS))
    try:
        return importlib.import_module("docgen")
    finally:
        sys.path.remove(str(_SCRIPTS))


def test_every_connector_doc_meets_the_heading_contract(docgen: ModuleType) -> None:
    # docGen only reads files inside a platform directory; AGENTS.md and
    # CLAUDE.md directly under docs/sources are guidance, not page content.
    docs = sorted(p for p in _SOURCE_DOCS.rglob("*.md") if p.parent != _SOURCE_DOCS)
    assert any(p.name.endswith("_post.md") for p in docs), "scanned no _post.md"
    failures: List[str] = []
    for path in docs:
        # docGen raises on any other name, so a file that passes the heading
        # check here would still break the docs build.
        parts = path.stem.split("_")
        if path.stem != "README" and not (
            len(parts) == 2 and parts[1] in ("pre", "post")
        ):
            failures.append(
                f"{path}: needs to be README or <plugin>_pre.md / <plugin>_post.md"
            )
            continue
        try:
            docgen.validate_source_doc_headings(
                str(path), path.read_text(encoding="utf-8")
            )
        except ValueError as exc:
            failures.append(str(exc))
    assert not failures, "\n".join(failures)


def test_a_valid_post_doc_passes(docgen: ModuleType) -> None:
    docgen.validate_source_doc_headings("example/example_post.md", _VALID_POST)


@pytest.mark.parametrize(
    "markdown",
    [
        pytest.param(_VALID_POST + "\n### Probe support\n\nText.\n", id="extra-h3"),
        pytest.param("## Setup\n\n" + _VALID_POST, id="h2"),
        pytest.param(
            _VALID_POST.split("### Troubleshooting")[0], id="missing-required-h3"
        ),
    ],
)
def test_a_post_doc_breaking_the_contract_is_refused(
    docgen: ModuleType, markdown: str
) -> None:
    with pytest.raises(ValueError):
        docgen.validate_source_doc_headings("example/example_post.md", markdown)
