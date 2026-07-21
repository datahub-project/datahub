"""Generate and extend DataHub plugin projects.

A plugin project is a single distribution (import *namespace*) that ships one or
more connectors, each as its own subpackage — mirroring how DataHub organizes
its own connectors (``datahub.ingestion.source.<connector>``):

    src/<namespace>/
        __init__.py
        datahub-plugin.yaml        # one shared manifest (multi-plugin form)
        <connector_a>/
            __init__.py
            source.py
            config.py
        <connector_b>/
            ...

``create_project`` bootstraps a new project with its first connector;
``add_connector`` appends another connector to an existing project (writing the
subpackage, appending a manifest entry, and wiring the entry point). Both share
one connector writer, so the two paths always produce the same shape.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Dict, List, Optional, Tuple

import yaml

from datahub.plugin.plugin_config import MANIFEST_FILENAME, PluginCapabilityType

logger = logging.getLogger(__name__)

# A namespace or connector name: lowercase, digits, hyphens, letter-initial.
_REF_PART_RE = re.compile(r"^[a-z][a-z0-9-]*$")

# Manifest keys that describe the package as a whole rather than a single plugin.
# Used when converting a flat (single-plugin) manifest into the multi form.
_PACKAGE_LEVEL_KEYS = {"api_version", "author", "url", "compatibility"}


def _slug(name: str) -> str:
    """Convert ``my-salesforce-source`` to ``my_salesforce_source``."""
    return name.replace("-", "_")


def _class_name(name: str) -> str:
    """Convert ``my-salesforce-source`` to ``MySalesforceSource``."""
    return "".join(part.capitalize() for part in name.split("-"))


def parse_connector_ref(ref: str) -> Tuple[Optional[str], str]:
    """Split a ``namespace/connector`` (or bare ``connector``) reference.

    Returns ``(namespace, connector)``; namespace is ``None`` when the reference
    has no slash. Both parts must match ``^[a-z][a-z0-9-]*$``.
    """

    def _check(part: str, what: str) -> str:
        if not _REF_PART_RE.match(part):
            raise ValueError(
                f"Invalid {what} {part!r}: must match ^[a-z][a-z0-9-]*$ "
                "(lowercase letters, digits, and hyphens, starting with a letter)"
            )
        return part

    if "/" in ref:
        namespace, _, connector = ref.partition("/")
        return _check(namespace, "namespace"), _check(connector, "connector name")
    return None, _check(ref, "connector name")


# ------------------------------------------------------------------
# Template strings
# ------------------------------------------------------------------

_PYPROJECT_TOML = Template("""\
[build-system]
requires = ["setuptools>=67", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "${namespace}"
version = "0.1.0"
description = "${description}"
requires-python = ">=3.9"
dependencies = [
    "acryl-datahub",
]

[project.entry-points."datahub.ingestion.${plugin_type}.plugins"]
${connector_id} = "${connector_module}.${plugin_type}:${class_name}"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
${namespace_slug} = ["datahub-plugin.yaml"]
""")

_SOURCE_PY = Template("""\
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

from ${connector_module}.config import ${config_class}


@platform_name("${connector_id}")
@config_class(${config_class})
class ${class_name}(Source):
    \"\"\"DataHub source plugin: ${display_name}.\"\"\"

    def __init__(self, config: ${config_class}, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "${class_name}":
        config = ${config_class}.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self):  # type: ignore[override]
        # TODO: Implement metadata extraction logic
        yield from []

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
""")

_SINK_PY = Template("""\
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback

from ${connector_module}.config import ${config_class}


# The base Sink is generic over (config, report) and already handles __init__,
# create(), and report construction — subclasses only implement writing.
class ${class_name}(Sink[${config_class}, SinkReport]):
    \"\"\"DataHub sink plugin: ${display_name}.\"\"\"

    def write_record_async(
        self,
        record_envelope: RecordEnvelope,
        write_callback: WriteCallback,
    ) -> None:
        # TODO: Implement write logic. You MUST signal completion via the
        # callback, or the pipeline will block waiting for this record.
        self.report.report_record_written(record_envelope)
        if write_callback:
            write_callback.on_success(record_envelope, {})

    def close(self) -> None:
        pass
""")

_TRANSFORMER_PY = Template("""\
from typing import Sequence

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import BaseTransformer

from ${connector_module}.config import ${config_class}


class ${class_name}(BaseTransformer):
    \"\"\"DataHub transformer plugin: ${display_name}.\"\"\"

    def __init__(self, config: ${config_class}, ctx: PipelineContext) -> None:
        super().__init__()
        self.config = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "${class_name}":
        config = ${config_class}.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> Sequence[str]:
        return ["dataset"]

    def transform_aspect(self, entity_urn, aspect_name, aspect):
        # TODO: Implement transformation logic
        return aspect
""")

_CONFIG_PY = Template("""\
from pydantic import Field

from datahub.configuration.common import ConfigModel


class ${config_class}(ConfigModel):
    \"\"\"Configuration for ${display_name}.\"\"\"

    # TODO: Add configuration fields
    example_field: str = Field(
        default="default_value",
        description="An example configuration field.",
    )
""")

_INIT_PY = ""

_TEST_PY = Template("""\
from ${connector_module}.config import ${config_class}


class Test${class_name}:
    def test_config_defaults(self) -> None:
        config = ${config_class}()
        assert config.example_field == "default_value"
""")

_RELEASE_YML = Template("""\
name: Release

on:
  push:
    tags: ['v*']

jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install build
      - run: python -m build --wheel
      - uses: softprops/action-gh-release@v2
        with:
          files: dist/*.whl
""")

_TEST_YML = Template("""\
name: Test

on:
  pull_request:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: pytest tests/
""")

_README_MD = Template("""\
# ${namespace}

${description}

This project bundles one or more DataHub connectors under the `${namespace_slug}`
namespace. Each connector lives in its own subpackage and is listed in
`src/${namespace_slug}/datahub-plugin.yaml`.

## Installation

```bash
datahub plugin install github:<owner>/${namespace}
```

## Configuration

```yaml
source:
  type: ${connector_id}
  config:
    example_field: "value"
```

> The `type` is the connector's manifest `id`. If it collides with a built-in
> connector of the same name, the built-in wins — namespace the id (e.g.
> `${namespace_slug}-${connector_id}`) if you need to shadow one.

## Adding another connector

```bash
# from the project root (a datahub-plugin.yaml already exists here)
datahub plugin init <another-connector> --type source
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```

## Publishing

```bash
git tag v0.1.0
git push --tags
```
""")


@dataclass
class ScaffoldResult:
    """Outcome of creating a project or adding a connector to one."""

    project_dir: Path
    namespace: str
    connector_id: str
    entry_point: str
    connector_dir: Path
    manifest_path: Path
    created_project: bool
    pyproject_updated: bool


def _build_ctx(
    namespace: str, connector: str, plugin_type: str, description: str
) -> Dict[str, str]:
    ns_slug = _slug(namespace)
    conn_slug = _slug(connector)
    cls = _class_name(connector)
    return {
        "namespace": namespace,
        "namespace_slug": ns_slug,
        "connector_id": connector,
        "connector_slug": conn_slug,
        "connector_module": f"{ns_slug}.{conn_slug}",
        "display_name": connector.replace("-", " ").title(),
        "class_name": cls,
        "config_class": f"{cls}Config",
        "plugin_type": plugin_type,
        "description": description or f"A DataHub {plugin_type} connector.",
    }


def _build_plugin_entry(ctx: Dict[str, str]) -> Dict[str, object]:
    """Build the manifest entry for one connector (shared by create + add)."""
    return {
        "id": ctx["connector_id"],
        "name": ctx["display_name"],
        "type": ctx["plugin_type"],
        "entry_point": f"{ctx['connector_module']}.{ctx['plugin_type']}:{ctx['class_name']}",
        "description": ctx["description"],
        "icon_url": None,
        "support_status": "COMMUNITY",
        "capabilities": [
            {
                "capability": "SCHEMA_METADATA",
                "description": "Extract schema metadata",
                "supported": True,
            }
        ],
    }


def _write_manifest(manifest_path: Path, data: Dict[str, object]) -> None:
    with open(manifest_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def _write_connector(
    namespace_pkg_dir: Path,
    tests_dir: Path,
    ctx: Dict[str, str],
    cap_type: PluginCapabilityType,
) -> Path:
    """Write one connector's subpackage (``<connector>/``) plus its test file."""
    conn_dir = namespace_pkg_dir / ctx["connector_slug"]
    conn_dir.mkdir(parents=True, exist_ok=True)

    _write(conn_dir / "__init__.py", _INIT_PY)
    _write(conn_dir / "config.py", _CONFIG_PY.substitute(ctx))

    type_template = {
        PluginCapabilityType.SOURCE: _SOURCE_PY,
        PluginCapabilityType.SINK: _SINK_PY,
        PluginCapabilityType.TRANSFORMER: _TRANSFORMER_PY,
    }[cap_type]
    _write(conn_dir / f"{cap_type.value}.py", type_template.substitute(ctx))

    tests_dir.mkdir(parents=True, exist_ok=True)
    _write(tests_dir / f"test_{ctx['connector_slug']}.py", _TEST_PY.substitute(ctx))
    return conn_dir


def create_project(
    namespace: str,
    connector: str,
    plugin_type: str,
    output_dir: str,
    description: str = "",
) -> ScaffoldResult:
    """Bootstrap a new plugin project at *output_dir*/*namespace* with one connector."""
    parse_connector_ref(f"{namespace}/{connector}")  # validate both names
    cap_type = PluginCapabilityType(plugin_type)
    ctx = _build_ctx(namespace, connector, cap_type.value, description)

    project_dir = Path(output_dir) / namespace
    ns_pkg_dir = project_dir / "src" / ctx["namespace_slug"]
    tests_dir = project_dir / "tests"
    workflows_dir = project_dir / ".github" / "workflows"
    for d in [ns_pkg_dir, tests_dir, workflows_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Namespace package: the shared manifest lives here (multi-plugin form).
    _write(ns_pkg_dir / "__init__.py", _INIT_PY)
    entry = _build_plugin_entry(ctx)
    manifest_path = ns_pkg_dir / MANIFEST_FILENAME
    _write_manifest(
        manifest_path,
        {
            "api_version": "datahub/v1",
            "author": "",
            "url": None,
            "compatibility": {"datahub_min": None, "python_min": "3.9"},
            "plugins": [entry],
        },
    )

    conn_dir = _write_connector(ns_pkg_dir, tests_dir, ctx, cap_type)
    _write(tests_dir / "__init__.py", _INIT_PY)

    # Project-level files
    _write(project_dir / "pyproject.toml", _PYPROJECT_TOML.substitute(ctx))
    _write(project_dir / "README.md", _README_MD.substitute(ctx))
    _write(project_dir / "LICENSE", "")
    _write(workflows_dir / "release.yml", _RELEASE_YML.substitute(ctx))
    _write(workflows_dir / "test.yml", _TEST_YML.substitute(ctx))

    return ScaffoldResult(
        project_dir=project_dir,
        namespace=namespace,
        connector_id=connector,
        entry_point=str(entry["entry_point"]),
        connector_dir=conn_dir,
        manifest_path=manifest_path,
        created_project=True,
        pyproject_updated=True,
    )


def add_connector(
    project_dir: str,
    manifest_path: str,
    connector: str,
    plugin_type: str,
    description: str = "",
) -> ScaffoldResult:
    """Add a connector to an existing project.

    The namespace is inferred from the package that holds the existing manifest,
    so the caller only supplies the connector name. Writes the connector
    subpackage + test, appends a manifest entry (converting a flat manifest to
    the multi-plugin form if needed), and adds the setuptools entry point.
    """
    parse_connector_ref(connector)  # validate the name early
    cap_type = PluginCapabilityType(plugin_type)

    manifest = Path(manifest_path)
    ns_pkg_dir = manifest.parent
    namespace_slug = ns_pkg_dir.name  # the import package = the namespace
    ctx = _build_ctx(namespace_slug, connector, cap_type.value, description)

    entry = _build_plugin_entry(ctx)
    _append_plugin_to_manifest(manifest, entry)

    conn_dir = _write_connector(ns_pkg_dir, Path(project_dir) / "tests", ctx, cap_type)

    pyproject_updated = _add_entry_point_to_pyproject(
        Path(project_dir) / "pyproject.toml",
        cap_type.value,
        str(entry["id"]),
        str(entry["entry_point"]),
    )

    return ScaffoldResult(
        project_dir=Path(project_dir),
        namespace=namespace_slug,
        connector_id=connector,
        entry_point=str(entry["entry_point"]),
        connector_dir=conn_dir,
        manifest_path=manifest,
        created_project=False,
        pyproject_updated=pyproject_updated,
    )


def _append_plugin_to_manifest(manifest_path: Path, entry: Dict[str, object]) -> None:
    """Append *entry* to the manifest, converting the flat form to multi if needed."""
    raw = yaml.safe_load(manifest_path.read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{manifest_path.name} is not a valid manifest mapping")

    if "plugins" in raw:
        plugins = raw["plugins"]
        if not isinstance(plugins, list):
            raise ValueError("'plugins' in the manifest must be a list")
    else:
        # Flat single-plugin manifest: hoist package-level keys and move the
        # remaining per-plugin keys into the first list entry.
        shared = {k: raw[k] for k in raw if k in _PACKAGE_LEVEL_KEYS}
        first = {k: v for k, v in raw.items() if k not in _PACKAGE_LEVEL_KEYS}
        raw = {**shared, "plugins": [first] if first else []}
        plugins = raw["plugins"]

    existing_ids = {p.get("id") for p in plugins if isinstance(p, dict)}
    if entry["id"] in existing_ids:
        raise ValueError(
            f"Connector id '{entry['id']}' already exists in {manifest_path.name}"
        )

    plugins.append(entry)
    _write_manifest(manifest_path, raw)


def _add_entry_point_to_pyproject(
    pyproject_path: Path,
    plugin_type: str,
    connector_id: str,
    entry_point: str,
) -> bool:
    """Add the setuptools entry point for a connector to pyproject.toml.

    Best-effort text edit tuned for the scaffold's own layout: inserts into the
    matching ``[project.entry-points."datahub.ingestion.<type>.plugins"]`` table,
    creating it if absent. Returns ``False`` when there is no pyproject.toml to
    edit, so the caller can print the line for the user to add by hand.
    """
    if not pyproject_path.is_file():
        return False

    text = pyproject_path.read_text()
    new_line = f'{connector_id} = "{entry_point}"'
    if new_line in text:
        return True

    header = f'[project.entry-points."datahub.ingestion.{plugin_type}.plugins"]'
    lines = text.splitlines()
    if header in lines:
        # Insert as the last entry of the existing table (before the next blank
        # line or the next table header).
        idx = lines.index(header) + 1
        while (
            idx < len(lines)
            and lines[idx].strip()
            and not lines[idx].lstrip().startswith("[")
        ):
            idx += 1
        lines.insert(idx, new_line)
    else:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(header)
        lines.append(new_line)

    pyproject_path.write_text("\n".join(lines) + "\n")
    return True


def _write(path: Path, content: str) -> None:
    path.write_text(content)
    logger.debug("Wrote %s", path)


# Kept for callers/tests that don't need namespaces: create a single-connector
# project where the namespace equals the connector name.
def scaffold_plugin(
    name: str,
    plugin_type: str,
    output_dir: str,
    description: str = "",
) -> Path:
    """Create a new single-connector project (namespace == connector name)."""
    namespace, connector = parse_connector_ref(name)
    result = create_project(
        namespace=namespace or connector,
        connector=connector,
        plugin_type=plugin_type,
        output_dir=output_dir,
        description=description,
    )
    return result.project_dir


__all__: List[str] = [
    "ScaffoldResult",
    "add_connector",
    "create_project",
    "parse_connector_ref",
    "scaffold_plugin",
]
