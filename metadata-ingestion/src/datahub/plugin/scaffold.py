"""Generate a new DataHub plugin project skeleton.

Uses string templates (no cookiecutter dependency) to produce a complete,
release-ready repository with source stub, tests, CI workflows, and manifest.
"""

import logging
import re
from pathlib import Path
from string import Template
from typing import Dict

from datahub.plugin.plugin_config import PluginCapabilityType

logger = logging.getLogger(__name__)


def _slug(name: str) -> str:
    """Convert a plugin name like ``my-salesforce-source`` to ``my_salesforce_source``."""
    return name.replace("-", "_")


def _class_name(name: str) -> str:
    """Convert ``my-salesforce-source`` to ``MySalesforceSource``."""
    return "".join(part.capitalize() for part in name.split("-"))


# ------------------------------------------------------------------
# Template strings
# ------------------------------------------------------------------

_PYPROJECT_TOML = Template("""\
[build-system]
requires = ["setuptools>=67", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "${name}"
version = "0.1.0"
description = "${description}"
requires-python = ">=3.9"
dependencies = [
    "acryl-datahub",
]

[project.entry-points."datahub.ingestion.${plugin_type}.plugins"]
${plugin_id} = "${module}.${plugin_type}:${class_name}"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
${module} = ["datahub-plugin.yaml"]
""")

_MANIFEST_YAML = Template("""\
api_version: datahub/v1
id: ${plugin_id}
name: "${display_name}"
type: ${plugin_type}
entry_point: "${module}.${plugin_type}:${class_name}"
description: "${description}"
author: ""
url: null
icon_url: null
compatibility:
  datahub_min: null
  python_min: "3.9"
support_status: COMMUNITY
capabilities:
  - capability: SCHEMA_METADATA
    description: "Extract schema metadata"
    supported: true
""")

_SOURCE_PY = Template("""\
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import config_class, platform_name
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

from ${module}.config import ${config_class}


@platform_name("${plugin_id}")
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
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback

from ${module}.config import ${config_class}


class ${class_name}(Sink[${config_class}]):
    \"\"\"DataHub sink plugin: ${display_name}.\"\"\"

    def __init__(self, config: ${config_class}, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = SinkReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "${class_name}":
        config = ${config_class}.parse_obj(config_dict)
        return cls(config, ctx)

    def write_record_async(
        self,
        record_envelope: RecordEnvelope,
        write_callback: WriteCallback,
    ) -> None:
        # TODO: Implement write logic
        pass

    def get_report(self) -> SinkReport:
        return self.report

    def close(self) -> None:
        pass
""")

_TRANSFORMER_PY = Template("""\
from typing import Sequence

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import BaseTransformer

from ${module}.config import ${config_class}


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
from ${module}.config import ${config_class}


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
# ${display_name}

${description}

## Installation

```bash
datahub plugin install github:<owner>/${name}
```

## Configuration

```yaml
source:
  type: ${plugin_id}
  config:
    example_field: "value"
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


def scaffold_plugin(
    name: str,
    plugin_type: str,
    output_dir: str,
    description: str = "",
) -> Path:
    """Generate a new plugin project at *output_dir*/*name*.

    Returns the path to the generated project root.
    """
    if not re.match(r"^[a-z][a-z0-9-]*$", name):
        raise ValueError(
            f"Invalid plugin name {name!r}: must match ^[a-z][a-z0-9-]*$ "
            f"(lowercase letters, digits, and hyphens, starting with a letter)"
        )

    cap_type = PluginCapabilityType(plugin_type)
    slug = _slug(name)
    cls = _class_name(name)
    config_cls = f"{cls}Config"

    ctx: Dict[str, str] = {
        "name": name,
        "plugin_id": name,
        "display_name": name.replace("-", " ").title(),
        "module": slug,
        "class_name": cls,
        "config_class": config_cls,
        "plugin_type": cap_type.value,
        "description": description or f"A DataHub {cap_type.value} plugin.",
    }

    project_dir = Path(output_dir) / name
    src_dir = project_dir / "src" / slug
    tests_dir = project_dir / "tests"
    workflows_dir = project_dir / ".github" / "workflows"

    for d in [src_dir, tests_dir, workflows_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Root files
    _write(project_dir / "pyproject.toml", _PYPROJECT_TOML.substitute(ctx))
    _write(project_dir / "README.md", _README_MD.substitute(ctx))
    _write(project_dir / "LICENSE", "")

    # Source files
    _write(src_dir / "__init__.py", _INIT_PY)
    _write(src_dir / "config.py", _CONFIG_PY.substitute(ctx))

    type_template = {
        PluginCapabilityType.SOURCE: _SOURCE_PY,
        PluginCapabilityType.SINK: _SINK_PY,
        PluginCapabilityType.TRANSFORMER: _TRANSFORMER_PY,
    }[cap_type]
    _write(src_dir / f"{cap_type.value}.py", type_template.substitute(ctx))

    # Manifest as package data (so it's findable after pip install)
    _write(src_dir / "datahub-plugin.yaml", _MANIFEST_YAML.substitute(ctx))

    # Tests
    _write(tests_dir / "__init__.py", _INIT_PY)
    _write(tests_dir / f"test_{cap_type.value}.py", _TEST_PY.substitute(ctx))

    # CI workflows
    _write(workflows_dir / "release.yml", _RELEASE_YML.substitute(ctx))
    _write(workflows_dir / "test.yml", _TEST_YML.substitute(ctx))

    return project_dir


def _write(path: Path, content: str) -> None:
    path.write_text(content)
    logger.debug("Wrote %s", path)
