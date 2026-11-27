# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# See https://stackoverflow.com/a/65147676
import builtins
import re
from typing import List

builtins.__sphinx_build__ = True

import sphinx_autodoc_typehints  # noqa: E402

from datahub.sdk import dataset as _sdk_dataset  # noqa: E402
from datahub.sdk.container import Container  # noqa: E402

# sphinx_autodoc_typehints evaluates the TYPE_CHECKING-only imports of every documented
# object's module. For pydantic's BaseModel.__init__ that import (AbstractSetIntStr) only
# exists for type checkers, so it always fails; BaseModel.__init__ has no useful docs anyway.
sphinx_autodoc_typehints._TYPE_GUARD_IMPORTS_RESOLVED.add("pydantic.main")

# `ParentContainerInputType` forward-references Container, which datahub.sdk._shared can only
# import under TYPE_CHECKING (circular import), so the name is missing when Dataset's
# signature is resolved.
_sdk_dataset.Container = Container

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "DataHub Python SDK"
copyright = "2023, Acryl Data"
author = "Acryl Data"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    # TODO: set up 'sphinx.ext.viewcode'
    # Via https://stackoverflow.com/a/51312475/5004662.
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    # This enables us to autogenerate docs for our CLI.
    "sphinx_click",
    "sphinx_markdown_builder",
]

markdown_anchor_sections = True
napoleon_use_param = True

# Move type hint info to function description instead of signature
# Via: https://chromium.googlesource.com/external/github.com/reclosedev/requests-cache/+/refs/heads/master/docs/conf.py#104.
autodoc_typehints = "description"
always_document_param_types = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "venv"]
source_suffix = [".rst"]

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "undoc-members": True,
    "show-inheritance": True,
}

autodoc_mock_imports = [
    "confluent_kafka",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = "alabaster"
html_theme = "pydata_sphinx_theme"

html_static_path = ["_static"]

# TODO enable html_show_sourcelink


_PDL_JAVADOC_CODE = re.compile(r"\{@code ([^}]*)\}")


def _flatten_generated_model_docs(app, what, name, obj, options, lines: List[str]) -> None:
    # schema_classes docstrings are PDL comments, which are free-form text with ragged
    # indentation and Javadoc tags. RST reads the indentation as nested blocks and the
    # trailing underscore in `[A-Za-z_]` as a hyperlink reference.
    if not name.startswith("datahub.metadata.schema_classes."):
        return
    flattened: List[str] = []
    in_list_item = False
    for line in lines:
        text = _PDL_JAVADOC_CODE.sub(r"``\1``", line.strip())
        if not text:
            in_list_item = False
        elif text.startswith("- "):
            if flattened and flattened[-1] and not in_list_item:
                flattened.append("")
            in_list_item = True
        elif in_list_item:
            text = f"  {text}"
        flattened.append(text)
    lines[:] = flattened


def _separate_field_lists(app, what, name, obj, options, lines: List[str]) -> None:
    # `@deprecated` emits a bare `.. deprecated::` block and PDL docs can end in a bullet
    # list. sphinx_autodoc_typehints then appends `:rtype:` / `:param:` fields right after
    # them without a blank line.
    fixed: List[str] = []
    for line in lines:
        if fixed and fixed[-1] and not fixed[-1].startswith(":") and line.startswith(":"):
            fixed.append("")
        elif fixed and fixed[-1].startswith(" ") and line and not line.startswith(" "):
            fixed.append("")
        fixed.append(line)
    lines[:] = fixed


def setup(app) -> None:
    # Priority 400 runs before napoleon / sphinx_autodoc_typehints (500) add their
    # field lists; 900 runs after them.
    app.connect("autodoc-process-docstring", _flatten_generated_model_docs, priority=400)
    app.connect("autodoc-process-docstring", _separate_field_lists, priority=900)
