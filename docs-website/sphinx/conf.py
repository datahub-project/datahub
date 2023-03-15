# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "DataHub Python SDK"
copyright = "2023, Acryl Data"
author = "Acryl Data"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    # Via https://stackoverflow.com/a/51312475/5004662.
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

napoleon_use_param = True


# Move type hint info to function description instead of signature
autodoc_typehints = "description"
always_document_param_types = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "venv"]
source_suffix = [".rst"]

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "undoc-members": True,
    # "special-members": "__init__",
    "show-inheritance": True,
}

# autodoc_class_signature = "separated"

# autodoc_type_aliases = {
#     "Aspect": "datahub.metadata.schema_classes.Aspect",
# }

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = "alabaster"
html_theme = "pydata_sphinx_theme"

html_static_path = ["_static"]

# TODO enable html_show_sourcelink
