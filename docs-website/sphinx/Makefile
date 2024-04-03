# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
VENV_DIR	  ?= venv
SPHINXOPTS    ?=
SPHINXBUILD   ?= $(VENV_DIR)/bin/sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build
VENV_SENTINEL = $(VENV_DIR)/.sentinel

# Put it first so that "make" without argument is like "make help".
help: venv
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

venv: $(VENV_SENTINEL)

$(VENV_SENTINEL): requirements.txt
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install --upgrade pip uv wheel setuptools
	VIRTUAL_ENV=$(VENV_DIR) $(VENV_DIR)/bin/uv pip install -r requirements.txt
	touch $(VENV_SENTINEL)

.PHONY: help html doctest linkcheck clean clean_all serve md

# Not using Python's http.server because it enables caching headers.
serve:
	serve -p 3001 _build/html/

md: html
	$(VENV_DIR)/bin/python3 convert_sphinx_to_docusaurus.py

# Route other targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
html doctest linkcheck clean: venv Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

clean_all: clean
	-rm -rf $(VENV_DIR)
