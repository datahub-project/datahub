# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) or any other agent when working with code in this repository.

## Coding conventions

- Keep code maintainable. This is not throw-away code. This goes to production. 
- Generate unit tests where appropriate. 
- Do not start generating random scripts to run the code you generated unless asked for.
- Do not add comments which are redundant given the function names

## Core concept docs

 - `docs/what/urn.md` defines what a URN is

## Overall Directory structure

- This is repository for DataHub project.
- `README.MD` should give some basic information about the project.
- This is a multi-project gradle project so you will find a lot of `build.gradle` in most folders

### metadata-ingestion module details
- `metadata-ingestion` contains source and tests for DataHub OSS CLI.
- `metadata-ingestion/developing.md` contains details about the environment used for testing.
- `.github/workflows/metadata-ingestion.yml` contains our github workflow that is used in CI.
- `metadata-ingestion/build.gradle` contains our build.gradle that has gradle tasks defined for this module
- `pyproject.toml`, `setup.py`, `setup.cfg` in the folder contain rules about the code style for the repository
- The `.md` files at top level in this folder gives you important information about the concepts of ingestion
- You can see examples of how to define various aspect types in `metadata-ingestion/src/datahub/emitter/mcp_builder.py`
- Source code goes in `metadata-ingestion/src/`
- Tests go in `metadata-ingestion/tests/` (not in `src/`)
- **Testing conventions for metadata-ingestion**:
    - Unit tests: `metadata-ingestion/tests/unit/`
    - Integration tests: `metadata-ingestion/tests/integration/`
    - Test files should mirror the source directory structure
    - Use pytest, not unittest
    - Use `assert` statements, not `self.assertEqual()` or `self.assertIsNone()`
    - Use regular classes, not `unittest.TestCase`
    - Import `pytest` in test files
    - Test files should be named `test_*.py` and placed in the appropriate test directory, not alongside source files
