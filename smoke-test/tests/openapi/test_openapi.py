"""
OpenAPI smoke tests for root-level v1, v2, v3 and openlineage fixtures.

For manual targeted testing, set OPENAPI_FIXTURE_GLOBS to a comma-separated
list of globs (or a single glob). Example:
  OPENAPI_FIXTURE_GLOBS=tests/openapi/v3/retention_policies.json pytest ...
  OPENAPI_FIXTURE_GLOBS=tests/openapi/v3/*.json pytest ...
"""

import logging
import os

from tests.utilities.concurrent_openapi import run_tests

logger = logging.getLogger(__name__)

_ROOT_FIXTURE_GLOBS = [
    "tests/openapi/v1/*.json",
    "tests/openapi/v2/*.json",
    "tests/openapi/v3/*.json",
    "tests/openapi/openlineage/*.json",
]


def test_openapi(auth_session):
    """Run OpenAPI v1, v2, v3 and openlineage fixtures."""
    globs = os.environ.get("OPENAPI_FIXTURE_GLOBS")
    if globs:
        fixture_globs = [g.strip() for g in globs.split(",")]
    else:
        fixture_globs = _ROOT_FIXTURE_GLOBS
    run_tests(
        auth_session,
        fixture_globs=fixture_globs,
        num_workers=10,
    )
