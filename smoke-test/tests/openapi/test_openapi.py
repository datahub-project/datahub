import logging

from tests.utilities.concurrent_openapi import run_tests

logger = logging.getLogger(__name__)


def test_openapi_all(auth_session):
    run_tests(auth_session, fixture_globs=["tests/openapi/*/*.json"], num_workers=10)


# def test_openapi_v1(auth_session):
#     run_tests(auth_session, fixture_globs=["tests/openapi/v1/*.json"], num_workers=4)
#
#
# def test_openapi_v2(auth_session):
#     run_tests(auth_session, fixture_globs=["tests/openapi/v2/*.json"], num_workers=4)
#
#
# def test_openapi_v3(auth_session):
#     run_tests(auth_session, fixture_globs=["tests/openapi/v3/*.json"], num_workers=4)
