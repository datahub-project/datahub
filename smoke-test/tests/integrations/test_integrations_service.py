import logging

import pytest

from tests.utils import wait_for_healthcheck_util

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


def test_healthchecks(wait_for_healthchecks):
    logger.info("Healthchecks passed")
    pass
