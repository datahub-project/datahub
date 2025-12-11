# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import time

import pytest

from datahub.utilities.threading_timeout import TimeoutException, threading_timeout


def test_timeout_no_timeout():
    # Should complete without raising an exception
    with threading_timeout(1.0):
        time.sleep(0.1)


def test_timeout_raises():
    # Should raise TimeoutException
    with pytest.raises(TimeoutException), threading_timeout(0.1):
        time.sleep(0.5)


def test_timeout_early_exit():
    # Test that context manager handles other exceptions properly
    with pytest.raises(ValueError), threading_timeout(1.0):
        raise ValueError("Early exit")


def test_timeout_zero():
    # Should not raise an exception
    with threading_timeout(0.0):
        pass
