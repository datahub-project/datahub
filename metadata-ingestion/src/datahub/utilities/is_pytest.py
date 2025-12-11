# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import sys

from datahub.configuration.env_vars import get_test_mode


def is_pytest_running() -> bool:
    return "pytest" in sys.modules and get_test_mode() == "1"
