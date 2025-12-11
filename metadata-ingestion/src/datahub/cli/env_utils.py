# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import os


def get_boolean_env_variable(key: str, default: bool = False) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    elif value.lower() in ("true", "1"):
        return True
    else:
        return False
