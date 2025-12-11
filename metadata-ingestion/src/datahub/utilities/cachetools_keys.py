# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Any

import cachetools.keys


def self_methodkey(self: Any, *args: Any, **kwargs: Any) -> Any:
    # Keeps the id of self around
    return cachetools.keys.hashkey(id(self), *args, **kwargs)
