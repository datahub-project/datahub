# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import doctest
from types import ModuleType


def assert_doctest(module: ModuleType) -> None:
    result = doctest.testmod(
        module,
        raise_on_error=True,
        verbose=True,
    )
    if result.attempted == 0:
        raise ValueError(f"No doctests found in {module.__name__}")
