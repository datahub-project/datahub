# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Optional, TypeVar

_T = TypeVar("_T")


def assert_not_null(value: Optional[_T]) -> _T:
    assert value is not None, "value is unexpectedly None"
    return value
