# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import List

global_warnings: List = []


def add_global_warning(warn: str) -> None:
    global_warnings.append(warn)


def get_global_warnings() -> List:
    return global_warnings


def clear_global_warnings() -> None:
    global_warnings.clear()
