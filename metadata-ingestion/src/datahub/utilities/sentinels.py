# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from enum import Enum
from typing import Final

# It's pretty annoying to create a true sentinel that works with typing.
# This approach using enums is inspired by:
# https://peps.python.org/pep-0484/#support-for-singleton-types-in-unions
#
# Can't wait for https://peps.python.org/pep-0661/


class Unset(Enum):
    token = 0


unset: Final = Unset.token


class Auto(Enum):
    token = 0


auto: Final = Auto.token
