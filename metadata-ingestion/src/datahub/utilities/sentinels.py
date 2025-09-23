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
