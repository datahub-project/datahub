# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import TYPE_CHECKING

try:
    # Via https://stackoverflow.com/a/65147676
    if not TYPE_CHECKING and __sphinx_build__:
        IS_SPHINX_BUILD = True

except NameError:
    IS_SPHINX_BUILD = False
