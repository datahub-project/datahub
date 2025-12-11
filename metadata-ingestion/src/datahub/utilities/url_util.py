# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import re


def remove_port_from_url(base_url: str) -> str:
    m = re.match("^(.*):([0-9]+)$", base_url)
    if m is not None:
        base_url = m[1]
    return base_url
