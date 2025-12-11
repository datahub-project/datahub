# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import re


def validate_host_port(host_port: str) -> None:
    """
    Validates that a host or host:port string is valid.
    This makes the assumption that the port is optional, and
    requires that there is no proto:// prefix or trailing path.
    """

    # The port can be provided but is not required.
    port = None
    if ":" in host_port:
        (host, port) = host_port.rsplit(":", 1)
    else:
        host = host_port

    assert re.match(
        # This regex is quite loose. Some invalid hostname's or IPs will slip through,
        # but it serves as a good first line of validation. We defer to the underlying
        # system for the remaining validation.
        r"^[\w\-\.\:]+$",
        host,
    ), f"host contains bad characters, found {host}"
    if port is not None:
        assert port.isdigit(), f"port must be all digits, found {port}"
