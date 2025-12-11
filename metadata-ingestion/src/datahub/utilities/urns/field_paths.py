# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

def get_simple_field_path_from_v2_field_path(field_path: str) -> str:
    """A helper function to extract simple . path notation from the v2 field path"""

    if field_path.startswith("[version=2.0]"):
        # this is a v2 field path
        tokens = [
            t
            for t in field_path.split(".")
            if not (t.startswith("[") or t.endswith("]"))
        ]
        path = ".".join(tokens)
        return path
    else:
        # not a v2, we assume this is a simple path
        return field_path
