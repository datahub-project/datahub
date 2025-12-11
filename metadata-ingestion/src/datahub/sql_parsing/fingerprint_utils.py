# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import hashlib


def generate_hash(text: str) -> str:
    # Once we move to Python 3.9+, we can set `usedforsecurity=False`.
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
