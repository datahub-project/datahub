# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import re


def remove_suffix(inp: str, suffix: str, remove_all: bool = False) -> str:
    while suffix and inp.endswith(suffix):
        inp = inp[: -len(suffix)]
        if not remove_all:
            break
    return inp


def remove_trailing_slashes(url: str) -> str:
    return remove_suffix(url, "/", remove_all=True)


def remove_protocol(url: str) -> str:
    pattern = re.compile(r"https?://")

    return pattern.sub(
        "",
        url,
    )
