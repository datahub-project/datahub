import itertools
import shlex
from typing import List, Union

import requests


def _format_header(name: str, value: Union[str, bytes]) -> str:
    if name == "Authorization":
        return f"{name!s}: <redacted>"
    return f"{name!s}: {value!s}"


def make_curl_command(
    session: requests.Session, method: str, url: str, payload: str
) -> str:
    fragments: List[str] = [
        "curl",
        *itertools.chain(
            *[
                ("-X", method),
                *[("-H", _format_header(k, v)) for (k, v) in session.headers.items()],
                ("--data", payload),
            ]
        ),
        url,
    ]
    return shlex.join(fragments)
