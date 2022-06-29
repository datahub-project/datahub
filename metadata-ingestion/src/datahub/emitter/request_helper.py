import itertools
import shlex
from typing import List

import requests


def _make_curl_command(
    session: requests.Session, method: str, url: str, payload: str
) -> str:
    fragments: List[str] = [
        "curl",
        *itertools.chain(
            *[
                ("-X", method),
                *[("-H", f"{k!s}: {v!s}") for (k, v) in session.headers.items()],
                ("--data", payload),
            ]
        ),
        url,
    ]
    return " ".join(shlex.quote(fragment) for fragment in fragments)
