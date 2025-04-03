import shlex
from typing import List, Optional, Union

import requests
from requests.auth import HTTPBasicAuth


def _format_header(name: str, value: Union[str, bytes]) -> str:
    if name == "Authorization":
        return f"{name!s}: <redacted>"
    return f"{name!s}: {value!s}"


def make_curl_command(
    session: requests.Session, method: str, url: str, payload: Optional[str] = None
) -> str:
    fragments: List[str] = ["curl", "-X", method]

    for header_name, header_value in session.headers.items():
        fragments.extend(["-H", _format_header(header_name, header_value)])

    if session.auth:
        if isinstance(session.auth, HTTPBasicAuth):
            fragments.extend(["-u", f"{session.auth.username}:<redacted>"])
        else:
            # For other auth types, they should be handled via headers
            fragments.extend(["-H", "<unknown auth type>"])

    if payload:
        fragments.extend(["--data", payload])

    fragments.append(url)
    return shlex.join(fragments)
