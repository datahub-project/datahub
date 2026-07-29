from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

import requests
from datahub.cli.cli_utils import (
    get_frontend_session_login_as,
    guess_frontend_url_from_gms_url,
)


@dataclass
class PersonaSession:
    persona: str
    user_urn: str
    frontend_url: str
    session: requests.Session
    login_count: int = 1

    def get_session(self) -> requests.Session:
        return self.session


_login_lock = threading.Lock()


def login_persona(
    persona: str,
    password: str,
    gms_url: str,
    frontend_url: Optional[str] = None,
) -> PersonaSession:
    fe_url = frontend_url or guess_frontend_url_from_gms_url(gms_url)
    with _login_lock:
        session = get_frontend_session_login_as(persona, password, fe_url)
    return PersonaSession(
        persona=persona,
        user_urn=f"urn:li:corpuser:{persona}",
        frontend_url=fe_url,
        session=session,
        login_count=1,
    )
