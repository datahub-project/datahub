from __future__ import annotations

from typing import Dict, Optional

from lib.graphql import execute_graphql, log, meets_expectation, resolve_operation_document
from lib.graphql_adapt import GraphqlQueryRegistry
from lib.persona_password_seed import resolve_persona_password
from lib.personas import PersonaOracle
from lib.session import PersonaSession, login_persona


def smoke_persona_login(
    persona: str,
    oracle: PersonaOracle,
    gms_url: str,
    registry: GraphqlQueryRegistry,
    frontend_url: Optional[str] = None,
) -> PersonaSession:
    ps = login_persona(
        persona,
        resolve_persona_password(persona, gms_url),
        gms_url,
        frontend_url=frontend_url,
    )
    document = resolve_operation_document(registry, "getMe")
    result = execute_graphql(ps.session, ps.frontend_url, document, "getMe", {})
    if not meets_expectation(result, 200):
        raise RuntimeError(
            f"Persona login smoke failed for {persona}: "
            f"{result.failure_label or 'getMe request failed'}"
        )
    corp = result.data.get("data", {}).get("me", {}).get("corpUser", {})
    urn = corp.get("urn")
    if urn != oracle.user_urn:
        raise RuntimeError(
            f"Persona login smoke failed for {persona}: expected urn {oracle.user_urn}, got {urn!r}"
        )
    log(f"init ok: {persona} -> {urn}")
    return ps
