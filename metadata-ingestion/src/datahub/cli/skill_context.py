"""Resolve the agent skill that invoked the CLI into a client component label.

Agent harnesses pass ``-C skill=<name>`` on the root command (see
``datahub --help``). That value becomes the ``component`` half of the
User-Agent every request carries, so server-side usage attribution can tell
which skill drove the traffic rather than just seeing "some CLI"::

    DataHub-Client/1.0 (cli; skill-datahub-search/claude-code; 1.7.0)

The ``caller`` half (``claude-code`` above) is detected separately by
:mod:`datahub.utilities.caller_context`.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Key read from the `-C key=value` pairs parsed onto the Click context.
SKILL_CONTEXT_KEY = "skill"

# Namespaces skill values so they can't collide with the components real
# integrations report (`airflow-plugin`, `datahub`, ...).
COMPONENT_PREFIX = "skill-"

# GMS parses the User-Agent by splitting the comment group on ";" and the
# component/caller pair on "/", so those characters (and anything else that
# would corrupt a header) cannot survive into the component. See
# metadata-operation-context/src/main/resources/datahub_user_agents.yaml.
_UNSAFE_CHARS = re.compile(r"[^a-z0-9._-]+")

# Long enough for any real skill name; short enough that a junk value can't
# bloat every request's headers.
_MAX_LENGTH = 64


def sanitize_skill_component(raw: str) -> Optional[str]:
    """Normalize a raw `-C skill=` value into a User-Agent-safe component.

    Returns None when nothing usable is left after cleaning.
    """
    cleaned = _UNSAFE_CHARS.sub("-", raw.strip().lower()).strip("-")
    if not cleaned:
        return None

    if not cleaned.startswith(COMPONENT_PREFIX):
        cleaned = f"{COMPONENT_PREFIX}{cleaned}"

    return cleaned[:_MAX_LENGTH].rstrip("-")


def infer_skill_component() -> Optional[str]:
    """Component label for the skill that invoked this CLI process, if any.

    Returns None outside the CLI (no Click context) and when no `-C skill=`
    pair was passed, which leaves the component resolving to DATAHUB_COMPONENT
    exactly as before.
    """
    try:
        import click

        ctx = click.get_current_context(silent=True)
        if ctx is None or not isinstance(ctx.obj, dict):
            return None

        raw = (ctx.obj.get("context") or {}).get(SKILL_CONTEXT_KEY)
        if not isinstance(raw, str) or not raw.strip():
            return None

        return sanitize_skill_component(raw)
    except Exception as e:
        # Attribution is best-effort telemetry; it must never fail a command.
        logger.debug(f"Could not resolve skill attribution context: {e}")
        return None
