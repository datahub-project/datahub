"""Helpers for decoding Presto/Athena view definitions.

Presto, Trino and Athena register catalog views whose stored "view text" is a
base64-encoded JSON document wrapped in a marker comment, e.g.::

    /* Presto View: <base64-of-json> */

The decoded JSON carries ``originalSql`` (the view's SQL), ``columns`` and other
fields. Several connectors (Hive Metastore, Glue) encounter this same encoding, so
the decode logic lives here to avoid duplication.
"""

import base64
import json
from typing import Any, Dict, List, TypedDict, cast

# Markers wrapping the base64-encoded payload of a Presto/Athena view.
PRESTO_VIEW_PREFIX = "/* Presto View: "
PRESTO_VIEW_SUFFIX = " */"


class PrestoViewDefinition(TypedDict, total=False):
    """Decoded payload of a Presto/Athena view.

    ``total=False`` because the decoder does not guarantee these keys are present
    (a malformed payload may omit them); only the documented subset that consumers
    rely on is declared.
    """

    originalSql: str
    columns: List[Dict[str, Any]]


def is_presto_view(view_text: str) -> bool:
    """Return True if ``view_text`` is a Presto/Athena base64-encoded view."""
    return view_text.startswith(PRESTO_VIEW_PREFIX)


def decode_presto_view(view_text: str) -> PrestoViewDefinition:
    """Decode a Presto/Athena view envelope into its JSON payload.

    Expects text of the form ``/* Presto View: <base64> */`` and returns the parsed
    JSON object (typically with ``originalSql`` and ``columns`` keys).

    Raises on malformed input (invalid base64, non-JSON, or a non-object payload).
    Callers that want best-effort behaviour should catch the exception.
    """
    encoded = view_text.split(PRESTO_VIEW_PREFIX, 1)[-1].rsplit(PRESTO_VIEW_SUFFIX, 1)[
        0
    ]
    payload = json.loads(base64.b64decode(encoded))
    if not isinstance(payload, dict):
        raise ValueError("Presto view payload is not a JSON object")
    return cast(PrestoViewDefinition, payload)
