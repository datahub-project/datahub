from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

# Bounds that keep a probe cheap and its output finite regardless of how large or
# deeply nested a live response is. They are defense against a pathological API,
# not tuning knobs, so they are module constants rather than config.
_MAX_FIELDS = 500
_MAX_DEPTH = 8
_ARRAY_SAMPLE = 20
_MAX_BYTES = 1_000_000
_JSON_CONTENT_HINT = "json"


def _json_type(value: object) -> str:
    # bool must be checked before int (bool is an int subclass in Python).
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


@dataclass
class ApiFieldShape:
    path: str  # dotted JSON path; array elements are marked with "[]"
    json_type: str
    nullable: bool = False

    def to_dict(self) -> Dict[str, object]:
        return {
            "path": self.path,
            "json_type": self.json_type,
            "nullable": self.nullable,
        }


@dataclass
class ApiEndpointProbe:
    endpoint: str
    status: Optional[int] = None
    content_type: Optional[str] = None
    fields: List[ApiFieldShape] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "endpoint": self.endpoint,
            "status": self.status,
            "content_type": self.content_type,
            "fields": [f.to_dict() for f in self.fields],
            "error": self.error,
        }


@dataclass
class ApiProbeResult:
    base_url: str
    supported: bool = True
    endpoints: List[ApiEndpointProbe] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "base_url": self.base_url,
            "supported": self.supported,
            "endpoints": [e.to_dict() for e in self.endpoints],
            "warnings": self.warnings,
        }


def infer_shape(value: object, prefix: str = "") -> List[ApiFieldShape]:
    """Record the structural shape of a JSON value — paths, types, nullability.

    Never records a value. This is the REST analog of the SQL probe's
    "names/counts only": the field names and their types cross the boundary, the
    data does not. Arrays are collapsed to a single "[]" path (a bounded sample of
    elements is inspected so a nullable/union member is still caught).
    """
    fields: List[ApiFieldShape] = []
    by_path: Dict[str, ApiFieldShape] = {}

    def record(path: str, json_type: str) -> None:
        existing = by_path.get(path)
        if existing is None:
            if len(fields) >= _MAX_FIELDS:
                return
            shape = ApiFieldShape(path, json_type, nullable=json_type == "null")
            by_path[path] = shape
            fields.append(shape)
            return
        # Merge repeated observations of the same path (e.g. across array items):
        # a null anywhere makes the field nullable; a concrete type replaces a
        # placeholder "null".
        if json_type == "null":
            existing.nullable = True
        elif existing.json_type == "null":
            existing.json_type = json_type

    def walk(node: object, path: str, depth: int) -> None:
        if depth > _MAX_DEPTH or len(fields) >= _MAX_FIELDS:
            return
        if path:
            record(path, _json_type(node))
        if isinstance(node, dict):
            for key, child in node.items():
                child_path = f"{path}.{key}" if path else str(key)
                walk(child, child_path, depth + 1)
        elif isinstance(node, list):
            for item in node[:_ARRAY_SAMPLE]:
                walk(item, f"{path}[]", depth + 1)

    walk(value, prefix, 0)
    return fields


def probe_api(
    base_url: str,
    endpoints: Sequence[str],
    headers: Optional[Dict[str, str]] = None,
    budget: int = 10,
    timeout: int = 15,
    verify_ssl: bool = True,
) -> ApiProbeResult:
    """Interrogate a REST source with no connector: GET each endpoint and report
    the response shape only. Read-only by construction (only GET is ever issued),
    budget-bounded, and value-free — safe to run against a live system the agent
    has never seen.
    """
    # Lazy import: requests is only needed when a REST probe actually runs, and it
    # keeps the agent package importable without pulling HTTP deps at import time.
    import json as _json

    import requests

    result = ApiProbeResult(base_url=base_url)
    if not verify_ssl:
        result.warnings.append(
            "verify_ssl is disabled; TLS certificate validation is off."
        )
    if budget > 0 and len(endpoints) > budget:
        result.warnings.append(
            f"probed {budget} of {len(endpoints)} endpoints (request budget)."
        )

    session = requests.Session()
    if headers:
        session.headers.update(headers)
    try:
        for endpoint in list(endpoints)[:budget]:
            url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
            probe = ApiEndpointProbe(endpoint=endpoint)
            resp = None
            try:
                # GET only: the read-only guarantee is that no other verb exists here.
                resp = session.get(url, timeout=timeout, verify=verify_ssl, stream=True)
                probe.status = resp.status_code
                probe.content_type = resp.headers.get("Content-Type")
                body = _read_bounded(resp)
                if _JSON_CONTENT_HINT not in (probe.content_type or "").lower():
                    probe.error = f"non-JSON response ({probe.content_type})"
                    continue
                try:
                    probe.fields = infer_shape(_json.loads(body))
                except ValueError:
                    probe.error = "response body is not valid JSON"
            except requests.RequestException as exc:
                probe.error = str(exc)
            finally:
                if resp is not None:
                    resp.close()
                result.endpoints.append(probe)
    finally:
        session.close()
    return result


def _read_bounded(resp: object) -> str:
    # Read at most _MAX_BYTES so a huge response can't blow up the probe; decode as
    # UTF-8, tolerating malformed bytes (we only need enough to parse the shape).
    chunks: List[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=8192):  # type: ignore[attr-defined]
        if not chunk:
            continue
        chunks.append(chunk)
        total += len(chunk)
        if total >= _MAX_BYTES:
            break
    return b"".join(chunks)[:_MAX_BYTES].decode("utf-8", errors="replace")
