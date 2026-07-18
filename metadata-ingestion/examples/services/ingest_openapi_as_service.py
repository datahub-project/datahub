"""Ingest an OpenAPI spec into DataHub's Service / API / serviceContract shape.

Emits:

* one :class:`Service` (subtype ``REST_API``) per OpenAPI document, carrying the
  whole spec verbatim in a ``serviceContract`` aspect (``format = OPENAPI``).
  The definition is stored as a ``LargeString`` (gzip+base64 when large) to handle
  large specs without hitting the GMS aspect-size limit.
* one :class:`Api` (subtype ``REST_ENDPOINT``) per path + method, with typed
  input/output signatures parsed from the operation's parameters, requestBody,
  and 200 response schema.
* a ``ServiceProperties.apis`` edge (``ServiceComposesApi``) linking the service
  to every operation it composes.

The per-operation ``api`` entities are the parsed projection of the spec; the
``serviceContract`` blob is the source of truth (it preserves ``components``,
``servers``, and anything the per-operation signatures drop).

Usage::

    export DATAHUB_GMS_URL=http://localhost:8080
    python ingest_openapi_as_service.py

    # Or point at any spec + any write target explicitly:
    python ingest_openapi_as_service.py \
        --spec-url https://my.instance/gms/openapi/v3/api-docs \
        --spec-token "$TOKEN" \
        --service-id my-api
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import ssl
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.api.entities.agent.api import API_SUBTYPE_REST_ENDPOINT, Api, ApiParam
from datahub.api.entities.common.large_string import make_large_string
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.source.openapi_parser import (
    get_schema_from_response,
    resolve_schema_references,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    ServiceDefinitionClass,
    ServiceDefinitionFormatClass,
    ServicePropertiesClass,
    StatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# DataHub catalogs its own API, so the platform is "datahub" (not "openapi").
PLATFORM = "datahub"
REST_API_SUBTYPE = "REST_API"
# Only these carry request/response semantics worth cataloging as an operation.
HTTP_METHODS = ("get", "post", "put", "patch", "delete", "head", "options")
# The service always gets the FULL spec on serviceContract (stored as a
# compressed LargeString); the per-operation api entities are capped so a 1000+
# endpoint spec doesn't flood the graph.
MAX_OPERATIONS = 40

# Defaults for the paved path: read DataHub GMS's own springdoc spec.
DEFAULT_PROFILE = None
DEFAULT_SPEC_PATH = "/openapi/v3/api-docs"
DEFAULT_SERVICE_ID = "datahub-gms-api"


@dataclass
class SpecSource:
    """Where the raw OpenAPI document came from."""

    url: str
    raw_text: str
    spec: dict


@dataclass
class ParsedOperation:
    """One OpenAPI path+method projected into an Api-ready shape."""

    api_id: str
    name: str  # display name, e.g. "GET /entities/{urn}"
    description: Optional[str]
    parameters: List[ApiParam] = field(default_factory=list)
    returns: List[ApiParam] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# 1. Fetch the spec
# --------------------------------------------------------------------------- #
def _read_profile_gms(profile: str) -> tuple[str, Optional[str]]:
    """Return (gms_url, token) from a ``~/.datahub/profiles/<profile>`` dir.

    Reads the token from ``token.env`` (key ``GMS_API_TOKEN``) if present, else
    falls back to the ``token:`` line in ``.datahubenv``. The server URL comes
    from the ``server:`` line in ``.datahubenv``.
    """
    base = os.path.expanduser(f"~/.datahub/profiles/{profile}")
    gms_url: Optional[str] = None
    token: Optional[str] = None

    env_path = os.path.join(base, ".datahubenv")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                stripped = line.strip()
                if stripped.startswith("server:"):
                    gms_url = stripped.split("server:", 1)[1].strip()
                elif stripped.startswith("token:"):
                    val = stripped.split("token:", 1)[1].strip()
                    if val and val.lower() != "null":
                        token = val

    token_env = os.path.join(base, "token.env")
    if os.path.exists(token_env):
        with open(token_env) as f:
            for line in f:
                if line.startswith("GMS_API_TOKEN="):
                    token = line.strip().split("=", 1)[1]

    if not gms_url:
        raise ValueError(f"No server URL found in profile {profile!r} ({env_path})")
    return gms_url, token


def fetch_spec(
    spec_url: Optional[str],
    token: Optional[str],
    profile: str,
    verify_ssl: bool = True,
) -> SpecSource:
    """Fetch the raw OpenAPI document and parse it.

    If ``spec_url`` is not given, the URL is derived from the CLI ``profile``
    (its GMS server + ``DEFAULT_SPEC_PATH``) and the profile's token is used.
    The raw text is preserved verbatim for the serviceContract blob.
    """
    if not spec_url:
        gms_url, profile_token = _read_profile_gms(profile)
        spec_url = gms_url.rstrip("/") + DEFAULT_SPEC_PATH
        if token is None:
            token = profile_token

    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    ctx: Optional[ssl.SSLContext] = None
    if not verify_ssl:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(spec_url, headers=headers)
    logger.info("Fetching OpenAPI spec from %s", spec_url)
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        raw_bytes = resp.read()
    raw_text = raw_bytes.decode("utf-8")

    spec = json.loads(raw_text)
    if not (spec.get("openapi") or spec.get("swagger")):
        raise ValueError(
            f"Fetched document from {spec_url} is not a valid OpenAPI/Swagger spec "
            "(missing 'openapi'/'swagger' key)."
        )
    if "paths" not in spec:
        raise ValueError(f"OpenAPI spec from {spec_url} has no 'paths'.")

    info = spec.get("info", {})
    logger.info(
        "Loaded spec: title=%r version=%r paths=%d",
        info.get("title"),
        info.get("version"),
        len(spec["paths"]),
    )
    return SpecSource(url=spec_url, raw_text=raw_text, spec=spec)


# --------------------------------------------------------------------------- #
# 2. Parse operations (one Api per path+method)
# --------------------------------------------------------------------------- #
def _normalize_path(path: str) -> str:
    """`/entities/{urn}` -> `entities.urn` (leading slash off, / -> ., braces gone).

    Spring wildcard mappings (``/admin/dashboard/**``) also drop their ``*`` so the
    minted urn id stays clean.
    """
    stripped = path.lstrip("/")
    stripped = stripped.replace("{", "").replace("}", "").replace("*", "")
    stripped = stripped.replace("/", ".")
    # Collapse any doubled dots left by stripped segments and trim edges.
    while ".." in stripped:
        stripped = stripped.replace("..", ".")
    return stripped.strip(".")


def _pick_json_content(content: Dict[str, dict]) -> Optional[dict]:
    """Pick the JSON media type from a content map, tolerating charset suffixes.

    springdoc emits e.g. ``application/json;charset=utf-8`` and ``*/*``, so an
    exact ``application/json`` lookup misses. Match by prefix instead.
    """
    if not content:
        return None
    for ct, media in content.items():
        if ct.split(";", 1)[0].strip().lower() == "application/json":
            return media
    # Fall back to */* or the first entry that carries a schema.
    for media in content.values():
        if media.get("schema"):
            return media
    return None


def _schema_top_level_params(schema: Optional[dict]) -> List[ApiParam]:
    """Turn a resolved object schema's top-level properties into ApiParams.

    Nesting is represented via the ``data_type`` hint (``array<Order>``,
    ``object``) rather than exploded into dotted params.
    """
    if not schema or not isinstance(schema, dict):
        return []
    props = schema.get("properties")
    if not props:
        return []
    required = set(schema.get("required", []) or [])
    out: List[ApiParam] = []
    for prop_name, prop_schema in props.items():
        out.append(
            ApiParam(
                name=prop_name,
                data_type=_type_hint(prop_schema),
                required=prop_name in required,
                description=(prop_schema or {}).get("description"),
            )
        )
    return out


def _type_hint(schema: Optional[dict]) -> str:
    """Friendly type hint for a property schema, keeping nesting as a hint."""
    if not isinstance(schema, dict):
        return "string"
    t = schema.get("type")
    if t == "array":
        items = schema.get("items") or {}
        inner = items.get("$ref", "").split("/")[-1] or items.get("type") or "object"
        return f"array<{inner}>"
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1] or "object"
    return t or "object"


def _param_data_type(param: dict) -> str:
    schema = param.get("schema") or {}
    return _type_hint(schema) if schema else "string"


def parse_operations(source: SpecSource) -> List[ParsedOperation]:
    """Enumerate every path+method as a ParsedOperation.

    NOTE: we deliberately do NOT use ``openapi_parser.get_endpoints`` for
    enumeration -- it keys by path and keeps a single method per path, silently
    collapsing e.g. GET/DELETE on the same path into one endpoint. Splitting one
    ``api`` per path+method is exactly the improvement this example demonstrates,
    so we iterate ``spec["paths"][path][method]`` explicitly. We still reuse
    ``get_schema_from_response`` / ``resolve_schema_references`` for $ref work.
    """
    spec = source.spec
    service_id_ops: List[ParsedOperation] = []

    for path in sorted(spec["paths"].keys()):
        path_item = spec["paths"][path]
        # Path-level shared parameters apply to every method on the path.
        shared_params = path_item.get("parameters", []) or []
        for method in HTTP_METHODS:
            op = path_item.get(method)
            if not isinstance(op, dict):
                continue

            api_id = f"{{sid}}.{method}.{_normalize_path(path)}"
            name = f"{method.upper()} {path}"
            description = op.get("summary") or op.get("description")

            # ---- input fields: path/query/header params + requestBody props ---
            in_params: List[ApiParam] = []
            for param in shared_params + (op.get("parameters", []) or []):
                if not isinstance(param, dict) or "name" not in param:
                    continue
                in_params.append(
                    ApiParam(
                        name=param["name"],
                        data_type=_param_data_type(param),
                        required=bool(param.get("required", False)),
                        description=param.get("description"),
                    )
                )

            request_body = op.get("requestBody")
            if isinstance(request_body, dict):
                media = _pick_json_content(request_body.get("content", {}) or {})
                if media and media.get("schema"):
                    resolved = get_schema_from_response(media["schema"], spec)
                    if resolved is None:
                        resolved = resolve_schema_references(media["schema"], spec)
                    in_params.extend(_schema_top_level_params(resolved))

            # ---- output fields: 200 response json schema top-level props -------
            out_params: List[ApiParam] = []
            responses = op.get("responses", {}) or {}
            r200 = responses.get("200") or responses.get(200)
            if isinstance(r200, dict):
                media = _pick_json_content(r200.get("content", {}) or {})
                if media and media.get("schema"):
                    resolved = get_schema_from_response(media["schema"], spec)
                    if resolved is None:
                        resolved = resolve_schema_references(media["schema"], spec)
                    out_params = _schema_top_level_params(resolved)

            service_id_ops.append(
                ParsedOperation(
                    api_id=api_id,
                    name=name,
                    description=description,
                    parameters=in_params,
                    returns=out_params,
                )
            )

    return service_id_ops


# --------------------------------------------------------------------------- #
# 3. Build the Service + its Apis
# --------------------------------------------------------------------------- #
def build_service(
    source: SpecSource,
    service_id: str,
    operations: List[ParsedOperation],
    max_operations: int = MAX_OPERATIONS,
) -> tuple[List[Api], List[MetadataChangeProposalWrapper]]:
    """Build the Api helpers and the Service's MCPs from parsed operations.

    Returns ``(apis, service_mcps)``. The Service always carries the FULL spec on
    ``serviceContract`` regardless of the operation cap.
    """
    info = source.spec.get("info", {})
    docs_url = source.url  # springdoc doc URL doubles as the operations' docs link

    total = len(operations)
    emitted_ops = operations
    if total > max_operations:
        # When capping, prefer operations that actually carry a typed signature
        # (params/returns) so the demo surfaces meaningful endpoints instead of
        # Spring wildcard/health mappings. Deterministic: signature-richness desc,
        # then the original sorted-path order (operations already arrive sorted).
        ranked = sorted(
            enumerate(operations),
            key=lambda iop: (
                -(len(iop[1].parameters) + len(iop[1].returns)),
                iop[0],
            ),
        )
        emitted_ops = [op for _, op in ranked[:max_operations]]
        emitted_names = {op.name for op in emitted_ops}
        skipped = [op.name for op in operations if op.name not in emitted_names]
        logger.warning(
            "Spec has %d operations; capping api entities at %d. Skipping %d "
            "operation(s) (serviceContract still carries the FULL spec): %s",
            total,
            max_operations,
            len(skipped),
            ", ".join(skipped[:20]) + (" ..." if len(skipped) > 20 else ""),
        )

    apis: List[Api] = [
        Api(
            id=op.api_id.format(sid=service_id),
            name=op.name,
            subtypes=[API_SUBTYPE_REST_ENDPOINT],
            description=op.description,
            platform=PLATFORM,
            external_url=docs_url,
            parameters=op.parameters or None,
            returns=op.returns or None,
        )
        for op in emitted_ops
    ]

    urn = f"urn:li:service:{service_id}"

    contract_def = make_large_string(source.raw_text)
    logger.info(
        "serviceContract carries the FULL spec: %d uncompressed bytes stored as "
        "%s (%d blob bytes).",
        contract_def.uncompressedSize,
        contract_def.compression,
        len(contract_def.blob.encode("utf-8")),
    )

    # apis field is filled with real urns after emit; placeholder here.
    service_aspects = [
        ServicePropertiesClass(
            displayName=info.get("title") or service_id,
            description=info.get("description"),
            apis=None,  # filled in main() after api.emit() returns urns
        ),
        ServiceDefinitionClass(
            # The FULL spec verbatim, stored as a compressed LargeString so even a
            # multi-MB spec fits under the aspect-size limit (no lossy trimming).
            format=ServiceDefinitionFormatClass.OPENAPI,
            rawSpec=contract_def,
            version=info.get("version"),
            externalUrl=source.url,
        ),
        SubTypesClass(typeNames=[REST_API_SUBTYPE]),
        DataPlatformInstanceClass(platform=make_data_platform_urn(PLATFORM)),
        StatusClass(removed=False),
    ]
    service_mcps = [
        MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)
        for aspect in service_aspects
    ]
    return apis, service_mcps


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--spec-url",
        default=os.environ.get("OPENAPI_SPEC_URL"),
        help="OpenAPI/springdoc doc URL. Default: derived from --profile.",
    )
    ap.add_argument(
        "--spec-token",
        default=os.environ.get("OPENAPI_SPEC_TOKEN"),
        help="Bearer token for the spec URL. Default: read from --profile.",
    )
    ap.add_argument(
        "--profile",
        default=os.environ.get("OPENAPI_SPEC_PROFILE", DEFAULT_PROFILE),
        help=f"CLI profile to read the spec URL+token from (default: {DEFAULT_PROFILE}).",
    )
    ap.add_argument(
        "--service-id",
        default=os.environ.get("OPENAPI_SERVICE_ID", DEFAULT_SERVICE_ID),
        help=f"Service id (urn:li:service:<id>). Default: {DEFAULT_SERVICE_ID}.",
    )
    ap.add_argument(
        "--max-operations",
        type=int,
        default=int(os.environ.get("OPENAPI_MAX_OPERATIONS", MAX_OPERATIONS)),
        help=f"Max api entities to emit (default {MAX_OPERATIONS}).",
    )
    ap.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="Disable TLS verification when fetching the spec.",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()

    # Deeply-nested DataHub schemas trip the parser's recursion-depth guard on
    # nearly every operation; those per-op WARNINGs are benign (partial schema is
    # still usable) but drown the log. Silence them to ERROR for the parse pass.
    logging.getLogger("datahub.ingestion.source.openapi_parser").setLevel(logging.ERROR)

    source = fetch_spec(
        spec_url=args.spec_url,
        token=args.spec_token,
        profile=args.profile,
        verify_ssl=not args.no_verify_ssl,
    )
    operations = parse_operations(source)
    logger.info("Parsed %d operation(s) from the spec.", len(operations))

    apis, service_mcps = build_service(
        source, args.service_id, operations, max_operations=args.max_operations
    )

    # Write to the target graph (DATAHUB_GMS_URL), NOT the spec source.
    graph = get_default_graph()

    api_urns = [api.emit(graph) for api in apis]

    # Re-stamp ServiceProperties with the real api urns (ServiceComposesApi edge).
    urn = f"urn:li:service:{args.service_id}"
    info = source.spec.get("info", {})
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=ServicePropertiesClass(
                displayName=info.get("title") or args.service_id,
                description=info.get("description"),
                apis=api_urns or None,
            ),
        )
    )
    # Emit the remaining service aspects (contract, subtypes, platform, status).
    for mcp in service_mcps:
        if isinstance(mcp.aspect, ServicePropertiesClass):
            continue  # already emitted with real api urns above
        graph.emit_mcp(mcp)

    logger.info(
        "Registered Service %s composing %d API(s) with an OpenAPI serviceContract.",
        urn,
        len(api_urns),
    )
    for api_urn in api_urns[:10]:
        logger.info("  %s", api_urn)
    if len(api_urns) > 10:
        logger.info("  ... and %d more", len(api_urns) - 10)


if __name__ == "__main__":
    main()
