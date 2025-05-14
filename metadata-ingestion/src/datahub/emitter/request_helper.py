import json
import shlex
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import requests
from requests.auth import HTTPBasicAuth

from datahub.emitter.aspect import JSON_CONTENT_TYPE, JSON_PATCH_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import ChangeTypeClass


def _decode_bytes(value: Union[str, bytes]) -> str:
    """Decode bytes to string, if necessary."""
    if isinstance(value, bytes):
        return value.decode()
    return value


def _format_header(name: str, value: Union[str, bytes]) -> str:
    if name == "Authorization":
        return f"{name!s}: <redacted>"
    return f"{name!s}: {_decode_bytes(value)}"


def make_curl_command(
    session: requests.Session, method: str, url: str, payload: Optional[str] = None
) -> str:
    fragments: List[str] = ["curl", "-X", method]

    for header_name, header_value in session.headers.items():
        fragments.extend(["-H", _format_header(header_name, header_value)])

    if session.auth:
        if isinstance(session.auth, HTTPBasicAuth):
            fragments.extend(
                ["-u", f"{_decode_bytes(session.auth.username)}:<redacted>"]
            )
        else:
            # For other auth types, they should be handled via headers
            fragments.extend(["-H", "<unknown auth type>"])

    if payload:
        fragments.extend(["--data", payload])

    fragments.append(url)
    return shlex.join(fragments)


@dataclass
class OpenApiRequest:
    """Represents an OpenAPI request for entity operations."""

    method: str
    url: str
    payload: List[Dict[str, Any]]

    @classmethod
    def from_mcp(
        cls,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        gms_server: str,
        async_flag: bool = False,
        search_sync_flag: bool = False,
    ) -> Optional["OpenApiRequest"]:
        """Factory method to create an OpenApiRequest from a MetadataChangeProposal."""
        if not mcp.aspectName or (
            mcp.changeType != ChangeTypeClass.DELETE and not mcp.aspect
        ):
            return None

        method = "post"
        url = f"{gms_server}/openapi/v3/entity/{mcp.entityType}?async={'true' if async_flag else 'false'}"
        payload = []

        if mcp.changeType == ChangeTypeClass.DELETE:
            method = "delete"
            url = f"{gms_server}/openapi/v3/entity/{mcp.entityType}/{mcp.entityUrn}"
        else:
            if mcp.aspect:
                mcp_headers = {}

                if not async_flag and search_sync_flag:
                    mcp_headers["X-DataHub-Sync-Index-Update"] = "true"

                if mcp.changeType == ChangeTypeClass.PATCH:
                    method = "patch"
                    obj = mcp.aspect.to_obj()
                    content_type = obj.get("contentType")
                    if obj.get("value") and content_type == JSON_PATCH_CONTENT_TYPE:
                        # Undo double serialization.
                        obj = json.loads(obj["value"])
                        patch_value = obj
                    else:
                        raise NotImplementedError(
                            f"ChangeType {mcp.changeType} only supports context type {JSON_PATCH_CONTENT_TYPE}, found {content_type}."
                        )

                    if isinstance(patch_value, list):
                        patch_value = {"patch": patch_value}

                    payload = [
                        {
                            "urn": mcp.entityUrn,
                            mcp.aspectName: {
                                "value": patch_value,
                                "systemMetadata": mcp.systemMetadata.to_obj()
                                if mcp.systemMetadata
                                else None,
                                "headers": mcp_headers,
                            },
                        }
                    ]
                else:
                    if isinstance(mcp, MetadataChangeProposalWrapper):
                        aspect_value = pre_json_transform(
                            mcp.to_obj(simplified_structure=True)
                        )["aspect"]["json"]
                    else:
                        obj = mcp.aspect.to_obj()
                        content_type = obj.get("contentType")
                        if obj.get("value") and content_type == JSON_CONTENT_TYPE:
                            # Undo double serialization.
                            obj = json.loads(obj["value"])
                        elif content_type == JSON_PATCH_CONTENT_TYPE:
                            raise NotImplementedError(
                                f"ChangeType {mcp.changeType} does not support patch."
                            )
                        aspect_value = pre_json_transform(obj)

                    payload = [
                        {
                            "urn": mcp.entityUrn,
                            mcp.aspectName: {
                                "value": aspect_value,
                                "systemMetadata": mcp.systemMetadata.to_obj()
                                if mcp.systemMetadata
                                else None,
                                "headers": mcp_headers,
                            },
                        }
                    ]
            else:
                raise ValueError(f"ChangeType {mcp.changeType} requires a value.")

        return cls(method=method, url=url, payload=payload)
