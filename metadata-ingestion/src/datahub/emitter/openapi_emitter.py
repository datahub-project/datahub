import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from datahub.cli.cli_utils import ensure_has_system_metadata
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.openapi_tracer import OpenAPITrace
from datahub.emitter.response_helper import extract_trace_data
from datahub.emitter.rest_emitter import (
    _DATAHUB_EMITTER_TRACE,
    BATCH_INGEST_MAX_PAYLOAD_LENGTH,
    INGEST_MAX_PAYLOAD_BYTES,
    DataHubRestEmitter,
)
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    items: List[str]
    total_bytes: int = 0

    def add_item(self, item: str) -> bool:
        item_bytes = len(item.encode())
        if not self.items:  # Always add at least one item even if over byte limit
            self.items.append(item)
            self.total_bytes += item_bytes
            return True
        self.items.append(item)
        self.total_bytes += item_bytes
        return True

    @staticmethod
    def join(chunk: "Chunk") -> str:
        return "[" + ",".join(chunk.items) + "]"


class DataHubOpenApiEmitter(DataHubRestEmitter, OpenAPITrace):
    def __init__(
        self,
        gms_server: str,
        token: Optional[str] = None,
        timeout_sec: Optional[float] = None,
        connect_timeout_sec: Optional[float] = None,
        read_timeout_sec: Optional[float] = None,
        retry_status_codes: Optional[List[int]] = None,
        retry_methods: Optional[List[str]] = None,
        retry_max_times: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        ca_certificate_path: Optional[str] = None,
        client_certificate_path: Optional[str] = None,
        disable_ssl_verification: bool = False,
        default_trace_mode: bool = False,
    ):
        super().__init__(
            gms_server,
            token,
            timeout_sec,
            connect_timeout_sec,
            read_timeout_sec,
            retry_status_codes,
            retry_methods,
            retry_max_times,
            extra_headers,
            ca_certificate_path,
            client_certificate_path,
            disable_ssl_verification,
            default_trace_mode,
        )

    def _to_request(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        async_flag: Optional[bool] = None,
        async_default: bool = False,
    ) -> Optional[Tuple[str, List[Dict[str, Any]]]]:
        if mcp.aspect and mcp.aspectName:
            resolved_async_flag = (
                async_flag if async_flag is not None else async_default
            )
            url = f"{self._gms_server}/openapi/v3/entity/{mcp.entityType}?async={'true' if resolved_async_flag else 'false'}"
            ensure_has_system_metadata(mcp)
            if isinstance(mcp, MetadataChangeProposalWrapper):
                aspect_value = pre_json_transform(
                    mcp.to_obj(simplified_structure=True)
                )["aspect"]["json"]
            else:
                obj = mcp.aspect.to_obj()
                if obj.get("value") and obj.get("contentType") == JSON_CONTENT_TYPE:
                    obj = json.loads(obj["value"])
                aspect_value = pre_json_transform(obj)
            return (
                url,
                [
                    {
                        "urn": mcp.entityUrn,
                        mcp.aspectName: {
                            "value": aspect_value,
                            "systemMetadata": mcp.systemMetadata.to_obj()
                            if mcp.systemMetadata
                            else None,
                        },
                    }
                ],
            )
        return None

    def emit_mcp(
        self,
        mcp: Union[MetadataChangeProposal, MetadataChangeProposalWrapper],
        async_flag: Optional[bool] = None,
        trace_flag: Optional[bool] = None,
        trace_timeout: Optional[timedelta] = None,
    ) -> None:
        request = self._to_request(mcp, async_flag)

        if request:
            response = self._emit_generic_payload(request[0], payload=request[1])

            if self._should_trace(async_flag, trace_flag):
                trace_data = extract_trace_data(response) if response else None
                if trace_data:
                    self.await_status([trace_data], trace_timeout)

    def emit_mcps(
        self,
        mcps: Sequence[Union[MetadataChangeProposal, MetadataChangeProposalWrapper]],
        async_flag: Optional[bool] = None,
        trace_flag: Optional[bool] = None,
        trace_timeout: Optional[timedelta] = None,
    ) -> int:
        """
        1. Grouping MCPs by their entity URL
        2. Breaking down large batches into smaller chunks based on both:
         * Total byte size (INGEST_MAX_PAYLOAD_BYTES)
         * Maximum number of items (BATCH_INGEST_MAX_PAYLOAD_LENGTH)

        The Chunk class encapsulates both the items and their byte size tracking
        Serializing the items only once with json.dumps(request[1]) and reusing that
        The chunking logic handles edge cases (always accepting at least one item per chunk)
        The joining logic is efficient with a simple string concatenation

        :param mcps: metadata change proposals to transmit
        :param async_flag: the mode
        :return:
        """
        if _DATAHUB_EMITTER_TRACE:
            logger.debug(f"Attempting to emit MCP batch of size {len(mcps)}")

        # group by entity url
        batches: Dict[str, List[Chunk]] = defaultdict(
            lambda: [Chunk(items=[])]
        )  # Initialize with one empty Chunk

        for mcp in mcps:
            request = self._to_request(mcp, async_flag, async_default=True)
            if request:
                current_chunk = batches[request[0]][-1]  # Get the last chunk
                # Only serialize once
                serialized_item = json.dumps(request[1][0])
                item_bytes = len(serialized_item.encode())

                # If adding this item would exceed max_bytes, create a new chunk
                # Unless the chunk is empty (always add at least one item)
                if current_chunk.items and (
                    current_chunk.total_bytes + item_bytes > INGEST_MAX_PAYLOAD_BYTES
                    or len(current_chunk.items) >= BATCH_INGEST_MAX_PAYLOAD_LENGTH
                ):
                    new_chunk = Chunk(items=[])
                    batches[request[0]].append(new_chunk)
                    current_chunk = new_chunk

                current_chunk.add_item(serialized_item)

        responses = []
        for url, chunks in batches.items():
            for chunk in chunks:
                response = super()._emit_generic(url, payload=Chunk.join(chunk))
                responses.append(response)

        if self._should_trace(async_flag, trace_flag, async_default=True):
            trace_data = []
            for response in responses:
                data = extract_trace_data(response) if response else None
                if data is not None:
                    trace_data.append(data)

            if trace_data:
                self.await_status(trace_data, trace_timeout)

        return len(responses)
