import json
import logging
import pathlib
from typing import Iterable, List, Union

from datahub.configuration.common import ConfigModel
from datahub.emitter.aspect import JSON_CONTENT_TYPE, JSON_PATCH_CONTENT_TYPE
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.fs.fs_base import get_path_schema
from datahub.ingestion.fs.fs_registry import fs_registry
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    UsageAggregationClass,
)

logger = logging.getLogger(__name__)


def _to_obj_for_file(
    obj: Union[
        MetadataChangeEventClass,
        MetadataChangeProposalClass,
        MetadataChangeProposalWrapper,
        UsageAggregationClass,
    ],
    simplified_structure: bool = True,
) -> dict:
    if isinstance(obj, MetadataChangeProposalWrapper):
        return obj.to_obj(simplified_structure=simplified_structure)
    elif isinstance(obj, MetadataChangeProposalClass) and simplified_structure:
        serialized = obj.to_obj()
        if serialized.get("aspect") and serialized["aspect"].get("contentType") in [
            JSON_CONTENT_TYPE,
            JSON_PATCH_CONTENT_TYPE,
        ]:
            serialized["aspect"] = {"json": json.loads(serialized["aspect"]["value"])}
        return serialized
    return obj.to_obj()


class FileSinkConfig(ConfigModel):
    filename: str

    legacy_nested_json_string: bool = False


class FileSink(Sink[FileSinkConfig, SinkReport]):
    """
    File sink that supports writing to various backends (local, S3, etc.)
    using the pluggable file system architecture.
    """

    def __post_init__(self) -> None:
        self.filename = self.config.filename

        # Determine file system based on path schema
        schema = get_path_schema(self.filename)
        fs_class = fs_registry.get(schema)
        self.fs = fs_class.create()

        # Initialize the records list
        self.records: List[dict] = []

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEventClass,
                MetadataChangeProposalClass,
                MetadataChangeProposalWrapper,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        record = record_envelope.record
        obj = _to_obj_for_file(
            record, simplified_structure=not self.config.legacy_nested_json_string
        )

        # Store records in memory until close()
        self.records.append(obj)

        self.report.report_record_written(record_envelope)
        if write_callback:
            write_callback.on_success(record_envelope, {})

    def close(self):
        """Write all records to the file system as a JSON array."""
        if not self.records:
            # Write empty array if no records
            content = "[]"
        else:
            # Convert records to JSON string
            content = "[\n"
            for i, record in enumerate(self.records):
                if i > 0:
                    content += ",\n"
                content += json.dumps(record, indent=4)
            content += "\n]"

        # Write to file system
        try:
            self.fs.write(self.filename, content)
            logger.info(
                f"Successfully wrote {len(self.records)} records to {self.filename}"
            )
        except Exception as e:
            logger.error(f"Failed to write to {self.filename}: {e}")
            raise


def write_metadata_file(
    file_path: Union[str, pathlib.Path],
    records: Iterable[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalClass,
            MetadataChangeProposalWrapper,
            UsageAggregationClass,
            dict,  # Serialized MCE or MCP
        ]
    ],
) -> None:
    """
    Write metadata records to any supported file system (local, S3, etc.).
    This function uses the pluggable file system architecture.
    """
    # Convert Path to string if needed
    file_path_str = str(file_path)

    # Determine file system based on path schema
    schema = get_path_schema(file_path_str)
    fs_class = fs_registry.get(schema)
    fs = fs_class.create()

    # Convert records to JSON string
    content = "[\n"
    record_list = list(records)  # Convert iterable to list

    for i, record in enumerate(record_list):
        if i > 0:
            content += ",\n"
        if not isinstance(record, dict):
            record = _to_obj_for_file(record)
        content += json.dumps(record, indent=4)
    content += "\n]"

    # Write to file system
    fs.write(file_path_str, content)
    logger.info(f"Successfully wrote {len(record_list)} records to {file_path_str}")
