import gzip
import json
import logging
import os
import pathlib
from enum import Enum
from io import BufferedWriter
from typing import Iterable, Iterator, Optional, Union

from pydantic import validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.com.linkedin.pegasus2avro.usage import UsageAggregation

logger = logging.getLogger(__name__)


class CompressionTypes(Enum):
    NONE = "NONE"
    GZIP = "GZIP"


class FileSinkConfig(ConfigModel):
    filename: str = "datahub_events.json"
    path: Optional[str] = None
    compression: CompressionTypes = CompressionTypes.NONE
    rotation_raw_bytes: int = 100 * 1024 * 1024  # Rotate files every 100 MB of raw data

    @validator("compression", pre=True)
    def upper_case_compression(cls, v):
        if v is not None and isinstance(v, str):
            return v.upper()
        return v


class FileSinkReport(SinkReport):
    compression: CompressionTypes
    filename: Optional[str]
    directory: Optional[str]
    current_file_path: Optional[str] = None
    current_file_raw_bytes_written: int = 0
    current_file_num_records_written: int = 0


class DirectoryFileProvider(Iterator[pathlib.Path]):
    def __init__(self, directory: str, file_name_base: str, file_name_extension: str):
        super().__init__()
        self.root_directory = directory
        self.file_name_base = file_name_base
        self.file_name_extension = file_name_extension
        self.next_file_name = (
            f"{self.root_directory}/{self.file_name_base}{self.file_name_extension}"
        )

        self.current_index = -1

    def __next__(self) -> pathlib.Path:
        next_file_name = self.next_file_name
        self.current_index += 1
        self.next_file_name = f"{self.root_directory}/{self.file_name_base}.{self.current_index}{self.file_name_extension}"
        return pathlib.Path(next_file_name)

    legacy_nested_json_string: bool = False


class FileSink(Sink[FileSinkConfig, FileSinkReport]):
    @staticmethod
    def _get_file_extension(
        config: FileSinkConfig,
    ) -> str:  # Keeping it for backward compatibility
        ext_base = "" if config.filename.endswith(".json") else ".json"
        if config.compression == CompressionTypes.GZIP:
            return ext_base + ".gz"
        else:
            return ext_base

    @staticmethod
    def _get_file_extension_new(config: FileSinkConfig) -> str:
        ext_base = ".json"
        if config.compression == CompressionTypes.GZIP:
            ext_base = ext_base + ".gz"

        return ext_base

    @staticmethod
    def _get_file_base_name(config: FileSinkConfig) -> str:
        if ".json" in config.filename:
            return config.filename.split(".json")[0]

        return config.filename

    def __init__(self, ctx: PipelineContext, config: FileSinkConfig):
        super().__init__(ctx, config)
        self.config = config
        self.report = FileSinkReport()

        self.report.compression = self.config.compression

        assert self.config.path or self.config.filename
        self.directory_mode = self.config.path is not None
        if self.directory_mode:
            assert self.config.path
            os.makedirs(self.config.path, exist_ok=True)
            self.file_name_iterator = DirectoryFileProvider(
                self.config.path,
                self._get_file_base_name(config),
                self._get_file_extension_new(self.config),
            )
            self.current_file: Optional[Union[gzip.GzipFile, BufferedWriter]] = None
        else:
            file_name = pathlib.Path(
                self.config.filename + self._get_file_extension(self.config)
            )
            fpath = file_name
            self._init_file(fpath)

    def _init_file(self, file_path: pathlib.Path) -> None:
        if self.config.compression == CompressionTypes.GZIP:
            self.current_file = gzip.open(file_path, "wb")
        elif self.config.compression == CompressionTypes.NONE:
            self.current_file = file_path.open("wb")
        assert self.current_file
        self.current_file.write(b"[\n")
        self.report.current_file_path = str(file_path)
        self.report.current_file_num_records_written = 0
        self.report.current_file_raw_bytes_written = 0

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FileSink":
        config = FileSinkConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def handle_work_unit_start(self, wu):
        self.id = wu.id

    def handle_work_unit_end(self, wu):
        pass

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
                UsageAggregation,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        self._open_file_if_needed()
        assert self.current_file
        record = record_envelope.record
        obj = record.to_obj()
        json_str = json.dumps(obj, indent=4)
        if self.report.current_file_num_records_written > 0:
            json_str = ",\n" + json_str

        json_bytes = json_str.encode("utf-8")
        self.current_file.write(json_bytes)
        # json.dump(obj, self.file, indent=4)
        self.report.current_file_num_records_written += 1
        self.report.current_file_raw_bytes_written += len(json_bytes)
        self.report.report_record_written(record_envelope)
        write_callback.on_success(record_envelope, {})
        self._rotate_file_if_needed()

    def _open_file_if_needed(self):
        if self.current_file is None:
            next_file = next(self.file_name_iterator)
            self._init_file(next_file)

    def _rotate_file_if_needed(self):
        if self.directory_mode:
            if (
                self.report.current_file_raw_bytes_written
                > self.config.rotation_raw_bytes
            ):
                self._close_file()

    def _close_file(self):
        if self.current_file is not None:
            self.current_file.write(b"\n]")
            self.current_file.close()
        self.current_file = None

    def get_report(self):
        return self.report

    def close(self):
        self._close_file()


def _to_obj_for_file(
    obj: Union[
        MetadataChangeEvent,
        MetadataChangeProposal,
        MetadataChangeProposalWrapper,
        UsageAggregation,
    ],
    simplified_structure: bool = True,
) -> dict:
    if isinstance(obj, MetadataChangeProposalWrapper):
        return obj.to_obj(simplified_structure=simplified_structure)
    return obj.to_obj()


def write_metadata_file(
    file: pathlib.Path,
    records: Iterable[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
            UsageAggregation,
        ]
    ],
) -> None:
    # This simplified version of the FileSink can be used for testing purposes.
    with file.open("w") as f:
        f.write("[\n")
        for i, record in enumerate(records):
            if i > 0:
                f.write(",\n")
            obj = _to_obj_for_file(record)
            json.dump(obj, f, indent=4)
        f.write("\n]")
