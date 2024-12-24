import datetime
import json
import logging
import os.path
import pathlib
from dataclasses import dataclass, field
from enum import auto
from functools import partial
from typing import Any, Iterable, Iterator, List, Optional, Tuple, Union

import ijson
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigEnum
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import (
    auto_status_aspect,
    auto_workunit_reporter,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.fs.fs_base import FileInfo, get_path_schema
from datahub.ingestion.fs.fs_registry import fs_registry
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import UsageAggregationClass

logger = logging.getLogger(__name__)


class FileReadMode(ConfigEnum):
    STREAM = auto()
    BATCH = auto()
    AUTO = auto()


class FileSourceConfig(StatefulIngestionConfigBase):
    _filename = pydantic_field_deprecated(
        "filename",
        message="filename is deprecated. Use path instead.",
    )
    path: str = Field(
        description=(
            "File path to folder or file to ingest, or URL to a remote file. "
            "If pointed to a folder, all files with extension {file_extension} (default json) within that folder will be processed."
        )
    )
    file_extension: str = Field(
        ".json",
        description=(
            "When providing a folder to use to read files, set this field to control file extensions that you want the source to process. "
            "* is a special value that means process every file regardless of extension"
        ),
    )
    read_mode: FileReadMode = FileReadMode.AUTO
    aspect: Optional[str] = Field(
        default=None,
        description="Set to an aspect to only read this aspect for ingestion.",
    )
    count_all_before_starting: bool = Field(
        default=True,
        description=(
            "When enabled, counts total number of records in the file before starting. "
            "Used for accurate estimation of completion time. Turn it off if startup time is too high."
        ),
    )

    _minsize_for_streaming_mode_in_bytes: int = (
        100 * 1000 * 1000  # Must be at least 100MB before we use streaming mode
    )

    _filename_populates_path_if_present = pydantic_renamed_field(
        "filename", "path", print_warning=False
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @validator("file_extension", always=True)
    def add_leading_dot_to_extension(cls, v: str) -> str:
        if v:
            if v.startswith("."):
                return v
            else:
                return "." + v
        return v


@dataclass
class FileSourceReport(StaleEntityRemovalSourceReport):
    total_num_files: int = 0
    num_files_completed: int = 0
    files_completed: list = field(default_factory=list)
    percentage_completion: str = "0%"
    estimated_time_to_completion_in_minutes: int = -1
    total_bytes_on_disk: Optional[int] = None
    total_bytes_read_completed_files: int = 0
    current_file_name: Optional[str] = None
    current_file_size: Optional[int] = None
    current_file_num_elements: Optional[int] = None
    current_file_bytes_read: Optional[int] = None
    current_file_elements_read: Optional[int] = None
    total_parse_time_in_seconds: float = 0
    total_count_time_in_seconds: float = 0
    total_deserialize_time_in_seconds: float = 0

    def add_deserialize_time(self, delta: datetime.timedelta) -> None:
        self.total_deserialize_time_in_seconds += round(delta.total_seconds(), 2)

    def add_parse_time(self, delta: datetime.timedelta) -> None:
        self.total_parse_time_in_seconds += round(delta.total_seconds(), 2)

    def add_count_time(self, delta: datetime.timedelta) -> None:
        self.total_count_time_in_seconds += round(delta.total_seconds(), 2)

    def append_total_bytes_on_disk(self, delta: int) -> None:
        if self.total_bytes_on_disk is not None:
            self.total_bytes_on_disk += delta
        else:
            self.total_bytes_on_disk = delta

    def reset_current_file_stats(self) -> None:
        self.current_file_name = None
        self.current_file_bytes_read = None
        self.current_file_num_elements = None
        self.current_file_elements_read = None

    def compute_stats(self) -> None:
        super().compute_stats()
        self.num_files_completed = len(self.files_completed)
        total_bytes_read = self.total_bytes_read_completed_files
        if self.current_file_bytes_read:
            total_bytes_read += self.current_file_bytes_read
        elif (
            self.current_file_elements_read
            and self.current_file_num_elements
            and self.current_file_size
        ):
            current_files_bytes_read = int(
                (self.current_file_elements_read / self.current_file_num_elements)
                * self.current_file_size
            )
            total_bytes_read += current_files_bytes_read
        percentage_completion = (
            100.0 * (total_bytes_read / self.total_bytes_on_disk)
            if self.total_bytes_on_disk
            else -1
        )

        if percentage_completion > 0:
            self.estimated_time_to_completion_in_minutes = int(
                (
                    self.running_time.total_seconds()
                    * (100 - percentage_completion)
                    / percentage_completion
                )
                / 60
            )
            self.percentage_completion = f"{percentage_completion:.2f}%"


@platform_name("Metadata File")
@config_class(FileSourceConfig)
@support_status(SupportStatus.CERTIFIED)
class GenericFileSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin pulls metadata from a previously generated file.
    The [metadata file sink](../../../../metadata-ingestion/sink_docs/metadata-file.md) can produce such files, and a number of
    samples are included in the [examples/mce_files](../../../../metadata-ingestion/examples/mce_files) directory.
    """

    def __init__(self, ctx: PipelineContext, config: FileSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report: FileSourceReport = FileSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = FileSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_filenames(self) -> Iterable[FileInfo]:
        path_str = str(self.config.path)
        schema = get_path_schema(path_str)
        fs_class = fs_registry.get(schema)
        fs = fs_class.create()
        for file_info in fs.list(path_str):
            if file_info.is_file and file_info.path.endswith(
                self.config.file_extension
            ):
                yield file_info

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        # No super() call, as we don't want helpers that create / remove workunits
        return [
            partial(auto_workunit_reporter, self.report),
            auto_status_aspect if self.config.stateful_ingestion else None,
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        for f in self.get_filenames():
            for i, obj in self.iterate_generic_file(f):
                id = f"{f.path}:{i}"
                if isinstance(
                    obj, (MetadataChangeProposalWrapper, MetadataChangeProposal)
                ):
                    if (
                        self.config.aspect is not None
                        and obj.aspectName is not None
                        and obj.aspectName != self.config.aspect
                    ):
                        continue

                    if isinstance(obj, MetadataChangeProposalWrapper):
                        yield MetadataWorkUnit(id, mcp=obj)
                    else:
                        yield MetadataWorkUnit(id, mcp_raw=obj)
                else:
                    yield MetadataWorkUnit(id, mce=obj)
            self.report.total_num_files += 1
            self.report.append_total_bytes_on_disk(f.size)

    def get_report(self):
        return self.report

    def close(self):
        super().close()

    def _iterate_file(self, file_status: FileInfo) -> Iterable[Any]:
        file_read_mode = self.config.read_mode
        if file_read_mode == FileReadMode.AUTO:
            if file_status.size < self.config._minsize_for_streaming_mode_in_bytes:
                file_read_mode = FileReadMode.BATCH
            else:
                file_read_mode = FileReadMode.STREAM

        # Open the file.
        schema = get_path_schema(file_status.path)
        fs_class = fs_registry.get(schema)
        fs = fs_class.create()
        self.report.current_file_name = file_status.path
        self.report.current_file_size = file_status.size
        fp = fs.open(file_status.path)

        with fp:
            if file_read_mode == FileReadMode.STREAM:
                yield from self._iterate_file_streaming(fp)
            else:
                yield from self._iterate_file_batch(fp)

        self.report.files_completed.append(file_status.path)
        self.report.num_files_completed += 1
        self.report.total_bytes_read_completed_files += self.report.current_file_size
        self.report.reset_current_file_stats()

    def _iterate_file_streaming(self, fp: Any) -> Iterable[Any]:
        # Count the number of elements in the file.
        if self.config.count_all_before_starting:
            count_start_time = datetime.datetime.now()
            parse_stream = ijson.parse(fp, use_float=True)
            total_elements = 0
            for _row in ijson.items(parse_stream, "item", use_float=True):
                total_elements += 1
            count_end_time = datetime.datetime.now()
            self.report.add_count_time(count_end_time - count_start_time)
            self.report.current_file_num_elements = total_elements
            fp.seek(0)

        # Read the file.
        self.report.current_file_elements_read = 0
        parse_start_time = datetime.datetime.now()
        parse_stream = ijson.parse(fp, use_float=True)
        for row in ijson.items(parse_stream, "item", use_float=True):
            parse_end_time = datetime.datetime.now()
            self.report.add_parse_time(parse_end_time - parse_start_time)
            self.report.current_file_elements_read += 1
            yield row

    def _iterate_file_batch(self, fp: Any) -> Iterable[Any]:
        # Read the file.
        contents = json.load(fp)

        # Maintain backwards compatibility with the single-object format.
        if isinstance(contents, list):
            for row in contents:
                yield row
        else:
            yield contents

    def iterate_mce_file(self, path: str) -> Iterator[MetadataChangeEvent]:
        # TODO: Remove this method, as it appears to be unused.
        schema = get_path_schema(path)
        fs_class = fs_registry.get(schema)
        fs = fs_class.create()
        file_status = fs.file_status(path)
        for obj in self._iterate_file(file_status):
            mce: MetadataChangeEvent = MetadataChangeEvent.from_obj(obj)
            yield mce

    def iterate_generic_file(
        self, file_status: FileInfo
    ) -> Iterator[
        Tuple[
            int,
            Union[
                MetadataChangeEvent,
                MetadataChangeProposalWrapper,
                MetadataChangeProposal,
            ],
        ]
    ]:
        for i, obj in enumerate(self._iterate_file(file_status)):
            try:
                deserialize_start_time = datetime.datetime.now()
                item = _from_obj_for_file(obj)
                if item is not None:
                    deserialize_duration = (
                        datetime.datetime.now() - deserialize_start_time
                    )
                    self.report.add_deserialize_time(deserialize_duration)
                    yield i, item
            except Exception as e:
                self.report.report_failure(f"path-{i}", str(e))

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        config = FileSourceConfig.parse_obj(config_dict)
        exists = os.path.exists(config.path)
        if not exists:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=f"{config.path} doesn't appear to be a valid file or directory.",
                )
            )
        is_dir = os.path.isdir(config.path)
        failure_message = None
        readable = os.access(config.path, os.R_OK)
        if not readable:
            failure_message = f"Cannot read {config.path}"
        if is_dir:
            executable = os.access(config.path, os.X_OK)
            if not executable:
                failure_message = f"Do not have execute permissions in {config.path}"

        if failure_message:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=failure_message
                )
            )
        else:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )

    @staticmethod
    def close_if_possible(stream):
        if hasattr(stream, "close") and callable(stream.close):
            stream.close()


def _from_obj_for_file(
    obj: dict,
) -> Union[
    MetadataChangeEvent,
    MetadataChangeProposal,
    MetadataChangeProposalWrapper,
    None,
]:
    item: Union[
        MetadataChangeEvent,
        MetadataChangeProposalWrapper,
        MetadataChangeProposal,
        UsageAggregationClass,
    ]

    if "proposedSnapshot" in obj:
        item = MetadataChangeEvent.from_obj(obj)
    elif "aspect" in obj:
        item = MetadataChangeProposalWrapper.from_obj(obj)
    else:
        item = UsageAggregationClass.from_obj(obj)
    if not item.validate():
        raise ValueError(f"failed to parse: {obj}")

    if isinstance(item, UsageAggregationClass):
        logger.warning(f"Dropping deprecated UsageAggregationClass: {item}")
        return None
    else:
        return item


def read_metadata_file(
    file: pathlib.Path,
) -> Iterable[
    Union[
        MetadataChangeEvent,
        MetadataChangeProposal,
        MetadataChangeProposalWrapper,
    ]
]:
    # This simplified version of the FileSource can be used for testing purposes.
    with file.open("r") as f:
        for obj in json.load(f):
            item = _from_obj_for_file(obj)
            if item:
                yield item
