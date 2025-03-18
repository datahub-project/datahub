import glob
import logging
import os
from datetime import datetime, timezone
from pathlib import PurePath
from typing import Iterable, List, Optional, Union

import h5py

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.hdf5.config import HDF5SourceConfig
from datahub.ingestion.source.hdf5.profiling import HDF5Profiler
from datahub.ingestion.source.hdf5.report import HDF5SourceReport
from datahub.ingestion.source.hdf5.util import decode_type, numpy_value_to_string
from datahub.ingestion.source.s3.source import BrowsePath
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass as SchemaFieldDataType,
    SchemaMetadataClass as SchemaMetadata,
    StringTypeClass,
)
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

field_type_mapping = {
    "bool_": BooleanTypeClass(),
    "int8": NumberTypeClass(),
    "int16": NumberTypeClass(),
    "int32": NumberTypeClass(),
    "int64": NumberTypeClass(),
    "uint8": NumberTypeClass(),
    "uint16": NumberTypeClass(),
    "uint32": NumberTypeClass(),
    "uint64": NumberTypeClass(),
    "intp": NumberTypeClass(),
    "uintp": NumberTypeClass(),
    "float16": NumberTypeClass(),
    "float32": NumberTypeClass(),
    "float64": NumberTypeClass(),
    "float128": NumberTypeClass(),
    "complex64": NumberTypeClass(),
    "complex128": NumberTypeClass(),
    "complex256": NumberTypeClass(),
    "str_": StringTypeClass(),
    "unicode_": StringTypeClass(),
    "string_": StringTypeClass(),
    "object_": RecordTypeClass(),
    "datetime64": DateTypeClass(),
    "timedelta64": DateTypeClass(),
    "void": NullTypeClass(),
}


class HDF5ContainerKey(ContainerKey):
    path: str


@platform_name("HDF5")
@config_class(HDF5SourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
class HDF5Source(StatefulIngestionSourceBase):
    config: HDF5SourceConfig
    report: HDF5SourceReport
    container_WU_creator: ContainerWUCreator
    platform: str = "hdf5"

    def __init__(self, ctx: PipelineContext, config: HDF5SourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.config = config
        self.report: HDF5SourceReport = HDF5SourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "HDF5Source":
        config = HDF5SourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    @staticmethod
    def local_browser(path_spec: str) -> Iterable[BrowsePath]:
        # Use glob to find all paths matching the pattern
        matching_paths = glob.glob(path_spec, recursive=True)

        # Filter to include only files (not directories)
        matching_files = [path for path in matching_paths if os.path.isfile(path)]

        for file in sorted(matching_files):
            # We need to make sure the path is in posix style which is not true on windows
            full_path = PurePath(os.path.normpath(file)).as_posix()
            yield BrowsePath(
                file=full_path,
                timestamp=datetime.fromtimestamp(
                    os.path.getmtime(full_path), timezone.utc
                ),
                size=os.path.getsize(full_path),
                partitions=[],
            )

    def h5py_dataset_iterator(
        self, node: Union[h5py.File, h5py.Group], prefix: str = ""
    ) -> Iterable[tuple[str, h5py.Dataset]]:
        for key in node.keys():
            item = node[key]
            path = f"{prefix}/{key}"
            if isinstance(item, h5py.Dataset):
                yield path, item
            elif isinstance(item, h5py.Group):
                yield from self.h5py_dataset_iterator(item, path)

    def hdf5_browser(
        self, browse_path: BrowsePath
    ) -> Iterable[tuple[str, h5py.Dataset]]:
        with h5py.File(browse_path.file, "r") as f:
            for path, dataset in self.h5py_dataset_iterator(f):
                yield path, dataset

    @staticmethod
    def dataset_name(path: str) -> str:
        return path.split("/")[-1]

    @staticmethod
    def construct_schema_field(f_name: str, f_type: str) -> SchemaField:
        dtype = decode_type(f_type)
        logger.debug(f"Field: {f_name} Type: {dtype}")
        return SchemaField(
            fieldPath=f_name,
            nativeDataType=dtype,
            type=SchemaFieldDataType(type=field_type_mapping.get(dtype)),
            description=None,
            nullable=False,
            recursive=False,
        )

    def construct_schema_metadata(
        self,
        name: str,
        dataset: h5py.Dataset,
    ) -> SchemaMetadata:
        canonical_schema: List[SchemaField] = []
        dropped_fields = set()

        if len(dataset.shape) == 1:
            logger.info(
                f"Attempting to extract fields from 1-dimensional dataset {name}"
            )
            if dataset.dtype.names is not None:
                logger.info(f"Dataset {name} is a compound datatype")
                for n, (f_name, f_type) in enumerate(dataset.dtype.descr):
                    if 0 < self.config.max_schema_size <= n:
                        dropped_fields.add(f_name)
                        continue
                    canonical_schema.append(self.construct_schema_field(f_name, f_type))
            else:
                f_name = "col0"
                f_type = dataset.dtype
                canonical_schema.append(self.construct_schema_field(f_name, f_type))
        else:
            logger.info(
                f"Attempting to extract fields from multidimensional dataset {name} shape {dataset.shape}"
            )
            rows = dataset.shape[0]
            columns = dataset.shape[1]
            logger.info(f"Found {rows} rows")
            for n in range(rows):
                f_name = f"row_{n + 1}_with_{columns}_values"
                if 0 < self.config.max_schema_size <= n:
                    dropped_fields.add(f_name)
                    continue
                f_type = dataset.dtype
                canonical_schema.append(
                    self.construct_schema_field(f_name, f_type.name)
                )

        if len(dropped_fields) > 0:
            self.report.report_warning(
                title="Too many schema fields",
                message="Ingested a subset of the schema because it has too many schema fields.",
                context=f"Dropped Fields: {len(dropped_fields)}, Dataset: {dataset.name}",
            )

        return SchemaMetadata(
            schemaName=name,
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=canonical_schema,
        )

    @staticmethod
    def get_dataset_attributes(dataset: h5py.Dataset) -> dict:
        attributes = {}
        for attr_name in dataset.attrs:
            attributes[attr_name] = numpy_value_to_string(dataset.attrs[attr_name])
        attributes["dataset_shape"] = str(dataset.shape)
        attributes["dataset_dtype"] = dataset.dtype.name
        attributes["dataset_size"] = str(dataset.size)
        return attributes

    def process_dataset(
        self, path: str, dataset: h5py.Dataset
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = self.dataset_name(path)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        attributes = self.get_dataset_attributes(dataset)
        dataset_properties = DatasetPropertiesClass(
            tags=[],
            customProperties=attributes,
        )

        schema_metadata = self.construct_schema_metadata(
            name=dataset_name,
            dataset=dataset,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=dataset_properties,
        ).as_workunit()

        yield from self.container_WU_creator.create_container_hierarchy(
            path, dataset_urn
        )

        if self.config.is_profiling_enabled():
            profiler = HDF5Profiler(
                self.config, self.report, dataset, dataset_urn, path
            )
            yield from profiler.get_workunits()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.container_WU_creator = ContainerWUCreator(
            self.platform,
            self.config.platform_instance,
            self.config.env,
        )

        with PerfTimer() as timer:
            for path_spec in self.config.path_list:
                for browse_path in self.local_browser(path_spec):
                    if not self.config.path_pattern.allowed(browse_path.file):
                        self.report.report_dropped(browse_path.file)
                        continue
                    basename = os.path.basename(browse_path.file)
                    filename = os.path.splitext(basename)[0]
                    for dspath, dataset in self.hdf5_browser(browse_path):
                        if not self.config.dataset_pattern.allowed(dspath):
                            self.report.report_dropped(dspath)
                            continue
                        logger.info(f"Processing dataset {dataset.name}")
                        path = f"/{filename}{dspath}"
                        logger.info(f"Processing path {path}")
                        yield from self.process_dataset(path, dataset)

            time_taken = timer.elapsed_seconds()

            logger.info(f"Finished ingestion; took {time_taken:.3f} seconds")

    def get_report(self):
        return self.report
