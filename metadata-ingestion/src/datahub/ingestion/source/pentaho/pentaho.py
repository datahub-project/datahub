"""Pentaho source implementation."""

import logging
import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_job_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.pentaho.config import PentahoSourceConfig
from datahub.ingestion.source.pentaho.context import ProcessingContext
from datahub.ingestion.source.pentaho.step_processors import (
    StepProcessor,
    TableInputProcessor,
    TableOutputProcessor,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataJobKeyClass,
    DataJobSnapshotClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)


@platform_name("Pentaho")
@support_status(SupportStatus.TESTING)
@config_class(PentahoSourceConfig)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Emits dataset-level lineage for TableInput and TableOutput steps in Pentaho .ktr/.kjb files only when the table names have no variable references like ${table}.",
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage is not supported.",
    supported=False,
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Supports platform instance",
    supported=True,
)
class PentahoSource(Source):
    """DataHub ingestion source for Pentaho Kettle files."""

    def __init__(self, config: PentahoSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        # Initialize step processors
        self.step_processors = [
            TableInputProcessor(self),
            TableOutputProcessor(self),
        ]

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "PentahoSource":
        config = PentahoSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Process all Pentaho files in the configured directory."""
        for root, _dirs, files in os.walk(self.config.base_folder):
            for file in files:
                file_path = os.path.join(root, file)

                if not self._should_process_file(file_path):
                    continue

                try:
                    if file.endswith(".ktr"):
                        yield from self._process_ktr(file_path)
                    elif file.endswith(".kjb"):
                        yield from self._process_kjb(file_path)
                except Exception as e:
                    logger.exception(f"Failed to process file {file_path}")
                    self.report.report_failure(f"Failed to process {file_path}: {e}")

    def get_report(self) -> SourceReport:
        return self.report

    def _should_process_file(self, file_path: str) -> bool:
        """Check if file should be processed."""
        if not (file_path.endswith(".ktr") or file_path.endswith(".kjb")):
            return False

        try:
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > self.config.file_size_limit_mb:
                logger.warning(f"Skipping {file_path}: size exceeds limit")
                return False
            return True
        except Exception as e:
            logger.warning(f"Error checking file {file_path}: {e}")
            return False

    def _normalize_platform_name(self, raw_platform: str) -> str:
        """Normalize platform name using configuration mappings."""
        if not raw_platform:
            return "file"

        raw_lower = raw_platform.lower()

        # Check exact matches first
        if raw_lower in self.config.platform_mappings:
            return self.config.platform_mappings[raw_lower]

        # Check if any mapping key is contained in the raw platform name
        for key, platform in self.config.platform_mappings.items():
            if key in raw_lower:
                return platform

        # Special cases for file-like platforms that should map to "file"
        file_indicators = ["csv", "text", "file", "excel", "xls"]
        if any(indicator in raw_lower for indicator in file_indicators):
            return "file"

        # Default to unknown for unrecognized platforms
        return "unknown"

    def _get_platform_from_connection(
        self,
        conn_name: str,
        step_type: Optional[str] = None,
        conn_type: Optional[str] = None,
    ) -> str:
        """Determine platform from connection name, step type, and connection type."""
        # Check connection type
        if conn_type:
            platform = self._normalize_platform_name(conn_type)
            if platform != "unknown":
                return platform

        # Check connection name
        if conn_name:
            platform = self._normalize_platform_name(conn_name)
            if platform != "unknown":
                return platform

        # Infer from step type for file-based operations
        if step_type:
            step_lower = step_type.lower()
            if any(
                file_type in step_lower
                for file_type in ["csv", "text", "excel", "file"]
            ):
                return "file"

        # Default fallback
        return "file"

    def _create_dataset_urn(self, platform: str, name: str) -> Optional[str]:
        """Create dataset URN with proper name formatting."""
        if not name or not platform:
[O            return None

        # Clean and format the dataset name
        clean_name = name.strip()
        if not clean_name:
            return None

        # Normalize platform name
        platform = self._normalize_platform_name(platform)

        if platform == "file":
            # If it's a full path, extract just the filename
            if os.sep in clean_name or "/" in clean_name:
                clean_name = os.path.basename(clean_name)

            # Add default extension if none present
            if not any(
                clean_name.endswith(ext) for ext in [".csv", ".txt", ".xls", ".xlsx"]
            ):
                if "." not in clean_name:
                    clean_name = f"{clean_name}.csv"  # Default extension

        # Create URN with proper platform instance handling
        if self.config.platform_instance:
            return make_dataset_urn_with_platform_instance(
                platform, clean_name, self.config.platform_instance, self.config.env
            )
        return make_dataset_urn(platform, clean_name, self.config.env)

    def _get_connection_type(
        self, root: Optional[ET.Element], conn_name: str
    ) -> Optional[str]:
        """Get connection type from the transformation's connection definitions."""
        if not conn_name or root is None:
            return None

        for connection in root.findall("connection"):
            if connection.findtext("name") == conn_name:
                return connection.findtext("type")
        return None

    def _get_step_processor(self, step_type: str) -> Optional[StepProcessor]:
        """Get the appropriate processor for a step type."""
        for processor in self.step_processors:
            if processor.can_process(step_type):
                return processor
        return None

    def _process_step(
        self,
        step: ET.Element,
        context: ProcessingContext,
        root: Optional[ET.Element] = None,
    ):
        """Process a single transformation step for table-level lineage."""
        step_type = step.findtext("type")
        step_name = step.findtext("name") or "unnamed_step"

        if not step_type:
            return

        # Track step for debugging
        context.add_step_info(step_name, step_type)

        # Find appropriate processor
        processor = self._get_step_processor(step_type)
        if processor:
            try:
                processor.process(step, context, root)
                logger.debug(f"Processed step {step_name} of type {step_type}")
            except Exception as e:
                logger.warning(
                    f"Failed to process step {step_name} of type {step_type}: {e}"
                )
                context.custom_properties[f"error_step {step_name}"] = str(e)
        else:
            logger.debug(f"No processor found for step type: {step_type}")
            context.custom_properties[f"unprocessed_step {step_name}"] = step_type

    def _process_ktr(self, file_path: str) -> Iterable[MetadataWorkUnit]:
        """Process a Pentaho transformation file."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            trans_name = (
                root.findtext("info/name")
                or os.path.splitext(os.path.basename(file_path))[0]
            )
            job_urn = make_data_job_urn(
                orchestrator="pentaho",
                flow_id=trans_name,
                job_id=trans_name,
                cluster=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            context = ProcessingContext(file_path, self.config)

            # Process each step
            for step in root.findall("step"):
                self._process_step(step, context, root)

            # Add transformation-specific properties
            context.custom_properties["file_type"] = "transformation"
            description = root.findtext("info/description")
            if description:
                context.custom_properties["description"] = description[
                    :500
                ]  # Limit length

            # Build aspects
            aspects: List[Any] = [
                DataJobKeyClass(flow=trans_name, jobId=trans_name),
                DataJobInfoClass(
                    name=trans_name,
                    type="TRANSFORMATION",
                    description=description,
                    customProperties=context.get_custom_properties(),
                ),
                OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=make_user_urn(self.config.default_owner),
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    ]
                ),
                GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:pentaho")]),
            ]

            # Add lineage if we have datasets
            if context.input_datasets or context.output_datasets:
                aspects.append(
                    DataJobInputOutputClass(
                        inputDatasets=list(context.input_datasets),
                        outputDatasets=list(context.output_datasets),
                    )
                )

            yield MetadataWorkUnit(
                id=f"pentaho-ktr-{trans_name}",
                mce=MetadataChangeEventClass(
                    proposedSnapshot=DataJobSnapshotClass(urn=job_urn, aspects=aspects)
                ),
            )

            logger.info(
                f"Successfully processed transformation: {trans_name} with {len(context.input_datasets)} inputs and {len(context.output_datasets)} outputs"
            )

        except Exception as e:
            logger.exception(f"Failed to process transformation {file_path}")
            self.report.report_failure(
                f"Failed to process transformation {file_path}: {e}"
            )

    def _process_kjb(self, file_path: str) -> Iterable[MetadataWorkUnit]:
        """Process a Pentaho job file."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            job_name = (
                root.findtext("name")
                or os.path.splitext(os.path.basename(file_path))[0]
            )
            job_urn = make_data_job_urn(
                orchestrator="pentaho",
                flow_id=job_name,
                job_id=job_name,
                cluster=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            context = ProcessingContext(file_path, self.config)

            # Process job entries to find transformation/job dependencies
            dependencies = []
            for entry in root.findall("entries/entry"):
                entry_type = entry.findtext("type")
                entry_name = entry.findtext("name")

                if entry_type in ["TRANS", "JOB"] and entry_name:
                    dependencies.append(f"{entry_type}:{entry_name}")
                    context.custom_properties[f"dependency_{len(dependencies)}"] = (
                        f"{entry_type}:{entry_name}"
                    )

            # Add job-specific properties
            context.custom_properties["file_type"] = "job"
            context.custom_properties["dependency_count"] = str(len(dependencies))
            description = root.findtext("description")
            if description:
                context.custom_properties["description"] = description[
                    :500
                ]  # Limit length

            # Build aspects
            aspects: List[Any] = [
                DataJobKeyClass(flow=job_name, jobId=job_name),
                DataJobInfoClass(
                    name=job_name,
                    type="JOB",
                    description=description,
                    customProperties=context.get_custom_properties(),
                ),
                OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=make_user_urn(self.config.default_owner),
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    ]
                ),
                GlobalTagsClass(tags=[TagAssociationClass(tag="urn:li:tag:pentaho")]),
            ]

            if context.input_datasets or context.output_datasets:
                aspects.append(
                    DataJobInputOutputClass(
                        inputDatasets=list(context.input_datasets),
                        outputDatasets=list(context.output_datasets),
                    )
                )

            yield MetadataWorkUnit(
                id=f"pentaho-kjb-{job_name}",
                mce=MetadataChangeEventClass(
                    proposedSnapshot=DataJobSnapshotClass(urn=job_urn, aspects=aspects)
                ),
            )

            logger.info(f"Successfully processed job: {job_name}")

        except Exception as e:
            logger.exception(f"Failed to process job {file_path}")
            self.report.report_failure(f"Failed to process job {file_path}: {e}")
