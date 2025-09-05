import glob
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
# from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes  # Not used
from datahub.ingestion.source.pentaho.config import PentahoSourceConfig
from datahub.ingestion.source.pentaho.lineage_extractor import PentahoLineageExtractor
from datahub.ingestion.source.pentaho.parser import PentahoKettleParser
from datahub.ingestion.source.pentaho.report import PentahoSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
)

logger = logging.getLogger(__name__)


@platform_name("Pentaho")
@config_class(PentahoSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, can be disabled via configuration `include_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
class PentahoSource(StatefulIngestionSourceBase):
    """
    This plugin extracts metadata from Pentaho Kettle files (.ktr and .kjb).
    
    It supports:
    - DataJob metadata extraction (job name, type, description)
    - Table-level lineage from TableInput and TableOutput steps
    - Configurable database platform mapping
    - Variable resolution in table names and SQL queries
    """
    
    platform = "pentaho"
    
    def __init__(self, config: PentahoSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = PentahoSourceReport()
        
        # Initialize components
        self.parser = PentahoKettleParser(
            resolve_variables=config.resolve_variables,
            variable_values=config.variable_values
        )
        
        self.lineage_extractor = PentahoLineageExtractor(
            database_mapping=config.database_mapping,
            default_database_platform=config.default_database_platform,
            env=config.env,
            report=self.report
        )
        
    @classmethod
    def create(cls, config_dict, ctx):
        config = PentahoSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)
    
    def get_report(self) -> SourceReport:
        return self.report
    
    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor
        ]
    
    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate work units for Pentaho jobs"""
        
        # Find all Kettle files
        kettle_files = self._find_kettle_files()
        
        if not kettle_files:
            logger.warning("No Pentaho Kettle files found matching the specified patterns")
            return
        
        logger.info(f"Found {len(kettle_files)} Pentaho Kettle files to process")
        
        # Process each file
        for file_path in kettle_files:
            try:
                yield from self._process_kettle_file(file_path)
            except Exception as e:
                error_msg = f"Failed to process file {file_path}: {e}"
                logger.error(error_msg, exc_info=True)
                self.report.report_file_failed(str(file_path), error_msg)
    
    def _find_kettle_files(self) -> List[Path]:
        """Find all Kettle files matching the configured patterns"""
        kettle_files = []
        
        for pattern in self.config.kettle_file_paths:
            try:
                # Use glob to find files matching the pattern
                matching_files = glob.glob(pattern, recursive=True)
                
                for file_path in matching_files:
                    path = Path(file_path)
                    
                    # Check file extension
                    if path.suffix.lower() not in ['.ktr', '.kjb']:
                        continue
                    
                    # Check file pattern filter
                    if not self.config.file_pattern.allowed(path.name):
                        self.report.report_file_skipped(
                            str(path), 
                            "File name not allowed by pattern"
                        )
                        continue
                    
                    kettle_files.append(path)
                    
            except Exception as e:
                logger.error(f"Error finding files with pattern {pattern}: {e}")
        
        # Remove duplicates and sort
        kettle_files = sorted(list(set(kettle_files)))
        
        return kettle_files
    
    def _process_kettle_file(self, file_path: Path) -> Iterable[MetadataWorkUnit]:
        """Process a single Kettle file and generate work units"""
        
        logger.info(f"Processing Kettle file: {file_path}")
        
        # Parse the file
        job = self.parser.parse_file(file_path)
        if not job:
            self.report.report_file_failed(str(file_path), "Failed to parse file")
            return
        
        # Report file processed
        file_type = file_path.suffix.lower()
        self.report.report_file_processed(str(file_path), file_type)
        
        # Extract lineage if enabled
        input_datasets = []
        output_datasets = []
        
        if self.config.include_lineage:
            try:
                input_datasets, output_datasets = self.lineage_extractor.extract_lineage(job)
                
                # Report step processing
                for step in job.steps:
                    self.report.report_step_processed(step.type)
                    
            except Exception as e:
                logger.warning(f"Failed to extract lineage from {file_path}: {e}")
                self.report.lineage_extraction_errors.append(f"{file_path}: {e}")
        
        # Report job processing
        has_lineage = bool(input_datasets or output_datasets)
        self.report.report_job_processed(job.name, has_lineage)
        
        # Generate work units if job metadata is enabled
        if self.config.include_job_metadata:
            yield from self._generate_job_workunits(
                job, input_datasets, output_datasets
            )
    
    def _generate_job_workunits(
        self,
        job,
        input_datasets: List[str],
        output_datasets: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Generate work units for a Pentaho job"""
        
        # Create job URN
        job_urn = builder.make_data_job_urn(
            orchestrator=self.platform,
            flow_id=self._get_flow_id(job),
            job_id=job.name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        
        # Create DataJob info
        job_type = f"PENTAHO_{job.type.upper()}"
        
        # Prepare custom properties
        custom_properties = {}
        if job.custom_properties:
            # Add prefix to custom properties
            for key, value in job.custom_properties.items():
                if value:  # Only add non-empty values
                    custom_properties[f"{self.config.custom_properties_prefix}{key}"] = value
        
        # Add file path
        custom_properties[f"{self.config.custom_properties_prefix}file_path"] = job.file_path
        custom_properties[f"{self.config.custom_properties_prefix}file_type"] = job.type
        
        # Add step counts
        step_counts = {}
        for step in job.steps:
            step_type = step.type.lower()
            step_counts[step_type] = step_counts.get(step_type, 0) + 1
        
        for step_type, count in step_counts.items():
            custom_properties[f"{self.config.custom_properties_prefix}steps.{step_type}"] = str(count)
        
        # Generate DataJobInfo aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=job.name,
                type=job_type,
                description=job.description,
                customProperties=custom_properties,
            ),
        ).as_workunit()
        
        # Generate browse path
        yield from self._generate_browse_path_workunits(job_urn, job)
        
        # Generate platform instance if configured
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.platform),
                    instance=builder.make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        
        # Generate lineage if available
        if input_datasets or output_datasets:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=output_datasets,
                ),
            ).as_workunit()
    
    def _get_flow_id(self, job) -> str:
        """Generate a flow ID for the job"""
        # Use the file name without extension as flow ID
        return Path(job.file_path).stem
    
    def _generate_browse_path_workunits(
        self, job_urn: str, job
    ) -> Iterable[MetadataWorkUnit]:
        """Generate browse path work units for the job"""
        
        file_path = Path(job.file_path)
        
        # Generate browse path using template
        browse_path = self.config.job_browse_path_template.format(
            job_type=job.type,
            file_name=file_path.stem,
            job_name=job.name,
        )
        
        # Split path and create browse path entries
        path_parts = [part for part in browse_path.split('/') if part]
        browse_path_entries = [
            BrowsePathEntryClass(id=part) for part in path_parts
        ]
        
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=BrowsePathsV2Class(path=browse_path_entries),
        ).as_workunit()