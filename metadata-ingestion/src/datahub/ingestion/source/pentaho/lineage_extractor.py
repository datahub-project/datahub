import logging
from typing import Dict, List, Optional, Set, Tuple

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.pentaho.parser import PentahoJob, PentahoStep
from datahub.ingestion.source.pentaho.report import PentahoSourceReport
try:
    from datahub.ingestion.source.sql.sql_parsing_common import SqlParsingResult
    from datahub.ingestion.source.sql.sql_parsing_builder import SqlParsingBuilder
    SQL_PARSING_AVAILABLE = True
except ImportError:
    SQL_PARSING_AVAILABLE = False
    SqlParsingResult = None
    SqlParsingBuilder = None
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class PentahoLineageExtractor:
    """Extracts lineage information from Pentaho jobs"""
    
    def __init__(
        self,
        database_mapping: Dict[str, str],
        default_database_platform: str,
        env: str = "PROD",
        report: Optional[PentahoSourceReport] = None,
    ):
        self.database_mapping = database_mapping
        self.default_database_platform = default_database_platform
        self.env = env
        self.report = report
        if SQL_PARSING_AVAILABLE:
            self.sql_parser = SqlParsingBuilder(
                usage_config=None,  # We don't need usage config for basic parsing
                generate_lineage=True,
                generate_queries=False,
                generate_usage_statistics=False,
                generate_operations=False,
            )
        else:
            self.sql_parser = None
    
    def extract_lineage(self, job: PentahoJob) -> Tuple[List[str], List[str]]:
        """
        Extract lineage from a Pentaho job.
        Returns (input_datasets, output_datasets)
        """
        input_datasets = []
        output_datasets = []
        
        for step in job.steps:
            if step.type.lower() == 'tableinput':
                # TableInput steps are sources (inputs to the job)
                dataset_urns = self._extract_input_datasets(step)
                input_datasets.extend(dataset_urns)
                
            elif step.type.lower() == 'tableoutput':
                # TableOutput steps are sinks (outputs from the job)
                dataset_urns = self._extract_output_datasets(step)
                output_datasets.extend(dataset_urns)
        
        # Remove duplicates while preserving order
        input_datasets = list(dict.fromkeys(input_datasets))
        output_datasets = list(dict.fromkeys(output_datasets))
        
        return input_datasets, output_datasets
    
    def _extract_input_datasets(self, step: PentahoStep) -> List[str]:
        """Extract input dataset URNs from a TableInput step"""
        dataset_urns = []
        
        try:
            # First try to extract from SQL if available
            if step.sql:
                sql_datasets = self._extract_datasets_from_sql(step.sql, step.connection_name)
                dataset_urns.extend(sql_datasets)
            
            # If no SQL or SQL parsing failed, try to use table_name
            if not dataset_urns and step.table_name:
                platform = self._get_platform_for_connection(step.connection_name)
                dataset_urn = builder.make_dataset_urn(
                    platform=platform,
                    name=step.table_name,
                    env=self.env
                )
                dataset_urns.append(dataset_urn)
                
                if self.report:
                    self.report.report_lineage_created("", dataset_urn)
            
        except Exception as e:
            error_msg = f"Failed to extract input datasets from step {step.name}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_lineage_failed("", step.name, error_msg)
        
        return dataset_urns
    
    def _extract_output_datasets(self, step: PentahoStep) -> List[str]:
        """Extract output dataset URNs from a TableOutput step"""
        dataset_urns = []
        
        try:
            if step.table_name:
                platform = self._get_platform_for_connection(step.connection_name)
                dataset_urn = builder.make_dataset_urn(
                    platform=platform,
                    name=step.table_name,
                    env=self.env
                )
                dataset_urns.append(dataset_urn)
                
                if self.report:
                    self.report.report_lineage_created(step.name, dataset_urn)
            
        except Exception as e:
            error_msg = f"Failed to extract output datasets from step {step.name}: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_lineage_failed(step.name, "", error_msg)
        
        return dataset_urns
    
    def _extract_datasets_from_sql(self, sql: str, connection_name: Optional[str]) -> List[str]:
        """Extract dataset URNs from SQL using DataHub's SQL parser"""
        dataset_urns = []
        
        if not self.sql_parser:
            logger.warning("SQL parsing not available, skipping SQL-based lineage extraction")
            return dataset_urns
        
        try:
            platform = self._get_platform_for_connection(connection_name)
            
            # Use DataHub's SQL parsing capabilities
            parsing_result = self.sql_parser.parse_sql_query(
                sql=sql,
                platform=platform,
                env=self.env,
                default_db=None,  # We don't have default database info
                default_schema=None,
            )
            
            if parsing_result.in_tables:
                for table_ref in parsing_result.in_tables:
                    # Convert table reference to dataset URN
                    table_name = str(table_ref)
                    dataset_urn = builder.make_dataset_urn(
                        platform=platform,
                        name=table_name,
                        env=self.env
                    )
                    dataset_urns.append(dataset_urn)
            
            if parsing_result.out_tables:
                for table_ref in parsing_result.out_tables:
                    # Convert table reference to dataset URN
                    table_name = str(table_ref)
                    dataset_urn = builder.make_dataset_urn(
                        platform=platform,
                        name=table_name,
                        env=self.env
                    )
                    dataset_urns.append(dataset_urn)
                    
        except Exception as e:
            error_msg = f"Failed to parse SQL: {sql[:100]}... Error: {e}"
            logger.warning(error_msg)
            if self.report:
                self.report.report_sql_parsing_error(sql, str(e))
        
        return dataset_urns
    
    def _get_platform_for_connection(self, connection_name: Optional[str]) -> str:
        """Get the platform name for a database connection"""
        if not connection_name:
            return self.default_database_platform
        
        platform = self.database_mapping.get(connection_name)
        if platform:
            if self.report:
                self.report.report_database_connection(connection_name)
            return platform
        
        logger.warning(f"Unknown database connection: {connection_name}, using default platform: {self.default_database_platform}")
        return self.default_database_platform
    
    def create_upstream_lineage(
        self,
        input_datasets: List[str],
        output_datasets: List[str]
    ) -> Optional[UpstreamLineageClass]:
        """Create upstream lineage aspect for a job"""
        if not input_datasets and not output_datasets:
            return None
        
        upstreams = []
        
        # Create upstream entries for input datasets
        for dataset_urn in input_datasets:
            upstream = UpstreamClass(
                dataset=dataset_urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstreams.append(upstream)
        
        if not upstreams:
            return None
        
        return UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=None,  # Could be enhanced later for column-level lineage
        )
    
    def extract_table_to_table_lineage(self, job: PentahoJob) -> List[Tuple[str, str]]:
        """
        Extract direct table-to-table lineage relationships.
        Returns list of (upstream_urn, downstream_urn) tuples.
        """
        lineage_edges = []
        
        input_datasets, output_datasets = self.extract_lineage(job)
        
        # Create lineage edges from all inputs to all outputs
        # This is a simplified approach - in reality, the lineage might be more complex
        for input_dataset in input_datasets:
            for output_dataset in output_datasets:
                lineage_edges.append((input_dataset, output_dataset))
                if self.report:
                    self.report.report_lineage_created(input_dataset, output_dataset)
        
        return lineage_edges