from dataclasses import dataclass, field
from typing import Dict, List

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class PentahoSourceReport(StaleEntityRemovalSourceReport):
    """Report for Pentaho source ingestion"""
    
    # File processing stats
    files_processed: int = 0
    files_failed: int = 0
    files_skipped: int = 0
    
    # Job extraction stats
    jobs_processed: int = 0
    jobs_with_lineage: int = 0
    jobs_without_lineage: int = 0
    
    # Step processing stats
    table_input_steps_processed: int = 0
    table_output_steps_processed: int = 0
    other_steps_processed: int = 0
    
    # Lineage stats
    lineage_edges_created: int = 0
    lineage_edges_failed: int = 0
    
    # Errors and warnings
    parse_errors: LossyList[str] = field(default_factory=LossyList)
    lineage_extraction_errors: LossyList[str] = field(default_factory=LossyList)
    sql_parsing_errors: LossyList[str] = field(default_factory=LossyList)
    variable_resolution_errors: LossyList[str] = field(default_factory=LossyList)
    
    # Detailed breakdown
    files_by_type: Dict[str, int] = field(default_factory=dict)  # .ktr vs .kjb
    database_connections_found: LossyDict[str, int] = field(default_factory=LossyDict)
    unresolved_variables: LossyDict[str, int] = field(default_factory=LossyDict)
    
    def report_file_processed(self, file_path: str, file_type: str) -> None:
        """Report that a file was successfully processed"""
        self.files_processed += 1
        self.files_by_type[file_type] = self.files_by_type.get(file_type, 0) + 1
        
    def report_file_failed(self, file_path: str, error: str) -> None:
        """Report that a file failed to process"""
        self.files_failed += 1
        self.parse_errors.append(f"{file_path}: {error}")
        
    def report_file_skipped(self, file_path: str, reason: str) -> None:
        """Report that a file was skipped"""
        self.files_skipped += 1
        
    def report_job_processed(self, job_name: str, has_lineage: bool) -> None:
        """Report that a job was processed"""
        self.jobs_processed += 1
        if has_lineage:
            self.jobs_with_lineage += 1
        else:
            self.jobs_without_lineage += 1
            
    def report_step_processed(self, step_type: str) -> None:
        """Report that a step was processed"""
        if step_type.lower() == "tableinput":
            self.table_input_steps_processed += 1
        elif step_type.lower() == "tableoutput":
            self.table_output_steps_processed += 1
        else:
            self.other_steps_processed += 1
            
    def report_lineage_created(self, upstream: str, downstream: str) -> None:
        """Report that a lineage edge was created"""
        self.lineage_edges_created += 1
        
    def report_lineage_failed(self, upstream: str, downstream: str, error: str) -> None:
        """Report that a lineage edge failed to create"""
        self.lineage_edges_failed += 1
        self.lineage_extraction_errors.append(f"{upstream} -> {downstream}: {error}")
        
    def report_sql_parsing_error(self, sql: str, error: str) -> None:
        """Report an SQL parsing error"""
        self.sql_parsing_errors.append(f"SQL: {sql[:100]}... Error: {error}")
        
    def report_variable_resolution_error(self, variable: str, error: str) -> None:
        """Report a variable resolution error"""
        self.variable_resolution_errors.append(f"Variable {variable}: {error}")
        
    def report_database_connection(self, connection_name: str) -> None:
        """Report a database connection found"""
        self.database_connections_found[connection_name] = (
            self.database_connections_found.get(connection_name, 0) + 1
        )
        
    def report_unresolved_variable(self, variable: str) -> None:
        """Report an unresolved variable"""
        self.unresolved_variables[variable] = (
            self.unresolved_variables.get(variable, 0) + 1
        )