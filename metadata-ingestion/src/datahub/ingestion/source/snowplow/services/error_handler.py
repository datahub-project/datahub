"""
Centralized error handling for Snowplow connector.

Provides consistent error handling, logging, and reporting across all processors and services.
"""

import logging
from typing import Optional

from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport

logger = logging.getLogger(__name__)


class ErrorHandler:
    """
    Centralized error handler for consistent error management.

    Benefits:
    - Consistent error logging format
    - Automatic report updates
    - Structured error context
    - Reduces error handling duplication
    """

    def __init__(self, report: SnowplowSourceReport):
        """
        Initialize error handler.

        Args:
            report: Source report for tracking failures
        """
        self.report = report

    def handle_api_error(
        self,
        error: Exception,
        operation: str,
        context: Optional[str] = None,
        entity: Optional[str] = None,
    ) -> None:
        """
        Handle API errors with consistent logging and reporting.

        Args:
            error: The exception that occurred
            operation: Description of the operation that failed (e.g., "fetch event specifications")
            context: Additional context (e.g., "organization_id=abc123")
            entity: Optional entity identifier for more specific context
        """
        error_context = f"{operation}"
        if entity:
            error_context += f" for {entity}"
        if context:
            error_context += f" ({context})"

        self.report.report_failure(
            title=f"API Error: {operation}",
            message=f"Failed to {operation}: {str(error)}",
            context=error_context,
            exc=error,
        )

        logger.error(
            f"API error during {operation}: {str(error)}",
            exc_info=error,
            extra={"context": context, "entity": entity},
        )

    def handle_validation_error(
        self,
        error: Exception,
        entity_type: str,
        entity_id: str,
        field: Optional[str] = None,
    ) -> None:
        """
        Handle validation errors for entity processing.

        Args:
            error: The validation exception
            entity_type: Type of entity (e.g., "schema", "pipeline")
            entity_id: Identifier for the entity
            field: Optional specific field that failed validation
        """
        error_context = f"{entity_type}: {entity_id}"
        if field:
            error_context += f", field: {field}"

        self.report.report_failure(
            title=f"Validation Error: {entity_type}",
            message=f"Validation failed for {entity_type} '{entity_id}': {str(error)}",
            context=error_context,
            exc=error,
        )

        logger.warning(
            f"Validation error for {entity_type} '{entity_id}': {str(error)}",
            extra={"entity_type": entity_type, "entity_id": entity_id, "field": field},
        )

    def handle_processing_error(
        self,
        error: Exception,
        operation: str,
        entity_type: str,
        entity_id: str,
        fatal: bool = False,
    ) -> None:
        """
        Handle processing errors during entity extraction.

        Args:
            error: The processing exception
            operation: What was being processed (e.g., "parse schema", "build lineage")
            entity_type: Type of entity being processed
            entity_id: Identifier for the entity
            fatal: Whether this error should stop processing (vs log and continue)
        """
        error_context = f"{operation} for {entity_type}: {entity_id}"

        self.report.report_failure(
            title=f"Processing Error: {operation}",
            message=f"Failed to {operation} for {entity_type} '{entity_id}': {str(error)}",
            context=error_context,
            exc=error,
        )

        log_level = logging.ERROR if fatal else logging.WARNING
        logger.log(
            log_level,
            f"Processing error during {operation} for {entity_type} '{entity_id}': {str(error)}",
            exc_info=error if fatal else False,
            extra={"entity_type": entity_type, "entity_id": entity_id, "fatal": fatal},
        )

    def handle_missing_dependency(
        self,
        dependency: str,
        operation: str,
        suggestion: Optional[str] = None,
    ) -> None:
        """
        Handle missing dependency errors.

        Args:
            dependency: Name of the missing dependency
            operation: What operation requires the dependency
            suggestion: Optional suggestion for resolving the issue
        """
        message = f"Missing dependency '{dependency}' required for {operation}"
        if suggestion:
            message += f". {suggestion}"

        self.report.report_failure(
            title=f"Missing Dependency: {dependency}",
            message=message,
            context=f"operation={operation}",
        )

        logger.error(
            f"Missing dependency: {dependency} (needed for {operation})",
            extra={
                "dependency": dependency,
                "operation": operation,
                "suggestion": suggestion,
            },
        )

    def log_warning(
        self,
        message: str,
        context: Optional[str] = None,
    ) -> None:
        """
        Log a warning without updating the report.

        Use for non-fatal issues that don't require report tracking.

        Args:
            message: Warning message
            context: Optional additional context
        """
        log_message = message
        if context:
            log_message += f" ({context})"

        logger.warning(log_message, extra={"context": context})

    def log_info(
        self,
        message: str,
        context: Optional[str] = None,
    ) -> None:
        """
        Log informational message.

        Args:
            message: Info message
            context: Optional additional context
        """
        log_message = message
        if context:
            log_message += f" ({context})"

        logger.info(log_message, extra={"context": context})
