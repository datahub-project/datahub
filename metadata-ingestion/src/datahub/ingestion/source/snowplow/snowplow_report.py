"""
Custom report class for Snowplow source.

Tracks extraction statistics, errors, and warnings.
"""

from dataclasses import dataclass, field
from typing import Dict, List

from datahub.ingestion.source.snowplow.constants import SchemaType
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class SnowplowSourceReport(StaleEntityRemovalSourceReport):
    """
    Report for Snowplow source ingestion.

    Tracks:
    - Schemas extracted (events and entities)
    - Event specifications extracted
    - Tracking scenarios extracted
    - Lineage extracted
    - Errors and warnings
    """

    # Connection info
    connection_mode: str = "unknown"  # "bdp", "iglu", or "both"
    organization_id: str = ""

    # Schema extraction stats
    num_event_schemas_found: int = 0
    num_entity_schemas_found: int = 0
    num_event_schemas_extracted: int = 0
    num_entity_schemas_extracted: int = 0
    num_event_schemas_filtered: int = 0
    num_entity_schemas_filtered: int = 0

    # Event specifications stats
    num_event_specs_found: int = 0
    num_event_specs_extracted: int = 0
    num_event_specs_filtered: int = 0

    # Tracking scenarios stats
    num_tracking_scenarios_found: int = 0
    num_tracking_scenarios_extracted: int = 0
    num_tracking_scenarios_filtered: int = 0

    # Data products stats
    num_data_products_found: int = 0
    num_data_products_extracted: int = 0
    num_data_products_filtered: int = 0

    # Pipeline stats (DataFlow in DataHub)
    num_pipelines_found: int = 0
    num_pipelines_extracted: int = 0
    num_pipelines_filtered: int = 0

    # Enrichment stats (DataJob in DataHub)
    num_enrichments_found: int = 0
    num_enrichments_extracted: int = 0
    num_enrichments_filtered: int = 0

    # Lineage stats
    num_warehouse_lineage_extracted: int = 0

    # Deployment stats
    num_schemas_deployed: int = 0
    num_schemas_not_deployed: int = 0

    # Hidden schemas
    num_hidden_schemas: int = 0
    num_hidden_schemas_skipped: int = 0

    # Schema parsing errors
    schema_parsing_errors: List[str] = field(default_factory=list)
    schema_parsing_warnings: List[str] = field(default_factory=list)

    # API errors
    api_errors: Dict[str, List[str]] = field(default_factory=dict)

    # Filtered items (for debugging)
    filtered_schemas: List[str] = field(default_factory=list)
    filtered_event_specs: List[str] = field(default_factory=list)
    filtered_tracking_scenarios: List[str] = field(default_factory=list)

    def report_schema_found(self, schema_type: str) -> None:
        """Record that a schema was found."""
        if schema_type == SchemaType.EVENT.value:
            self.num_event_schemas_found += 1
        elif schema_type == SchemaType.ENTITY.value:
            self.num_entity_schemas_found += 1

    def report_schema_extracted(self, schema_type: str) -> None:
        """Record that a schema was successfully extracted."""
        if schema_type == SchemaType.EVENT.value:
            self.num_event_schemas_extracted += 1
        elif schema_type == SchemaType.ENTITY.value:
            self.num_entity_schemas_extracted += 1

    def report_schema_filtered(self, schema_type: str, schema_name: str) -> None:
        """Record that a schema was filtered out."""
        if schema_type == SchemaType.EVENT.value:
            self.num_event_schemas_filtered += 1
        elif schema_type == SchemaType.ENTITY.value:
            self.num_entity_schemas_filtered += 1
        self.filtered_schemas.append(schema_name)

    def report_event_spec_found(self) -> None:
        """Record that an event specification was found."""
        self.num_event_specs_found += 1

    def report_event_spec_extracted(self) -> None:
        """Record that an event specification was extracted."""
        self.num_event_specs_extracted += 1

    def report_event_spec_filtered(self, spec_name: str) -> None:
        """Record that an event specification was filtered out."""
        self.num_event_specs_filtered += 1
        self.filtered_event_specs.append(spec_name)

    def report_tracking_scenario_found(self) -> None:
        """Record that a tracking scenario was found."""
        self.num_tracking_scenarios_found += 1

    def report_tracking_scenario_extracted(self) -> None:
        """Record that a tracking scenario was extracted."""
        self.num_tracking_scenarios_extracted += 1

    def report_tracking_scenario_filtered(self, scenario_name: str) -> None:
        """Record that a tracking scenario was filtered out."""
        self.num_tracking_scenarios_filtered += 1
        self.filtered_tracking_scenarios.append(scenario_name)

    def report_pipeline_found(self) -> None:
        """Record that a pipeline was found."""
        self.num_pipelines_found += 1

    def report_pipeline_extracted(self) -> None:
        """Record that a pipeline was extracted."""
        self.num_pipelines_extracted += 1

    def report_pipeline_filtered(self, pipeline_name: str) -> None:
        """Record that a pipeline was filtered out."""
        self.num_pipelines_filtered += 1

    def report_enrichment_found(self) -> None:
        """Record that an enrichment was found."""
        self.num_enrichments_found += 1

    def report_enrichment_extracted(self) -> None:
        """Record that an enrichment was extracted."""
        self.num_enrichments_extracted += 1

    def report_enrichment_filtered(self, enrichment_name: str) -> None:
        """Record that an enrichment was filtered out."""
        self.num_enrichments_filtered += 1

    def report_warehouse_lineage_extracted(self) -> None:
        """Record that warehouse lineage was extracted."""
        self.num_warehouse_lineage_extracted += 1

    def report_schema_deployed(self) -> None:
        """Record that a schema is deployed."""
        self.num_schemas_deployed += 1

    def report_schema_not_deployed(self) -> None:
        """Record that a schema is not deployed."""
        self.num_schemas_not_deployed += 1

    def report_hidden_schema(self, skipped: bool = False) -> None:
        """Record a hidden schema."""
        self.num_hidden_schemas += 1
        if skipped:
            self.num_hidden_schemas_skipped += 1

    def report_schema_parsing_error(self, error: str) -> None:
        """Record a schema parsing error."""
        self.schema_parsing_errors.append(error)

    def report_schema_parsing_warning(self, warning: str) -> None:
        """Record a schema parsing warning."""
        self.schema_parsing_warnings.append(warning)

    def report_api_error(self, api_name: str, error: str) -> None:
        """Record an API error."""
        if api_name not in self.api_errors:
            self.api_errors[api_name] = []
        self.api_errors[api_name].append(error)

    def compute_stats(self) -> None:
        """Compute final statistics (called at end of ingestion)."""
        # Total schemas (computed for potential future use)
        # total_schemas_found = self.num_event_schemas_found + self.num_entity_schemas_found
        # total_schemas_extracted = self.num_event_schemas_extracted + self.num_entity_schemas_extracted
        pass

        # Add to warnings if significant filtering occurred
        if self.num_event_schemas_filtered > 0:
            self.report_warning(
                "schema_filtering",
                f"Filtered out {self.num_event_schemas_filtered} event schemas. "
                f"Check schema_pattern configuration.",
            )

        if self.num_entity_schemas_filtered > 0:
            self.report_warning(
                "schema_filtering",
                f"Filtered out {self.num_entity_schemas_filtered} entity schemas. "
                f"Check schema_pattern configuration.",
            )

        # Report hidden schemas
        if self.num_hidden_schemas_skipped > 0:
            self.report_warning(
                "hidden_schemas",
                f"Skipped {self.num_hidden_schemas_skipped} hidden schemas. "
                f"Set include_hidden_schemas=true to include them.",
            )

        # Report schema parsing errors
        if self.schema_parsing_errors:
            for error in self.schema_parsing_errors[:5]:  # Limit to 5 errors
                self.report_warning("schema_parsing", error)

        # Report API errors
        for api_name, errors in self.api_errors.items():
            for error in errors[:3]:  # Limit to 3 errors per API
                self.report_failure(api_name, error)
