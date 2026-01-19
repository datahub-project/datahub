"""
PII Pseudonymization enrichment lineage extractor.

Extracts field-level lineage for the PII Pseudonymization enrichment, which
pseudonymizes (hashes/encrypts) PII fields to protect user privacy.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/pii-pseudonymization-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow.enrichments/pii_enrichment_config/jsonschema/2-0-0

Output:
This enrichment MODIFIES existing fields in-place by pseudonymizing them.
For example, if configured to pseudonymize `user_id`, the original value is
replaced with a pseudonymized (hashed) version.

Lineage Pattern:
- field → field (same field name, transformation type: PSEUDONYMIZED)
- Example: user_id → user_id (pseudonymized)

Field Types:
- POJO fields: Atomic event fields (user_id, user_ipaddress, etc.)
- JSON fields: Fields within contexts/unstruct_event matching specific schemas
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment
from datahub.ingestion.source.snowplow.utils.field_reference_parser import (
    FieldReferenceParser,
    JsonField,
)

logger = logging.getLogger(__name__)


class PiiPseudonymizationLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for PII Pseudonymization enrichment lineage.

    This enrichment reads PII fields and writes pseudonymized (hashed/encrypted)
    versions back to the same fields. The configuration specifies which fields
    to pseudonymize and the pseudonymization strategy.

    Supports two types of field references:
    1. POJO fields: Atomic event fields (user_id, user_ipaddress, etc.)
    2. JSON fields: Fields within self-describing JSON (contexts, unstruct_event)
       identified by schema criterion and JSONPath

    Transformation Type:
    IN_PLACE - The field is modified in-place (same field name, pseudonymized value)
    """

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles PII Pseudonymization enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for PII enrichment
        """
        return "pii_enrichment_config" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for PII Pseudonymization enrichment.

        Parses the enrichment configuration to identify which fields are
        pseudonymized and creates lineage records for each.

        Args:
            enrichment: PII enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List of field lineages for pseudonymized fields
        """
        if not warehouse_table_urn:
            return []

        # Get parameters from enrichment
        parameters = self._get_parameters(enrichment)
        if not parameters:
            logger.debug(
                f"PII Pseudonymization: No parameters found for {enrichment.filename}"
            )
            return []

        # Parse PII configuration using the generic field reference parser
        field_config = FieldReferenceParser.parse_from_key(parameters, "pii")

        if field_config.is_empty():
            logger.debug(
                f"PII Pseudonymization: No PII fields configured in {enrichment.filename}"
            )
            return []

        lineages: List[FieldLineage] = []

        # Process POJO fields (atomic event fields)
        for pojo_field in field_config.pojo_fields:
            lineages.append(
                self._create_pojo_lineage(
                    field_name=pojo_field.field_name,
                    event_schema_urn=event_schema_urn,
                    warehouse_table_urn=warehouse_table_urn,
                )
            )

        # Process JSON fields (fields within contexts/unstruct_event)
        for json_field in field_config.json_fields:
            lineages.extend(
                self._create_json_lineages(
                    json_field_config=json_field,
                    event_schema_urn=event_schema_urn,
                    warehouse_table_urn=warehouse_table_urn,
                )
            )

        if lineages:
            pojo_count = len(field_config.pojo_fields)
            json_count = len(field_config.json_fields)
            logger.debug(
                f"PII Pseudonymization: Extracted {len(lineages)} field lineages "
                f"({pojo_count} POJO fields, {json_count} JSON field configs)"
            )

        return lineages

    def _get_parameters(self, enrichment: Enrichment) -> Optional[dict]:
        """Safely get enrichment parameters."""
        if not enrichment.content:
            return None
        if not enrichment.content.data:
            return None
        return enrichment.content.data.parameters

    def _create_pojo_lineage(
        self,
        field_name: str,
        event_schema_urn: str,
        warehouse_table_urn: str,
    ) -> FieldLineage:
        """
        Create lineage for a POJO (atomic event) field.

        POJO fields are in-place transformations: the same field name
        in the input becomes the pseudonymized value in the output.

        Args:
            field_name: Name of the POJO field (e.g., "user_id")
            event_schema_urn: URN of the event schema
            warehouse_table_urn: URN of the warehouse table

        Returns:
            FieldLineage for this POJO field
        """
        upstream_field = f"urn:li:schemaField:({event_schema_urn},{field_name})"
        downstream_field = f"urn:li:schemaField:({warehouse_table_urn},{field_name})"

        return FieldLineage(
            upstream_fields=[upstream_field],
            downstream_fields=[downstream_field],
            transformation_type="IN_PLACE",
        )

    def _create_json_lineages(
        self,
        json_field_config: JsonField,
        event_schema_urn: str,
        warehouse_table_urn: str,
    ) -> List[FieldLineage]:
        """
        Create lineages for JSON field references.

        JSON fields reference nested data within contexts or unstruct_event.
        The field path includes the context prefix in the warehouse table.

        Args:
            json_field_config: Parsed JSON field configuration
            event_schema_urn: URN of the event schema
            warehouse_table_urn: URN of the warehouse table

        Returns:
            List of FieldLineage objects for each field in the JSONPath
        """
        lineages: List[FieldLineage] = []

        # Get all field paths from the JSONPath
        # e.g., "$.customer.email" → ["customer.email"]
        # e.g., "$.['clientId', 'userId']" → ["clientId", "userId"]
        field_paths = json_field_config.get_field_names()

        for field_path in field_paths:
            # The upstream field is the field within the event schema
            # For JSON fields, we use the full path (e.g., "customer.email")
            upstream_field = f"urn:li:schemaField:({event_schema_urn},{field_path})"

            # The downstream field in the warehouse may have a context prefix
            # e.g., "contexts_com_acme_checkout_1.customer.email"
            # For simplicity, we use the leaf field name
            leaf_field = field_path.split(".")[-1] if "." in field_path else field_path
            downstream_field = (
                f"urn:li:schemaField:({warehouse_table_urn},{leaf_field})"
            )

            lineages.append(
                FieldLineage(
                    upstream_fields=[upstream_field],
                    downstream_fields=[downstream_field],
                    transformation_type="IN_PLACE",
                )
            )

        return lineages
