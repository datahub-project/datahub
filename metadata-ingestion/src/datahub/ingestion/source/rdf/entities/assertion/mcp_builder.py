"""
Assertion MCP Builder

Builds DataHub MCPs for data quality assertions.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.assertion.ast import DataHubAssertion
from datahub.ingestion.source.rdf.entities.assertion.urn_generator import (
    AssertionUrnGenerator,
)
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder

logger = logging.getLogger(__name__)


class AssertionMCPBuilder(EntityMCPBuilder[DataHubAssertion]):
    """
    Builds DataHub MCPs for data quality assertions.
    """

    @property
    def entity_type(self) -> str:
        return "assertion"

    def __init__(self):
        self.urn_generator = AssertionUrnGenerator()

    def build_mcps(  # noqa: C901
        self, entity: DataHubAssertion, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single assertion using FieldValuesAssertion API for Column Assertions.

        Note: The dataset referenced by entity.dataset_urn must exist in DataHub before
        assertions can be visible. Assertions will not appear if:
        1. The dataset doesn't exist
        2. The field doesn't exist in the dataset schema
        3. The assertion hasn't been evaluated yet (some DataHub versions)
        """
        try:
            from datahub.api.entities.assertion.assertion_operator import (
                GreaterThanOrEqualToOperator,
                InOperator,
                LessThanOrEqualToOperator,
                MatchesRegexOperator,
                NotNullOperator,
            )
            from datahub.api.entities.assertion.field_assertion import (
                FieldValuesAssertion,
            )

            # Generate assertion URN
            assertion_urn = self.urn_generator.generate_assertion_urn(
                entity.dataset_urn,
                entity.field_name or "dataset",
                entity.operator or "CUSTOM",
            )

            # Only create Column Assertions for field-level assertions
            if not entity.field_name:
                logger.warning(
                    f"Skipping dataset-level assertion {entity.assertion_key} - only field-level (Column) assertions are supported"
                )
                return []

            # Log warning if dataset might not exist (helpful for debugging)
            logger.debug(
                f"Creating assertion for dataset {entity.dataset_urn}, field {entity.field_name}. "
                f"Ensure the dataset exists in DataHub before assertions will be visible."
            )

            # Extract constraint value from parameters based on operator type
            constraint_value = None
            pattern_value = None

            if entity.parameters:
                # Handle pattern constraint (highest priority)
                if "pattern" in entity.parameters:
                    pattern_value = entity.parameters["pattern"]
                elif "constraint_value" in entity.parameters:
                    constraint_value = entity.parameters["constraint_value"]
                    # For REGEX_MATCH, pattern might be in constraint_value
                    if entity.operator == "REGEX_MATCH" or entity.operator == "MATCHES":
                        pattern_value = constraint_value

                # Extract based on operator type (if not already extracted)
                if constraint_value is None and pattern_value is None:
                    if entity.operator in [
                        "GREATER_THAN_OR_EQUAL",
                        "GREATER_THAN_OR_EQUAL_TO",
                    ]:
                        # Try minValue first, then minInclusive (handle 0.0 case with 'in' check)
                        constraint_value = (
                            entity.parameters.get("minValue")
                            if "minValue" in entity.parameters
                            else entity.parameters.get("minInclusive")
                        )
                    elif entity.operator in [
                        "LESS_THAN_OR_EQUAL",
                        "LESS_THAN_OR_EQUAL_TO",
                    ]:
                        # Try maxLength, maxValue, or maxInclusive (handle 0 case with 'in' checks)
                        if "maxLength" in entity.parameters:
                            constraint_value = entity.parameters["maxLength"]
                        elif "maxValue" in entity.parameters:
                            constraint_value = entity.parameters["maxValue"]
                        elif "maxInclusive" in entity.parameters:
                            constraint_value = entity.parameters["maxInclusive"]
                    elif entity.operator == "IN":
                        constraint_value = (
                            entity.parameters.get("enum")
                            if "enum" in entity.parameters
                            else entity.parameters.get("allowedValues")
                        )

            # Map operator to condition based on operator type and parameters
            # Note: Operators from extractor use different names than DataHub conditions
            condition = None

            # Pattern/regex matching
            if (
                entity.operator == "REGEX_MATCH" or entity.operator == "MATCHES"
            ) and pattern_value:
                condition = MatchesRegexOperator(
                    type="matches_regex", value=str(pattern_value)
                )
            # Greater than or equal (handles both _TO and without _TO variants)
            elif (
                entity.operator == "GREATER_THAN_OR_EQUAL_TO"
                or entity.operator == "GREATER_THAN_OR_EQUAL"
            ) and constraint_value is not None:
                # Extract numeric value
                value = self._extract_numeric_value(constraint_value)
                if value is not None:
                    condition = GreaterThanOrEqualToOperator(
                        type="greater_than_or_equal_to", value=value
                    )
            # Less than or equal (handles both _TO and without _TO variants, and maxLength)
            elif (
                entity.operator == "LESS_THAN_OR_EQUAL_TO"
                or entity.operator == "LESS_THAN_OR_EQUAL"
            ) and constraint_value is not None:
                # Extract numeric value
                value = self._extract_numeric_value(constraint_value)
                if value is not None:
                    condition = LessThanOrEqualToOperator(
                        type="less_than_or_equal_to", value=value
                    )
            # Not null
            elif entity.operator == "NOT_NULL":
                condition = NotNullOperator(type="is_not_null")
            # IN operator
            elif entity.operator == "IN" and constraint_value:
                # For IN operator, constraint_value should be a list
                if isinstance(constraint_value, list):
                    condition = InOperator(type="in", value=constraint_value)
                elif isinstance(constraint_value, str):
                    # Try to parse as comma-separated list
                    values = [v.strip() for v in constraint_value.split(",")]
                    condition = InOperator(type="in", value=values)
            # EQUALS operator (for datatype constraints) - skip, not a valid Column Assertion
            elif entity.operator == "EQUALS":
                logger.warning(
                    f"Skipping EQUALS assertion {entity.assertion_key} (Dataset={entity.dataset_urn}, Field={entity.field_name}) - datatype constraints are not Column Assertions"
                )
                return []

            # Skip assertion if no condition can be created - no defaulting
            if condition is None:
                logger.info(
                    f"Skipping assertion {entity.assertion_key} "
                    f"(Dataset={entity.dataset_urn}, Field={entity.field_name}): "
                    f"could not create condition for operator '{entity.operator}' "
                    f"with parameters {entity.parameters}"
                )
                return []

            # Create FieldValuesAssertion using the high-level API (creates Column Assertions)
            # Note: type must be "field", and use condition not operator
            # Match old behavior: use field_name or "" (old code allowed empty strings)
            field_name = entity.field_name or ""
            field_assertion = FieldValuesAssertion(
                type="field",  # Required: must be "field" for Column Assertions
                entity=str(entity.dataset_urn),
                field=field_name,  # Match old behavior: allow empty string
                condition=condition,  # Use condition, not operator
                exclude_nulls=True,
                failure_threshold={
                    "type": "count",
                    "value": 0,
                },  # Fail on any violation
                description=entity.description
                or f"Assertion for {field_name or 'dataset'}",
            )

            # Get the assertion info aspect from the FieldValuesAssertion
            assertion_info = field_assertion.get_assertion_info()

            mcp = MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=assertion_info,
            )

            # Log assertion details for verbose mode
            logger.debug(
                f"Created Column Assertion: URN={assertion_urn}, "
                f"Dataset={entity.dataset_urn}, Field={entity.field_name}, "
                f"Operator={entity.operator}, Description={entity.description or 'N/A'}"
            )

            return [mcp]

        except Exception as e:
            logger.warning(
                f"Error building MCP for assertion {entity.assertion_key}: {e}"
            )
            return []

    def _extract_numeric_value(self, value: Any) -> Optional[float]:
        """Extract numeric value from various formats."""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                # Try to parse as float
                return float(value)
            return None
        except (ValueError, TypeError):
            return None

    def build_all_mcps(
        self, entities: List[DataHubAssertion], context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for all assertions."""
        mcps = []
        created_count = 0
        skipped_count = 0

        logger.info(f"Building MCPs for {len(entities)} assertions...")

        for entity in entities:
            entity_mcps = self.build_mcps(entity, context)
            if entity_mcps:
                mcps.extend(entity_mcps)
                created_count += 1
            else:
                skipped_count += 1

        logger.info(
            f"Built {len(mcps)} assertion MCPs: {created_count} assertions created, {skipped_count} skipped"
        )
        return mcps
