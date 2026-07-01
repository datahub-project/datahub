"""
Snowplow Enrichment Field Reference Parser.

Parses field references from Snowplow enrichment configurations. This is a common
pattern used by several enrichments (PII Pseudonymization, SQL Enrichment,
API Request Enrichment) to specify which fields to read from or transform.

Field Reference Structure:
{
  "pojo": [{"field": "user_id"}, {"field": "user_ipaddress"}],
  "json": [
    {
      "field": "contexts",
      "schemaCriterion": "iglu:com.acme/checkout/jsonschema/1-*-*",
      "jsonPath": "$.customer.email"
    }
  ]
}

Two types of field references:
1. POJO (Plain Old Java Object): Scalar fields in the enriched atomic event
2. JSON: Fields within self-describing JSON (contexts, unstruct_event, derived_contexts)

Reference:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/pii-pseudonymization-enrichment/
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/custom-sql-enrichment/
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/custom-api-request-enrichment/
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)


@dataclass
class PojoField:
    """
    A POJO (Plain Old Java Object) field reference.

    These are scalar fields in the enriched atomic event like user_id, user_ipaddress.
    Used by enrichments to reference specific fields for input or transformation.
    """

    field_name: str

    def __hash__(self) -> int:
        return hash(self.field_name)


@dataclass
class JsonField:
    """
    A JSON field reference for self-describing JSON data.

    These reference fields within self-describing JSON structures:
    - contexts: Array of context entities attached to events
    - unstruct_event: The unstructured event payload
    - derived_contexts: Contexts added by enrichments

    The field is identified by:
    - json_field: Which container to look in (contexts, unstruct_event, derived_contexts)
    - schema_criterion: Iglu schema pattern to match (supports wildcards like 1-*-*)
    - json_path: JSONPath expression to extract the specific field value
    """

    # Which JSON field to look in: "contexts", "unstruct_event", or "derived_contexts"
    json_field: str
    # Iglu schema pattern to match, e.g., "iglu:com.acme/checkout/jsonschema/1-*-*"
    schema_criterion: str
    # JSONPath expression, e.g., "$.customer.email" or "$.['clientId', 'userId']"
    json_path: str
    # Extracted field names from the JSONPath
    extracted_fields: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Parse JSONPath to extract field names."""
        self.extracted_fields = self._parse_json_path(self.json_path)

    @staticmethod
    def _parse_json_path(json_path: str) -> List[str]:
        """
        Parse JSONPath expression to extract field names.

        Handles various JSONPath formats:
        - $.email → ["email"]
        - $.customer.email → ["customer.email"]
        - $.['clientId', 'userId'] → ["clientId", "userId"]
        - $.data.['field1', 'field2'] → ["data.field1", "data.field2"]

        Args:
            json_path: JSONPath expression starting with $

        Returns:
            List of extracted field paths
        """
        if not json_path or not json_path.startswith("$"):
            return []

        # Remove the leading $
        path = json_path[1:]

        # Handle bracket notation with multiple fields: $.['field1', 'field2']
        bracket_match = re.search(r"\.\[([^\]]+)\]$", path)
        if bracket_match:
            # Get the prefix before the bracket (e.g., ".data" from "$.data.['f1', 'f2']")
            prefix = path[: bracket_match.start()]
            if prefix.startswith("."):
                prefix = prefix[1:]

            # Extract field names from bracket notation
            bracket_content = bracket_match.group(1)
            # Parse comma-separated quoted strings: 'field1', 'field2'
            field_names = re.findall(r"['\"]([^'\"]+)['\"]", bracket_content)

            if prefix:
                return [f"{prefix}.{f}" for f in field_names]
            return field_names

        # Handle simple dot notation: $.customer.email or $.email
        if path.startswith("."):
            path = path[1:]

        # Return the full path as a single field
        if path:
            return [path]

        return []

    def get_field_names(self) -> List[str]:
        """Get the extracted field names (full paths)."""
        return self.extracted_fields

    def get_leaf_field_names(self) -> List[str]:
        """
        Get just the leaf field names (last segment of each path).

        e.g., "customer.email" → "email"
        """
        return [f.split(".")[-1] for f in self.extracted_fields]

    def matches_schema(self, iglu_uri: str) -> bool:
        """
        Check if an Iglu URI matches this field's schema criterion.

        The schema_criterion can contain wildcards like "1-*-*" to match
        any version within the major version.

        Args:
            iglu_uri: Full Iglu URI like "iglu:com.acme/checkout/jsonschema/1-0-0"

        Returns:
            True if the URI matches the criterion pattern
        """
        if not self.schema_criterion or not iglu_uri:
            return False

        # Convert criterion pattern to regex
        # "iglu:com.acme/checkout/jsonschema/1-*-*" → regex pattern
        pattern = re.escape(self.schema_criterion)
        pattern = pattern.replace(r"\*", r"[^/\-]+")  # * matches version segments

        try:
            return bool(re.fullmatch(pattern, iglu_uri))
        except re.error:
            logger.warning(f"Invalid schema criterion pattern: {self.schema_criterion}")
            return False

    def __hash__(self) -> int:
        return hash((self.json_field, self.schema_criterion, self.json_path))


@dataclass
class FieldReferenceConfig:
    """
    Parsed field reference configuration from an enrichment.

    Contains both POJO fields (atomic event fields) and JSON fields
    (fields within self-describing JSON).
    """

    pojo_fields: List[PojoField] = field(default_factory=list)
    json_fields: List[JsonField] = field(default_factory=list)

    def get_all_field_names(self) -> Set[str]:
        """
        Get all field names (both POJO and JSON leaf fields).

        Returns:
            Set of all field names
        """
        names: Set[str] = set()

        # Add POJO field names
        for pojo in self.pojo_fields:
            names.add(pojo.field_name)

        # Add JSON field leaf names
        for json_field in self.json_fields:
            names.update(json_field.get_leaf_field_names())

        return names

    def get_pojo_field_names(self) -> Set[str]:
        """Get just the POJO (atomic) field names."""
        return {pojo.field_name for pojo in self.pojo_fields}

    def get_json_fields_for_schema(self, iglu_uri: str) -> List[JsonField]:
        """
        Get JSON fields that match a specific schema.

        Args:
            iglu_uri: Iglu URI to match against

        Returns:
            List of JsonField objects that match the schema
        """
        return [jf for jf in self.json_fields if jf.matches_schema(iglu_uri)]

    def is_empty(self) -> bool:
        """Check if no fields are configured."""
        return not self.pojo_fields and not self.json_fields


class FieldReferenceParser:
    """
    Parser for Snowplow enrichment field reference configurations.

    This parser handles the common field reference pattern used by multiple
    enrichments (PII, SQL, API Request) to specify which fields to access.

    Handles multiple config formats:
    1. Standard Snowplow format with pojo/json arrays
    2. Legacy formats with fieldName/fieldNames keys
    """

    @classmethod
    def parse(
        cls,
        config: Dict[str, Any],
        include_legacy_formats: bool = True,
    ) -> FieldReferenceConfig:
        """
        Parse field references from a configuration dict.

        Args:
            config: Configuration dict containing pojo/json arrays directly
            include_legacy_formats: Whether to parse legacy format variations

        Returns:
            Parsed FieldReferenceConfig with pojo and json fields
        """
        result = FieldReferenceConfig()

        if not config:
            return result

        # Parse based on config structure
        if isinstance(config, dict):
            cls._parse_dict_format(config, result, include_legacy_formats)
        elif isinstance(config, list) and include_legacy_formats:
            # Legacy format: [{"fieldName": "email"}, ...]
            cls._parse_legacy_list_format(config, result)

        return result

    @classmethod
    def parse_from_key(
        cls,
        parameters: Dict[str, Any],
        key: str,
        include_legacy_formats: bool = True,
    ) -> FieldReferenceConfig:
        """
        Parse field references from a specific key in parameters.

        This is useful when the field references are nested under a key like "pii".

        Args:
            parameters: The enrichment parameters dict
            key: The key containing the field reference config (e.g., "pii", "inputs")
            include_legacy_formats: Whether to parse legacy format variations

        Returns:
            Parsed FieldReferenceConfig
        """
        if not parameters or key not in parameters:
            return FieldReferenceConfig()

        config = parameters[key]
        return cls.parse(config, include_legacy_formats)

    @classmethod
    def _parse_dict_format(
        cls,
        config: Dict[str, Any],
        result: FieldReferenceConfig,
        include_legacy: bool,
    ) -> None:
        """Parse dict-based config format."""
        # Standard Snowplow format with pojo array
        if "pojo" in config:
            cls._parse_pojo_fields(config["pojo"], result)

        # Standard Snowplow format with json array
        if "json" in config:
            cls._parse_json_fields(config["json"], result)

        # Legacy format: {"fieldNames": ["email", ...]}
        if include_legacy and "fieldNames" in config:
            field_names = config["fieldNames"]
            if isinstance(field_names, list):
                for name in field_names:
                    if isinstance(name, str):
                        result.pojo_fields.append(PojoField(field_name=name))

    @classmethod
    def _parse_pojo_fields(cls, pojo_config: Any, result: FieldReferenceConfig) -> None:
        """Parse POJO fields from config."""
        if not isinstance(pojo_config, list):
            return

        for item in pojo_config:
            if not isinstance(item, dict):
                continue

            # Standard format: {"field": "user_id"}
            if "field" in item:
                result.pojo_fields.append(PojoField(field_name=item["field"]))
            # Alternative format: {"ppiField": "user_id"}
            elif "ppiField" in item:
                result.pojo_fields.append(PojoField(field_name=item["ppiField"]))
            # Legacy format: {"fieldName": "user_id"}
            elif "fieldName" in item:
                result.pojo_fields.append(PojoField(field_name=item["fieldName"]))

    @classmethod
    def _parse_json_fields(cls, json_config: Any, result: FieldReferenceConfig) -> None:
        """Parse JSON fields from config."""
        if not isinstance(json_config, list):
            return

        for item in json_config:
            if not isinstance(item, dict):
                continue

            # Require all three fields for valid JSON field reference
            json_field = item.get("field")
            schema_criterion = item.get("schemaCriterion")
            json_path = item.get("jsonPath")

            if json_field and schema_criterion and json_path:
                result.json_fields.append(
                    JsonField(
                        json_field=json_field,
                        schema_criterion=schema_criterion,
                        json_path=json_path,
                    )
                )
            else:
                logger.debug(
                    f"Skipping incomplete JSON field config: field={json_field}, "
                    f"schemaCriterion={schema_criterion}, jsonPath={json_path}"
                )

    @classmethod
    def _parse_legacy_list_format(
        cls, config: List[Any], result: FieldReferenceConfig
    ) -> None:
        """Parse legacy list format: [{"fieldName": "email"}, ...]."""
        for item in config:
            if isinstance(item, dict) and "fieldName" in item:
                result.pojo_fields.append(PojoField(field_name=item["fieldName"]))
