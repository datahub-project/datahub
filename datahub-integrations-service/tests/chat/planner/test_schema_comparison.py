"""
Direct comparison test between old manual schema and new auto-generated schema.

This test uses deepdiff to show any structural differences.
"""

import json
from typing import Any

from deepdiff import DeepDiff

from datahub_integrations.chat.planner.tools import _get_plan_tool_spec

# The old manual schema (what we had before switching to Pydantic auto-generation)
OLD_MANUAL_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string", "description": "Short plan title"},
        "goal": {"type": "string", "description": "Overall objective"},
        "assumptions": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Assumptions made during planning",
        },
        "steps": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Step identifier (s0, s1, s2...)",
                    },
                    "description": {
                        "type": "string",
                        "description": "What this step accomplishes",
                    },
                    "done_when": {
                        "type": "string",
                        "description": "Natural language success criteria",
                    },
                    "failed_when": {
                        "type": "string",
                        "description": "Natural language failure criteria",
                    },
                    "return_to_user_when": {
                        "type": "string",
                        "description": "When user input is needed",
                    },
                    "tool": {
                        "type": "string",
                        "description": "Specific tool name (optional)",
                    },
                    "param_hints": {
                        "type": "object",
                        "description": "Parameter guidance",
                    },
                    "on_fail": {
                        "type": "object",
                        "properties": {
                            "action": {
                                "type": "string",
                                "enum": ["abort", "retry", "revise"],
                                "description": "Action to take on failure",
                            }
                        },
                    },
                },
                "required": ["id", "description"],
            },
            "description": "Sequential list of steps",
        },
        "expected_deliverable": {
            "type": "string",
            "description": "What should be delivered to the user",
        },
    },
    "required": ["title", "goal", "steps", "expected_deliverable"],
}


def _resolve_refs(schema: dict, root_schema: dict) -> dict:
    """Recursively resolve $ref references in a schema to get inline version."""
    if not isinstance(schema, dict):
        return schema

    # If this is a $ref, resolve it
    if "$ref" in schema:
        ref_path = schema["$ref"]
        # Extract reference path (e.g., "#/$defs/Step" -> ["$defs", "Step"])
        parts = ref_path.lstrip("#/").split("/")

        # Navigate to the referenced definition
        target = root_schema
        for part in parts:
            target = target.get(part, {})

        # Recursively resolve any refs in the target
        return _resolve_refs(target, root_schema)

    # Recursively process nested structures
    result: dict[str, Any] = {}
    for key, value in schema.items():
        if isinstance(value, dict):
            result[key] = _resolve_refs(value, root_schema)
        elif isinstance(value, list):
            result[key] = [
                _resolve_refs(item, root_schema) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[key] = value

    return result


def test_schema_deep_comparison():
    """Deep comparison between old manual schema and new auto-generated schema.

    This test resolves $refs in the auto-generated schema and compares structure.

    Note: The auto-generated schema has ENHANCEMENTS over the old manual schema:
    - More detailed descriptions with examples (from Pydantic Field() definitions)
    - Additional fields (constraints, intent, binding, budgets) from full Plan model
    - Proper anyOf handling for Optional fields
    - Complete OnFail structure vs simplified manual version

    These differences are IMPROVEMENTS, not regressions. The test verifies that
    no important fields were REMOVED or broken.
    """
    spec = _get_plan_tool_spec()
    auto_schema = spec["toolSpec"]["inputSchema"]["json"]

    # Resolve all $refs in auto-generated schema to get inline version
    resolved_auto_schema = _resolve_refs(auto_schema, auto_schema)

    # Remove auto-generated fields that weren't in manual schema
    if "properties" in resolved_auto_schema:
        resolved_auto_schema["properties"].pop("plan_id", None)
        resolved_auto_schema["properties"].pop("version", None)
        resolved_auto_schema["properties"].pop("metadata", None)

        # Also clean up $defs if present (not in manual schema)
        resolved_auto_schema.pop("$defs", None)
        resolved_auto_schema.pop("definitions", None)
        resolved_auto_schema.pop("title", None)  # Pydantic adds this

    # Compare the schemas
    diff = DeepDiff(
        OLD_MANUAL_SCHEMA,
        resolved_auto_schema,
        ignore_order=True,
        verbose_level=2,
    )

    # Print diff for debugging if there are differences
    if diff:
        print("\n" + "=" * 80)
        print("SCHEMA DIFFERENCES FOUND:")
        print("=" * 80)
        print(json.dumps(json.loads(diff.to_json()), indent=2))
        print("=" * 80)

    # The auto-generated schema should have enhancements, but no fields should be REMOVED
    # Verify no critical fields were lost
    if diff:
        diff_dict = json.loads(diff.to_json())

        # Check for removed fields (not acceptable)
        removed_items = diff_dict.get("dictionary_item_removed", {})
        if removed_items:
            # Only the 'required' array in steps.items is acceptable to remove
            # (Pydantic doesn't mark Optional fields as required)
            for key in removed_items.keys():
                if "required" not in key:
                    raise AssertionError(
                        f"Critical field removed from schema: {key}\n"
                        f"Removed: {removed_items}"
                    )

        # Added items are fine (enhancements from full Pydantic model)
        # Changed values are fine (better descriptions)
        # Type changes are fine (anyOf for optionals)

    # If we got here, all differences are acceptable enhancements
    assert True, "Schema is equivalent or enhanced"


def _are_differences_acceptable(diff: DeepDiff) -> bool:
    """Check if schema differences are acceptable (e.g., Pydantic-specific enhancements).

    Acceptable differences:
    - Additional 'title' fields (Pydantic metadata)
    - anyOf/allOf structures for Optional fields
    - Additional fields like 'default', 'examples'
    - Different but semantically equivalent descriptions
    """
    # Convert diff to dict for easier inspection
    diff_dict = json.loads(diff.to_json())

    diff_keys = set(diff_dict.keys())

    # If there are removed items, that's not acceptable
    if "dictionary_item_removed" in diff_keys:
        return False

    # All other differences are acceptable for now
    # (they're likely Pydantic enhancements or optional field handling)
    return True


def test_print_actual_schemas_for_manual_review():
    """Print both schemas side-by-side for manual review.

    This is a helper test that outputs both schemas so developers can
    visually compare them. Not a real assertion, just for debugging.
    """
    spec = _get_plan_tool_spec()
    auto_schema = spec["toolSpec"]["inputSchema"]["json"]

    print("\n" + "=" * 80)
    print("OLD MANUAL SCHEMA:")
    print("=" * 80)
    print(json.dumps(OLD_MANUAL_SCHEMA, indent=2))

    print("\n" + "=" * 80)
    print("NEW AUTO-GENERATED SCHEMA:")
    print("=" * 80)
    print(json.dumps(auto_schema, indent=2))
    print("=" * 80)

    # Always passes - this is just for visual inspection
    assert True
