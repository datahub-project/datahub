from functools import reduce
from operator import getitem
from typing import Any, Dict, List, Optional

from datahub_executor.common.types import (
    AssertionEvaluationParameters,
)


def extract_assertion_entity_from_graphql(assertion: dict) -> dict:
    if (
        "relationships" in assertion
        and "relationships" in assertion["relationships"]
        and len(assertion["relationships"]["relationships"]) > 0
    ):
        graphql_entity = assertion["relationships"]["relationships"][0]["entity"]

        platform_urn = graphql_entity["platform"]["urn"]
        entity_urn = graphql_entity["urn"]
        exists = graphql_entity["exists"] if "exists" in graphql_entity else None

        table_name = (
            graphql_entity["properties"]["name"]
            if "properties" in graphql_entity
            and graphql_entity["properties"] is not None
            and "name" in graphql_entity["properties"]
            else None
        )
        qualified_name = (
            graphql_entity["properties"]["qualifiedName"]
            if "properties" in graphql_entity
            and graphql_entity["properties"] is not None
            and "qualifiedName" in graphql_entity["properties"]
            else None
        )

        sub_types = (
            graphql_entity["subTypes"]["typeNames"]
            if "subTypes" in graphql_entity
            and graphql_entity["subTypes"] is not None
            and "typeNames" in graphql_entity["subTypes"]
            else []
        )

        return {
            "urn": entity_urn,
            "platformUrn": platform_urn,
            "platformInstance": None,
            "subTypes": sub_types,
            "table_name": table_name,
            "qualified_name": qualified_name,
            "exists": exists,
        }
    else:
        raise Exception(
            "Failed to find an entity associated with assertion! Skipping evaluation..."
        )


def extract_assertion_monitor_parameters(
    monitor: Optional[Dict[str, Any]], assertion_urn: str
) -> AssertionEvaluationParameters:
    if not monitor:
        raise Exception(
            f"Found no monitor associated with assertion {assertion_urn}. Cannot extract parameters and therefore cannot run!"
        )

    assertion_specs = safe_get(monitor, ["info", "assertionMonitor", "assertions"], [])

    if not assertion_specs:
        raise Exception(
            f"Found no valid monitor parameters for assertion with urn {assertion_urn}. Unable to run!"
        )

    for spec in assertion_specs:
        if safe_get(spec, ["assertion", "urn"]) == assertion_urn:
            parameters = safe_get(spec, ["parameters"], {})
            return AssertionEvaluationParameters.model_validate(parameters)

    raise Exception(
        f"Found no valid monitor parameters for assertion with urn {assertion_urn}. Unable to run!"
    )


def extract_assertion_monitor_executor_id(monitor: dict, default: str) -> str:
    """
    Extract the executor ID from an assertion's monitor info.

    Args:
        assertion (dict): A dictionary containing assertion data, possibly with nested monitor info.
        default (str): The default value to return if executor ID is not found.

    Returns:
        str: The executor ID if found, otherwise the default value.
    """
    monitor_info = monitor.get("info", {})
    executor_id = monitor_info.get("executorId", default)
    return executor_id


def safe_get(data: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    """Get a nested value from a dictionary safely."""
    try:
        return reduce(getitem, keys, data)
    except (KeyError, TypeError):
        return default
