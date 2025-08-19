import re
from typing import Any, Dict, List, Tuple, Union

import mlflow
import pandas as pd
from mlflow.metrics import MetricValue

from datahub_integrations.experimentation.docs_generation.eval_common import (
    to_entity_info_model,
)
from datahub_integrations.gen_ai.description_context import ExtractedTableInfo
from datahub_integrations.gen_ai.linkify import urn_regex


def has_table_description_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> MetricValue:
    scores = [
        (desc is not None and desc != "")
        for desc, _target in zip(predictions, targets, strict=False)
    ]
    return mlflow.metrics.MetricValue(
        scores=scores,
        aggregate_results={
            "pass_percentage": 100
            * len(list(filter(lambda x: x, scores)))
            / len(scores),
            "total_count": len(scores),
        },
    )


def extract_valid_links(
    entity_info_any: Union[str, Dict[str, Any], ExtractedTableInfo],
) -> List[Tuple[str, str]]:
    """
    Extract all valid links (table_name, URN) from entity_info that can be referenced in descriptions.
    These include:
    1. The table itself (table_name, urn)
    2. Upstream tables (upstream_table_name, upstream_table_urn)
    3. Downstream tables (downstream_table_name, downstream_table_urn)

    Args:
        entity_info: Dictionary containing entity information including lineage

    Returns:
        List of tuples containing (table_name, urn) that can be referenced in descriptions
    """
    entity_info = to_entity_info_model(entity_info_any)
    valid_links = set()

    # Add the table itself
    if entity_info.urn and entity_info.table_name:
        valid_links.add((entity_info.table_name, entity_info.urn))

    # Extract upstream table info
    if entity_info.table_upstream_lineage_info:
        for upstream in entity_info.table_upstream_lineage_info:
            if upstream.upstream_table_urn and upstream.upstream_table_name:
                valid_links.add(
                    (upstream.upstream_table_name, upstream.upstream_table_urn)
                )
                if upstream.upstream_table_description:
                    for name, link in extract_links(
                        upstream.upstream_table_description
                    ):
                        valid_links.add((name, link))

    # Extract downstream table info
    if entity_info.table_downstream_lineage_info:
        for downstream in entity_info.table_downstream_lineage_info:
            if downstream.downstream_table_urn and downstream.downstream_table_name:
                valid_links.add(
                    (
                        downstream.downstream_table_name,
                        downstream.downstream_table_urn,
                    )
                )
                if downstream.downstream_table_description:
                    for name, link in extract_links(
                        downstream.downstream_table_description
                    ):
                        valid_links.add((name, link))

    return list(valid_links)


def is_valid_link(text: str, url: str, valid_links: List[Tuple[str, str]]) -> bool:
    """Check if a link is valid according to our criteria."""
    if not text.startswith("@"):
        return False

    # Check if there's a matching urn. DataHub UI automatically
    # fixes display name
    return any(urn == url for _, urn in valid_links)


def extract_links(description: str) -> List[Tuple[str, str]]:
    """Extract all markdown links from text as (text, url) tuples."""
    if not description:
        return []
    # Match markdown links [text](url) where url can contain parentheses
    pattern = rf"\[([^\]]+)\]\(({urn_regex})\)"
    return re.findall(pattern, description)


def has_valid_links_metric_fn(
    predictions: pd.Series, targets: pd.Series
) -> MetricValue:
    """
    Evaluate the validity of links in table descriptions.
    A valid link should:
    1. Be in markdown format [text](url)
    2. Have text starting with @
    3. Have url that exists in the entity_info's valid links
    4. Have text that matches the table name (without the @ prefix)

    Returns a MetricValue with:
    - scores: List of booleans indicating if each description has valid links
    - aggregate_results: Dictionary with pass_percentage and total_count
    """

    scores = []
    justifications: List[str] = []
    for desc, target in zip(predictions, targets, strict=False):
        if not desc:
            scores.append(False)
            justifications.append("No description")
            continue

        # Extract valid links from entity_info
        valid_links = extract_valid_links(target)

        links = extract_links(desc)
        if not links:
            scores.append(True)  # No links is considered valid
            justifications.append("No links")
            continue

        # Check if all links are valid
        invalid_links = [
            (text, url)
            for text, url in links
            if not is_valid_link(text, url, valid_links)
        ]

        scores.append(len(invalid_links) == 0)
        justifications.append(
            f"Invalid links: {invalid_links}"
            if len(invalid_links) > 0
            else "Valid links"
        )

    return mlflow.metrics.MetricValue(
        scores=scores,
        justifications=justifications,
        aggregate_results={
            "pass_percentage": 100
            * len(list(filter(lambda x: x, scores)))
            / len(scores),
            "total_count": len(scores),
        },
    )
