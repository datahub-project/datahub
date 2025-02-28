import pathlib
from typing import Dict, List, Optional

import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigModel
from datahub.ingestion.graph.client import DataHubGraph
from pydantic import dataclasses, root_validator

_GLOSSARY_NODE_GQL = (
    pathlib.Path(__file__).parent / "term_suggestion_universe.gql"
).read_text()


_MAX_DESCRIPTION_LENGTH = 8000


class GlossaryUniverseConfig(ConfigModel):
    glossary_terms: Optional[List[str]] = None
    glossary_nodes: Optional[List[str]] = None

    @root_validator(skip_on_failure=True)
    def check_not_all_none(cls, values: dict) -> dict:
        # Extract values
        glossary_terms = values.get("glossary_terms")
        glossary_nodes = values.get("glossary_nodes")

        # Check if all are None
        if glossary_terms is None and glossary_nodes is None:
            raise ValueError("At least one glossary term or node urn must be provided.")

        return values


@dataclasses.dataclass
class GlossaryInfo:
    glossary: dict


def fetch_glossary_info(
    graph_client: DataHubGraph,
    universe: GlossaryUniverseConfig,
) -> GlossaryInfo:
    assert not all(
        [
            universe.glossary_terms is None,
            universe.glossary_nodes is None,
        ]
    ), "No glossary term or node urns are provided!!"

    # Gather Terms Information:
    if universe.glossary_terms is not None:
        glossary_terms_info = get_glossary_term_names_and_definition(
            terms=universe.glossary_terms,
            graph_client=graph_client,
            fetch_parent=True,
        )
    else:
        glossary_terms_info = {}
    if universe.glossary_nodes:
        terms_collected_from_groups = get_terms_from_group_urns(
            groups=universe.glossary_nodes,
            graph_client=graph_client,
        )
        glossary_terms_info.update(terms_collected_from_groups)

    glossary = GlossaryInfo(
        glossary=glossary_terms_info,
    )
    return glossary


def extract_terms_with_parents_from_group(
    glossary: dict, graph_client: DataHubGraph
) -> Dict[str, Dict[str, str | Dict[str, str]]]:
    terms = {}

    def recursive_extract(
        node: dict, parent_info: Optional[Dict[str, str]] = None
    ) -> None:
        if isinstance(node, dict):
            # Check if the current node is a term
            if node.get("type") == "GLOSSARY_TERM":
                urn = node.get("urn", "")
                if node.get("properties") is None:
                    node_info = get_glossary_term_names_and_definition(
                        terms=[urn], graph_client=graph_client, fetch_parent=False
                    )
                    term_name = node_info.get(urn, {}).get("term_name")
                    term_description = node_info.get(urn, {}).get("term_description")
                else:
                    term_name = node["properties"]["name"]
                    term_description = node["properties"]["description"]
                if isinstance(term_description, str):
                    term_description = term_description[:_MAX_DESCRIPTION_LENGTH]
                terms[urn] = {
                    "term_name": term_name,
                    "term_description": term_description,
                    "parent_node": parent_info,
                }

            # If the node has 'children', process the children
            children = node.get("children", {})
            relationships = children.get("relationships", [])

            for relationship in relationships:
                entity = relationship.get("entity")
                if entity:
                    # Pass current node's information as parent info to the children
                    current_parent_info = {
                        "parent_name": node.get("properties", {}).get("name", ""),
                        "parent_description": node.get("properties", {}).get(
                            "description", ""
                        )[:_MAX_DESCRIPTION_LENGTH],
                    }
                    recursive_extract(entity, current_parent_info)

    recursive_extract(glossary)
    return terms


def get_terms_from_group_urns(
    groups: List[str], graph_client: DataHubGraph
) -> Dict[str, Dict[str, str | Dict[str, str]]]:
    extracted_terms = {}
    for group_urn in groups:
        entity = graph_client.execute_graphql(_GLOSSARY_NODE_GQL, {"urn": group_urn})
        if entity.get("glossaryNode") is not None:
            terms = extract_terms_with_parents_from_group(
                glossary=entity["glossaryNode"], graph_client=graph_client
            )
            extracted_terms.update(terms)
    return extracted_terms


def get_glossary_term_names_and_definition(
    terms: List[str], graph_client: DataHubGraph, fetch_parent: bool = False
) -> dict:
    glossary_term_info = {}
    for term in terms:
        term_details: models.GlossaryTermInfoClass | None = (
            graph_client.get_entity_semityped(term).get("glossaryTermInfo")
        )
        if term_details is None:
            continue
        term_name = term_details.get("name", "")
        term_def = term_details.get("definition", "")[:_MAX_DESCRIPTION_LENGTH]
        parent_node_info_dict = {}
        parent_node = term_details.get("parentNode")
        if fetch_parent and parent_node is not None:
            parent_node_details = graph_client.get_entity_semityped(parent_node).get(
                "glossaryNodeInfo"
            )
            if parent_node_details is not None:
                parent_node_name = parent_node_details.name
                parent_node_def = parent_node_details.definition
                if isinstance(parent_node_def, str):
                    parent_node_def = parent_node_def[:_MAX_DESCRIPTION_LENGTH]
                parent_node_info_dict = {
                    "parent_name": parent_node_name,
                    "parent_description": parent_node_def,
                }
        glossary_term_info[term] = {
            "term_name": term_name,
            "term_description": term_def,
            "parent_node": parent_node_info_dict,
        }

    return glossary_term_info
