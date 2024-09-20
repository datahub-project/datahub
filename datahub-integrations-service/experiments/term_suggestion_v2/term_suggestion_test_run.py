import pathlib
from typing import Dict, List, Optional, Tuple

import json5
from helper import create_datahub_graph, write_llm_output_to_csv
from loguru import logger

from datahub_integrations.gen_ai.term_suggestion_v2 import (
    TermSuggestionBundle,
    get_term_recommendations,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import (
    GlossaryUniverseConfig,
    fetch_glossary_info,
)

current_dir = pathlib.Path().resolve()

urns_dict: Dict[str, List[str]] = json5.loads(
    (current_dir / "../docs_generation/test_urns.json").read_text()
)  # type: ignore
graph_client = create_datahub_graph("longtailcompanions")
universe_config = GlossaryUniverseConfig(
    glossary_nodes=["urn:li:glossaryNode:PIITerms"],
)
glossary_info = fetch_glossary_info(graph_client=graph_client, universe=universe_config)

raw_llm_responses: list = []
parsed_llm_responses: List[
    Tuple[
        str,
        str,
        Optional[List[TermSuggestionBundle]],
        Optional[Dict[str, List[TermSuggestionBundle]]],
    ]
] = []

for instance, urns in urns_dict.items():
    graph_client = create_datahub_graph(instance)
    for urn in urns:
        print(urn)
        try:
            table_terms, column_terms, raw_llm_response = get_term_recommendations(
                table_urn=urn, graph_client=graph_client, glossary_info=glossary_info
            )
            raw_llm_responses.append([instance, urn, raw_llm_response])
            parsed_llm_responses.append((instance, urn, table_terms, column_terms))
        except Exception as e:
            logger.exception(f"Exception Occurred {e}")
            raw_llm_responses.append([urn, instance, None])
            parsed_llm_responses.append((urn, instance, None, None))

write_llm_output_to_csv(llm_response=parsed_llm_responses)
