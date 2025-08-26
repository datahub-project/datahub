import pprint

import click
import datahub.metadata.schema_classes as models
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.urns import DatasetUrn
from loguru import logger

from datahub_integrations.gen_ai.term_suggestion_action import (
    BulkTermSuggester,
    TermSuggestionActionConfig,
    get_urns_to_process,
)
from datahub_integrations.gen_ai.term_suggestion_v2 import (
    get_entity_info_for_term_suggestion,
)

gql = """
query listTermProposals($urn: String!) {
  dataset(urn: $urn) {
    urn
    proposals(type: TERM_ASSOCIATION, status: PENDING) {
      urn
      subResourceType
      subResource
    }
  }
}

mutation rejectProposal($urn: String!) {
  rejectProposal(urn: $urn)
}
"""


@click.group()
def main():
    pass


@main.command()
@click.option("--urn", required=True)
@click.option("--action-config", required=True)
def generate(urn: str, action_config: str):
    full_action_config = load_config_file(
        action_config, allow_stdin=True, resolve_env_vars=False
    )
    assert full_action_config["action"]["type"] == "ai_term_suggestion"
    action_config_raw = full_action_config["action"]["config"]
    action_config_parsed = TermSuggestionActionConfig.parse_obj(action_config_raw)

    graph = get_default_graph()
    logger.info(f"Using graph {graph}")

    bulk_term_suggester = BulkTermSuggester(graph, action_config_parsed)

    bulk_term_suggester.process_urns([urn])


@main.command()
@click.option("--urn", required=True)
def dump_entity_info(urn: str):
    assert DatasetUrn.from_string(urn)

    graph = get_default_graph()
    logger.info(f"Using graph {graph}")

    table_info, column_info = get_entity_info_for_term_suggestion(graph, urn)

    pprint.pprint(table_info)
    pprint.pprint(column_info)


@main.command()
@click.option("--urn", required=True)
@click.option("--clear-proposals", is_flag=True, default=True)
@click.option("--clear-terms", is_flag=True, default=False)
def clear(urn: str, clear_proposals: bool, clear_terms: bool):
    assert DatasetUrn.from_string(urn)

    graph = get_default_graph()
    logger.info(f"Using graph {graph}")

    # Clear dataset terms.
    if clear_terms:
        raise NotImplementedError("Clearing terms is not implemented yet.")

    # Clear term proposals.
    if clear_proposals:
        proposals = graph.execute_graphql(
            gql,
            operation_name="listTermProposals",
            variables={"urn": urn},
        )["dataset"]["proposals"]

        if not proposals:
            logger.debug(f"No term proposals found for {urn}")

        for proposal in proposals:
            proposal_urn = proposal["urn"]

            logger.info(f"Rejecting proposal {proposal_urn}")
            graph.execute_graphql(
                gql,
                operation_name="rejectProposal",
                variables={"urn": proposal_urn},
            )

    # Clear inferred metadata aspect.
    graph.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=models.EntityInferenceMetadataClass(
                # TODO: Replace this with a patch.
                glossaryTermsInference=models.InferenceGroupMetadataClass(
                    lastInferredAt=get_sys_time(),
                    version=0,
                ),
            ),
        )
    )


@main.command()
def get_next_urns() -> None:
    graph = get_default_graph()

    urns = get_urns_to_process(graph, max_items=50)
    pprint.pprint(urns)


if __name__ == "__main__":
    main()
