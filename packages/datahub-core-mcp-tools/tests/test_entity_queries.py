"""What the get_entities documents select on each GMS they are compiled for."""

from typing import Iterator

import pytest
from graphql import (
    FieldNode,
    FragmentDefinitionNode,
    InlineFragmentNode,
    OperationDefinitionNode,
    SelectionSetNode,
    parse,
)

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo
from datahub_core_mcp_tools.gql_compiler import compile_gql
from datahub_core_mcp_tools.graphql_helpers import load_gql_document

OSS_1_7 = GmsServerInfo(version="v1.7.0", server_env="core")
OSS_1_8 = GmsServerInfo(version="v1.8.0", server_env="core")
CLOUD = GmsServerInfo(version="v0.3.18", server_env="cloud")


def _paths(selection_set: SelectionSetNode, prefix: str) -> Iterator[str]:
    for selection in selection_set.selections:
        if isinstance(selection, FieldNode):
            path = f"{prefix}.{selection.name.value}"
            yield path
            if selection.selection_set:
                yield from _paths(selection.selection_set, path)
        elif isinstance(selection, InlineFragmentNode) and selection.type_condition:
            yield from _paths(
                selection.selection_set,
                f"{prefix}.on {selection.type_condition.name.value}",
            )


def selected(document: str, server: GmsServerInfo) -> set[str]:
    """Every field path the compiled document selects, rooted at fragment or operation."""
    paths: set[str] = set()
    for definition in parse(
        compile_gql(load_gql_document(document), server).query
    ).definitions:
        if isinstance(definition, (FragmentDefinitionNode, OperationDefinitionNode)):
            assert definition.name is not None
            paths |= set(_paths(definition.selection_set, definition.name.value))
    return paths


@pytest.mark.parametrize("server", [OSS_1_7, OSS_1_8, CLOUD], ids=str)
def test_oss_additions_are_selected_everywhere(server: GmsServerInfo) -> None:
    entity = selected("entity_details.gql", server)
    assertion = selected("assertion_entity.gql", server)

    assert "entityPreview.on Dataset.properties.externalUrl" in entity
    assert "entityPreview.on DataJob.inputOutput.inputDatasets.urn" in entity
    assert "entityPreview.on DataJob.inputOutput.outputDatasets.urn" in entity
    # OSS servers put an errored run's message here.
    assert (
        "assertionDetails.runEvents.runEvents.result.error.properties.value"
        in assertion
    )


def test_cloud_only_fields_are_sent_to_cloud_alone() -> None:
    cloud_only = {
        "assertionDetails.runEvents.runEvents.result.metric.value",
        "assertionDetails.runEvents.runEvents.result.error.displayMessage",
        "assertionDetails.info.freshnessAssertion.failureSeverityConfig.defaultSeverity",
        "entityPreview.on Dataset.documentation",
        "entityPreview.on DataContract.result.type",
        "dataContractAssertionSummary.assertionStatus",
    }
    documentation = "documentationFields.documentations.documentation"

    for server in (OSS_1_7, OSS_1_8):
        paths = selected("entity_details.gql", server) | selected(
            "assertion_entity.gql", server
        )
        assert not cloud_only & paths
    cloud = selected("entity_details.gql", CLOUD) | selected(
        "assertion_entity.gql", CLOUD
    )
    assert cloud_only <= cloud
    # Column documentation exists on OSS too.
    assert documentation in selected("entity_details.gql", OSS_1_7)


def test_custom_assertion_definitions_need_oss_1_8() -> None:
    scope = "assertionDetails.info.customAssertion.scope"

    assert scope not in selected("assertion_entity.gql", OSS_1_7)
    assert scope in selected("assertion_entity.gql", OSS_1_8)
