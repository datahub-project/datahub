import datetime
import json
import logging
import os
import pathlib
import signal
import subprocess
import time
from typing import Any, Iterable, Iterator, List, Optional, Set, Union

import datahub.metadata.schema_classes as models
import pydantic
import pytest
import yaml
from datahub.api.entities.dataset.dataset import (
    Dataset,
    SchemaFieldSpecification,
    SchemaSpecification,
)
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from pydantic import BaseModel

from tests.integrations_service_utils import (
    bootstrap_action,
    rollback_action,
    start_action,
    stop_action,
    wait_until_action_has_processed_event,
)
from tests.utils import wait_for_writes_to_sync

"""
This file contains tests for the documentation propagation feature. The tests
are designed to test the propagation of documentation from one schema field to
another via lineage relationships. The tests are designed to test the
functionality of the documentation propagation feature in the following
scenarios:
- Bootstrap: Test that the documentation is propagated correctly at bootstrap
- Live: Test that the documentation is propagated correctly when the action is
    live
- Rollback: Test that the documentation is rolled back correctly when the
    action is rolled back
The tests are designed to test the functionality of the documentation
propagation feature in the following scenarios:
- 1:1 lineage relationship
- 1:N lineage relationship
- N:1 lineage relationship
- 2 hop lineage relationship
- Sibling lineage relationship

TODOs:

- Add tests for chart fields
- Add tests for concurrent bootstrap and live actions



"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def kill_action_processes(action_urn=None):
    """
    Note : this is a temporary solution that only works with local deployments.
    This function will be replaced with a more robust solution in the future if needed.
    """
    # Find processes
    try:
        output = subprocess.check_output(["ps", "aux"], universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ps command: {e}")
        return

    pids = []
    for line in output.splitlines():
        if "action_runner.py" in line:
            if action_urn is None or action_urn in line:
                parts = line.split()
                if len(parts) > 1:
                    pids.append(int(parts[1]))

    if not pids:
        message = (
            f"No processes found for action URN: {action_urn}"
            if action_urn
            else "No action_runner.py processes found"
        )
        print(message)
        return

    # Kill processes
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
            print(f"Killed process {pid}")
        except ProcessLookupError:
            print(f"Process {pid} not found")
        except PermissionError:
            print(f"Permission denied to kill process {pid}")

    message = (
        f"All processes for action URN {action_urn} have been terminated"
        if action_urn
        else "All action_runner.py processes have been terminated"
    )
    print(message)


def cleanup(
    auth_session: Any,
    graph_client: DataHubGraph,
    urns: Iterable[str],
    test_resources_dir: str,
    remove_actions: bool = True,
):
    # breakpoint()
    for urn in urns:
        graph_client.delete_entity(urn, hard=True)
    if remove_actions:
        actions_gql = (pathlib.Path(test_resources_dir) / "actions.gql").read_text()
        all_actions = graph_client.execute_graphql(
            query=actions_gql, operation_name="listActions"
        )
        logger.debug(f"Got actions: {all_actions}")
        try:
            for action in all_actions["listActionPipelines"]["actionPipelines"]:
                action_urn = action["urn"]
                try:
                    stop_action(action_urn, auth_session.integrations_service_url())
                except Exception as e:
                    logger.error(f"Error stopping action: {e}")
                # ensure all action processes that have been spawned are stopped
                # action processes are named like actions/action_runner.py
                # /tmp/datahub/recipes/urn:li:dataHubAction:1721600759.json --port
                # .. 8080
                try:
                    kill_action_processes(action_urn)
                except Exception as e:
                    logger.error(f"Error killing action processes: {e}")
                graph_client.delete_entity(action_urn, hard=True)
        except Exception as e:
            logger.error(f"Error deleting action: {e}")
        try:
            kill_action_processes()
        except Exception as e:
            logger.error(f"Error killing action processes: {e}")


@pytest.fixture(scope="function")
def create_test_action(
    auth_session: Any, graph_client: DataHubGraph, test_resources_dir, test_action_urn
) -> Iterator[None]:
    action_urn = test_action_urn
    recipe_file = test_resources_dir / "doc_propagation_action_recipe.yaml"

    with open(recipe_file, "r") as f:
        recipe_json_str = json.dumps(yaml.safe_load(f))

    cleanup(
        auth_session,
        graph_client,
        [action_urn],
        test_resources_dir,
    )

    # Provision the action
    from datahub.metadata.schema_classes import (
        DataHubActionConfigClass,
        DataHubActionInfoClass,
    )

    graph_client.emit(
        MetadataChangeProposalWrapper(
            entityUrn=action_urn,
            aspect=DataHubActionInfoClass(
                name="test_docs_propagation",
                type="datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction",
                config=DataHubActionConfigClass(
                    recipe=recipe_json_str,
                ),
            ),
        )
    )
    yield
    # we cleanup the action after all the tests have run
    cleanup(
        auth_session,
        graph_client,
        [test_action_urn],
        test_resources_dir,
    )


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/propagation/"


@pytest.fixture(scope="module")
def test_action_urn():
    return f"urn:li:dataHubAction:{int(time.time())}"


class PropagationExpectation(BaseModel):
    schema_field_urn: str  # the field that should be tested
    propagation_found: bool  # whether any propagation is expected
    propagation_source: Optional[str] = None  # the expected propagation source field
    propagation_via: Optional[str] = None  # the expected propagation via field
    propagation_origin: Optional[str] = None  # the expected propagation origin field
    propagated_description: Optional[str] = None  # the expected propagated description


class PropagationTestScenario(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    base_graph: List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]
    base_expectations: List[PropagationExpectation]
    mutations: List[MetadataChangeProposalWrapper]
    post_mutation_expectations: List[PropagationExpectation] = pydantic.Field(
        default_factory=list
    )

    def get_urns(self) -> list[str]:
        urns: Set[str] = set()
        for mcp in self.base_graph:
            if mcp.entityUrn:
                urns.add(mcp.entityUrn)
        for mcp in self.mutations:
            if mcp.entityUrn:
                urns.add(mcp.entityUrn)
        for expectation in self.base_expectations + self.post_mutation_expectations:
            urns.add(expectation.schema_field_urn)
        return list(urns)


def create_2_2_graph_lineage(
    test_action_urn: str, prefix: str = "graph2_2"
) -> PropagationTestScenario:
    """
    Creates a 2 hop graph with 3 datasets and 5 schema fields each.
    1st hop is the same as the 1_1 graph.
    2nd hop creates lineage between col 0 of the 2nd dataset and col 1 of the
    3rd dataset.
    Other columns of the 3rd dataset have 1:1 lineage to the 2nd dataset.

    We test the following propagation scenarios:
    - column 0 of the 3rd dataset should have propagated description from column
    0 of the first dataset via column 0 of the 2nd dataset.
    - none of the other columns should have any propagated description by
    default.
    - On mutation of documentation of column 1 of the second dataset, column 1
    of the 3rd dataset should have the same description propagated.
    """

    base_propagation_scenario: PropagationTestScenario = create_1_1_graph_lineage(
        test_action_urn, prefix
    )

    dataset_3_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_2")
    dataset_2_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_1")
    dataset_1_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_0")

    dataset_3_schema_field_urns = [
        make_schema_field_urn(dataset_3_urn, f"column_{i}") for i in range(5)
    ]

    dataset_3 = Dataset(
        id=None,
        urn=dataset_3_urn,
        platform="snowflake",
        name="table_foo_2",
        description="this is table foo 2",
        subtype=None,
        subtypes=None,
        schema=SchemaSpecification(
            file=None,
            fields=[
                SchemaFieldSpecification(
                    id=f"column_{i}",
                    type="string",
                    description=None,
                    urn=make_schema_field_urn(dataset_3_urn, f"column_{i}"),
                )
                for i in range(5)
            ],
        ),
        downstreams=None,
        properties=None,
    )

    base_mcps = [mcp for mcp in dataset_3.generate_mcp()]

    lineage_aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=dataset_2_urn,
                type="COPY",
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                upstreams=[make_schema_field_urn(dataset_2_urn, f"column_{i}")],
                downstreams=[make_schema_field_urn(dataset_3_urn, f"column_{i}")],
            )
            for i in range(5)
        ],
    )
    base_mcps += [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_3_urn,
            aspect=lineage_aspect,
        )
    ]

    field_0_propagation_expectation = PropagationExpectation(
        schema_field_urn=dataset_3_schema_field_urns[0],
        propagation_found=True,
        propagated_description="this is column 0",
        propagation_source=test_action_urn,
        propagation_via=make_schema_field_urn(dataset_2_urn, "column_0"),
        propagation_origin=make_schema_field_urn(dataset_1_urn, "column_0"),
    )

    no_propagation_expectations = [
        PropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urn,
            propagation_found=False,
        )
        for dataset_3_schema_field_urn in dataset_3_schema_field_urns[1:]
    ]

    mutation_mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_2_urn,
        aspect=EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="column_1",
                    description="this is the updated description",
                ),
            ],
            created=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:foobar",
            ),
            lastModified=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:foobar",
            ),
        ),
    )

    post_mutation_expectations = [
        PropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urns[0],
            propagation_found=True,
            propagated_description="this is the updated description",
            propagation_source=test_action_urn,
            propagation_via=make_schema_field_urn(dataset_2_urn, "column_0"),
            propagation_origin=make_schema_field_urn(dataset_1_urn, "column_0"),
        ),
        PropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urns[1],
            propagation_found=True,
            propagated_description="this is the updated description",
            propagation_source=test_action_urn,
            propagation_via=None,
            propagation_origin=make_schema_field_urn(dataset_2_urn, "column_1"),
        ),
    ] + [
        x
        for x in no_propagation_expectations
        if x.schema_field_urn
        not in [dataset_3_schema_field_urns[1], dataset_3_schema_field_urns[0]]
    ]

    return PropagationTestScenario(
        base_graph=base_propagation_scenario.base_graph + base_mcps,
        base_expectations=base_propagation_scenario.base_expectations
        + [field_0_propagation_expectation],
        mutations=base_propagation_scenario.mutations + [mutation_mcp],
        post_mutation_expectations=base_propagation_scenario.post_mutation_expectations
        + post_mutation_expectations,
    )


def create_2_2_graph_lineage_siblings(
    test_action_urn: str, prefix: str = "graph2_2_lineage_siblings"
) -> PropagationTestScenario:
    """
    Creates a 2 hop graph that builds upon the 1_1 graph by adding a sibling
    This mimics the dbt source -> table -> BI dataset lineage scenario.
    dataset_1 = snowflake, table_foo_0
    dataset_2 = snowflake, table_foo_1
    dataset_3 = dbt, table_foo_2

    dataset_3 has a sibling relationship with dataset_1
    """

    base_propagation_scenario: PropagationTestScenario = create_1_1_graph_lineage(
        test_action_urn, prefix=prefix, column_docs=False
    )

    dataset_3_urn = make_dataset_urn("dbt", f"{prefix}.table_foo_2")
    dataset_3 = Dataset(
        id=None,
        urn=dataset_3_urn,
        platform="dbt",
        name="table_foo_2",
        description="this is table foo 2",
        subtype="Source",
        subtypes=None,
        schema=SchemaSpecification(
            file=None,
            fields=[
                SchemaFieldSpecification(
                    id=f"column_{i}",
                    type="string",
                    description=f"Description for dbt column {i}",
                    urn=make_schema_field_urn(dataset_3_urn, f"column_{i}"),
                )
                for i in range(5)
            ],
        ),
        downstreams=None,
        properties=None,
    )

    dataset_1_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_0")
    dataset_2_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_1")

    base_mcps = [mcp for mcp in dataset_3.generate_mcp()]

    # create sibling relnship

    sibling_relnship = models.SiblingsClass(
        siblings=[
            dataset_1_urn,
        ],
        primary=True,
    )
    base_mcps += [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_3_urn,
            aspect=sibling_relnship,
        )
    ]

    dataset_1_field_0_propagation_expectation = PropagationExpectation(
        schema_field_urn=make_schema_field_urn(dataset_1_urn, "column_0"),
        propagation_found=True,
        propagated_description="Description for dbt column 0",
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=make_schema_field_urn(dataset_3_urn, "column_0"),
    )

    dataset_2_field_0_propagation_expectation = PropagationExpectation(
        schema_field_urn=make_schema_field_urn(dataset_2_urn, "column_0"),
        propagation_found=True,
        propagated_description="Description for dbt column 0",
        propagation_source=test_action_urn,
        propagation_via=make_schema_field_urn(dataset_1_urn, "column_0"),
        propagation_origin=make_schema_field_urn(dataset_3_urn, "column_0"),
    )

    return PropagationTestScenario(
        base_graph=base_propagation_scenario.base_graph + base_mcps,
        base_expectations=[
            dataset_1_field_0_propagation_expectation,
            dataset_2_field_0_propagation_expectation,
        ],
        mutations=[],
        post_mutation_expectations=[],
    )


def create_1_1_graph_lineage(
    test_action_urn: str, prefix: str = "graph1_1", column_docs: bool = True
) -> PropagationTestScenario:
    """ "
    Creates a 1 - 1 graph with 2 datasets and 5 schema fields each.
    The first dataset has 5 schema fields, the second dataset has 5 schema
    fields.
    The first schema field of the first dataset is upstream to the first schema
    field of the second dataset. (1:1 relnship)
    The second and third schema field of the first dataset are upstream to the
    second field of the second dataset. (N:1 relnship)
    The remaining fields don't have any lineage to each other.
    The goal is to test all kinds of lineage relationships over a single hop.
    """
    dataset_urns = [
        make_dataset_urn("snowflake", f"{prefix}.table_foo_{i}") for i in range(2)
    ]
    downstream_dataset_urn = dataset_urns[1]
    upstream_dataset_urn = dataset_urns[0]
    downstream_schema_field_urns = [
        make_schema_field_urn(downstream_dataset_urn, f"column_{i}") for i in range(5)
    ]
    upstream_schema_field_urns = [
        make_schema_field_urn(upstream_dataset_urn, f"column_{i}") for i in range(5)
    ]
    mcps = []
    for i, dataset_urn in enumerate(dataset_urns):
        dataset = Dataset(
            id=f"table_foo_{i}",
            platform="snowflake",
            name=f"table_foo_{i}",
            description=f"this is table foo {i}",
            subtype=None,
            subtypes=None,
            downstreams=None,
            properties=None,
            urn=dataset_urn,
            schema=SchemaSpecification(
                file=None,
                fields=[
                    SchemaFieldSpecification(
                        id=f"column_{j}",
                        type="string",
                        description=(
                            f"this is column {j}" if i == 0 and column_docs else None
                        ),  # only first dataset has field descriptions, if column_docs is True
                        urn=make_schema_field_urn(dataset_urn, f"column_{j}"),
                    )
                    for j in range(5)
                ],
            ),
        )
        mcps += [mcp for mcp in dataset.generate_mcp()]

    upstream_downstream_pairs = [
        ([upstream_schema_field_urns[0]], [downstream_schema_field_urns[0]]),
        (
            [upstream_schema_field_urns[1], upstream_schema_field_urns[2]],
            [downstream_schema_field_urns[1]],
        ),
    ]

    upstream_lineage_aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_dataset_urn,
                type="TRANSFORMED",
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                upstreams=upstream_schema_field_urns,
                downstreams=downstream_schema_field_urns,
            )
            for upstream_schema_field_urns, downstream_schema_field_urns in upstream_downstream_pairs
        ],
    )
    mcps += [
        MetadataChangeProposalWrapper(
            entityUrn=downstream_dataset_urn,
            aspect=upstream_lineage_aspect,
        )
    ]

    no_propagation_expectations = [
        PropagationExpectation(
            schema_field_urn=downstream_schema_field_urn,
            propagation_found=False,
        )
        for downstream_schema_field_urn in (
            downstream_schema_field_urns[2:]
            if column_docs
            else downstream_schema_field_urns
        )
    ]
    field_1_propagation_expectation = PropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[0],
        propagation_found=True,
        propagated_description="this is column 0",
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=upstream_schema_field_urns[0],
    )
    field_2_propagation_expectation = PropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[1],
        propagation_found=False,  # field 2 should not have propagation because it has 2 upstream fields
    )

    mutation_mcp = MetadataChangeProposalWrapper(
        entityUrn=upstream_dataset_urn,
        aspect=EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="column_0",
                    description="this is the updated description",
                ),
            ],
            created=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:foobar",
            ),
            lastModified=AuditStampClass(
                time=int(
                    datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
                ),
                actor="urn:li:corpuser:foobar",
            ),
        ),
    )

    post_mutation_expectations = [
        PropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[0],
            propagation_found=True,
            propagated_description="this is the updated description",
            propagation_source=test_action_urn,
            propagation_via=None,
            propagation_origin=upstream_schema_field_urns[0],
        ),
        PropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[1],
            propagation_found=False,
        ),
    ] + [
        x
        for x in no_propagation_expectations
        if x.schema_field_urn != downstream_schema_field_urns[0]
    ]

    return PropagationTestScenario(
        base_graph=mcps,
        base_expectations=(
            no_propagation_expectations
            + [field_1_propagation_expectation, field_2_propagation_expectation]
            if column_docs
            else []
        ),
        mutations=[mutation_mcp],
        post_mutation_expectations=post_mutation_expectations,
    )


@pytest.fixture(
    params=[
        ("1x1 graph", create_1_1_graph_lineage),
        ("2x2 graph", create_2_2_graph_lineage),
        ("2x2 graph with siblings", create_2_2_graph_lineage_siblings),
    ],
    ids=lambda x: x[0],
)
def graph_lineage_data(request, test_action_urn, create_test_action):
    _, graph_func = request.param
    return graph_func(test_action_urn)


def test_main_loop(
    auth_session: Any,
    graph_client: DataHubGraph,
    graph_lineage_data: PropagationTestScenario,
    test_action_urn: str,
    pytestconfig,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/propagation/"
    time_stages = {}
    time_stages["cleanup"] = time.time()
    cleanup(
        auth_session,
        graph_client,
        urns=graph_lineage_data.get_urns(),
        test_resources_dir=test_resources_dir,
        remove_actions=False,
    )
    time_stages["cleanup"] = time.time() - time_stages["cleanup"]

    try:
        # First test bootstrap
        time_stages["bootstrap"] = time.time()
        docs_propagation_bootstrap(
            auth_session, graph_client, graph_lineage_data, test_action_urn
        )
        time_stages["bootstrap"] = time.time() - time_stages["bootstrap"]

        # Then test rollback
        time_stages["rollback"] = time.time()
        docs_propagation_rollback(
            auth_session,
            graph_client,
            graph_lineage_data,
            test_action_urn,
            post_mutation=False,
        )
        time_stages["rollback"] = time.time() - time_stages["rollback"]

        # Finally test live
        time_stages["live"] = time.time()
        docs_propagation_live(
            auth_session, graph_client, graph_lineage_data, test_action_urn
        )
        time_stages["live"] = time.time() - time_stages["live"]

        # Test rollback again
        time_stages["live_rollback"] = time.time()
        docs_propagation_rollback(
            auth_session,
            graph_client,
            graph_lineage_data,
            test_action_urn,
            post_mutation=True,
        )
        time_stages["live_rollback"] = time.time() - time_stages["live_rollback"]

    finally:
        time_stages["post_test_cleanup"] = time.time()
        cleanup(
            auth_session,
            graph_client,
            urns=graph_lineage_data.get_urns(),
            test_resources_dir=test_resources_dir,
            remove_actions=False,
        )
        time_stages["post_test_cleanup"] = (
            time.time() - time_stages["post_test_cleanup"]
        )
        print(f"---------- Time stages: {time_stages} ------------")


def docs_propagation_bootstrap(
    auth_session: Any,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
) -> None:
    time_stages = {}

    time_stages["bootstrap_data_setup"] = time.time()
    # Apply base graph
    for mcp in scenario.base_graph:
        graph_client.emit(mcp, async_flag=False)

    wait_for_writes_to_sync()
    time_stages["bootstrap_data_setup"] = (
        time.time() - time_stages["bootstrap_data_setup"]
    )

    time_stages["bootstrap_action"] = time.time()
    bootstrap_action(
        test_action_urn,
        auth_session.integrations_service_url(),
        wait_for_completion=True,
    )
    time_stages["bootstrap_action"] = time.time() - time_stages["bootstrap_action"]

    time_stages["bootstrap_check_expectations"] = time.time()
    # Check base expectations
    for expectation in scenario.base_expectations:
        documentation_aspect = graph_client.get_aspect(
            expectation.schema_field_urn, DocumentationClass
        )
        if expectation.propagation_found:
            try:
                assert documentation_aspect is not None
                first_element = documentation_aspect.documentations[0]
                assert first_element.documentation == expectation.propagated_description
                assert first_element.attribution
                assert first_element.attribution.sourceDetail
                assert (
                    first_element.attribution.sourceDetail.get("origin")
                    == expectation.propagation_origin
                )
                assert (
                    first_element.attribution.sourceDetail.get("via")
                    == expectation.propagation_via
                )
                assert (
                    first_element.attribution.sourceDetail.get("propagated") == "true"
                )
                assert (
                    first_element.attribution.actor
                    == "urn:li:corpuser:__datahub_system"
                )
            except Exception:
                logger.debug(f"Failed expectation: {expectation}")
                # breakpoint()
                raise
        else:
            assert not documentation_aspect or not documentation_aspect.documentations
    time_stages["bootstrap_check_expectations"] = (
        time.time() - time_stages["bootstrap_check_expectations"]
    )
    print(f"---------- Bootstrap time stages: {time_stages} ------------")


def docs_propagation_live(
    auth_session,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
) -> None:
    time_stages = {}
    time_stages["start_action"] = time.time()
    start_action(
        test_action_urn,
        auth_session.integrations_service_url(),
        wait_for_completion=True,
    )
    time_stages["start_action"] = time.time() - time_stages["start_action"]

    try:
        # Apply mutations
        audit_timestamp = datetime.datetime.now(datetime.timezone.utc)
        time_stages["apply_mutations"] = time.time()
        for mcp in scenario.mutations:
            graph_client.emit(mcp)
        time_stages["apply_mutations"] = time.time() - time_stages["apply_mutations"]
        time_stages["wait_for_action_has_processed_event"] = time.time()
        wait_until_action_has_processed_event(
            test_action_urn, auth_session.integrations_service_url(), audit_timestamp
        )
        time_stages["wait_for_action_has_processed_event"] = (
            time.time() - time_stages["wait_for_action_has_processed_event"]
        )

        # Check post-mutation expectations
        for expectation in scenario.post_mutation_expectations:
            documentation_aspect = graph_client.get_aspect(
                expectation.schema_field_urn, DocumentationClass
            )
            if expectation.propagation_found:
                try:
                    assert documentation_aspect is not None
                    first_element = documentation_aspect.documentations[0]
                    assert (
                        first_element.documentation
                        == expectation.propagated_description
                    )
                    assert first_element.attribution
                    assert first_element.attribution.sourceDetail
                    assert (
                        first_element.attribution.sourceDetail.get("origin")
                        == expectation.propagation_origin
                    )
                    assert (
                        first_element.attribution.sourceDetail.get("via")
                        == expectation.propagation_via
                    )
                    assert (
                        first_element.attribution.sourceDetail.get("propagated")
                        == "true"
                    )
                    assert (
                        first_element.attribution.actor
                        == "urn:li:corpuser:__datahub_system"
                    )
                except Exception:
                    raise
            else:
                assert (
                    not documentation_aspect or not documentation_aspect.documentations
                )
    finally:
        time_stages["stop_action"] = time.time()
        stop_action(test_action_urn, auth_session.integrations_service_url())
        time_stages["stop_action"] = time.time() - time_stages["stop_action"]
        print(f"---------- Live time stages: {time_stages} ------------")


def docs_propagation_rollback(
    auth_session,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
    post_mutation: bool = False,
) -> None:
    wait_for_writes_to_sync()
    rollback_action(
        test_action_urn,
        auth_session.integrations_service_url(),
        wait_for_completion=True,
    )

    # Check that all fields have no propagated documentation after rollback
    all_expectations = scenario.base_expectations
    if post_mutation:
        all_expectations += scenario.post_mutation_expectations
    for expectation in all_expectations:
        if expectation.propagation_found:
            documentation_aspect = graph_client.get_aspect(
                expectation.schema_field_urn, DocumentationClass
            )
            if documentation_aspect:
                assert not documentation_aspect.documentations
            else:
                logger.warning(
                    f"Documentation aspect not found for {expectation.schema_field_urn}. Unexpected but not fatal."
                )
