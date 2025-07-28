import datetime
import json
import logging
import os
import pathlib
import signal
import subprocess
import time
from typing import Any, Iterable, Iterator, List, Optional, Set, Union

import pydantic
import pytest
import yaml
from pydantic import BaseModel

import datahub.metadata.schema_classes as models
from datahub.api.entities.dataset.dataset import (
    Dataset,
    SchemaFieldSpecification,
    SchemaSpecification,
)
from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataHubActionConfigClass,
    DataHubActionInfoClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataAttributionClass,
    MetadataChangeProposalClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import Urn
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


@pytest.fixture(scope="module")
def load_glossary(auth_session, test_resources_dir):
    glossary_file = test_resources_dir / "test_glossary.dhub.yaml"

    pipeline = {
        "source": {
            "type": "datahub-business-glossary",
            "config": {
                "file": str(glossary_file),
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": auth_session.gms_url(),
                "token": auth_session.gms_token(),
            },
        },
    }

    # Run the pipeline

    ingest_pipeline = Pipeline.create(
        config_dict=pipeline,
    )
    ingest_pipeline.run()
    ingest_pipeline.raise_from_status()
    wait_for_writes_to_sync()


@pytest.fixture(scope="function")
def create_test_action(
    auth_session: Any, graph_client: DataHubGraph, test_resources_dir, test_action_urn
) -> Iterator[None]:
    try:
        action_urn = test_action_urn
        recipe_file = test_resources_dir / "tag_propagation_generic_action_recipe.yaml"

        with open(recipe_file, "r") as f:
            recipe_json_str = json.dumps(yaml.safe_load(f))

        cleanup(
            auth_session,
            graph_client,
            [action_urn],
            test_resources_dir,
        )

        # Provision the action
        graph_client.emit(
            MetadataChangeProposalWrapper(
                entityUrn=action_urn,
                aspect=DataHubActionInfoClass(
                    name="test_generic_tag_propagation",
                    type="datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction",
                    config=DataHubActionConfigClass(
                        recipe=recipe_json_str,
                    ),
                ),
            )
        )
        yield
    finally:
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


class DocumentationPropagationExpectation(PropagationExpectation):
    propagated_description: Optional[str] = None  # the expected propagated description


class TagPropagationExpectation(PropagationExpectation):
    propagated_tag: Optional[str] = None  # the expected propagated tag


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

    field_0_propagation_expectation = TagPropagationExpectation(
        schema_field_urn=dataset_3_schema_field_urns[0],
        propagation_found=False,  # no propagation by default
        propagation_source=test_action_urn,
        propagation_via=make_schema_field_urn(dataset_2_urn, "column_0"),
        propagation_origin=make_schema_field_urn(dataset_1_urn, "column_0"),
        propagated_tag="urn:li:tag:AccountBalance",
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
                    globalTags=GlobalTags(
                        tags=[
                            TagAssociationClass(
                                tag="urn:li:tag:AccountBalance",
                            )
                        ],
                    ),
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

    post_mutation_expectations = (
        [
            TagPropagationExpectation(
                schema_field_urn=dataset_3_schema_field_urns[0],
                propagation_found=True,
                propagated_tag="urn:li:tag:TestTagNode1.TestTag1_2",  # This comes from base scenario mutation
                propagation_source=test_action_urn,
                propagation_via=make_schema_field_urn(dataset_2_urn, "column_0"),
                propagation_origin=make_schema_field_urn(dataset_1_urn, "column_0"),
            ),
            TagPropagationExpectation(
                schema_field_urn=dataset_3_schema_field_urns[1],
                propagation_found=True,
                propagated_tag="urn:li:tag:AccountBalance",
                propagation_source=test_action_urn,
                propagation_via=None,
                propagation_origin=make_schema_field_urn(dataset_2_urn, "column_1"),
            ),
        ]
        + [
            x
            for x in no_propagation_expectations
            if x.schema_field_urn
            not in [dataset_3_schema_field_urns[1], dataset_3_schema_field_urns[0]]
        ]
    )

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

    dataset_1_field_0_propagation_expectation = DocumentationPropagationExpectation(
        schema_field_urn=make_schema_field_urn(dataset_1_urn, "column_0"),
        propagation_found=True,
        propagated_description="Description for dbt column 0",
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=make_schema_field_urn(dataset_3_urn, "column_0"),
    )

    dataset_2_field_0_propagation_expectation = DocumentationPropagationExpectation(
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
    field_tag_map = {
        0: "urn:li:tag:TestTagNode1.TestTag1_1",
        1: "urn:li:tag:TestTagNode1.TestTag1_2",
        2: "urn:li:tag:TestTagNode2.TestTag2_1",
        3: "urn:li:tag:TestTagNode2.TestTag2_2",
        4: "urn:li:tag:TestTagNode2.TestTag2_3",
    }
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
                        globalTags=[field_tag_map[j]] if i == 0 else None,
                    )
                    for j in range(5)
                ],
            ),
        )
        mcps += [mcp for mcp in dataset.generate_mcp()]

    upstream_downstream_pairs = [
        ([upstream_schema_field_urns[0]], [downstream_schema_field_urns[0]]),  # 0 -> 0
        (
            [upstream_schema_field_urns[1], upstream_schema_field_urns[2]],  # 1, 2 -> 1
            [downstream_schema_field_urns[1]],
        ),
        ([upstream_schema_field_urns[3]], [downstream_schema_field_urns[3]]),  # 3 -> 3
        ([upstream_schema_field_urns[4]], [downstream_schema_field_urns[4]]),  # 4 -> 4
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
            schema_field_urn=downstream_schema_field_urns[
                4
            ],  # no propagation for the last field because TestTerm2_3 should not be targeted by the matching rule
            propagation_found=False,
        ),
        PropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[
                2
            ],  # no propagation for the 3rd field because it has no lineage
            propagation_found=False,
        ),
    ]

    field_0_propagation_expectation = TagPropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[0],
        propagation_found=True,
        propagated_tag=field_tag_map[0],
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=upstream_schema_field_urns[0],
    )
    field_1_propagation_expectation = TagPropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[1],
        propagation_found=True,  # This behavior is different from documentation propagation because term propagation doesn't restrict itself to 1:1 lineage
        propagated_tag=field_tag_map[1],
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=upstream_schema_field_urns[1],
    )
    field_1_propagation_expectation_also = TagPropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[1],
        propagation_found=True,
        propagated_tag=field_tag_map[
            2
        ],  # Since multiple fields are upstream with terms that are targeted, we expect all terms to be propagated
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=upstream_schema_field_urns[2],
    )

    field_3_propagation_expectation = TagPropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[3],
        propagation_found=True,
        propagated_tag=field_tag_map[3],
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=upstream_schema_field_urns[3],
    )

    # We add glossary terms to the first two columns of the upstream dataset
    mutation_mcp = MetadataChangeProposalWrapper(
        entityUrn=upstream_dataset_urn,
        aspect=EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="column_0",
                    globalTags=GlobalTags(
                        tags=[
                            TagAssociationClass(
                                tag="urn:li:tag:TestTagNode1.TestTag1_2",
                            )
                        ],
                    ),
                ),
                EditableSchemaFieldInfoClass(
                    fieldPath="column_1",
                    globalTags=GlobalTags(
                        tags=[
                            TagAssociationClass(
                                tag="urn:li:tag:TestTagNode1.TestTag1_1",
                            )
                        ],
                    ),
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
        TagPropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[0],
            propagation_found=True,
            propagated_tag="urn:li:tag:TestTagNode1.TestTag1_2",
            propagation_source=test_action_urn,
            propagation_via=None,
            propagation_origin=upstream_schema_field_urns[0],
        ),
        TagPropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[1],
            propagation_found=True,
            propagated_tag="urn:li:tag:TestTagNode1.TestTag1_1",
            propagation_source=test_action_urn,
            propagation_via=None,
            propagation_origin=upstream_schema_field_urns[1],
        ),
    ] + [
        x
        for x in no_propagation_expectations
        if x.schema_field_urn
        not in [downstream_schema_field_urns[0], downstream_schema_field_urns[1]]
    ]

    return PropagationTestScenario(
        base_graph=mcps,
        base_expectations=(
            no_propagation_expectations
            + [
                field_0_propagation_expectation,
                field_1_propagation_expectation,
                field_1_propagation_expectation_also,
                field_3_propagation_expectation,
            ]
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
    scenario_name, graph_func = request.param
    # Store the scenario name for access in the test
    request.node.scenario_name = scenario_name
    return graph_func(test_action_urn)


def _extract_lineage_info(mcp):
    """Extract lineage information from MCP."""
    lineage_info: List[tuple[str, str]] = []
    if not (
        hasattr(mcp, "aspect")
        and mcp.aspect
        and hasattr(mcp.aspect, "fineGrainedLineages")
        and mcp.aspect.fineGrainedLineages
    ):
        return lineage_info

    for lineage in mcp.aspect.fineGrainedLineages:
        if hasattr(lineage, "upstreams") and hasattr(lineage, "downstreams"):
            for upstream in lineage.upstreams:
                for downstream in lineage.downstreams:
                    lineage_info.append((upstream, downstream))
    return lineage_info


def _print_lineage_relationships(lineage_info, datasets):
    """Print lineage relationship information."""
    if not lineage_info:
        return

    print("🔗 LINEAGE RELATIONSHIPS:")
    for upstream, downstream in lineage_info:
        upstream_parts = upstream.split(":")
        downstream_parts = downstream.split(":")
        upstream_field = upstream_parts[-1] if len(upstream_parts) > 4 else "unknown"
        downstream_field = (
            downstream_parts[-1] if len(downstream_parts) > 4 else "unknown"
        )
        upstream_dataset = (
            ":".join(upstream_parts[:-1]) if len(upstream_parts) > 4 else upstream
        )
        downstream_dataset = (
            ":".join(downstream_parts[:-1]) if len(downstream_parts) > 4 else downstream
        )

        upstream_name = datasets.get(upstream_dataset, {}).get("name", "unknown")
        downstream_name = datasets.get(downstream_dataset, {}).get("name", "unknown")

        print(
            f"   📤 {upstream_name}.{upstream_field} → {downstream_name}.{downstream_field}"
        )
    print()


def _extract_tag_dataset_info(mcp):
    """Extract dataset information from MCP for tag propagation."""
    if not (
        hasattr(mcp, "entityUrn")
        and mcp.entityUrn
        and mcp.entityUrn.startswith("urn:li:dataset:")
    ):
        return None, None

    try:
        dataset_prefix = "urn:li:dataset:("
        if mcp.entityUrn.startswith(dataset_prefix) and mcp.entityUrn.endswith(")"):
            tuple_content = mcp.entityUrn[len(dataset_prefix) : -1]
            platform_urn_end = tuple_content.find(",")
            if platform_urn_end != -1:
                platform_urn = tuple_content[:platform_urn_end]
                remaining = tuple_content[platform_urn_end + 1 :]
                parts = remaining.rsplit(",", 1)
                if len(parts) == 2:
                    dataset_name = parts[0]
                    platform = (
                        platform_urn.split(":")[-1]
                        if ":" in platform_urn
                        else "unknown"
                    )
                    return mcp.entityUrn, {
                        "platform": platform,
                        "name": dataset_name,
                        "fields": [],
                    }
    except Exception:
        pass

    return mcp.entityUrn, {"platform": "unknown", "name": "unknown", "fields": []}


def _extract_tag_schema_fields(mcp, datasets, initial_tags):
    """Extract schema field information from MCP for tag propagation."""
    if not (
        hasattr(mcp, "aspect")
        and mcp.aspect
        and hasattr(mcp.aspect, "fields")
        and mcp.aspect.fields
    ):
        return

    for field in mcp.aspect.fields:
        field_info = {"name": field.fieldPath, "tags": []}
        if hasattr(field, "globalTags") and field.globalTags:
            field_info["tags"] = (
                [tag for tag in field.globalTags.tags] if field.globalTags.tags else []
            )
            if field_info["tags"]:
                field_urn = f"{mcp.entityUrn}:{field.fieldPath}"
                initial_tags[field_urn] = field_info["tags"]
        datasets[mcp.entityUrn]["fields"].append(field_info)


def _print_tag_datasets_setup(datasets):
    """Print dataset setup information for tag propagation."""
    print("📊 DATASETS SETUP:")
    for idx, (dataset_urn, info) in enumerate(datasets.items(), 1):
        print(f"   {idx}. {info['platform']}.{info['name']}")
        print(f"      URN: {dataset_urn}")
        if info["fields"]:
            print(f"      Fields: {len(info['fields'])} fields")
            fields_with_tags = [f for f in info["fields"] if f.get("tags")]
            if fields_with_tags:
                print("      🏷️  Fields with initial tags:")
                for field in fields_with_tags:
                    tags_str = ", ".join([t.tag.split(":")[-1] for t in field["tags"]])
                    print(f"         • {field['name']}: {tags_str}")
            else:
                print("      🚫 No initial tags on any fields")
        print()


def _print_tag_base_expectations(scenario, datasets):
    """Print base expectations information for tag propagation."""
    if not scenario.base_expectations:
        return

    print("🎯 EXPECTED PROPAGATIONS (Bootstrap Phase):")
    for _idx, expectation in enumerate(scenario.base_expectations, 1):
        field_parts = expectation.schema_field_urn.split(":")
        field_name = field_parts[-1] if len(field_parts) > 4 else "unknown"
        dataset_parts = field_parts[:-1] if len(field_parts) > 4 else []
        dataset_urn = ":".join(dataset_parts) if dataset_parts else "unknown"
        dataset_name = datasets.get(dataset_urn, {}).get("name", "unknown")

        if expectation.propagation_found:
            if hasattr(expectation, "propagated_tag") and expectation.propagated_tag:
                tag_name = expectation.propagated_tag.split(":")[-1]
                origin_field = (
                    expectation.propagation_origin.split(":")[-1]
                    if expectation.propagation_origin
                    else "unknown"
                )
                origin_dataset_urn = (
                    ":".join(expectation.propagation_origin.split(":")[:-1])
                    if expectation.propagation_origin
                    else "unknown"
                )
                origin_dataset_name = datasets.get(origin_dataset_urn, {}).get(
                    "name", "unknown"
                )

                print(
                    f"   ✅ {dataset_name}.{field_name} should get tag '{tag_name}' from {origin_dataset_name}.{origin_field}"
                )
                if expectation.propagation_via:
                    via_field = expectation.propagation_via.split(":")[-1]
                    via_dataset_urn = ":".join(
                        expectation.propagation_via.split(":")[:-1]
                    )
                    via_dataset_name = datasets.get(via_dataset_urn, {}).get(
                        "name", "unknown"
                    )
                    print(f"      🔄 Via: {via_dataset_name}.{via_field}")
            elif (
                hasattr(expectation, "propagated_description")
                and expectation.propagated_description
            ):
                print(
                    f"   ✅ {dataset_name}.{field_name} should get description: '{expectation.propagated_description[:50]}...'"
                )
        else:
            print(f"   ❌ {dataset_name}.{field_name} should have NO propagation")
    print()


def _print_tag_mutations(scenario, datasets):
    """Print mutation information for tag propagation."""
    if not scenario.mutations:
        return

    print("🔄 LIVE PHASE MUTATIONS:")
    for idx, mcp in enumerate(scenario.mutations, 1):
        if hasattr(mcp, "aspect") and hasattr(mcp.aspect, "editableSchemaFieldInfo"):
            dataset_name = datasets.get(mcp.entityUrn, {}).get("name", "unknown")
            print(f"   {idx}. Adding tags to {dataset_name}:")
            for field_info in mcp.aspect.editableSchemaFieldInfo:
                if field_info.globalTags and field_info.globalTags.tags:
                    tags = [
                        tag.tag.split(":")[-1] for tag in field_info.globalTags.tags
                    ]
                    print(f"      • {field_info.fieldPath}: {', '.join(tags)}")
    print()


def _print_tag_post_mutation_expectations(scenario, datasets):
    """Print post-mutation expectations for tag propagation."""
    if not scenario.post_mutation_expectations:
        return

    print("🎯 EXPECTED RESULTS (After Live Mutations):")
    for _idx, expectation in enumerate(scenario.post_mutation_expectations, 1):
        field_parts = expectation.schema_field_urn.split(":")
        field_name = field_parts[-1] if len(field_parts) > 4 else "unknown"
        dataset_parts = field_parts[:-1] if len(field_parts) > 4 else []
        dataset_urn = ":".join(dataset_parts) if dataset_parts else "unknown"
        dataset_name = datasets.get(dataset_urn, {}).get("name", "unknown")

        if expectation.propagation_found:
            if hasattr(expectation, "propagated_tag") and expectation.propagated_tag:
                tag_name = expectation.propagated_tag.split(":")[-1]
                origin_field = (
                    expectation.propagation_origin.split(":")[-1]
                    if expectation.propagation_origin
                    else "unknown"
                )
                origin_dataset_urn = (
                    ":".join(expectation.propagation_origin.split(":")[:-1])
                    if expectation.propagation_origin
                    else "unknown"
                )
                origin_dataset_name = datasets.get(origin_dataset_urn, {}).get(
                    "name", "unknown"
                )

                print(
                    f"   ✅ {dataset_name}.{field_name} should have tag '{tag_name}' from {origin_dataset_name}.{origin_field}"
                )
        else:
            print(f"   ❌ {dataset_name}.{field_name} should have NO propagation")
    print()


def print_test_scenario_summary(
    scenario: PropagationTestScenario, scenario_name: str
) -> None:
    """Print a clear summary of what the test scenario will do."""
    print(f"\n{'=' * 120}")
    print(f"🧪 TEST SCENARIO: {scenario_name.upper()}")
    print(f"{'=' * 120}")

    datasets = {}
    lineage_info: List[tuple[str, str]] = []
    initial_tags: dict[str, list] = {}

    # Extract information from base_graph
    for mcp in scenario.base_graph:
        dataset_urn, dataset_info = _extract_tag_dataset_info(mcp)
        if dataset_urn and dataset_urn not in datasets:
            datasets[dataset_urn] = dataset_info

        if dataset_urn:
            _extract_tag_schema_fields(mcp, datasets, initial_tags)

        lineage_info.extend(_extract_lineage_info(mcp))

    _print_tag_datasets_setup(datasets)
    _print_lineage_relationships(lineage_info, datasets)
    _print_tag_base_expectations(scenario, datasets)
    _print_tag_mutations(scenario, datasets)
    _print_tag_post_mutation_expectations(scenario, datasets)

    print("🧪 TEST PHASES:")
    print(
        "   1. 🏗️  Bootstrap: Set up data, run initial propagation, verify base expectations"
    )
    print("   2. ↩️  Rollback: Remove all propagated data, verify cleanup")
    print(
        "   3. 🔴 Live: Start live action, apply mutations, verify real-time propagation"
    )
    print(
        "   4. ↩️  Live Rollback: Remove all propagated data again, verify final cleanup"
    )

    print(f"{'=' * 120}\n")


def test_main_loop(
    auth_session: Any,
    graph_client: DataHubGraph,
    graph_lineage_data: PropagationTestScenario,
    test_action_urn: str,
    pytestconfig,
    load_glossary,
    request,
) -> None:
    # Print scenario summary at the start
    scenario_name = getattr(request.node, "scenario_name", "Unknown Scenario")
    print_test_scenario_summary(graph_lineage_data, scenario_name)

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
        run_propagation_bootstrap(
            auth_session, graph_client, graph_lineage_data, test_action_urn
        )
        time_stages["bootstrap"] = time.time() - time_stages["bootstrap"]

        # Then test rollback
        time_stages["rollback"] = time.time()
        run_propagation_rollback(
            auth_session,
            graph_client,
            graph_lineage_data,
            test_action_urn,
            post_mutation=False,
        )
        time_stages["rollback"] = time.time() - time_stages["rollback"]

        # Finally test live
        time_stages["live"] = time.time()
        run_propagation_live(
            auth_session, graph_client, graph_lineage_data, test_action_urn
        )
        time_stages["live"] = time.time() - time_stages["live"]

        # Test rollback again
        time_stages["live_rollback"] = time.time()
        run_propagation_rollback(
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


def check_attribution(
    action_urn: str,
    attribution: Optional[MetadataAttributionClass],
    expectation: PropagationExpectation,
) -> None:
    """
    Raises assertion error if the expectation is not met.
    """
    # TODO: Check actor attribution as well
    if expectation.propagation_found:
        assert attribution
        assert attribution.source == expectation.propagation_source
        assert attribution.sourceDetail
        assert attribution.sourceDetail.get("origin") == expectation.propagation_origin
        # assert attribution.sourceDetail.get("via") == expectation.propagation_via
        assert attribution.sourceDetail.get("propagated") == "true"
        # Allow either system user (local vs CI environments)
        assert attribution.actor in [
            "urn:li:corpuser:__datahub_system",
            "urn:li:corpuser:admin",
        ]


def check_expectation(
    graph_client: DataHubGraph,
    action_urn: str,
    expectation: PropagationExpectation,
    rollback: bool = False,
) -> None:
    """
    Raises assertion error if the expectation is not met.
    """

    if isinstance(expectation, TagPropagationExpectation):
        field_urn = expectation.schema_field_urn
        dataset_urn = Urn.create_from_string(field_urn).entity_ids[0]
        # propagated terms are stored in the editable schema metadata aspect
        editable_schema_metadata_aspect = graph_client.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )
        if editable_schema_metadata_aspect:
            field_info = next(
                (
                    x
                    for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                    if x.fieldPath == Urn.create_from_string(field_urn).entity_ids[1]
                ),
                None,
            )
            if field_info:
                assert field_info.globalTags
                tags = field_info.globalTags.tags
                tags_propagated_from_action = [
                    x
                    for x in tags
                    if x.attribution and x.attribution.source == action_urn
                ]
                if tags_propagated_from_action:
                    if not rollback:
                        term_propagated_from_action = next(
                            (
                                x
                                for x in tags_propagated_from_action
                                if x.tag == expectation.propagated_tag
                            ),
                            None,
                        )
                        assert term_propagated_from_action
                        assert (
                            term_propagated_from_action.tag
                            == expectation.propagated_tag
                        )
                        check_attribution(
                            action_urn,
                            term_propagated_from_action.attribution,
                            expectation,
                        )
                    else:
                        assert [
                            x
                            for x in tags_propagated_from_action
                            if x.tag == expectation.propagated_tag
                        ] == [], "Propagated term should have been rolled back"
                else:
                    if not rollback:
                        try:
                            assert not expectation.propagation_found
                        except Exception:
                            # breakpoint()
                            raise
            else:
                if not rollback:
                    try:
                        assert not expectation.propagation_found
                    except Exception:
                        raise
        else:
            if not rollback:
                try:
                    assert not expectation.propagation_found
                except Exception:
                    raise


def run_propagation_bootstrap(
    auth_session: Any,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
) -> None:
    time_stages = {}

    time_stages["bootstrap_data_setup"] = time.time()
    # Apply base graph
    for mcp in scenario.base_graph:
        graph_client.emit(mcp)

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
        try:
            check_expectation(graph_client, test_action_urn, expectation)
        except Exception:
            logger.debug(f"Failed expectation: {expectation}")
            raise
    time_stages["bootstrap_check_expectations"] = (
        time.time() - time_stages["bootstrap_check_expectations"]
    )
    print(f"---------- Bootstrap time stages: {time_stages} ------------")
    # breakpoint()


def run_propagation_live(
    auth_session: Any,
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
            check_expectation(graph_client, test_action_urn, expectation)

    finally:
        time_stages["stop_action"] = time.time()
        stop_action(test_action_urn, auth_session.integrations_service_url())
        time_stages["stop_action"] = time.time() - time_stages["stop_action"]
        print(f"---------- Live time stages: {time_stages} ------------")


def run_propagation_rollback(
    auth_session: Any,
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
        check_expectation(graph_client, test_action_urn, expectation, rollback=True)
        # if expectation.propagation_found:
        #     documentation_aspect = graph_client.get_aspect(
        #         expectation.schema_field_urn, DocumentationClass
        #     )
        #     if documentation_aspect:
        #         assert not documentation_aspect.documentations
        #     else:
        #         logger.warning(
        #             f"Documentation aspect not found for {expectation.schema_field_urn}. Unexpected but not fatal."
        #         )
