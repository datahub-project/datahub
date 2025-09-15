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
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataHubActionConfigClass,
    DataHubActionInfoClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    MetadataChangeProposalClass,
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

pytestmark = pytest.mark.skip(reason="Covered by test_framework_term_propagation.py")

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
    # First, clean up any existing glossary terms that might interfere
    logger.info("Cleaning up existing glossary terms...")
    try:
        # Delete existing glossary nodes and terms that might conflict
        existing_terms_to_clean = [
            "urn:li:glossaryNode:TestGlossaryNode1",
            "urn:li:glossaryNode:TestGlossaryNode2",
            "urn:li:glossaryTerm:TestTerm1_1",
            "urn:li:glossaryTerm:TestTerm1_2",
            "urn:li:glossaryTerm:TestTerm2_1",
            "urn:li:glossaryTerm:TestTerm2_2",
            "urn:li:glossaryTerm:TestTerm2_3",
            # Also clean up any old terms that might exist from previous bad runs
            "urn:li:glossaryTerm:TestTerm11",
            "urn:li:glossaryTerm:TestTerm12",
        ]

        for term_urn in existing_terms_to_clean:
            try:
                auth_session.graph_client().delete_entity(term_urn, hard=True)
                logger.info(f"Cleaned up: {term_urn}")
            except Exception as e:
                logger.debug(f"Could not clean up {term_urn}: {e}")

        wait_for_writes_to_sync()
    except Exception as e:
        logger.warning(f"Error during glossary cleanup: {e}")

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
    logger.info("Loading glossary from file...")
    ingest_pipeline = Pipeline.create(
        config_dict=pipeline,
    )
    ingest_pipeline.run()
    ingest_pipeline.raise_from_status()
    wait_for_writes_to_sync()
    logger.info("Glossary loading completed")


@pytest.fixture(scope="function")
def create_test_action(
    auth_session: Any, graph_client: DataHubGraph, test_resources_dir, test_action_urn
) -> Iterator[None]:
    try:
        action_urn = test_action_urn
        recipe_file = test_resources_dir / "term_propagation_generic_action_recipe.yaml"

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
                    name="test_docs_propagation",
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


class TermPropagationExpectation(PropagationExpectation):
    propagated_term: Optional[str] = None  # the expected propagated term


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

    dataset_3_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo2_2")
    dataset_2_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_1")
    dataset_1_urn = make_dataset_urn("snowflake", f"{prefix}.table_foo_0")

    dataset_3_schema_field_urns = [
        make_schema_field_urn(dataset_3_urn, f"column_{i}") for i in range(5)
    ]

    dataset_3 = Dataset(
        id=None,
        urn=dataset_3_urn,
        platform="snowflake",
        name="table_foo2_2",
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

    field_0_propagation_expectation = TermPropagationExpectation(
        schema_field_urn=dataset_3_schema_field_urns[0],
        propagation_found=False,  # Bootstrap doesn't create multi-hop propagation
        propagation_source=test_action_urn,
        propagation_via=make_schema_field_urn(dataset_2_urn, "column_0"),
        propagation_origin=make_schema_field_urn(dataset_1_urn, "column_0"),
        propagated_term="urn:li:glossaryTerm:TestTerm1_1",
    )

    no_propagation_expectations = [
        PropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urns[
                4
            ],  # no propagation for the last field because TestTerm2_3 should not be targeted by the matching rule
            propagation_found=False,
        ),
        PropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urns[
                2
            ],  # no propagation for the 3rd field because it has no lineage
            propagation_found=False,
        ),
    ]

    mutation_mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_2_urn,
        aspect=EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="column_1",
                    glossaryTerms=GlossaryTermsClass(
                        auditStamp=AuditStampClass(
                            time=int(
                                datetime.datetime.now(datetime.timezone.utc).timestamp()
                                * 1000
                            ),
                            actor="urn:li:corpuser:foobar",
                        ),
                        terms=[
                            GlossaryTermAssociationClass(
                                urn="urn:li:glossaryTerm:TestTerm1_1",
                                actor="urn:li:corpuser:foobar",
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
        TermPropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urns[0],
            propagation_found=True,
            propagated_term="urn:li:glossaryTerm:TestTerm1_2",
            propagation_source=test_action_urn,
            propagation_via=make_schema_field_urn(dataset_2_urn, "column_0"),
            propagation_origin=make_schema_field_urn(dataset_1_urn, "column_0"),
        ),
        TermPropagationExpectation(
            schema_field_urn=dataset_3_schema_field_urns[1],
            propagation_found=True,
            propagated_term="urn:li:glossaryTerm:TestTerm1_1",
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
    dataset_3 = dbt, table_foo2_2

    dataset_3 has a sibling relationship with dataset_1
    """

    base_propagation_scenario: PropagationTestScenario = create_1_1_graph_lineage(
        test_action_urn, prefix=prefix, column_docs=False
    )

    dataset_3_urn = make_dataset_urn("dbt", f"{prefix}.table_foo2_2")
    dataset_3 = Dataset(
        id=None,
        urn=dataset_3_urn,
        platform="dbt",
        name="table_foo2_2",
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
    field_glossary_term_map = {
        0: "urn:li:glossaryTerm:TestTerm1_1",
        1: "urn:li:glossaryTerm:TestTerm1_2",
        2: "urn:li:glossaryTerm:TestTerm2_1",
        3: "urn:li:glossaryTerm:TestTerm2_2",
        4: "urn:li:glossaryTerm:TestTerm2_3",
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
                        glossaryTerms=[field_glossary_term_map[j]] if i == 0 else None,
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

    field_0_propagation_expectation = TermPropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[0],
        propagation_found=True,
        propagated_term=field_glossary_term_map[0],
        propagation_source=test_action_urn,
        propagation_via=None,
        propagation_origin=upstream_schema_field_urns[0],
    )

    field_3_propagation_expectation = TermPropagationExpectation(
        schema_field_urn=downstream_schema_field_urns[3],
        propagation_found=True,
        propagated_term=field_glossary_term_map[3],
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
                    glossaryTerms=GlossaryTermsClass(
                        auditStamp=AuditStampClass(
                            time=int(
                                datetime.datetime.now(datetime.timezone.utc).timestamp()
                                * 1000
                            ),
                            actor="urn:li:corpuser:foobar",
                        ),
                        terms=[
                            GlossaryTermAssociationClass(
                                urn="urn:li:glossaryTerm:TestTerm1_2",
                                actor="urn:li:corpuser:foobar",
                            )
                        ],
                    ),
                ),
                EditableSchemaFieldInfoClass(
                    fieldPath="column_1",
                    glossaryTerms=GlossaryTermsClass(
                        auditStamp=AuditStampClass(
                            time=int(
                                datetime.datetime.now(datetime.timezone.utc).timestamp()
                                * 1000
                            ),
                            actor="urn:li:corpuser:foobar",
                        ),
                        terms=[
                            GlossaryTermAssociationClass(
                                urn="urn:li:glossaryTerm:TestTerm1_1",
                                actor="urn:li:corpuser:foobar",
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
        TermPropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[0],
            propagation_found=True,
            propagated_term="urn:li:glossaryTerm:TestTerm1_2",
            propagation_source=test_action_urn,
            propagation_via=None,
            propagation_origin=upstream_schema_field_urns[0],
        ),
        TermPropagationExpectation(
            schema_field_urn=downstream_schema_field_urns[1],
            propagation_found=True,
            propagated_term="urn:li:glossaryTerm:TestTerm1_1",
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
        # ("2x2 graph with siblings", create_2_2_graph_lineage_siblings),
    ],
    ids=lambda x: x[0],
)
def graph_lineage_data(request, test_action_urn, create_test_action):
    scenario_name, graph_func = request.param
    # Store the scenario name for access in the test
    request.node.scenario_name = scenario_name
    return graph_func(test_action_urn)


def _extract_dataset_info(mcp):
    """Extract dataset information from MCP."""
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


def _extract_schema_fields(mcp, datasets, initial_terms):
    """Extract schema field information from MCP."""
    if not (
        hasattr(mcp, "aspect")
        and mcp.aspect
        and hasattr(mcp.aspect, "fields")
        and mcp.aspect.fields
    ):
        return

    for field in mcp.aspect.fields:
        field_info = {"name": field.fieldPath, "terms": []}
        if hasattr(field, "glossaryTerms") and field.glossaryTerms:
            field_info["terms"] = (
                [term for term in field.glossaryTerms.terms]
                if field.glossaryTerms.terms
                else []
            )
            if field_info["terms"]:
                field_urn = f"{mcp.entityUrn}:{field.fieldPath}"
                initial_terms[field_urn] = field_info["terms"]
        datasets[mcp.entityUrn]["fields"].append(field_info)


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


def _print_datasets_setup(datasets):
    """Print dataset setup information."""
    print("📊 DATASETS SETUP:")
    for idx, (dataset_urn, info) in enumerate(datasets.items(), 1):
        print(f"   {idx}. {info['platform']}.{info['name']}")
        print(f"      URN: {dataset_urn}")
        if info["fields"]:
            print(f"      Fields: {len(info['fields'])} fields")
            fields_with_terms = [f for f in info["fields"] if f.get("terms")]
            if fields_with_terms:
                print("      🏷️  Fields with initial terms:")
                for field in fields_with_terms:
                    terms_str = ", ".join(
                        [t.urn.split(":")[-1] for t in field["terms"]]
                    )
                    print(f"         • {field['name']}: {terms_str}")
            else:
                print("      🚫 No initial terms on any fields")
        print()


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


def _print_base_expectations(scenario, datasets):
    """Print base expectations information."""
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
            if hasattr(expectation, "propagated_term") and expectation.propagated_term:
                term_name = expectation.propagated_term.split(":")[-1]
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
                    f"   ✅ {dataset_name}.{field_name} should get term '{term_name}' from {origin_dataset_name}.{origin_field}"
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


def _print_mutations(scenario, datasets):
    """Print mutation information."""
    if not scenario.mutations:
        return

    print("🔄 LIVE PHASE MUTATIONS:")
    for idx, mcp in enumerate(scenario.mutations, 1):
        if hasattr(mcp, "aspect") and hasattr(mcp.aspect, "editableSchemaFieldInfo"):
            dataset_name = datasets.get(mcp.entityUrn, {}).get("name", "unknown")
            print(f"   {idx}. Adding terms to {dataset_name}:")
            for field_info in mcp.aspect.editableSchemaFieldInfo:
                if field_info.glossaryTerms and field_info.glossaryTerms.terms:
                    terms = [
                        term.urn.split(":")[-1]
                        for term in field_info.glossaryTerms.terms
                    ]
                    print(f"      • {field_info.fieldPath}: {', '.join(terms)}")
    print()


def _print_post_mutation_expectations(scenario, datasets):
    """Print post-mutation expectations."""
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
            if hasattr(expectation, "propagated_term") and expectation.propagated_term:
                term_name = expectation.propagated_term.split(":")[-1]
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
                    f"   ✅ {dataset_name}.{field_name} should have term '{term_name}' from {origin_dataset_name}.{origin_field}"
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
    initial_terms: dict[str, list] = {}

    # Extract information from base_graph
    for mcp in scenario.base_graph:
        dataset_urn, dataset_info = _extract_dataset_info(mcp)
        if dataset_urn and dataset_urn not in datasets:
            datasets[dataset_urn] = dataset_info

        if dataset_urn:
            _extract_schema_fields(mcp, datasets, initial_terms)

        lineage_info.extend(_extract_lineage_info(mcp))

    _print_datasets_setup(datasets)
    _print_lineage_relationships(lineage_info, datasets)
    _print_base_expectations(scenario, datasets)
    _print_mutations(scenario, datasets)
    _print_post_mutation_expectations(scenario, datasets)

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
        docs_propagation_bootstrap(
            auth_session, graph_client, graph_lineage_data, test_action_urn
        )
        time_stages["bootstrap"] = time.time() - time_stages["bootstrap"]

        # # Then test rollback
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


def check_attribution(
    action_urn: str,
    attribution: Optional[MetadataAttributionClass],
    expectation: PropagationExpectation,
) -> None:
    """
    Raises assertion error if the expectation is not met.
    """
    logger.info(f"\\n{'=' * 60}")
    logger.info("CHECKING ATTRIBUTION")
    logger.info(f"Expected propagation found: {expectation.propagation_found}")
    logger.info(f"Attribution object: {attribution}")
    logger.info(f"{'=' * 60}")

    if expectation.propagation_found:
        if not attribution:
            logger.error("FAILURE: Expected attribution but got None")
            raise AssertionError("Expected attribution but got None")

        logger.info(f"Attribution source: {attribution.source}")
        logger.info(f"Expected source: {expectation.propagation_source}")

        if attribution.source != expectation.propagation_source:
            logger.error("FAILURE: Attribution source mismatch")
            logger.error(f"Expected: {expectation.propagation_source}")
            logger.error(f"Actual: {attribution.source}")
            raise AssertionError(
                f"Attribution source mismatch: expected {expectation.propagation_source}, got {attribution.source}"
            )

        if not attribution.sourceDetail:
            logger.error("FAILURE: Expected sourceDetail but got None")
            raise AssertionError("Expected sourceDetail but got None")

        logger.info(f"Attribution sourceDetail: {attribution.sourceDetail}")

        # Debug logging for term propagation
        logger.info("\\n[ATTRIBUTION DEBUG] =====================================")
        logger.info(f"[ATTRIBUTION DEBUG] ENTITY: {expectation.schema_field_urn}")
        logger.info("[ATTRIBUTION DEBUG] EXPECTED PROPERTIES:")
        logger.info(
            f"[ATTRIBUTION DEBUG]   - propagation_found: {expectation.propagation_found}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - propagation_source: {expectation.propagation_source}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - propagation_via: {expectation.propagation_via}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - propagation_origin: {expectation.propagation_origin}"
        )
        logger.info("[ATTRIBUTION DEBUG] ACTUAL PROPERTIES:")
        logger.info(f"[ATTRIBUTION DEBUG]   - attribution.source: {attribution.source}")
        logger.info(f"[ATTRIBUTION DEBUG]   - attribution.actor: {attribution.actor}")
        logger.info(f"[ATTRIBUTION DEBUG]   - attribution.time: {attribution.time}")
        logger.info(
            f"[ATTRIBUTION DEBUG]   - attribution.sourceDetail type: {type(attribution.sourceDetail)}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - attribution.sourceDetail.origin: {attribution.sourceDetail.get('origin') if hasattr(attribution.sourceDetail, 'get') else 'N/A (not dict-like)'}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - attribution.sourceDetail.via: {attribution.sourceDetail.get('via') if hasattr(attribution.sourceDetail, 'get') else 'N/A (not dict-like)'}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - attribution.sourceDetail.propagation_depth: {attribution.sourceDetail.get('propagation_depth') if hasattr(attribution.sourceDetail, 'get') else 'N/A (not dict-like)'}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - attribution.sourceDetail.propagation_direction: {attribution.sourceDetail.get('propagation_direction') if hasattr(attribution.sourceDetail, 'get') else 'N/A (not dict-like)'}"
        )
        logger.info(
            f"[ATTRIBUTION DEBUG]   - attribution.sourceDetail.propagated: {attribution.sourceDetail.get('propagated') if hasattr(attribution.sourceDetail, 'get') else 'N/A (not dict-like)'}"
        )
        logger.info("[ATTRIBUTION DEBUG] FULL ATTRIBUTION:")
        logger.info(
            f"[ATTRIBUTION DEBUG]   - Full sourceDetail: {attribution.sourceDetail}"
        )
        logger.info("[ATTRIBUTION DEBUG] =====================================")

        # Check origin
        actual_origin = (
            attribution.sourceDetail.get("origin")
            if hasattr(attribution.sourceDetail, "get")
            else None
        )
        if actual_origin != expectation.propagation_origin:
            logger.error("FAILURE: Origin mismatch")
            logger.error(f"Expected: {expectation.propagation_origin}")
            logger.error(f"Actual: {actual_origin}")
            raise AssertionError(
                f"Origin mismatch: expected {expectation.propagation_origin}, got {actual_origin}"
            )

        # Check via
        actual_via = (
            attribution.sourceDetail.get("via")
            if hasattr(attribution.sourceDetail, "get")
            else None
        )
        if actual_via != expectation.propagation_via:
            logger.error("FAILURE: Via mismatch")
            logger.error(f"Expected: {expectation.propagation_via}")
            logger.error(f"Actual: {actual_via}")
            raise AssertionError(
                f"Via mismatch: expected {expectation.propagation_via}, got {actual_via}"
            )

        # Check propagated flag
        actual_propagated = (
            attribution.sourceDetail.get("propagated")
            if hasattr(attribution.sourceDetail, "get")
            else None
        )
        if actual_propagated != "true":
            logger.error("FAILURE: Propagated flag mismatch")
            logger.error("Expected: 'true'")
            logger.error(f"Actual: {actual_propagated}")
            raise AssertionError(
                f"Propagated flag mismatch: expected 'true', got {actual_propagated}"
            )

        # Check actor - Allow either system user (local vs CI environments)
        allowed_actors = [
            "urn:li:corpuser:__datahub_system",
            "urn:li:corpuser:admin",
        ]
        if attribution.actor not in allowed_actors:
            logger.error("FAILURE: Actor mismatch")
            logger.error(f"Expected one of: {allowed_actors}")
            logger.error(f"Actual: {attribution.actor}")
            raise AssertionError(
                f"Actor mismatch: expected one of {allowed_actors}, got {attribution.actor}"
            )

        logger.info("SUCCESS: All attribution checks passed")

    logger.info(f"ATTRIBUTION CHECK COMPLETE\\n{'=' * 60}")


def _log_expectation_details(
    expectation: PropagationExpectation, action_urn: str, rollback: bool
) -> None:
    """Log expectation details for debugging."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"CHECKING EXPECTATION - {'ROLLBACK' if rollback else 'NORMAL'}")
    logger.info(f"Field URN: {expectation.schema_field_urn}")
    logger.info(f"Expected propagation found: {expectation.propagation_found}")
    if hasattr(expectation, "propagated_term"):
        logger.info(f"Expected propagated term: {expectation.propagated_term}")
    if hasattr(expectation, "propagated_description"):
        logger.info(
            f"Expected propagated description: {expectation.propagated_description}"
        )
    logger.info(f"Expected propagation source: {expectation.propagation_source}")
    logger.info(f"Expected propagation via: {expectation.propagation_via}")
    logger.info(f"Expected propagation origin: {expectation.propagation_origin}")
    logger.info(f"Action URN: {action_urn}")
    logger.info(f"{'=' * 80}")


def _check_propagated_terms_for_rollback(
    terms_propagated_from_action: List, expectation: TermPropagationExpectation
) -> None:
    """Check if terms were properly rolled back."""
    rollback_terms = [
        x for x in terms_propagated_from_action if x.urn == expectation.propagated_term
    ]
    if rollback_terms:
        logger.error(
            f"FAILURE: Expected no propagated terms after rollback, but found: {[x.urn for x in rollback_terms]}"
        )
        raise AssertionError("Propagated term should have been rolled back")
    else:
        logger.info("SUCCESS: No propagated terms found after rollback as expected")


def _check_propagated_terms_for_normal(
    terms_propagated_from_action: List,
    expectation: TermPropagationExpectation,
    action_urn: str,
) -> None:
    """Check if expected terms were properly propagated in normal mode."""
    term_propagated_from_action = next(
        (
            x
            for x in terms_propagated_from_action
            if x.urn == expectation.propagated_term
        ),
        None,
    )

    if term_propagated_from_action:
        logger.info(
            f"SUCCESS: Found expected propagated term {expectation.propagated_term}"
        )
        assert term_propagated_from_action.urn == expectation.propagated_term
        check_attribution(
            action_urn, term_propagated_from_action.attribution, expectation
        )
    else:
        logger.error(
            f"FAILURE: Expected term {expectation.propagated_term} not found in propagated terms"
        )
        logger.error(
            f"Available propagated terms: {[x.urn for x in terms_propagated_from_action]}"
        )
        raise AssertionError(
            f"Expected term {expectation.propagated_term} not found in propagated terms from action"
        )


def _handle_no_propagated_terms(
    expectation: TermPropagationExpectation,
    action_urn: str,
    rollback: bool,
    terms: List,
) -> None:
    """Handle case when no terms were propagated from action."""
    logger.info("No terms propagated from action found")
    if not rollback:
        if expectation.propagation_found:
            logger.error(
                "FAILURE: Expected propagation but found no terms propagated from action"
            )
            logger.error(
                f"Terms on field: {[(x.urn, x.attribution.source if x.attribution else None) for x in terms]}"
            )
            raise AssertionError(
                f"Expected propagation but found no terms propagated from action {action_urn}"
            )
        else:
            logger.info("SUCCESS: No propagation expected and none found")


def _log_term_additions_in_mcp(
    mcp: Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass], phase: str
) -> None:
    """Log when terms are being added to entities for propagation tracking."""
    if not hasattr(mcp, "aspect") or not mcp.aspect:
        return

    # Check if this is an EditableSchemaMetadataClass with glossary terms
    if isinstance(mcp.aspect, EditableSchemaMetadataClass):
        if (
            hasattr(mcp.aspect, "editableSchemaFieldInfo")
            and mcp.aspect.editableSchemaFieldInfo
        ):
            for field_info in mcp.aspect.editableSchemaFieldInfo:
                if field_info.glossaryTerms and field_info.glossaryTerms.terms:
                    logger.info(
                        f"\n🏷️  [{phase}] ADDING TERMS TO ENTITY FOR PROPAGATION:"
                    )
                    logger.info(f"   📍 Entity: {mcp.entityUrn}")
                    logger.info(f"   🔍 Field: {field_info.fieldPath}")
                    logger.info(
                        f"   📝 Terms being added: {[term.urn for term in field_info.glossaryTerms.terms]}"
                    )
                    logger.info(
                        f"   👤 Actor: {field_info.glossaryTerms.terms[0].actor if field_info.glossaryTerms.terms else 'N/A'}"
                    )
                    logger.info(
                        f"   ⏰ Timestamp: {field_info.glossaryTerms.auditStamp.time if field_info.glossaryTerms.auditStamp else 'N/A'}"
                    )

                    # Check if any of these terms are in the propagation target list
                    propagation_target_terms = [
                        "urn:li:glossaryTerm:TestTerm1_1",
                        "urn:li:glossaryTerm:TestTerm1_2",
                        "urn:li:glossaryTerm:TestTerm2_1",
                        "urn:li:glossaryTerm:TestTerm2_2",
                    ]
                    added_terms = [term.urn for term in field_info.glossaryTerms.terms]
                    matching_terms = [
                        term for term in added_terms if term in propagation_target_terms
                    ]

                    if matching_terms:
                        logger.info(
                            f"   🎯 PROPAGATION TRIGGER: These terms should be propagated: {matching_terms}"
                        )
                    else:
                        logger.info(
                            "   ❌ NO PROPAGATION: None of these terms are in propagation target list"
                        )

                    logger.info(f"   {'=' * 80}")


def _print_entity_state_summary(
    graph_client: DataHubGraph,
    expectation: TermPropagationExpectation,
    action_urn: str,
    rollback: bool = False,
) -> None:
    """Print a clear summary of current vs expected entity state."""
    field_urn = expectation.schema_field_urn
    dataset_urn = Urn.create_from_string(field_urn).entity_ids[0]
    field_path = Urn.create_from_string(field_urn).entity_ids[1]

    logger.info(f"\n{'=' * 100}")
    logger.info("ENTITY STATE SUMMARY")
    logger.info(f"{'=' * 100}")
    logger.info(f"📍 Entity: {field_urn}")
    logger.info(f"📊 Dataset: {dataset_urn}")
    logger.info(f"🏷️  Field: {field_path}")
    logger.info(f"🔄 Operation: {'ROLLBACK' if rollback else 'NORMAL'}")
    logger.info(f"⚙️  Action: {action_urn}")

    # Get current state
    editable_schema_metadata_aspect = graph_client.get_aspect(
        dataset_urn, EditableSchemaMetadataClass
    )

    current_terms = []
    current_terms_from_action = []

    if (
        editable_schema_metadata_aspect
        and editable_schema_metadata_aspect.editableSchemaFieldInfo
    ):
        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )
        if field_info and field_info.glossaryTerms:
            current_terms = field_info.glossaryTerms.terms
            current_terms_from_action = [
                x
                for x in current_terms
                if x.attribution and x.attribution.source == action_urn
            ]

    logger.info("\n📋 CURRENT STATE:")
    logger.info(f"   • Total terms on field: {len(current_terms)}")
    if current_terms:
        logger.info(f"   • All terms: {[x.urn for x in current_terms]}")
        logger.info(
            f"   • Terms with attribution: {[(x.urn, x.attribution.source if x.attribution else 'None') for x in current_terms]}"
        )
    else:
        logger.info("   • No terms found on field")

    logger.info(
        f"   • Terms from action {action_urn}: {len(current_terms_from_action)}"
    )
    if current_terms_from_action:
        for term in current_terms_from_action:
            attribution_details = ""
            if term.attribution and term.attribution.sourceDetail:
                if hasattr(term.attribution.sourceDetail, "get"):
                    origin = term.attribution.sourceDetail.get("origin", "N/A")
                    via = term.attribution.sourceDetail.get("via", "N/A")
                    attribution_details = f" (origin: {origin}, via: {via})"
            logger.info(f"     ↳ {term.urn}{attribution_details}")

    logger.info("\n🎯 EXPECTED STATE:")
    logger.info(
        f"   • Should have propagation: {'❌ NO' if rollback else ('✅ YES' if expectation.propagation_found else '❌ NO')}"
    )

    if not rollback and expectation.propagation_found:
        logger.info(f"   • Expected term: {expectation.propagated_term}")
        logger.info(f"   • Expected source: {expectation.propagation_source}")
        logger.info(f"   • Expected origin: {expectation.propagation_origin}")
        logger.info(f"   • Expected via: {expectation.propagation_via}")
    elif rollback:
        logger.info(
            f"   • After rollback: No terms from action {action_urn} should remain"
        )
    else:
        logger.info("   • No propagation expected")

    # Comparison
    logger.info("\n⚖️  COMPARISON:")
    if rollback:
        if not current_terms_from_action:
            logger.info("   ✅ MATCH: No terms from action found (rollback successful)")
        else:
            logger.info(
                f"   ❌ MISMATCH: Found {len(current_terms_from_action)} terms from action (rollback incomplete)"
            )
            logger.info(
                f"      → Remaining terms: {[x.urn for x in current_terms_from_action]}"
            )
    else:
        if expectation.propagation_found:
            expected_term_found = any(
                x.urn == expectation.propagated_term for x in current_terms_from_action
            )
            if expected_term_found:
                logger.info(
                    f"   ✅ MATCH: Expected term {expectation.propagated_term} found"
                )
            else:
                logger.info(
                    f"   ❌ MISMATCH: Expected term {expectation.propagated_term} NOT found"
                )
                logger.info(
                    f"      → Available terms from action: {[x.urn for x in current_terms_from_action]}"
                )
        else:
            if not current_terms_from_action:
                logger.info("   ✅ MATCH: No propagation expected and none found")
            else:
                logger.info(
                    f"   ❌ MISMATCH: No propagation expected but found {len(current_terms_from_action)} terms from action"
                )
                logger.info(
                    f"      → Unexpected terms: {[x.urn for x in current_terms_from_action]}"
                )

    logger.info(f"{'=' * 100}\n")


def check_expectation(
    graph_client: DataHubGraph,
    action_urn: str,
    expectation: PropagationExpectation,
    rollback: bool = False,
) -> None:
    """
    Raises assertion error if the expectation is not met.
    """
    _log_expectation_details(expectation, action_urn, rollback)

    if isinstance(expectation, TermPropagationExpectation):
        # Print entity state summary for easy comparison
        _print_entity_state_summary(graph_client, expectation, action_urn, rollback)

        field_urn = expectation.schema_field_urn
        dataset_urn = Urn.create_from_string(field_urn).entity_ids[0]
        field_path = Urn.create_from_string(field_urn).entity_ids[1]

        logger.info(f"Dataset URN: {dataset_urn}")
        logger.info(f"Field path: {field_path}")

        # propagated terms are stored in the editable schema metadata aspect
        editable_schema_metadata_aspect = graph_client.get_aspect(
            dataset_urn, EditableSchemaMetadataClass
        )

        if not editable_schema_metadata_aspect:
            logger.info("No editable schema metadata aspect found")
            if not rollback and expectation.propagation_found:
                logger.error(
                    "FAILURE: Expected propagation but no editable schema metadata aspect found"
                )
                raise AssertionError(
                    f"Expected propagation but no editable schema metadata aspect found for {dataset_urn}"
                )
            else:
                logger.info("SUCCESS: No editable schema metadata aspect as expected")
            return

        logger.info("Found editable schema metadata aspect")
        logger.info(
            f"Number of editable schema fields: {len(editable_schema_metadata_aspect.editableSchemaFieldInfo) if editable_schema_metadata_aspect.editableSchemaFieldInfo else 0}"
        )

        if editable_schema_metadata_aspect.editableSchemaFieldInfo:
            available_fields = [
                x.fieldPath
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
            ]
            logger.info(f"Available field paths: {available_fields}")

        field_info = next(
            (
                x
                for x in editable_schema_metadata_aspect.editableSchemaFieldInfo
                if x.fieldPath == field_path
            ),
            None,
        )

        if not field_info:
            logger.info(f"No field info found for {field_path}")
            if not rollback and expectation.propagation_found:
                logger.error(
                    f"FAILURE: Expected propagation but field {field_path} not found in editable schema"
                )
                raise AssertionError(
                    f"Expected propagation but field {field_path} not found in editable schema"
                )
            else:
                logger.info("SUCCESS: No field info as expected")
            return

        logger.info(f"Found field info for {field_path}")

        if not field_info.glossaryTerms:
            logger.info("No glossary terms found on field")
            if not rollback and expectation.propagation_found:
                logger.error(
                    "FAILURE: Expected propagation but field has no glossary terms"
                )
                raise AssertionError(
                    f"Expected propagation but field {field_path} has no glossary terms"
                )
            else:
                logger.info("SUCCESS: No glossary terms as expected")
            return

        logger.info("Found glossary terms on field")
        terms = field_info.glossaryTerms.terms
        logger.info(f"Total terms on field: {len(terms)}")

        terms_propagated_from_action = [
            x for x in terms if x.attribution and x.attribution.source == action_urn
        ]

        logger.info(f"All terms on field: {[x.urn for x in terms]}")
        logger.info(
            f"Terms with attribution: {[(x.urn, x.attribution.source if x.attribution else None) for x in terms]}"
        )
        logger.info(
            f"Terms propagated from action: {[x.urn for x in terms_propagated_from_action]}"
        )
        logger.info(f"Expected term: {expectation.propagated_term}")

        if terms_propagated_from_action:
            logger.info(
                f"Found {len(terms_propagated_from_action)} terms propagated from action"
            )
            if rollback:
                _check_propagated_terms_for_rollback(
                    terms_propagated_from_action, expectation
                )
            else:
                _check_propagated_terms_for_normal(
                    terms_propagated_from_action, expectation, action_urn
                )
        else:
            _handle_no_propagated_terms(expectation, action_urn, rollback, terms)

    logger.info(f"EXPECTATION CHECK COMPLETE\n{'=' * 80}")


def docs_propagation_bootstrap(
    auth_session: Any,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
) -> None:
    logger.info(f"\n{'=' * 100}")
    logger.info("STARTING BOOTSTRAP PHASE")
    logger.info(f"Action URN: {test_action_urn}")
    logger.info(f"Number of base graph MCPs: {len(scenario.base_graph)}")
    logger.info(f"Number of base expectations: {len(scenario.base_expectations)}")
    logger.info(f"{'=' * 100}")

    time_stages = {}

    time_stages["bootstrap_data_setup"] = time.time()
    logger.info("Setting up base graph data...")
    # Apply base graph
    for i, mcp in enumerate(scenario.base_graph):
        logger.info(
            f"Emitting MCP {i + 1}/{len(scenario.base_graph)}: {mcp.entityUrn} - {type(mcp.aspect).__name__}"
        )
        _log_term_additions_in_mcp(mcp, "Base Graph Setup")
        graph_client.emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)

    logger.info("Waiting for writes to sync...")
    wait_for_writes_to_sync()
    time_stages["bootstrap_data_setup"] = (
        time.time() - time_stages["bootstrap_data_setup"]
    )
    logger.info(
        f"Data setup completed in {time_stages['bootstrap_data_setup']:.2f} seconds"
    )

    time_stages["bootstrap_action"] = time.time()
    logger.info("Running bootstrap action...")
    bootstrap_action(
        test_action_urn,
        auth_session.integrations_service_url(),
        wait_for_completion=True,
    )
    time_stages["bootstrap_action"] = time.time() - time_stages["bootstrap_action"]
    logger.info(
        f"Bootstrap action completed in {time_stages['bootstrap_action']:.2f} seconds"
    )

    time_stages["bootstrap_check_expectations"] = time.time()
    logger.info(f"Checking {len(scenario.base_expectations)} base expectations...")
    # Check base expectations
    for i, expectation in enumerate(scenario.base_expectations):
        logger.info(f"\nChecking expectation {i + 1}/{len(scenario.base_expectations)}")
        try:
            check_expectation(graph_client, test_action_urn, expectation)
            logger.info(f"✓ Expectation {i + 1} passed")
        except Exception as e:
            logger.error(f"✗ Expectation {i + 1} failed: {e}")
            logger.error(f"Failed expectation details: {expectation}")
            raise
    time_stages["bootstrap_check_expectations"] = (
        time.time() - time_stages["bootstrap_check_expectations"]
    )
    logger.info(
        f"All expectations checked in {time_stages['bootstrap_check_expectations']:.2f} seconds"
    )
    logger.info("BOOTSTRAP PHASE COMPLETED SUCCESSFULLY")
    logger.info(f"{'=' * 100}")
    print(f"---------- Bootstrap time stages: {time_stages} ------------")


def docs_propagation_live(
    auth_session: Any,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
) -> None:
    logger.info(f"\n{'=' * 100}")
    logger.info("STARTING LIVE PHASE")
    logger.info(f"Action URN: {test_action_urn}")
    logger.info(f"Number of mutations: {len(scenario.mutations)}")
    logger.info(
        f"Number of post-mutation expectations: {len(scenario.post_mutation_expectations)}"
    )
    logger.info(f"{'=' * 100}")

    time_stages = {}
    time_stages["start_action"] = time.time()
    logger.info("Starting live action...")
    start_action(
        test_action_urn,
        auth_session.integrations_service_url(),
        wait_for_completion=True,
    )
    time_stages["start_action"] = time.time() - time_stages["start_action"]
    logger.info(f"Live action started in {time_stages['start_action']:.2f} seconds")

    try:
        # Apply mutations
        audit_timestamp = datetime.datetime.now(datetime.timezone.utc)
        time_stages["apply_mutations"] = time.time()
        logger.info(f"Applying {len(scenario.mutations)} mutations...")
        for i, mcp in enumerate(scenario.mutations):
            logger.info(
                f"Applying mutation {i + 1}/{len(scenario.mutations)}: {mcp.entityUrn} - {type(mcp.aspect).__name__}"
            )
            _log_term_additions_in_mcp(mcp, "Live Mutation")
            graph_client.emit(mcp, emit_mode=EmitMode.SYNC_PRIMARY)
        time_stages["apply_mutations"] = time.time() - time_stages["apply_mutations"]
        logger.info(
            f"Mutations applied in {time_stages['apply_mutations']:.2f} seconds"
        )

        time_stages["wait_for_action_has_processed_event"] = time.time()
        logger.info(
            f"Waiting for action to process events (audit timestamp: {audit_timestamp})..."
        )
        wait_until_action_has_processed_event(
            test_action_urn, auth_session.integrations_service_url(), audit_timestamp
        )
        time_stages["wait_for_action_has_processed_event"] = (
            time.time() - time_stages["wait_for_action_has_processed_event"]
        )
        logger.info(
            f"Action processed events in {time_stages['wait_for_action_has_processed_event']:.2f} seconds"
        )

        # Check post-mutation expectations
        logger.info(
            f"Checking {len(scenario.post_mutation_expectations)} post-mutation expectations..."
        )
        for i, expectation in enumerate(scenario.post_mutation_expectations):
            logger.info(
                f"\nChecking post-mutation expectation {i + 1}/{len(scenario.post_mutation_expectations)}"
            )
            try:
                check_expectation(graph_client, test_action_urn, expectation)
                logger.info(f"✓ Post-mutation expectation {i + 1} passed")
            except Exception as e:
                logger.error(f"✗ Post-mutation expectation {i + 1} failed: {e}")
                logger.error(f"Failed expectation details: {expectation}")
                raise

        logger.info("All post-mutation expectations passed")
        logger.info("LIVE PHASE COMPLETED SUCCESSFULLY")

    finally:
        time_stages["stop_action"] = time.time()
        logger.info("Stopping live action...")
        stop_action(test_action_urn, auth_session.integrations_service_url())
        time_stages["stop_action"] = time.time() - time_stages["stop_action"]
        logger.info(f"Live action stopped in {time_stages['stop_action']:.2f} seconds")
        logger.info(f"{'=' * 100}")
        print(f"---------- Live time stages: {time_stages} ------------")


def docs_propagation_rollback(
    auth_session: Any,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
    post_mutation: bool = False,
) -> None:
    logger.info(f"\n{'=' * 100}")
    logger.info("STARTING ROLLBACK PHASE")
    logger.info(f"Action URN: {test_action_urn}")
    logger.info(f"Post-mutation rollback: {post_mutation}")
    logger.info(f"{'=' * 100}")

    logger.info("Waiting for writes to sync before rollback...")
    wait_for_writes_to_sync()

    logger.info("Executing rollback action...")
    rollback_action(
        test_action_urn,
        auth_session.integrations_service_url(),
        wait_for_completion=True,
    )
    logger.info("Rollback action completed")

    # Check that all fields have no propagated documentation after rollback
    all_expectations = scenario.base_expectations
    if post_mutation:
        all_expectations += scenario.post_mutation_expectations

    logger.info(f"Checking {len(all_expectations)} rollback expectations...")
    logger.info(f"Base expectations: {len(scenario.base_expectations)}")
    if post_mutation:
        logger.info(
            f"Post-mutation expectations: {len(scenario.post_mutation_expectations)}"
        )

    for i, expectation in enumerate(all_expectations):
        logger.info(f"\nChecking rollback expectation {i + 1}/{len(all_expectations)}")
        try:
            check_expectation(graph_client, test_action_urn, expectation, rollback=True)
            logger.info(f"✓ Rollback expectation {i + 1} passed")
        except Exception as e:
            logger.error(f"✗ Rollback expectation {i + 1} failed: {e}")
            logger.error(f"Failed expectation details: {expectation}")
            raise

    logger.info("All rollback expectations passed")
    logger.info("ROLLBACK PHASE COMPLETED SUCCESSFULLY")
    logger.info(f"{'=' * 100}")
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
