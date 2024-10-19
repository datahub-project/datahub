import datetime
import json
import logging
import os
import pathlib
import signal
import subprocess
import time
from typing import Dict, Iterable, Iterator, List, Optional, Set, Union

import datahub.metadata.schema_classes as models
import pydantic
import pytest
import yaml
from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_schema_field_urn,
    make_tag_urn,
    make_term_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryCredential
from datahub.metadata._schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    TagAssociationClass,
)
from datahub.metadata._urns.urn_defs import SchemaFieldUrn
from datahub.metadata.com.linkedin.pegasus2avro.schema import EditableSchemaFieldInfo
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from pydantic import BaseModel

from tests.consistency_utils import wait_for_writes_to_sync
from tests.integrations_service_utils import (
    get_live_logs,
    start_action,
    stop_action,
    wait_until_action_has_processed_event,
)
from tests.propagation.bigquery.util import BigqueryTestHelper
from tests.utils import (
    get_gms_url,
    get_integrations_service_url,
    wait_for_healthcheck_util,
)

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

SMOKE_TEST_PROJECT_ID = "harshal-playground-306419"
SMOKE_TEST_DATASET_ID = "propagation_test_smoke_test_" + str(int(time.time()))
SMOKE_TEST_TABLE_PREFIX = "test_table"
SMOKE_TEST_NUMBER_OF_TABLES = 2

pytestmark = pytest.mark.integration("integration")
pytest.skip("bigquery sync tests are disabled", allow_module_level=True)


def generate_bigquery_credentials() -> BigQueryCredential:
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    client_email = os.getenv("BIGQUERY_CLIENT_EMAIL")
    private_key = os.getenv("BIGQUERY_PRIVATE_KEY")
    private_key_id = os.getenv("BIGQUERY_PRIVATE_KEY_ID")
    client_id = os.getenv("BIGQUERY_CLIENT_ID")

    assert project_id
    assert client_email
    assert private_key
    assert private_key_id
    assert client_id

    return BigQueryCredential(
        project_id=project_id,
        client_email=client_email,
        private_key=private_key,
        private_key_id=private_key_id,
        client_id=client_id,
    )


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.fixture(scope="session")
def graph_client() -> DataHubGraph:
    return DataHubGraph(config=DatahubClientConfig(server=get_gms_url()))


@pytest.fixture(scope="session")
def bigquery_test_helper() -> BigqueryTestHelper:
    test_dir = pathlib.Path(__file__).parent.resolve()
    recipe_file = test_dir / "bq_doc_propagation_action_recipe.yaml"
    with open(recipe_file, "r") as f:
        recipe = yaml.safe_load(f)
        recipe["action"]["config"]["bigquery"][
            "credential"
        ] = generate_bigquery_credentials().dict()
        # raise Exception(recipe["action"]["config"]["bigquery"])
        bq_config = recipe["action"]["config"]["bigquery"]
        test_helper = BigqueryTestHelper(config=bq_config)
        return test_helper


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
    bigquery_test_helper: BigqueryTestHelper,
    graph_client: DataHubGraph,
    urns: Iterable[str],
    test_resources_dir: str,
    remove_actions: bool = True,
):
    bigquery_test_helper.delete_dataset(SMOKE_TEST_DATASET_ID, delete_contents=True)
    for urn in urns:
        logger.info(f"Deleting entity: {urn}")
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
                    stop_action(action_urn, get_integrations_service_url())
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
    graph_client: DataHubGraph,
    bigquery_test_helper: BigqueryTestHelper,
    test_resources_dir,
    test_action_urn,
) -> Iterator[None]:
    action_urn = test_action_urn
    recipe_file = test_resources_dir / "bigquery/bq_doc_propagation_action_recipe.yaml"

    with open(recipe_file, "r") as f:
        recipe = yaml.safe_load(f)
        recipe["action"]["config"]["bigquery"][
            "credential"
        ] = generate_bigquery_credentials().dict()
        recipe_json_str = json.dumps(recipe)

    cleanup(
        bigquery_test_helper,
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

    # Create test Dataset and tables in BigQuery
    test_dataset = bigquery_test_helper.create_dataset(SMOKE_TEST_DATASET_ID)
    bigquery_test_helper.create_table(
        test_dataset.dataset_id, SMOKE_TEST_TABLE_PREFIX, SMOKE_TEST_NUMBER_OF_TABLES
    )
    ingest_biquery_data(SMOKE_TEST_DATASET_ID)

    yield
    # we cleanup the action after all the tests have run
    cleanup(
        bigquery_test_helper,
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
    table_name: str  # the table that should be tested
    schema_field_urn: Optional[str] = None  # the field that should be tested


class TagPropagationExpectation(PropagationExpectation):
    tags: Dict[str, str] = {}  # the expected propagated tags


class TermPropagationExpectation(PropagationExpectation):
    propagated_term: Optional[str] = None  # the expected propagated term


class DescriptionPropagationExpectation(PropagationExpectation):
    description: Optional[str] = None  # the expected propagated term


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
            if expectation.schema_field_urn:
                urns.add(expectation.schema_field_urn)
        return list(urns)


def ingest_biquery_data(dataset: str):
    # Ingest data into BigQuery
    credentials = generate_bigquery_credentials()
    credential_dict = credentials.dict()
    pipeline = Pipeline.create(
        {
            "run_id": "bigquery_ingestion",
            "source": {
                "type": "bigquery",
                "config": {
                    "credential": credential_dict,
                    "project_id": "harshal-playground-306419",
                    "schema_pattern": {
                        "allow": [dataset],
                    },
                    "include_usage_statistics": False,
                    "include_table_lineage": False,
                    "profiling": {
                        "enabled": False,
                    },
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    pipeline.pretty_print_summary()
    logger.info("BigQuery data ingested successfully")


def table_description_sync(
    test_action_urn: str,
    bigquery_test_helper: BigqueryTestHelper,
    prefix: str = "table_description_sync",
) -> PropagationTestScenario:
    return PropagationTestScenario(
        base_graph=[],
        mutations=[
            MetadataChangeProposalWrapper(
                entityUrn=make_dataset_urn(
                    "bigquery",
                    f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
                    "PROD",
                ),
                aspect=models.EditableDatasetPropertiesClass(
                    description="test_table_description",
                ),
            ),
        ],
        base_expectations=[
            DescriptionPropagationExpectation(
                table_name=f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
                description="test_table_description",
            ),
        ],  # no expectations for this test
    )


def column_description_sync(
    test_action_urn: str,
    bigquery_test_helper: BigqueryTestHelper,
    prefix: str = "column_description_sync",
) -> PropagationTestScenario:
    dataset_urn = make_dataset_urn(
        "bigquery",
        f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
        "PROD",
    )

    return PropagationTestScenario(
        base_graph=[],
        mutations=[
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=[
                        EditableSchemaFieldInfo(
                            fieldPath="field_1",
                            description="Description for bigquery column",
                        )
                    ]
                ),
            )
        ],
        base_expectations=[
            DescriptionPropagationExpectation(
                table_name=f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
                schema_field_urn=make_schema_field_urn(dataset_urn, "field_1"),
                description="Description for bigquery column",
            ),
        ],  # no expectations for this test
    )


def table_tag_sync(
    test_action_urn: str,
    bigquery_test_helper: BigqueryTestHelper,
    prefix: str = "column_description_sync",
) -> PropagationTestScenario:
    tag_to_add = make_tag_urn("purchase")
    tag_association_to_add = TagAssociationClass(tag=tag_to_add)
    global_tags = GlobalTagsClass(tags=[tag_association_to_add])

    return PropagationTestScenario(
        base_graph=[],
        mutations=[
            MetadataChangeProposalWrapper(
                entityUrn=make_dataset_urn(
                    "bigquery",
                    f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
                    "PROD",
                ),
                aspect=global_tags,
            ),
        ],
        base_expectations=[
            TagPropagationExpectation(
                table_name=f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
                tags={
                    "purchase": "urn_li_encoded_tag_ovzg4otmne5hiylhhjyhk4tdnbqxgzi_"
                },
            ),
        ],  # no expectations for this test
    )


def column_glossary_term_to_policytag_sync(
    test_action_urn: str,
    bigquery_test_helper: BigqueryTestHelper,
    prefix: str = "column_description_sync",
) -> PropagationTestScenario:
    dataset_urn = make_dataset_urn(
        "bigquery",
        f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
        "PROD",
    )
    term_urn = make_term_urn("rateofreturn")
    term_properties_aspect = GlossaryTermInfoClass(
        definition="A rate of return (RoR) is the net gain or loss of an investment over a specified time period.",
        name="Rate of Return",
        termSource="",
    )

    glossary_term_mcp = MetadataChangeProposalWrapper(
        entityUrn=term_urn,
        aspect=term_properties_aspect,
    )

    mutation_mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=EditableSchemaMetadataClass(
            editableSchemaFieldInfo=[
                EditableSchemaFieldInfoClass(
                    fieldPath="field_1",
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
                                urn="urn:li:glossaryTerm:rateofreturn",
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

    return PropagationTestScenario(
        base_graph=[glossary_term_mcp],
        mutations=[mutation_mcp],
        base_expectations=[
            TermPropagationExpectation(
                table_name=f"{SMOKE_TEST_PROJECT_ID}.{SMOKE_TEST_DATASET_ID}.{SMOKE_TEST_TABLE_PREFIX}_0",
                schema_field_urn=make_schema_field_urn(dataset_urn, "field_1"),
                propagated_term="urn_li_glossaryTerm_rateofreturn",
            ),
        ],  # no expectations for this test
    )


@pytest.fixture(
    params=[
        (
            "column_glossary_term_to_policytag_sync",
            column_glossary_term_to_policytag_sync,
        ),
        ("table description sync", table_description_sync),
        ("column_description_sync", column_description_sync),
        ("table_tag_sync", table_tag_sync),
    ],
    ids=lambda x: x[0],
)
def graph_lineage_data(
    request, bigquery_test_helper, test_action_urn, create_test_action
):
    _, graph_func = request.param
    return graph_func(test_action_urn, bigquery_test_helper)


def test_main_loop(
    bigquery_test_helper: BigqueryTestHelper,
    graph_client: DataHubGraph,
    graph_lineage_data: PropagationTestScenario,
    test_action_urn: str,
    pytestconfig,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/propagation/"
    time_stages = {}

    try:
        # Finally test live
        time_stages["live"] = time.time()
        docs_propagation_live(
            bigquery_test_helper, graph_client, graph_lineage_data, test_action_urn
        )
        time_stages["live"] = time.time() - time_stages["live"]

    finally:
        time_stages["post_test_cleanup"] = time.time()
        cleanup(
            bigquery_test_helper=bigquery_test_helper,
            graph_client=graph_client,
            urns=graph_lineage_data.get_urns(),
            test_resources_dir=test_resources_dir,
            remove_actions=False,
        )
        time_stages["post_test_cleanup"] = (
            time.time() - time_stages["post_test_cleanup"]
        )
        print(f"---------- Time stages: {time_stages} ------------")


def docs_propagation_live(
    bigquery_test_helper: BigqueryTestHelper,
    graph_client: DataHubGraph,
    scenario: PropagationTestScenario,
    test_action_urn: str,
) -> None:
    time_stages = {}
    time_stages["start_action"] = time.time()
    start_action(
        test_action_urn, get_integrations_service_url(), wait_for_completion=True
    )
    time_stages["start_action"] = time.time() - time_stages["start_action"]

    try:
        time_stages["bootstrap_data_setup"] = time.time()
        # Apply base graph
        for mcp in scenario.base_graph:
            graph_client.emit(mcp, async_flag=False)

        wait_for_writes_to_sync()
        time_stages["bootstrap_data_setup"] = (
            time.time() - time_stages["bootstrap_data_setup"]
        )

        # Apply mutations
        audit_timestamp = datetime.datetime.now(datetime.timezone.utc)
        time_stages["apply_mutations"] = time.time()
        for mcp in scenario.mutations:
            graph_client.emit(mcp)
        time_stages["apply_mutations"] = time.time() - time_stages["apply_mutations"]
        time_stages["wait_for_action_has_processed_event"] = time.time()
        wait_until_action_has_processed_event(
            test_action_urn, get_integrations_service_url(), audit_timestamp
        )
        time_stages["wait_for_action_has_processed_event"] = (
            time.time() - time_stages["wait_for_action_has_processed_event"]
        )

        # Check base expectations
        for expectation in scenario.base_expectations:
            try:
                check_expectation(
                    graph_client, bigquery_test_helper, test_action_urn, expectation
                )
            except Exception:
                logger.debug(f"Failed expectation: {expectation}")
                print(
                    f"Action logs: {get_live_logs(test_action_urn, get_integrations_service_url())}"
                )
                raise

    finally:
        time_stages["stop_action"] = time.time()
        stop_action(test_action_urn, get_integrations_service_url())
        time_stages["stop_action"] = time.time() - time_stages["stop_action"]
        print(f"---------- Live time stages: {time_stages} ------------")


def check_expectation(
    graph_client: DataHubGraph,
    bigquery_helper: BigqueryTestHelper,
    action_urn: str,
    expectation: PropagationExpectation,
) -> None:
    """
    Raises assertion error if the expectation is not met.
    """

    if isinstance(expectation, TermPropagationExpectation):
        assert expectation.schema_field_urn
        urn = SchemaFieldUrn.from_string(expectation.schema_field_urn)
        bigquery_helper.expect_policy_tags(
            expectation.table_name,
            urn.field_path,
            [expectation.propagated_term] if expectation.propagated_term else [],
        )
    elif isinstance(expectation, DescriptionPropagationExpectation):
        if expectation.schema_field_urn:
            assert expectation.description
            urn = SchemaFieldUrn.from_string(expectation.schema_field_urn)
            bigquery_helper.expect_column_description(
                expectation.table_name, urn.field_path, expectation.description
            )
        else:
            assert expectation.description
            bigquery_helper.expect_table_description(
                expectation.table_name, expectation.description
            )
    elif isinstance(expectation, TagPropagationExpectation):
        bigquery_helper.expect_table_labels(expectation.table_name, expectation.tags)
