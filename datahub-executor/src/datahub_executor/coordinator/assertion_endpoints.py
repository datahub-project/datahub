import logging

import fastapi

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.client.fetcher.monitors.graphql.query import (
    GRAPHQL_GET_ASSERTION_QUERY,
    GRAPHQL_GET_DATASET_QUERY,
)
from datahub_executor.common.graph import DataHubAssertionGraph
from datahub_executor.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
)
from datahub_executor.common.types import Assertion

from .assertion_handlers import (
    handle_post_evaluate_assertion,
    handle_post_evaluate_assertion_urn,
)
from .types import (
    AssertionResultSchema,
    EvaluateAssertionInputSchema,
    EvaluateAssertionUrnInputSchema,
)

logger = logging.getLogger(__name__)

assertions_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


def _evaluation_dependency_setup() -> tuple[DataHubAssertionGraph, AssertionEngine]:
    graph = create_datahub_graph()
    engine = create_assertion_engine(graph)
    return graph, engine


@assertions_router.post("/evaluate_assertion")
def evaluate_assertion(
    assertion_input: EvaluateAssertionInputSchema,
) -> AssertionResultSchema:
    # setup the data sources, and dependencies for handler.
    graph, engine = _evaluation_dependency_setup()

    result = graph.execute_graphql(
        GRAPHQL_GET_DATASET_QUERY,
        variables={"datasetUrn": assertion_input.entityUrn},
    )
    table_name = None
    qualified_name = None

    if dataset := result.get("dataset", None):
        table_name = (
            dataset["properties"]["name"]
            if "properties" in dataset
            and dataset["properties"] is not None
            and "name" in dataset["properties"]
            else None
        )
        qualified_name = (
            dataset["properties"]["qualifiedName"]
            if "properties" in dataset
            and dataset["properties"] is not None
            and "qualifiedName" in dataset["properties"]
            else None
        )

    # call API handler
    return handle_post_evaluate_assertion(
        assertion_input, engine, table_name, qualified_name
    )


@assertions_router.post("/evaluate_assertion_urn")
def evaluate_assertion_urn(
    assertion_urn_input: EvaluateAssertionUrnInputSchema,
) -> AssertionResultSchema:
    # setup the data sources, and dependencies for handler.
    graph, engine = _evaluation_dependency_setup()

    result = graph.execute_graphql(
        GRAPHQL_GET_ASSERTION_QUERY,
        variables={"assertionUrn": assertion_urn_input.assertionUrn},
    )

    try:
        assertion = Assertion.parse_obj(result["assertion"])
    except Exception as e:
        # on non-existent assertion, graphql is still returning a result in result["assertion"] but the parsing fails
        logger.warning(e)
        raise fastapi.HTTPException(status_code=404)

    return handle_post_evaluate_assertion_urn(assertion, assertion_urn_input, engine)


if __name__ == "__main__":
    # For development only
    pass
