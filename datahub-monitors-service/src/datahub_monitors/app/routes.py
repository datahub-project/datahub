import logging

import fastapi

from datahub_monitors.app.monitors import (
    create_assertion_engine,
    create_assertion_graph,
)
from datahub_monitors.assertion.engine.engine import AssertionEngine
from datahub_monitors.graph import DataHubAssertionGraph
from datahub_monitors.graphql.query import (
    GRAPHQL_GET_ASSERTION_QUERY,
    GRAPHQL_GET_DATASET_QUERY,
)
from datahub_monitors.types import Assertion

from .assertions.handlers import (
    handle_post_evaluate_assertion,
    handle_post_evaluate_assertion_urn,
)
from .schemas import (
    AssertionResultSchema,
    EvaluateAssertionInputSchema,
    EvaluateAssertionUrnInputSchema,
)

logger = logging.getLogger(__name__)

internal_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


def _evaluation_dependency_setup() -> tuple[DataHubAssertionGraph, AssertionEngine]:
    graph = create_assertion_graph()
    engine = create_assertion_engine(graph)
    return graph, engine


@internal_router.post("/evaluate_assertion")
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
            if "properties" in dataset and "name" in dataset["properties"]
            else None
        )
        qualified_name = (
            dataset["properties"]["qualifiedName"]
            if "properties" in dataset and "qualifiedName" in dataset["properties"]
            else None
        )

    # call API handler
    return handle_post_evaluate_assertion(
        assertion_input, engine, table_name, qualified_name
    )


@internal_router.post("/evaluate_assertion_urn")
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
