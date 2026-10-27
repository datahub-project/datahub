import graphql
import pytest

from datahub.api.graphql.assertion import Assertion
from datahub.api.graphql.operation import Operation


@pytest.mark.parametrize(
    "query",
    [
        Assertion.ASSERTION_QUERY,
        Operation.REPORT_OPERATION_MUTATION,
        Operation.QUERY_OPERATIONS,
    ],
    ids=["assertion_query", "report_operation_mutation", "query_operations"],
)
def test_graphql_constants_parse(query):
    # The module-scope gql(...) calls used to fail the import on a malformed
    # query. execute_graphql does not: its minify step returns the query
    # unchanged on any parse error, so a typo would only surface against a live
    # GMS. graphql-core is a base-install dependency, so this runs in testQuick.
    graphql.parse(query)


def test_report_operation_sends_expected_variables(requests_mock):
    # Exercises the real execute_graphql rather than a MagicMock: a misspelled
    # keyword (variables=) would be accepted by a mock and raise in production.
    requests_mock.post(
        "http://gms/api/graphql", json={"data": {"reportOperation": "ok"}}
    )
    api = Operation(datahub_host="http://gms", datahub_token="tok")

    assert (
        api.report_operation(urn="urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)")
        == "ok"
    )

    sent = requests_mock.last_request.json()["variables"]
    assert sent["operationType"] == "UPDATE"
    assert sent["sourceType"] == "DATA_PROCESS"
    assert "partition" not in sent
