import logging
from typing import Any, Dict, List, Optional

from gql import GraphQLRequest, gql

from datahub.api.graphql.base import BaseApi

logger = logging.getLogger(__name__)


class Assertion(BaseApi):
    ASSERTION_QUERY = """
query dataset($urn: String!, $start: Int, $count: Int, $status: AssertionRunStatus,$limit: Int, $startTimeMillis:Long, $endTimeMillis:Long, $filter:FilterInput) {
  dataset(urn: $urn) {
    assertions(start: $start, count: $count){
      __typename
      total
      assertions{
        __typename
        runEvents(status: $status, limit: $limit, startTimeMillis: $startTimeMillis, endTimeMillis: $endTimeMillis, filter: $filter) {
          total
          failed
          succeeded
          runEvents {
            __typename
            timestampMillis
            partitionSpec {
               __typename
              type
              partition
              timePartition {
                startTimeMillis
                durationMillis
              }
            }
            result {
              __typename
              type
              rowCount
              missingCount
              unexpectedCount
              actualAggValue
              externalUrl
            }
            assertionUrn
          }
        }
      }
    }
  }
}
"""

    ASSERTION_RESULT_FRAGMENT: str = """
fragment assertionResult on AssertionResult {
  type
  rowCount
  missingCount
  unexpectedCount
  actualAggValue
  externalUrl
  nativeResults {
    value
  }
  error {
    type
    properties {
      value
    }
  }
}"""

    RUN_ASSERTION_RESULT_FRAGMENT: str = """
fragment runAssertionResult on RunAssertionResult {
  assertion { urn }
  result { ... assertionResult }
}"""

    RUN_ASSERTION_MUTATION: str = f"""
${ASSERTION_RESULT_FRAGMENT}

mutation runAssertion($urn: String!, $saveResult: Boolean, $parameters: [StringMapEntryInput!], $async: Boolean) {
  runAssertion(urn: $urn, saveResult: $saveResult, parameters: $parameters, async: $async) {
    ... assertionResult
  }
}""" 

    RUN_ASSERTIONS_MUTATION: str = """
%s
%s
mutation runAssertions($urns: [String!]!, $saveResults: Boolean, $parameters: [StringMapEntryInput!], $async: Boolean) {
  runAssertions(urns: $urns, saveResults: $saveResults, parameters: $parameters, async: $async) {
    passingCount
    failingCount
    errorCount
    results { ... runAssertionResult }
  }
}""" % (ASSERTION_RESULT_FRAGMENT, RUN_ASSERTION_RESULT_FRAGMENT)

    RUN_ASSERTIONS_FOR_ASSET_MUTATION: str = """
%s
%s
mutation runAssertionsForAsset($urn: String!, $tagUrns: [String!], $parameters: [StringMapEntryInput!], $async: Boolean) {
  runAssertionsForAsset(urn: $urn, tagUrns: $tagUrns, parameters: $parameters, async: $async) {
    passingCount
    failingCount
    errorCount
    results { ... runAssertionResult }
  }
}""" % (ASSERTION_RESULT_FRAGMENT, RUN_ASSERTION_RESULT_FRAGMENT)

    @staticmethod
    def _build_string_map_entries(
        params: Optional[Dict[str, str]],
    ) -> Optional[List[Dict[str, str]]]:
        """Convert a dictionary to a list of StringMapEntryInput objects for GraphQL."""
        if not params:
            return None
        return [{"key": k, "value": v} for k, v in params.items()]

    def query_assertion(
        self,
        urn: str,
        status: Optional[str] = None,
        start_time_millis: Optional[int] = None,
        end_time_millis: Optional[int] = None,
        limit: Optional[int] = None,
        filter: Optional[Dict[str, Optional[str]]] = None,
    ) -> List[Dict[Any, Any]]:
        r"""
        Query assertions for a dataset.

        :param urn: The DataHub dataset unique identifier.
        :param status: The assertion status to filter for. Every status will be accepted if it is not set.
            See valid status at https://docs.datahub.com/docs/graphql/enums#assertionrunstatus
        :param start_time_millis: The start time in milliseconds from the assertions will be queried.
        :param end_time_millis: The end time in milliseconds until the assertions will be queried.
        :param filter: Additional key value filters which will be applied as AND query
        """

        result = self.client.execute(
            gql(Assertion.ASSERTION_QUERY),
            variable_values={
                "urn": urn,
                "filter": self.gen_filter(filter) if filter else None,
                "limit": limit,
                "status": status,
                "startTimeMillis": start_time_millis,
                "endTimeMillis": end_time_millis,
            },
        )

        assertions = []
        if "dataset" in result and "assertions" in result["dataset"]:
            assertions = result["dataset"]["assertions"]["assertions"]

        return assertions

    def run_assertion(
        self,
        urn: str,
        save_result: Optional[bool] = None,
        parameters: Optional[Dict[str, str]] = None,
        async_flag: Optional[bool] = None,
    ) -> Dict[str, Any]:
        r"""
        Run a single native assertion by URN.

        :param urn: The DataHub assertion unique identifier.
        :param save_result: If True, the result is stored for later viewing in the UI.
        :param parameters: Key/value pairs for injecting runtime parameters into SQL fragments.
        :param async_flag: If True, returns immediately with null result; poll run events later.
        """
        variable_values: Dict[str, Any] = {"urn": urn}

        if save_result is not None:
            variable_values["saveResult"] = save_result

        if parameters is not None:
            variable_values["parameters"] = Assertion._build_string_map_entries(
                parameters
            )

        if async_flag is not None:
            variable_values["async"] = async_flag

        request = GraphQLRequest(
            Assertion.RUN_ASSERTION_MUTATION, variable_values=variable_values
        )

        result = self.client.execute(request)

        return result["runAssertion"]

    def run_assertions(
        self,
        urns: List[str],
        save_results: Optional[bool] = None,
        parameters: Optional[Dict[str, str]] = None,
        async_flag: Optional[bool] = None,
    ) -> Dict[str, Any]:
        r"""
        Run multiple native assertions by URN.

        :param urns: List of DataHub assertion unique identifiers.
        :param save_results: If True, the results are stored for later viewing in the UI.
        :param parameters: Key/value pairs for injecting runtime parameters into SQL fragments.
        :param async_flag: If True, returns immediately with null results; poll run events later.
        """
        variable_values: Dict[str, Any] = {"urns": urns}

        if save_results is not None:
            variable_values["saveResults"] = save_results

        if parameters is not None:
            variable_values["parameters"] = Assertion._build_string_map_entries(
                parameters
            )

        if async_flag is not None:
            variable_values["async"] = async_flag

        request = GraphQLRequest(
            Assertion.RUN_ASSERTIONS_MUTATION, variable_values=variable_values
        )

        result = self.client.execute(request)

        return result["runAssertions"]

    def run_assertions_for_asset(
        self,
        urn: str,
        tag_urns: Optional[List[str]] = None,
        parameters: Optional[Dict[str, str]] = None,
        async_flag: Optional[bool] = None,
    ) -> Dict[str, Any]:
        r"""
        Run all native assertions attached to an asset, optionally filtered by tags.

        :param urn: The DataHub dataset unique identifier.
        :param tag_urns: Optional list of tag URNs to filter which assertions to run.
        :param parameters: Key/value pairs for injecting runtime parameters into SQL fragments.
        :param async_flag: If True, returns immediately with null results; poll run events later.
        """
        variable_values: Dict[str, Any] = {"urn": urn}

        if tag_urns is not None:
            variable_values["tagUrns"] = tag_urns

        if parameters is not None:
            variable_values["parameters"] = Assertion._build_string_map_entries(
                parameters
            )

        if async_flag is not None:
            variable_values["async"] = async_flag

        request = GraphQLRequest(
            Assertion.RUN_ASSERTIONS_FOR_ASSET_MUTATION,
            variable_values=variable_values,
        )

        result = self.client.execute(request)

        return result["runAssertionsForAsset"]
