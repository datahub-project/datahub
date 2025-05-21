import logging
from typing import Any, Dict, List, Optional

from gql import gql

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
