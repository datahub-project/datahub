import logging
from typing import Any, Dict, List, Optional

from gql import gql

from datahub.api.graphql.base import BaseApi

logger = logging.getLogger(__name__)


class Operation(BaseApi):
    REPORT_OPERATION_MUTATION: str = """
mutation reportOperation($urn: String!, $sourceType: OperationSourceType!, $operationType: OperationType!, $partition: String, $numAffectedRows: Long, $customProperties: [StringMapEntryInput!]) {
  reportOperation(input: {
    urn: $urn
    sourceType: $sourceType
    operationType: $operationType
    partition: $partition
    numAffectedRows: $numAffectedRows
    customProperties: $customProperties
  })
}"""

    QUERY_OPERATIONS: str = """
    query dataset($urn: String!, $startTimeMillis: Long, $endTimeMillis: Long, $limit: Int, $filter:FilterInput) {
  dataset(urn: $urn) {
    urn
    operations (startTimeMillis: $startTimeMillis, endTimeMillis: $endTimeMillis, limit: $limit, filter: $filter) {
      __typename
      actor
      operationType
      sourceType
      numAffectedRows
      partition
      timestampMillis
      lastUpdatedTimestamp
      customProperties {
        key
        value
      }
    }
  }
}"""

    def report_operation(
        self,
        urn: str,
        source_type: str = "DATA_PROCESS",
        operation_type: str = "UPDATE",
        partition: Optional[str] = None,
        num_affected_rows: int = 0,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> str:
        r"""
        Report operation metadata for a dataset.
        :param source_type: The source type to filter on. If not set it will accept any source type.
            Default value: DATA_PROCESS
            See valid types here: https://docs.datahub.com/docs/graphql/enums#operationsourcetype
        :param operation_type: The operation type to filter on. If not set it will accept any source type.
            Default value: "UPDATE"
            See valid types here: https://docs.datahub.com/docs/graphql/enums/#operationtype
        :param partition: The partition to set the operation.
        :param num_affected_rows: The number of rows affected by this operation.
        :param custom_properties: Key/value pair of custom propertis
        """
        variable_values = {
            "urn": urn,
            "sourceType": source_type,
            "operationType": operation_type,
            "numAffectedRows": num_affected_rows,
        }

        if partition is not None:
            variable_values["partition"] = partition

        if num_affected_rows is not None:
            variable_values["numAffectedRows"] = num_affected_rows

        if custom_properties is not None:
            variable_values["customProperties"] = custom_properties

        result = self.client.execute(
            gql(Operation.REPORT_OPERATION_MUTATION), variable_values
        )

        return result["reportOperation"]

    def query_operations(
        self,
        urn: str,
        start_time_millis: Optional[int] = None,
        end_time_millis: Optional[int] = None,
        limit: Optional[int] = None,
        source_type: Optional[str] = None,
        operation_type: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> List[Dict[Any, Any]]:
        r"""
        Query operations for a dataset.

        :param urn: The DataHub dataset unique identifier.
        :param start_time_millis: The start time in milliseconds from the operations will be queried.
        :param end_time_millis: The end time in milliseconds until the operations will be queried.
        :param limit: The maximum number of items to return.
        :param source_type: The source type to filter on. If not set it will accept any source type.
            See valid types here: https://docs.datahub.com/docs/graphql/enums#operationsourcetype
        :param operation_type: The operation type to filter on. If not set it will accept any source type.
            See valid types here: https://docs.datahub.com/docs/graphql/enums#operationsourcetype
        :param partition: The partition to check the operation.
        """

        result = self.client.execute(
            gql(Operation.QUERY_OPERATIONS),
            variable_values={
                "urn": urn,
                "startTimeMillis": start_time_millis,
                "end_time_millis": end_time_millis,
                "limit": limit,
                "filter": self.gen_filter(
                    {
                        "sourceType": source_type,
                        "operationType": operation_type,
                        "partition": partition,
                    }
                ),
            },
        )
        if "dataset" in result and "operations" in result["dataset"]:
            operations = []
            if source_type is not None:
                for operation in result["dataset"]["operations"]:
                    if operation["sourceType"] == source_type:
                        operations.append(operation)
            else:
                operations = result["dataset"]["operations"]

            return operations
        return []
