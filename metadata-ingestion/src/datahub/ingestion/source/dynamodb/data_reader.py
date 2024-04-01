from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List

from datahub.ingestion.source.common.data_reader import DataReader

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

PAGE_SIZE = 100


class DynamoDBTableItemsReader(DataReader):
    """
    DynamoDB is a NoSQL database and may have different attributes (columns) present
    in different items (rows) of the table.
    """

    @staticmethod
    def create(client: "DynamoDBClient") -> "DynamoDBTableItemsReader":
        return DynamoDBTableItemsReader(client)

    def __init__(self, client: "DynamoDBClient") -> None:
        # The lifecycle of this client is managed externally
        self.client = client

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int, **kwargs: Any
    ) -> Dict[str, list]:
        """
        For dynamoDB, table_id should be in formation ( table-name ) or (region, table-name )
        """
        column_values: Dict[str, list] = defaultdict(list)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/scan.html
        paginator = self.client.get_paginator("scan")
        response_iterator = paginator.paginate(
            TableName=table_id[-1],
            PaginationConfig={
                "MaxItems": sample_size,
                "PageSize": PAGE_SIZE,
            },
        )
        # iterate through pagination result to retrieve items
        for page in response_iterator:
            items: List[Dict] = page["Items"]
            if len(items) > 0:
                for item in items:
                    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/scan.html#scan
                    # For an item (row), the key is the attribute name and the value is a dict with only one entry,
                    # whose key is the data type and value is the data
                    # for complex data types - L (list) or M (map) - we will recursively process the value into json-like form
                    for attribute_name, attribute_value in item.items():
                        column_values[attribute_name].append(
                            self._get_value(attribute_value)
                        )

        # Note: Consider including items configured via `include_table_item` in sample data ?

        return column_values

    def _get_value(self, attribute_value: Dict[str, Any]) -> Any:
        # Each attribute value is described as a name-value pair.
        # The name is the data type, and the value is the data itself.
        for data_type, value in attribute_value.items():
            if data_type == "L" and isinstance(value, list):
                return [self._get_value(e) for e in value]
            elif data_type == "M" and isinstance(value, dict):
                return {
                    nested_attr: self._get_value(nested_value)
                    for nested_attr, nested_value in value.items()
                }
            else:
                return value

    def close(self) -> None:
        pass
