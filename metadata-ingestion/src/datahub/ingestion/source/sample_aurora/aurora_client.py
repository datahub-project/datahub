import logging
import requests

# from utils import get_secret

logger = logging.getLogger(__name__)

AURORA_BEARER_TOKEN = "<ENTER_AURORA_BEARER_TOKEN>"

AURORA_DATA_CATALOG_URL = "https://datastores.staging-zende.sk/api/v1/dataset-catalog"
AURORA_DATA_CATALOG_GROUP_URL = "https://datastores.staging-zende.sk/api/v1"

HEADERS = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {AURORA_BEARER_TOKEN}'
}


class AuroraClient:
    def __init__(self, token=AURORA_BEARER_TOKEN, aurora_data_catalog_url=AURORA_DATA_CATALOG_URL,
                 aurora_data_catalog_group_url=AURORA_DATA_CATALOG_GROUP_URL, headers=None):
        if headers is None:
            headers = HEADERS
        self._token = token
        self.aurora_data_catalog_url = aurora_data_catalog_url
        self.aurora_data_catalog_group_url = aurora_data_catalog_group_url
        self.headers = headers

    # Hiding the implementation of fetching the internal datasets
    def get_aurora_datasets(self):
        # Returns a List of Aurora Objects.
        # Each Aurora Object contains a unique key which is group name (folder name) & list of Schemas
        # Each of the schema needs to be uploaded to Acryl in the sructure:
        # folderName
        #   Schemas1
        #   Schemas2, and all the associated schemas related to the group

        return None

    # 2. Transform DataTypes Fields from Aurora to Acryl
    # Acryl Field Data Types: https://datahubproject.io/docs/graphql/enums/#schemafielddatatype
    # Acryl add Custom Fields: https://datahubproject.io/docs/graphql/enums/#schemafielddatatype
    @staticmethod
    def transform_aurora_field(field):
        type_map = {
            "bigint": "NumberType",
            "tinyint": "NumberType",
            "int": "NumberType",
            "decimal": "NumberType",
            "double": "NumberType",
            "float": "NumberType",
            "bit": "NumberType",
            "boolean": "BooleanType",
            "char": "StringType",
            "string": "StringType",
            "varchar": "StringType",
            "text": "StringType",
            "tinytext": "StringType",
            "mediumtext": "StringType",
            "longtext": "StringType",
            "year": "StringType",
            "set": "StringType",
            "enum": "EnumType",
            "binary": "BinaryType",
            "varbinary": "BinaryType",
            "blob": "BinaryType",
            "tinyblob": "BinaryType",
            "mediumblob": "BinaryType",
            "longblob": "BinaryType",
            "date": "DateType",
            "datetime": "DateType",
            "time": "DateType",
            "timestamp": "TimeType",
            "json": "MapType"
        }

        # How to add browsePaths here?
        transformed_field = {
            # "fieldPath": "ID" if field["name"] == "id" else field["name"], --> Inital assunmption was id can't be a key, but after testing it was an incorrect assumption
            "fieldPath": field["name"],
            "jsonPath": "null",
            "nullable": field["isNullable"],
            "isKey": field["isKey"],
            "description": field["description"],
            'position': field['position'],
            "type": {
                "type": {
                    "__type": type_map[field["type"].split('(')[0].lower()]
                }
            },
            "nativeDataType": field["type"],
            "recursive": False
        }

        return transformed_field
