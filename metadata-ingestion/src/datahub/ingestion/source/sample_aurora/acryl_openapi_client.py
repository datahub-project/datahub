import datetime
import json

import logging

from requests_session_wrapper import RequestWrapper

logger = logging.getLogger(__name__)


class AcrylOpenapiClient:
    def __init__(self, environment, auth, host, timeout=1800):
        self.environment = environment
        self.auth = auth
        self.host = host
        self.timeout = timeout

    # Using Open API's Entities (/entities) endpoint:
    # https://datahubproject.io/docs/api/openapi/openapi-usage-guide/#post
    def add_dataset(self, endpoint: str, platform_name: str, dataset_name: str, schema: list) -> int:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.auth}"
        }
        current_timestamp = datetime.datetime.now().timestamp()
        data = [
            {
                "aspect":
                    {
                        "__type": "SchemaMetadata",
                        "schemaName": f"{dataset_name}",
                        "platform": f"urn:li:dataPlatform:{platform_name}",
                        "platformSchema": {
                            "__type": "OtherSchema",
                            "rawSchema": "schema"
                        },
                        "version": 0,
                        "created": {
                            "time": current_timestamp,
                            "actor": "urn:li:corpuser:etl",
                            "impersonator": "urn:li:corpuser:datacatalog"
                        },
                        "lastModified": {
                            "time": current_timestamp,
                            "actor": "urn:li:corpuser:etl",
                            "impersonator": "urn:li:corpuser:datacatalog"
                        },
                        "hash": "",
                        "fields": schema
                    },
                "entityType": "dataset",
                "entityUrn": f"urn:li:dataset:(urn:li:dataPlatform:{platform_name},{dataset_name},{self.environment})"
            }
        ]
        request_url = self.host + endpoint
        session = RequestWrapper(status_forcelist=[502, 503, 504], method_whitelist=["POST"]).create_session()

        print(f"Adding {dataset_name} dataset to Acryl!!")

        response = session.post(url=request_url, headers=headers, data=json.dumps(data), timeout=self.timeout)

        if dataset_name == "usageservice.schema_migrations":
            print(f"Success response for dataset_name is: {dataset_name} ")
            print(f"Success Response.schema is: {schema} ")
            print(f"Success response.url is: {response.url} ")

        if response.status_code == 400:
            # Provided schemas which got 400 as the response code in this doc:
            # https://docs.google.com/document/d/1dnB2T_ho-gLGQNAsSRKpl-Fj_lYdQ6M3bD_kKp5przA/edit
            print(f"Errored Response.schema.fieldPath is: {schema[0]['fieldPath']} ")
            print(f"Errored Response.schema: {schema} ")

        return response.status_code
