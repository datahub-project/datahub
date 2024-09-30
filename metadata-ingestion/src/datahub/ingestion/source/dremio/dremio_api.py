"""This Module contains utility functions for dremio source"""

import json
import logging
import re
from datetime import datetime
from time import sleep
from typing import Dict, List, Optional
from urllib.parse import quote

import requests

from sqlglot import parse_one

from datahub.ingestion.source.dremio.dremio_sql_queries import DremioSQLQueries

logger = logging.getLogger(__name__)

_dml_queries = [
    "CREATE",
    "DELETE",
    "INSERT",
    "MERGE",
    "UPDATE",
]
_ddl_queries = [
    "ALTER BRANCH",
    "ALTER PIPE",
    "ALTER SOURCE",
    "ALTER TABLE",
    "ALTER TAG",
    "ALTER VIEW",
    "ANALYZE TABLE",
    "COPY INTO",
    "CREATE BRANCH",
    "CREATE FOLDER",
    "CREATE PIPE",
    "CREATE ROLE",
    "CREATE TABLE",
    "CREATE TAG",
    "CREATE USER",
    "CREATE VIEW",
    "DESCRIBE PIPE",
    "DESCRIBE TABLE",
    "DROP BRANCH",
    "DROP FOLDER",
    "DROP PIPE",
    "DROP ROLE",
    "DROP TABLE",
    "DROP TAG",
    "DROP USER",
    "DROP VIEW",
    "GRANT ROLE",
    "GRANT TO ROLE",
    "GRANT TO USER",
    "MERGE BRANCH",
    "REVOKE FROM ROLE",
    "REVOKE FROM USER",
    "REVOKE ROLE",
    "SHOW BRANCHES",
    "SHOW CREATE TABLE",
    "SHOW CREATE VIEW",
    "SHOW LOGS",
    "SHOW TABLES",
    "SHOW TAGS",
    "SHOW VIEWS",
    "USE",
    "VACUUM CATALOG",
    "VACUUM TABLE",
]
_select_queries = [
    "SELECT",
    "WITH",
]
_data_manipulation_queries = ["INSERT INTO", "MERGE INTO", "CREATE TABLE"]


class DremioQuery:
    job_id: str
    username: str
    submitted_ts: datetime
    query: str
    query_without_comments: str
    query_type: str
    query_subtype: str
    queried_datasets: List[str]
    affected_dataset: str

    def __init__(
        self,
        job_id: str,
        username: str,
        submitted_ts: str,
        query: str,
        queried_datasets: str,
        affected_datasets: Optional[str] = None,
    ):
        self.job_id = job_id
        self.username = username
        self.submitted_ts = self._get_submitted_ts(submitted_ts)
        self.query = self._get_query(query)
        self.query_without_comments = self.get_raw_query(query)
        self.query_type = self._get_query_type()
        self.query_subtype = self._get_query_subtype()
        self.queried_datasets = self._get_queried_datasets(queried_datasets)
        if affected_datasets:
            self.affected_dataset = affected_datasets
        else:
            self.affected_dataset = self._get_affected_tables()

    def get(self, attr):
        return getattr(self, attr, None)

    def _get_submitted_ts(self, timestamp: str) -> datetime:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    def _get_query(self, query: str) -> str:
        return str(query).replace("\'", "'")

    def _get_query_type(self) -> str:
        query_operator = re.split(
            pattern=r"\s+",
            string=self.query_without_comments.strip(),
            maxsplit=1,
        )[0]

        if query_operator in _select_queries:
            return "SELECT"
        if query_operator in _dml_queries:
            return "DML"
        return "DDL"

    def _get_query_subtype(self) -> str:
        for query_operator in _select_queries + _dml_queries + _ddl_queries:
            if self.query_without_comments.upper().startswith(query_operator):
                return query_operator
        return "UNDEFINED"

    def _get_queried_datasets(self, queried_datasets: str) -> List[str]:
        return list(
            {
                dataset.strip()
                for dataset in queried_datasets.strip("[]").split(",")
            }
        )

    def _get_affected_tables(self) -> str:
        # TO DO
        # for manipulation_operator in _data_manipulation_queries:
        #     if self.query_without_comments.upper().startswith(manipulation_operator):

        return ""

    @staticmethod
    def get_raw_query(sql_query: str) -> str:
        parsed = parse_one(sql_query)
        return parsed.sql(comments=False)


class DremioAPIOperations:
    dremio_url: str
    username: str
    _password: str
    is_PAT: bool
    headers: dict = {}

    _retry_count: int = 5
    _timeout: int = 10

    def __init__(self, connection_args: dict):
        self.set_connection_details(
            host=connection_args.get("hostname"),
            port=connection_args.get("port"),
            tls=connection_args.get("tls"),
        )
        self.base_url = (
            self.dremio_url + "/api/v3"
            if not connection_args.get("is_dremio_cloud", False)
            else f"https://api.{(connection_args.get('dremio_cloud_region') + '.') if connection_args.get('dremio_cloud_region') else ''}.dremio.cloud:443"
        )
        self.username = connection_args.get("username")
        self._password = connection_args.get("password")
        self._is_PAT = connection_args.get("authentication_method") == "PAT"
        self.is_dremio_cloud: bool = connection_args.get("is_dremio_cloud")

        self._verify: bool = (
            connection_args.get("tls")
            and not connection_args.get("disable_certificate_verification")
        )
        self.set_credentials()
        self.all_tables = []
        self.all_tables_and_columns = self._get_all_tables_and_columns()

    def set_connection_details(
        self,
        host: str,
        port: int,
        tls: bool,
    ) -> None:
        if tls:
            self.dremio_url = f"https://{host}:{str(port)}"
        else:
            self.dremio_url = f"http://{host}:{str(port)}"

    def set_credentials(self) -> None:
        if not self.base_url.endswith("dremio.cloud:443"):
            for retry in range(self._retry_count):
                logger.info("Dremio login attempt #{}".format(retry))
                if self._get_sticky_headers():
                    pass
                break
            else:
                raise "Credentials cannot be refreshed. Please check your username and password"

    def execute_get_request(self, url: str) -> Dict:
        """execute a get request on dremio"""
        response = requests.get(
            url=(self.base_url + url),
            headers=self.headers,
            verify=self._verify,
            timeout=self._timeout,
        )
        return response.json()

    def execute_post_request(self, url: str, data: str) -> Dict:
        """execute a get request on dremio"""
        response = requests.post(
            url=(self.base_url + url),
            headers=self.headers,
            data=data,
            verify=self._verify,
            timeout=self._timeout,
        )
        return response.json()

    def _execute_post_request_to_get_headers(self, headers: Dict, data: str) -> Dict:
        """execute a get request on dremio"""
        response = requests.post(
            url=f"{self.dremio_url}/apiv2/login",
            headers=headers,
            data=data,
            verify=self._verify,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()

    def _get_sticky_headers(self) -> None:
        """Get authentication token and headers"""
        if self._is_PAT:
            self.headers = {
                "content-type": "application/json",
                "authorization": f"BEARER {self._password}",
            }
        else:
            response = self._execute_post_request_to_get_headers(
                headers={"content-type": "application/json"},
                data=json.dumps({"userName": self.username, "password": self._password}),
            )
            self.headers = {
                "content-type": "application/json",
                "authorization": f"_dremio{response['token']}",
            }

    def execute_query(self, query: str) -> List[Dict]:
        """Execute sql query"""
        response = self.execute_post_request(
            url="/sql",
            data=json.dumps({"sql": query})
        )
        return self.fetch_results(response["id"])

    def get_job_status(self, job_id: str):
        """Check job status"""
        return self.execute_get_request(f"/job/{job_id}/")

    def get_job_result(self, job_id: str, offset: int = 0, limit: int = 500):
        """Get job results in batches"""
        return self.execute_get_request(
            f"/job/{job_id}/results?offset={offset}&limit={limit}",
        )

    def fetch_results(self, job_id: str):
        """Cumulate job results"""
        while self.get_job_status(job_id)["jobState"] != "COMPLETED":
            sleep(3)

        limit = 500
        offset = 0
        rows = []

        while True:
            result = self.get_job_result(job_id, offset, limit)
            rows.extend(result["rows"])

            offset = offset + limit
            if offset >= result["rowCount"]:
                break

        return rows

    def get_dataset_id(self, schema: str, dataset: str) -> str:
        schema_split = schema.split(".")
        schema_str = ""
        increment_val = 1
        last_val = 0
        while increment_val <= len(schema_split):
            url_encoded = quote(
                ".".join(
                    schema_split[
                        last_val:increment_val
                    ]
                ),
                safe=''
            )
            response = self.execute_get_request(
                f"/catalog/by-path/{schema_str}/{url_encoded}",
            )
            if response.get("errorMessage") is None:
                last_val = increment_val
                if len(schema_str) == 0:
                    schema_str = url_encoded
                else:
                    schema_str = f"{schema_str}/{url_encoded}"
            increment_val += 1

        return self.execute_get_request(
            f"/catalog/by-path/{schema_str}/{quote(dataset, safe='')}",
        ).get("id")

    def _get_all_tables_and_columns(self) -> Dict:
        all_tables_and_columns_dict: Dict = {}
        all_tables_and_columns = self.execute_query(
            DremioSQLQueries.QUERY_ALL_TABLES_AND_COLUMNS
        )

        schema_list = []
        for record in all_tables_and_columns:
            schema_list.append(
                record.get("TABLE_SCHEMA")
            )
        distinct_schemas = set(schema_list)

        distinct_schemas_dict_lookup = []

        for distinct_schema in distinct_schemas:
            distinct_schemas_dict_lookup.append(
                self.validate_schema_format(distinct_schema)
            )

        tables_list = []

        for record in all_tables_and_columns:
            for schemas in distinct_schemas_dict_lookup:
                if record.get("TABLE_SCHEMA") == schemas.get("original_path"):
                    tables_list.append(
                        '"' +
                        "\".\"".join(schemas.get("formatted_path")) +
                        "." +
                        record.get("TABLE_NAME") +
                        '"'
                    )

        self.all_tables = list(set(tables_list))

        for record in all_tables_and_columns:
            if (
                f"{record.get('TABLE_SCHEMA')}.{record.get('TABLE_NAME')}"
                not in all_tables_and_columns_dict
            ):
                all_tables_and_columns_dict[
                    f"{record.get('TABLE_SCHEMA')}.{record.get('TABLE_NAME')}"
                ] = []
            all_tables_and_columns_dict[
                f"{record.get('TABLE_SCHEMA')}.{record.get('TABLE_NAME')}"
            ].append(record)

        return all_tables_and_columns_dict

    def validate_schema_format(self, schema):

        if "." in schema:
            schema_path = self.execute_get_request(
                f"/catalog/{self.get_dataset_id(schema=schema, dataset='')}").get("path")
            return {"original_path": schema, "formatted_path": schema_path}
        return {"original_path": schema, "formatted_path": [schema]}

    def test_for_enterprise_edition(self):
        response = requests.get(
            url=f"{self.base_url}/catalog/privileges",
            headers=self.headers,
            verify=self._verify,
            timeout=self._timeout,
        )

        if response.status_code == 200:
            return True

        return False

    def get_view_parents(self, schema: str, dataset: str) -> List:
        parents_list = []

        if not self.is_dremio_cloud:
            dataset_id = self.get_dataset_id(schema=schema, dataset=dataset)
            parents = self.execute_get_request(
                f"/catalog/{dataset_id}/graph",
            ).get("parents")

            if not parents:
                return []

            for parent in parents:
                parents_list.append(
                    ".".join(parent.get("path")[1:])
                )

        return parents_list

    def extract_all_queries(self):
        queries: List[DremioQuery] = []

        for query in self.execute_query(query=DremioSQLQueries.QUERY_ALL_JOBS):
            queries.append(
                DremioQuery(
                    job_id=query.get("job_id"),
                    username=query.get("user_name"),
                    submitted_ts=query.get("submitted_ts"),
                    query=query.get("query"),
                    queried_datasets=query.get("queried_datasets"),
                )
            )

        return queries

    def get_sources(self) -> List[Dict]:
        """
        Query the Dremio sources API and return source information.
        """
        response = self.execute_get_request("/catalog")
        sources_and_spaces = response.get("data", [])

        # Process sources to include alias information
        processed_sources = []
        for source in sources_and_spaces:
            if source.get("containerType") == "SOURCE":
                source_config = self.execute_get_request(f"/catalog/{source.get('id')}")
                processed_source = {
                    "name": source_config.get("name"),
                    "type": source_config.get("type"),
                    "rootPath": source_config.get("config").get("rootPath"),
                    "databaseName": source_config.get("config").get("databaseName"),
                }
                processed_sources.append(processed_source)

        return processed_sources

    def get_source_by_id(self, source_id: str) -> Optional[Dict]:
        """
        Fetch source details by ID.
        """
        response = self.execute_get_request(f"/source/{source_id}")
        return response if response else None

    def get_source_for_dataset(self, schema: str, dataset: str) -> Optional[Dict]:
        """
        Get source information for a dataset given its schema and name.
        """
        dataset_id = self.get_dataset_id(schema, dataset)
        if not dataset_id:
            return None

        catalog_entry = self.execute_get_request(f"/catalog/{dataset_id}")
        if not catalog_entry or 'path' not in catalog_entry:
            return None

        source_id = catalog_entry['path'][0]
        return self.get_source_by_id(source_id)

    def retrieve_table_and_column_list(self) -> Dict:
        return self.all_tables_and_columns
