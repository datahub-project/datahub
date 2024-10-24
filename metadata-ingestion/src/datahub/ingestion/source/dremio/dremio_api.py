"""This Module contains utility functions for dremio source"""
import concurrent.futures
import warnings
from time import sleep, time
from collections import deque, defaultdict
from enum import Enum

from itertools import product

from dataclasses import dataclass

import uuid

import json
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Deque, Union
from urllib.parse import quote
from urllib3.exceptions import InsecureRequestWarning

import requests

from sqlglot import parse_one

from datahub.emitter.mce_builder import (
    make_term_urn,
)
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import DremioToDataHubSourceTypeMapping
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


@dataclass
class DremioDatasetColumn:
    name: str
    ordinal_position: int
    is_nullable: bool
    data_type: str
    column_size: int

class DremioEdition(Enum):
    CLOUD = "CLOUD"
    ENTERPRISE = "ENTERPRISE"
    COMMUNITY = "COMMUNITY"

class DremioDatasetType(Enum):
    VIEW = "View"
    TABLE = "Table"

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

    def get_raw_query(self, sql_query: str) -> str:
        parsed = parse_one(sql_query)
        return parsed.sql(comments=False)

class DremioAPIOperations:
    dremio_url: str
    username: str
    edition: DremioEdition
    allow_schema_pattern: List[str]
    allow_dataset_pattern: List[str]
    deny_schema_pattern: List[str]
    deny_dataset_pattern: List[str]

    _max_workers: int
    _retry_count: int = 5
    _timeout: int = 1800

    def __init__(self, connection_args: DremioSourceConfig):
        self.dremio_to_datahub_source_mapper = DremioToDataHubSourceTypeMapping()
        self.allow_schema_pattern = connection_args.schema_pattern.allow
        self.allow_dataset_pattern = connection_args.dataset_pattern.allow
        self.deny_schema_pattern = connection_args.schema_pattern.deny
        self.deny_dataset_pattern = connection_args.dataset_pattern.deny
        self._password = connection_args.password
        self._max_workers = connection_args.max_workers

        if connection_args.is_dremio_cloud:
            self.is_dremio_cloud = True
            self._is_PAT = True
            self._verify = True
            self.edition = DremioEdition.CLOUD

            cloud_region: str  = connection_args.dremio_cloud_region
            if cloud_region != "us":
                self.base_url: str  = f"https://api.{cloud_region}.dremio.cloud:443/v0"
            else:
                self.base_url: str  = "https://api.dremio.cloud:443/v0"

        else:
            host: str = connection_args.hostname
            port: int = connection_args.port
            tls: bool = connection_args.tls

            self.username: str = connection_args.username

            if tls:
                self.base_url: str = f"https://{host}:{str(port)}/api/v3"
            else:
                self.base_url: str = f"http://{host}:{str(port)}/api/v3"

            self.is_dremio_cloud: bool = False
            self._is_PAT: bool = connection_args.authentication_method == "PAT"

            self.set_connection_details(
                host=host,
                port=port,
                tls=tls,
            )

            self._verify: bool = (
                    connection_args.tls
                    and not connection_args.disable_certificate_verification
            )

            if not self._verify:
                warnings.simplefilter('ignore', InsecureRequestWarning)

            self.set_credentials()

            if self.test_for_enterprise_edition():
                self.edition = DremioEdition.ENTERPRISE
            else:
                self.edition = DremioEdition.COMMUNITY

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
                if self.__get_sticky_headers():
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

    def __execute_post_request_to_get_headers(self, headers: Dict, data: str) -> Dict:
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

    def __get_sticky_headers(self) -> None:
        """Get authentication token and headers"""
        if self._is_PAT:
            self.headers = {
                "content-type": "application/json",
                "authorization": f"BEARER {self._password}",
            }
        else:
            response = self.__execute_post_request_to_get_headers(
                headers={"content-type": "application/json"},
                data=json.dumps({"userName": self.username, "password": self._password}),
            )
            self.headers = {
                "content-type": "application/json",
                "authorization": f"_dremio{response['token']}",
            }

    def execute_query(self, query: str) -> List[Dict]:
        """Execute SQL query with timeout and error handling"""
        try:
            response = self.execute_post_request(
                url="/sql",
                data=json.dumps({"sql": query})
            )

            if 'errorMessage' in response:
                raise RuntimeError(f"SQL Error: {response['errorMessage']}")

            job_id = response["id"]

            return self.fetch_results(job_id)

        except requests.RequestException as e:
            raise RuntimeError(f"Error executing query: {str(e)}")

    def fetch_results(self, job_id: str) -> List[Dict]:
        """Fetch job results with status checking"""
        start_time = time()
        while True:
            status = self.get_job_status(job_id)
            if status["jobState"] == "COMPLETED":
                break
            elif status["jobState"] == "FAILED":
                error_message = status.get("errorMessage", "Unknown error")
                raise RuntimeError(f"Query failed: {error_message}")
            elif status["jobState"] == "CANCELED":
                raise RuntimeError("Query was canceled")

            if time() - start_time > self._timeout:
                self.cancel_query(job_id)
                raise TimeoutError("Query execution timed out while fetching results")

            sleep(3)

        return self._fetch_all_results(job_id)

    def _fetch_all_results(self, job_id: str) -> List[Dict]:
        """Fetch all results for a completed job"""
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

    def cancel_query(self, job_id: str) -> None:
        """Cancel a running query"""
        try:
            self.execute_post_request(
                url=f"/job/{job_id}/cancel",
                data=json.dumps({})
            )
        except Exception as e:
            logger.error(f"Failed to cancel query {job_id}: {str(e)}")

    def get_job_status(self, job_id: str):
        """Check job status"""
        return self.execute_get_request(
            url=f"/job/{job_id}/",
        )

    def get_job_result(self, job_id: str, offset: int = 0, limit: int = 500):
        """Get job results in batches"""
        return self.execute_get_request(
            url=f"/job/{job_id}/results?offset={offset}&limit={limit}",
        )

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
                url=f"/catalog/by-path/{schema_str}/{url_encoded}",
            )
            if response.get("errorMessage") is None:
                last_val = increment_val
                if len(schema_str) == 0:
                    schema_str = url_encoded
                else:
                    schema_str = f"{schema_str}/{url_encoded}"
            increment_val += 1

        return self.execute_get_request(
            url=f"/catalog/by-path/{schema_str}/{quote(dataset, safe='')}",
        ).get("id")

    def community_get_formatted_tables(
            self,
            tables_and_columns: List[Dict[str, Any]]
    ):
        schema_list = []
        schema_dict_lookup = []
        dataset_list = []
        column_dictionary: Dict[str, List[DremioDatasetColumn]] = defaultdict(list)

        for record in tables_and_columns:

            column_dictionary[record.get('FULL_TABLE_PATH')].append(
                DremioDatasetColumn(
                    name=record.get("COLUMN_NAME"),
                    ordinal_position=record.get("ORDINAL_POSITION"),
                    is_nullable=record.get("IS_NULLABLE"),
                    data_type=record.get("DATA_TYPE"),
                    column_size=record.get("COLUMN_SIZE"),
                )
            )

            if record.get("TABLE_SCHEMA") not in schema_list:
                schema_list.append(
                    record.get("TABLE_SCHEMA")
                )

        distinct_tables_list = list(
            {
                tuple(
                    dictionary[key] for key in (
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "FULL_TABLE_PATH",
                        "VIEW_DEFINITION",
                    ) if key in dictionary
                ): dictionary for dictionary in tables_and_columns
            }.values())


        for schema in schema_list:
            schema_dict_lookup.append(
                self.validate_schema_format(schema)
            )

        for table, schemas in product(distinct_tables_list, schema_dict_lookup):
            if table.get("TABLE_SCHEMA") == schemas.get("original_path"):
                dataset_list.append(
                    {
                        "TABLE_SCHEMA": "[" + ", ".join(schemas.get("formatted_path") + [
                            table.get("TABLE_NAME")]) + "]",
                        "TABLE_NAME": table.get("TABLE_NAME"),
                        "COLUMNS": column_dictionary.get(
                            table.get('FULL_TABLE_PATH')
                        ),

                        "VIEW_DEFINITION": table.get("VIEW_DEFINITION"),
                        "RESOURCE_ID": self.get_dataset_id(
                            schema=".".join(schemas.get("formatted_path")),
                            dataset=table.get("TABLE_NAME"),
                        ),

                        "LOCATION_ID": self.get_dataset_id(
                            schema=".".join(schemas.get("formatted_path")),
                            dataset="",
                        ),
                    }
                )

        return dataset_list

    def get_all_tables_and_columns(self, containers: Deque) -> List[Dict]:
        if self.edition == DremioEdition.ENTERPRISE:
            query_template = DremioSQLQueries.QUERY_DATASETS_EE
        elif self.edition == DremioEdition.CLOUD:
            query_template = DremioSQLQueries.QUERY_DATASETS_CLOUD
        else:
            query_template = DremioSQLQueries.QUERY_DATASETS_CE

        def get_pattern_condition(
                patterns: Union[str, List[str]],
                field: str,
                allow: bool = True
        ) -> str:
            if not patterns:
                return ""

            if isinstance(patterns, str):
                patterns = [patterns.upper()]

            if ".*" in patterns and allow:
                return ""

            patterns = [p.upper() for p in patterns if p != ".*"]
            if not patterns:
                return ""

            operator = "REGEXP_LIKE" if allow else "NOT REGEXP_LIKE"
            pattern_str = "|".join(f"({p})" for p in patterns)
            return f"AND {operator}({field}, '{pattern_str}')"

        schema_field = (
            "CONCAT(REPLACE(REPLACE(REPLACE(UPPER(TABLE_SCHEMA), ', ', '.'), '[', ''), ']', ''))"
        )
        table_field = "UPPER(TABLE_NAME)"

        schema_condition = get_pattern_condition(self.allow_schema_pattern, schema_field)
        table_condition = get_pattern_condition(self.allow_dataset_pattern, table_field)
        deny_schema_condition = get_pattern_condition(self.deny_schema_pattern, schema_field, allow=False)
        deny_table_condition = get_pattern_condition(self.deny_dataset_pattern, table_field, allow=False)

        all_tables_and_columns = []

        for schema in containers:
            try:
                formatted_query = query_template.format(
                    schema_pattern=schema_condition,
                    table_pattern=table_condition,
                    deny_schema_pattern=deny_schema_condition,
                    deny_table_pattern=deny_table_condition,
                    container_name=schema.container_name.lower(),
                )

                all_tables_and_columns.extend(
                    self.execute_query(
                        query=formatted_query,
                    )
                )
            except Exception as exc:
                logger.warning(f"{schema.subclass} {schema.container_name} had no tables or views")
                logger.debug(exc)

        tables = []

        if self.edition == DremioEdition.COMMUNITY:
            tables = self.community_get_formatted_tables(all_tables_and_columns)

        else:
            column_dictionary: Dict[str, List[DremioDatasetColumn]] = defaultdict(list)

            for record in all_tables_and_columns:
                column_dictionary[record.get('FULL_TABLE_PATH')].append(
                    DremioDatasetColumn(
                        name=record.get("COLUMN_NAME"),
                        ordinal_position=record.get("ORDINAL_POSITION"),
                        is_nullable=record.get("IS_NULLABLE"),
                        data_type=record.get("DATA_TYPE"),
                        column_size=record.get("COLUMN_SIZE"),
                    )
                )

            distinct_tables_list = list(
                {
                    tuple(
                        dictionary[key] for key in (
                            "TABLE_SCHEMA",
                            "TABLE_NAME",
                            "FULL_TABLE_PATH",
                            "VIEW_DEFINITION",
                            "LOCATION_ID",
                            "OWNER",
                            "OWNER_TYPE",
                            "CREATED",
                            "FORMAT_TYPE",
                        ) if key in dictionary
                    ): dictionary for dictionary in all_tables_and_columns
                }.values())

            for table in distinct_tables_list:
                tables.append(
                    {
                        "TABLE_NAME": table.get("TABLE_NAME"),
                        "TABLE_SCHEMA": table.get("TABLE_SCHEMA"),
                        "COLUMNS": column_dictionary.get(
                            table.get('FULL_TABLE_PATH')
                        ),
                        "VIEW_DEFINITION": table.get("VIEW_DEFINITION"),
                        "RESOURCE_ID": table.get("RESOURCE_ID"),
                        "LOCATION_ID": table.get("LOCATION_ID"),
                        "OWNER": table.get("OWNER"),
                        "OWNER_TYPE": table.get("OWNER_TYPE"),
                        "CREATED": table.get("CREATED"),
                        "FORMAT_TYPE": table.get("FORMAT_TYPE"),
                    }
                )

        return tables

    def validate_schema_format(self, schema):

        if "." in schema:
            schema_path = self.execute_get_request(
                url=f"/catalog/{self.get_dataset_id(schema=schema, dataset='')}").get("path")
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

    def get_view_parents(self, dataset_id) -> List:
        parents_list = []

        if self.edition == DremioEdition.ENTERPRISE:
            parents = self.execute_get_request(
                url=f"/catalog/{dataset_id}/graph",
            ).get("parents")

            if not parents:
                return []

            for parent in parents:
                parents_list.append(
                    ".".join(parent.get("path"))
                )

        return parents_list

    def extract_all_queries(self) -> Deque[DremioQuery]:
        queries: Deque[DremioQuery] = deque()

        if self.edition == DremioEdition.CLOUD:
            jobs_query = DremioSQLQueries.QUERY_ALL_JOBS_CLOUD
        else:
            jobs_query = DremioSQLQueries.QUERY_ALL_JOBS

        for query in self.execute_query(query=jobs_query):
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

    def get_source_by_id(self, source_id: str) -> Optional[Dict]:
        """
        Fetch source details by ID.
        """
        response = self.execute_get_request(
            url=f"/source/{source_id}",
        )
        return response if response else None

    def get_source_for_dataset(self, schema: str, dataset: str) -> Optional[Dict]:
        """
        Get source information for a dataset given its schema and name.
        """
        dataset_id = self.get_dataset_id(schema, dataset)
        if not dataset_id:
            return None

        catalog_entry = self.execute_get_request(
            url=f"/catalog/{dataset_id}",
        )
        if not catalog_entry or 'path' not in catalog_entry:
            return None

        source_id = catalog_entry['path'][0]
        return self.get_source_by_id(source_id)

    def get_tags_for_resource(self, resource_id: str) -> Optional[List[str]]:
        """
        Get Dremio tags for a given resource_id.
        """

        try:
            tags = self.execute_get_request(
                url=f"/catalog/{resource_id}/collaboration/tag",
            )
            return tags.get("tags")
        except Exception as exc:
            logging.info(
                "Resource ID {} has no tags: {}".format(
                    resource_id,
                    exc,
                )
            )
        return

    def get_description_for_resource(self, resource_id: str) -> Optional[str]:
        """
        Get Dremio wiki entry for a given resource_id.
        """

        try:
            tags = self.execute_get_request(
                url=f"/catalog/{resource_id}/collaboration/wiki",
            )
            return tags.get("text")
        except Exception as exc:
            logging.info(
                "Resource ID {} has no wiki entry: {}".format(
                    resource_id,
                    exc,
                )
            )
        return

    def get_source_type(
            self,
            dremio_source_type: str,
            datahub_source_type: Optional[str],
    ) -> Optional[str]:
        """
        Get Dremio wiki entry for a given resource_id.
        """

        lookup_datahub_source_type = self.dremio_to_datahub_source_mapper.get_datahub_source_type(
            dremio_source_type=dremio_source_type,
        )

        if lookup_datahub_source_type:
            return lookup_datahub_source_type

        self.dremio_to_datahub_source_mapper.add_mapping(
            dremio_source_type=dremio_source_type,
            datahub_source_type=datahub_source_type
        )
        return datahub_source_type

    def get_source_category(
            self,
            dremio_source_type: str,
    ) -> Optional[str]:
        """
        Get Dremio wiki entry for a given resource_id.
        """

        return self.dremio_to_datahub_source_mapper.get_category(
            source_type=dremio_source_type,
        )

    def get_containers_for_location(
            self,
            resource_id: str,
            path: List[str]
    ) -> List[Dict[str, str]]:
        containers = []

        def traverse_path(location_id: str, entity_path: List[str]):
            nonlocal containers
            try:
                response = self.execute_get_request(url=f"/catalog/{location_id}")

                if response.get("entityType") == "folder":
                    containers.append(
                        {
                            "id": location_id,
                            "name": entity_path[-1],
                            "path": entity_path[:-1],
                            "container_type": "FOLDER",
                        }
                    )

                for container in response.get("children", []):
                    if container.get("type") == "CONTAINER":
                        traverse_path(container.get("id"), container.get("path"))

            except Exception as exc:
                logging.info("Location {} contains no tables or views. Skipping...".format(id))
                logging.info("Error message: {}".format(exc))

            return containers

        return traverse_path(location_id=resource_id, entity_path=path)

    def get_all_containers(self):
        """
        Query the Dremio sources API and return source information.
        """
        containers = []

        response = self.execute_get_request(url="/catalog")

        def process_source(source):
            if source.get("containerType") == "SOURCE":
                source_config = self.execute_get_request(
                    url=f"/catalog/{source.get('id')}",
                )

                if source_config.get("config", {}).get("database"):
                    db = source_config.get("config", {}).get("database")
                else:
                    db = source_config.get("config", {}).get("databaseName", "")

                return {
                    "id": source.get("id"),
                    "name": source.get("path")[0],
                    "path": [],
                    "container_type": "SOURCE",
                    "source_type": source_config.get("type"),
                    "root_path": source_config.get("config", {}).get("rootPath"),
                    "database_name": db,
                }
            else:
                return {
                    "id": source.get("id"),
                    "name": source.get("path")[0],
                    "path": [],
                    "container_type": "SPACE",
                }

        def process_source_and_containers(source):
            container = process_source(source)
            sub_containers = self.get_containers_for_location(
                resource_id=container.get("id"),
                path=[container.get("name")],
            )
            return [container] + sub_containers

        # Use ThreadPoolExecutor to parallelize the processing of sources
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_source = {executor.submit(process_source_and_containers, source): source
                                for source in response.get("data", [])}

            for future in concurrent.futures.as_completed(future_to_source):
                containers.extend(future.result())

        return containers



class DremioGlossaryTerm:
    urn: str
    glossary_term: str

    def __init__(self, glossary_term: str):
        self.urn = self.set_glossary_term(
            glossary_term=glossary_term,
        )
        self.glossary_term = glossary_term

    def set_glossary_term(self, glossary_term: str):
        namespace = uuid.NAMESPACE_DNS

        return make_term_urn(
            term=str(
                uuid.uuid5(
                    namespace,
                    glossary_term
                )
            )
        )


class DremioDataset:
    resource_id: str
    resource_name: str
    path: List[str]
    location_id: str
    columns: List[DremioDatasetColumn]
    sql_definition: Optional[str]
    dataset_type: DremioDatasetType
    owner: Optional[str]
    owner_type: Optional[str]
    created: str
    parents: Optional[List[str]]
    description: Optional[str]
    format_type: Optional[str]
    glossary_terms: List[DremioGlossaryTerm] = []

    def __init__(
            self,
            dataset_details: dict,
            api_operations: DremioAPIOperations,
    ):
        self.glossary_terms: List[DremioGlossaryTerm] = []
        self.resource_id = dataset_details.get("RESOURCE_ID")
        self.resource_name = dataset_details.get("TABLE_NAME")
        self.path = dataset_details.get("TABLE_SCHEMA")[1:-1].split(", ")[:-1]
        self.location_id = dataset_details.get("LOCATION_ID")

        #Protect against null columns returned
        if dataset_details.get("COLUMNS")[0].name:
            self.columns = dataset_details.get("COLUMNS")
        else:
            self.columns = []
        self.sql_definition = dataset_details.get("VIEW_DEFINITION")

        if self.sql_definition:
            self.dataset_type = DremioDatasetType.VIEW
        else:
            self.dataset_type = DremioDatasetType.TABLE

        if api_operations.edition in (
            DremioEdition.ENTERPRISE,
            DremioEdition.CLOUD,
        ):
            self.created = dataset_details.get("CREATED")
            self.owner = dataset_details.get("OWNER")
            self.owner_type = dataset_details.get("OWNER_TYPE")
            self.format_type = dataset_details.get("FORMAT_TYPE")

        self.description = api_operations.get_description_for_resource(
            resource_id=self.resource_id
        )

        for glossary_term in api_operations.get_tags_for_resource(
            resource_id=self.resource_id
        ):
            self.glossary_terms.append(
                DremioGlossaryTerm(glossary_term=glossary_term)
            )

        if self.sql_definition and api_operations.edition == DremioEdition.ENTERPRISE:
            self.parents = api_operations.get_view_parents(
                dataset_id=self.resource_id,
            )

    def get_profile_data(self, profiler) -> Dict:
        full_table_name = '"' + '"."'.join(self.path) + '"."' + self.resource_name + '"'
        columns = [(col.name, col.data_type) for col in self.columns]
        return profiler.profile_table(full_table_name, columns)


class DremioContainer:
    container_name: str
    location_id: str
    path: List[str]
    description: Optional[str]
    subclass: str

    def __init__(
        self,
        container_name: str,
        location_id: str,
        path: List[str],
        api_operations: DremioAPIOperations,
    ):
        self.container_name = container_name
        self.location_id = location_id
        self.path = path

        self.description = api_operations.get_description_for_resource(
            resource_id=location_id,
        )

class DremioSource(DremioContainer):
    subclass: str = "Dremio Source"
    dremio_source_type: str
    root_path: Optional[str]
    database_name: Optional[str]

    def __init__(
            self,
            container_name: str,
            location_id: str,
            path: List[str],
            api_operations: DremioAPIOperations,
            dremio_source_type: str,
            root_path: Optional[str]=None,
            database_name: Optional[str]=None,
    ):
        super().__init__(
            container_name=container_name,
            location_id=location_id,
            path=path,
            api_operations=api_operations,
        )
        self.dremio_source_type = dremio_source_type
        self.root_path = root_path
        self.database_name = database_name

class DremioSpace(DremioContainer):
    subclass: str = "Dremio Space"

class DremioFolder(DremioContainer):
    subclass: str = "Dremio Folder"

class DremioCatalog:
    dremio_api: DremioAPIOperations
    edition: DremioEdition

    def __init__(self, dremio_api: DremioAPIOperations):
        self.dremio_api = dremio_api
        self.edition = dremio_api.edition
        self.datasets: Deque[DremioDataset] = deque()
        self.sources: Deque[DremioSource] = deque()
        self.spaces: Deque[DremioSpace] = deque()
        self.folders: Deque[DremioFolder] = deque()
        self.glossary_terms: Deque[DremioGlossaryTerm] = deque()
        self.queries: Deque[DremioQuery] = deque()

        self.datasets_populated = False
        self.containers_populated = False
        self.queries_populated = False

    def set_datasets(self) -> None:
        if not self.datasets_populated:
            self.set_containers()
            for dataset_details in self.dremio_api.get_all_tables_and_columns(
                containers=(self.spaces + self.sources)
            ):
                dremio_dataset = DremioDataset(
                    dataset_details=dataset_details,
                    api_operations=self.dremio_api,
                )

                self.datasets.append(dremio_dataset)

                for glossary_term in dremio_dataset.glossary_terms:
                    if glossary_term not in self.glossary_terms:
                        self.glossary_terms.append(glossary_term)

            self.datasets_populated = True

    def force_reset_datasets(self) -> None:
        self.datasets_populated = False
        self.set_datasets()

    def get_datasets(self) -> Deque[DremioDataset]:
        self.set_datasets()
        return self.datasets

    def set_containers(self) -> None:
        if not self.containers_populated:
            for container in self.dremio_api.get_all_containers():
                container_type = container.get("container_type")
                if container_type == "SOURCE":
                    self.sources.append(
                            DremioSource(
                            container_name=container.get("name"),
                            location_id=container.get("id"),
                            path=[],
                            api_operations=self.dremio_api,
                            dremio_source_type=container.get("source_type"),
                            root_path=container.get("root_path"),
                            database_name=container.get("database_name"),
                        )
                    )
                elif container_type == "SPACE":
                    self.spaces.append(
                            DremioSpace(
                            container_name=container.get("name"),
                            location_id=container.get("id"),
                            path=[],
                            api_operations=self.dremio_api,
                        )
                    )
                elif container_type == "FOLDER":
                    self.folders.append(
                        DremioFolder(
                            container_name=container.get("name"),
                            location_id=container.get("id"),
                            path=container.get("path"),
                            api_operations=self.dremio_api,
                        )
                    )
                else:
                    self.spaces.append(
                        DremioSpace(
                            container_name=container.get("name"),
                            location_id=container.get("id"),
                            path=[],
                            api_operations=self.dremio_api,
                        )
                    )

        logging.info("Containers retrieved from source")

        self.containers_populated = True

    def force_reset_containers(self) -> None:
        self.containers_populated = False
        self.set_containers()

    def get_containers(self)  -> Deque:
        self.set_containers()
        return self.sources + self.spaces + self.folders

    def get_sources(self)  -> Deque[DremioSource]:
        self.set_containers()
        return self.sources

    def get_glossary_terms(self)  -> Deque[DremioGlossaryTerm]:
        self.set_datasets()
        self.set_containers()
        return self.glossary_terms

    def force_set_glossary_terms(self) -> None:
        self.force_reset_containers()
        self.force_reset_datasets()

    def get_queries(self)  -> Deque[DremioQuery]:
        self.queries = self.dremio_api.extract_all_queries()
        self.queries_populated = True
        return self.queries


