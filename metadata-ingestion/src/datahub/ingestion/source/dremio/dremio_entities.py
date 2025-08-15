import itertools
import logging
import re
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Deque, Dict, List, Optional

from sqlglot import parse_one

from datahub.emitter.mce_builder import make_term_urn
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
    DremioEntityContainerType,
)

logger = logging.getLogger(__name__)


QUERY_TYPES = {
    "DML": ["CREATE", "DELETE", "INSERT", "MERGE", "UPDATE"],
    "DDL": [
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
    ],
    "SELECT": ["SELECT", "WITH"],
    "DATA_MANIPULATION": ["INSERT INTO", "MERGE INTO", "CREATE TABLE"],
}


@dataclass
class DremioContainerResponse:
    container_type: str
    name: str
    id: str
    path: Optional[List[str]] = None
    source_type: Optional[str] = None
    root_path: Optional[str] = None
    database_name: Optional[str] = None


class DremioDatasetType(Enum):
    VIEW = "View"
    TABLE = "Table"


class DremioGlossaryTerm:
    urn: str
    glossary_term: str

    def __init__(self, glossary_term: str):
        self.urn = self.set_glossary_term(
            glossary_term=glossary_term,
        )
        self.glossary_term = glossary_term

    def set_glossary_term(self, glossary_term: str) -> str:
        namespace = uuid.NAMESPACE_DNS

        return make_term_urn(term=str(uuid.uuid5(namespace, glossary_term)))


@dataclass
class DremioDatasetColumn:
    name: str
    ordinal_position: int
    data_type: str
    column_size: int
    is_nullable: bool


class DremioQuery:
    query_without_comments: str
    query_type: str
    query_subtype: str
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
        return str(query).replace("'", "'")

    def _get_query_type(self) -> str:
        query_operator = re.split(
            pattern=r"\s+",
            string=self.query_without_comments.strip(),
            maxsplit=1,
        )[0]

        if query_operator in QUERY_TYPES["SELECT"]:
            return "SELECT"
        if query_operator in QUERY_TYPES["DML"]:
            return "DML"
        return "DDL"

    def _get_query_subtype(self) -> str:
        for query_operator in (
            QUERY_TYPES["SELECT"] + QUERY_TYPES["DML"] + QUERY_TYPES["DDL"]
        ):
            if self.query_without_comments.upper().startswith(query_operator):
                return query_operator
        return "UNDEFINED"

    def _get_queried_datasets(self, queried_datasets: str) -> List[str]:
        return list(
            {dataset.strip() for dataset in queried_datasets.strip("[]").split(",")}
        )

    def _get_affected_tables(self) -> str:
        # TO DO
        # for manipulation_operator in _data_manipulation_queries:
        #     if self.query_without_comments.upper().startswith(manipulation_operator):

        return ""

    def get_raw_query(self, sql_query: str) -> str:
        try:
            parsed = parse_one(sql_query)
            return parsed.sql(comments=False)
        except Exception as e:
            logger.warning(e)
            return sql_query


class DremioDataset:
    resource_id: str
    resource_name: str
    path: List[str]
    location_id: str
    columns: List[DremioDatasetColumn]
    sql_definition: Optional[str]
    dataset_type: DremioDatasetType
    default_schema: Optional[str]
    owner: Optional[str]
    owner_type: Optional[str]
    created: str
    parents: Optional[List[str]]
    description: Optional[str]
    format_type: Optional[str]
    glossary_terms: List[DremioGlossaryTerm] = []

    def __init__(
        self,
        dataset_details: Dict[str, Any],
        api_operations: DremioAPIOperations,
    ):
        self.glossary_terms: List[DremioGlossaryTerm] = []
        self.resource_id = dataset_details.get("RESOURCE_ID", "")
        self.resource_name = dataset_details.get("TABLE_NAME", "")
        self.path = dataset_details.get("TABLE_SCHEMA", "")[1:-1].split(", ")[:-1]
        self.location_id = dataset_details.get("LOCATION_ID", "")
        # self.columns = dataset_details.get("COLUMNS", [])
        # Initialize DremioDatasetColumn instances for each column
        self.columns = [
            DremioDatasetColumn(
                name=col.get("name"),
                ordinal_position=col.get("ordinal_position"),
                is_nullable=col.get("is_nullable", False),
                data_type=col.get("data_type"),
                column_size=col.get("column_size"),
            )
            for col in dataset_details.get("COLUMNS", [])
        ]

        self.sql_definition = dataset_details.get("VIEW_DEFINITION")

        if self.sql_definition:
            self.dataset_type = DremioDatasetType.VIEW
            self.default_schema = api_operations.get_context_for_vds(
                resource_id=self.resource_id
            )
        else:
            self.dataset_type = DremioDatasetType.TABLE

        self.owner = dataset_details.get("OWNER")
        self.owner_type = dataset_details.get("OWNER_TYPE")

        if api_operations.edition in (
            DremioEdition.ENTERPRISE,
            DremioEdition.CLOUD,
        ):
            self.created = dataset_details.get("CREATED", "")
            self.format_type = dataset_details.get("FORMAT_TYPE")

        self.description = api_operations.get_description_for_resource(
            resource_id=self.resource_id
        )

        glossary_terms = api_operations.get_tags_for_resource(
            resource_id=self.resource_id
        )
        if glossary_terms is not None:
            for glossary_term in glossary_terms:
                self.glossary_terms.append(
                    DremioGlossaryTerm(glossary_term=glossary_term)
                )

        if self.sql_definition and api_operations.edition == DremioEdition.ENTERPRISE:
            self.parents = api_operations.get_view_parents(
                dataset_id=self.resource_id,
            )


class DremioContainer:
    subclass: str = "Dremio Container"
    container_name: str
    location_id: str
    path: List[str]
    description: Optional[str]

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


class DremioSourceContainer(DremioContainer):
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
        root_path: Optional[str] = None,
        database_name: Optional[str] = None,
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
        self.sources: Deque[DremioSourceContainer] = deque()
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

            containers: Deque[DremioContainer] = deque()
            containers.extend(self.spaces)  # Add DremioSpace elements
            containers.extend(self.sources)  # Add DremioSource elements

            for dataset_details in self.dremio_api.get_all_tables_and_columns(
                containers=containers
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

    def get_datasets(self) -> Deque[DremioDataset]:
        self.set_datasets()
        return self.datasets

    def set_containers(self) -> None:
        if not self.containers_populated:
            for container in self.dremio_api.get_all_containers():
                container_type = container.get("container_type")
                if container_type == DremioEntityContainerType.SOURCE:
                    self.sources.append(
                        DremioSourceContainer(
                            container_name=container.get("name"),
                            location_id=container.get("id"),
                            path=[],
                            api_operations=self.dremio_api,
                            dremio_source_type=container.get("source_type")
                            or "unknown",
                            root_path=container.get("root_path"),
                            database_name=container.get("database_name"),
                        )
                    )
                elif container_type == DremioEntityContainerType.SPACE:
                    self.spaces.append(
                        DremioSpace(
                            container_name=container.get("name"),
                            location_id=container.get("id"),
                            path=[],
                            api_operations=self.dremio_api,
                        )
                    )
                elif container_type == DremioEntityContainerType.FOLDER:
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

    def get_containers(self) -> Deque:
        self.set_containers()
        return deque(itertools.chain(self.sources, self.spaces, self.folders))

    def get_sources(self) -> Deque[DremioSourceContainer]:
        self.set_containers()
        return self.sources

    def get_glossary_terms(self) -> Deque[DremioGlossaryTerm]:
        self.set_datasets()
        self.set_containers()
        return self.glossary_terms

    def is_valid_query(self, query: Dict[str, Any]) -> bool:
        required_fields = [
            "job_id",
            "user_name",
            "submitted_ts",
            "query",
            "queried_datasets",
        ]
        return all(query.get(field) for field in required_fields)

    def get_queries(self) -> Deque[DremioQuery]:
        for query in self.dremio_api.extract_all_queries():
            if not self.is_valid_query(query):
                continue
            self.queries.append(
                DremioQuery(
                    job_id=query["job_id"],
                    username=query["user_name"],
                    submitted_ts=query["submitted_ts"],
                    query=query["query"],
                    queried_datasets=query["queried_datasets"],
                )
            )
        self.queries_populated = True
        return self.queries
