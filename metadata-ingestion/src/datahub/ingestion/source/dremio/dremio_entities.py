import logging
import re
import uuid
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field
from sqlglot import parse_one

from datahub.emitter.mce_builder import make_term_urn
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
    DremioEntityContainerType,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport

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


class DremioContainerResponse(BaseModel):
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


class DremioGlossaryTerm(BaseModel):
    urn: str
    glossary_term: str

    @classmethod
    def from_term(cls, glossary_term: str) -> "DremioGlossaryTerm":
        namespace = uuid.NAMESPACE_DNS
        urn = make_term_urn(term=str(uuid.uuid5(namespace, glossary_term)))
        return cls(urn=urn, glossary_term=glossary_term)

    def set_glossary_term(self, glossary_term: str) -> str:
        namespace = uuid.NAMESPACE_DNS
        return make_term_urn(term=str(uuid.uuid5(namespace, glossary_term)))


class DremioDatasetColumn(BaseModel):
    name: str
    ordinal_position: int
    data_type: str
    column_size: int
    is_nullable: bool


class DremioQuery(BaseModel):
    """Represents a Dremio query with parsed metadata."""

    job_id: str
    username: str
    submitted_ts: datetime
    query: str
    queried_datasets: List[str]
    affected_datasets: Optional[str] = None

    # Computed fields
    query_without_comments: str = Field(default="")
    query_type: str = Field(default="")
    query_subtype: str = Field(default="")
    affected_dataset: str = Field(default="")

    class Config:
        # Allow extra fields for backward compatibility
        extra = "allow"

    def __init__(self, **data):
        # Handle raw input transformation
        if "submitted_ts" in data and isinstance(data["submitted_ts"], str):
            data["submitted_ts"] = self._parse_timestamp(data["submitted_ts"])

        if "query" in data:
            data["query"] = self._clean_query(data["query"])

        if "queried_datasets" in data and isinstance(data["queried_datasets"], str):
            data["queried_datasets"] = self._parse_queried_datasets(
                data["queried_datasets"]
            )

        super().__init__(**data)

        # Set computed fields after initialization
        self.query_without_comments = self.get_raw_query(self.query)
        self.query_type = self._get_query_type()
        self.query_subtype = self._get_query_subtype()

        if self.affected_datasets:
            self.affected_dataset = self.affected_datasets
        else:
            self.affected_dataset = self._get_affected_tables()

    @staticmethod
    def _parse_timestamp(timestamp: str) -> datetime:
        """Parse timestamp string to datetime object."""
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    @staticmethod
    def _clean_query(query: str) -> str:
        """Clean query string."""
        return str(query).replace("'", "'")

    @staticmethod
    def _parse_queried_datasets(queried_datasets: str) -> List[str]:
        """Parse queried datasets string to list."""
        return list(
            {dataset.strip() for dataset in queried_datasets.strip("[]").split(",")}
        )

    def _get_query_type(self) -> str:
        """Determine the query type based on the first operator."""
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
        """Determine the query subtype based on the operator."""
        for query_operator in (
            QUERY_TYPES["SELECT"] + QUERY_TYPES["DML"] + QUERY_TYPES["DDL"]
        ):
            if self.query_without_comments.upper().startswith(query_operator):
                return query_operator
        return "UNDEFINED"

    def _get_affected_tables(self) -> str:
        """Get affected tables from the query (placeholder implementation)."""
        # TODO: Implement proper affected tables extraction
        return ""

    def get_raw_query(self, sql_query: str) -> str:
        """Get query without comments using SQL parsing."""
        try:
            parsed = parse_one(sql_query)
            return parsed.sql(comments=False)
        except Exception:
            # Fall back to original query if SQL parsing fails
            return sql_query

    def get(self, attr: str) -> Any:
        """Backward compatibility method for attribute access."""
        return getattr(self, attr, None)


class DremioDataset(BaseModel):
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

    @classmethod
    def from_dataset_details(
        cls,
        dataset_details: Dict[str, Any],
        api_operations: DremioAPIOperations,
    ) -> "DremioDataset":
        glossary_terms: List[DremioGlossaryTerm] = []
        resource_id = dataset_details.get("RESOURCE_ID", "")
        resource_name = dataset_details.get("TABLE_NAME", "")
        path = dataset_details.get("TABLE_SCHEMA", "")[1:-1].split(", ")[:-1]
        location_id = dataset_details.get("LOCATION_ID", "")

        # Initialize DremioDatasetColumn instances for each column
        columns = [
            DremioDatasetColumn(
                name=col.get("name"),
                ordinal_position=col.get("ordinal_position"),
                is_nullable=col.get("is_nullable", False),
                data_type=col.get("data_type"),
                column_size=col.get("column_size"),
            )
            for col in dataset_details.get("COLUMNS", [])
        ]

        sql_definition = dataset_details.get("VIEW_DEFINITION")

        if sql_definition:
            dataset_type = DremioDatasetType.VIEW
            default_schema = api_operations.get_context_for_vds(resource_id=resource_id)
        else:
            dataset_type = DremioDatasetType.TABLE
            default_schema = None

        owner = dataset_details.get("OWNER")
        owner_type = dataset_details.get("OWNER_TYPE")

        if api_operations.edition in (
            DremioEdition.ENTERPRISE,
            DremioEdition.CLOUD,
        ):
            created = dataset_details.get("CREATED", "")
            format_type = dataset_details.get("FORMAT_TYPE")
        else:
            created = ""
            format_type = None

        description = api_operations.get_description_for_resource(
            resource_id=resource_id
        )

        glossary_terms_data = api_operations.get_tags_for_resource(
            resource_id=resource_id
        )
        if glossary_terms_data is not None:
            for glossary_term in glossary_terms_data:
                glossary_terms.append(DremioGlossaryTerm.from_term(glossary_term))

        # Get parents for views in Enterprise edition
        parents = None
        if sql_definition and api_operations.edition == DremioEdition.ENTERPRISE:
            parents = api_operations.get_view_parents(
                dataset_id=resource_id,
            )

        return cls(
            resource_id=resource_id,
            resource_name=resource_name,
            path=path,
            location_id=location_id,
            columns=columns,
            sql_definition=sql_definition,
            dataset_type=dataset_type,
            default_schema=default_schema,
            owner=owner,
            owner_type=owner_type,
            created=created,
            parents=parents,
            description=description,
            format_type=format_type,
            glossary_terms=glossary_terms,
        )


class DremioContainer(BaseModel):
    subclass: str = "Dremio Container"
    container_name: str
    location_id: str
    path: List[str]
    description: Optional[str]

    @classmethod
    def from_container_details(
        cls,
        container_name: Optional[str],
        location_id: Optional[str],
        path: Optional[List[str]],
        api_operations: DremioAPIOperations,
    ) -> "DremioContainer":
        # Only validate that we have the essential fields needed for processing
        if not container_name or not location_id:
            raise ValueError(
                f"Container requires both name and location_id: name={container_name}, location_id={location_id}"
            )

        description = api_operations.get_description_for_resource(
            resource_id=location_id,
        )

        return cls(
            container_name=container_name,
            location_id=location_id,
            path=path or [],
            description=description,
        )


class DremioSourceContainer(DremioContainer):
    subclass: str = "Dremio Source"
    dremio_source_type: Optional[str] = None
    root_path: Optional[str] = None
    database_name: Optional[str] = None

    @classmethod
    def from_source_details(
        cls,
        container_name: Optional[str],
        location_id: Optional[str],
        path: Optional[List[str]],
        api_operations: DremioAPIOperations,
        dremio_source_type: Optional[str],
        root_path: Optional[str] = None,
        database_name: Optional[str] = None,
    ) -> "DremioSourceContainer":
        # Only validate that we have the essential fields needed for processing
        if not container_name or not location_id:
            raise ValueError(
                f"Container requires both name and location_id: name={container_name}, location_id={location_id}"
            )

        description = api_operations.get_description_for_resource(
            resource_id=location_id,
        )

        return cls(
            container_name=container_name,
            location_id=location_id,
            path=path or [],
            description=description,
            dremio_source_type=dremio_source_type,
            root_path=root_path,
            database_name=database_name,
        )


class DremioSpace(DremioContainer):
    subclass: str = "Dremio Space"


class DremioFolder(DremioContainer):
    subclass: str = "Dremio Folder"


class DremioCatalog:
    dremio_api: DremioAPIOperations
    edition: DremioEdition

    def __init__(
        self,
        dremio_api: DremioAPIOperations,
        report: Optional["DremioSourceReport"] = None,
    ):
        self.dremio_api = dremio_api
        self.edition = dremio_api.edition
        self.report = report
        # Cache for containers to avoid re-fetching
        self._containers_cache: Optional[List[Dict]] = None

    def get_datasets(self) -> Iterable[DremioDataset]:
        """Generator that yields DremioDataset objects one at a time to reduce memory usage."""
        # Get containers for dataset queries
        containers: List[DremioContainer] = []
        for container in self.get_containers():
            if isinstance(container, (DremioSourceContainer, DremioSpace)):
                containers.append(container)

        # Process datasets one at a time
        for dataset_details in self.dremio_api.get_all_tables_and_columns(
            containers=containers
        ):
            try:
                dremio_dataset = DremioDataset.from_dataset_details(
                    dataset_details=dataset_details,
                    api_operations=self.dremio_api,
                )
                yield dremio_dataset
            except Exception as e:
                if self.report:
                    self.report.warning(
                        message="Failed to create dataset from API response",
                        context=f"Dataset: {dataset_details.get('TABLE_NAME', 'unknown')}",
                        exc=e,
                    )
                continue

    def get_containers(self) -> Iterable[DremioContainer]:
        """Generator that yields DremioContainer objects one at a time to reduce memory usage."""
        # Stream containers directly from API without caching to prevent OOM
        for container in self.dremio_api.get_all_containers():
            try:
                container_type = container.get("container_type")
                if container_type == DremioEntityContainerType.SOURCE.value:
                    yield DremioSourceContainer.from_source_details(
                        container_name=container.get("name"),
                        location_id=container.get("id"),
                        path=[],
                        api_operations=self.dremio_api,
                        dremio_source_type=container.get("source_type"),
                        root_path=container.get("root_path"),
                        database_name=container.get("database_name"),
                    )
                elif container_type == DremioEntityContainerType.SPACE.value:
                    yield DremioSpace.from_container_details(
                        container_name=container.get("name"),
                        location_id=container.get("id"),
                        path=[],
                        api_operations=self.dremio_api,
                    )
                elif container_type == DremioEntityContainerType.FOLDER.value:
                    yield DremioFolder.from_container_details(
                        container_name=container.get("name"),
                        location_id=container.get("id"),
                        path=container.get("path"),
                        api_operations=self.dremio_api,
                    )
                else:
                    # Default to Space for unknown types
                    yield DremioSpace.from_container_details(
                        container_name=container.get("name"),
                        location_id=container.get("id"),
                        path=[],
                        api_operations=self.dremio_api,
                    )
            except Exception as e:
                if self.report:
                    self.report.warning(
                        message="Failed to create container from API response",
                        context=f"Container: {container.get('name', 'unknown')}",
                        exc=e,
                    )
                continue

    def get_sources(self) -> Iterable[DremioSourceContainer]:
        """Generator that yields only DremioSourceContainer objects."""
        for container in self.get_containers():
            if isinstance(container, DremioSourceContainer):
                yield container

    def get_glossary_terms(self) -> Iterable[DremioGlossaryTerm]:
        """Generator that yields unique glossary terms from datasets."""
        seen_terms = set()
        for dataset in self.get_datasets():
            for glossary_term in dataset.glossary_terms:
                if glossary_term.glossary_term not in seen_terms:
                    seen_terms.add(glossary_term.glossary_term)
                    yield glossary_term

    def is_valid_query(self, query: Dict[str, Any]) -> bool:
        required_fields = [
            "job_id",
            "user_name",
            "submitted_ts",
            "query",
            "queried_datasets",
        ]
        return all(query.get(field) for field in required_fields)

    def get_queries(self) -> Iterable[DremioQuery]:
        """Generator that yields DremioQuery objects one at a time to reduce memory usage."""
        for query in self.dremio_api.extract_all_queries():
            if not self.is_valid_query(query):
                continue
            try:
                yield DremioQuery(
                    job_id=query["job_id"],
                    username=query["user_name"],
                    submitted_ts=query["submitted_ts"],
                    query=query["query"],
                    queried_datasets=query["queried_datasets"],
                )
            except Exception as e:
                if self.report:
                    self.report.warning(
                        message="Failed to create query from API response",
                        context=f"Query ID: {query.get('job_id', 'unknown')}",
                        exc=e,
                    )
                continue
