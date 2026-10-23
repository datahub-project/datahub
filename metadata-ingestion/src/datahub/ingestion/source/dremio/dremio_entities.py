import logging
import uuid
from datetime import datetime
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Set

from datahub.emitter.mce_builder import make_term_urn
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)

# Import Pydantic models from separate module to avoid circular dependencies
from datahub.ingestion.source.dremio.dremio_models import (
    DremioContainerMatch,
    DremioContainerPathIndex,
    DremioContainerResponse,
    DremioDatasetColumn,
    DremioDatasetResponse,
    DremioDatasetType,
    DremioEntityContainerType,
)

logger = logging.getLogger(__name__)


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


class DremioQuery:
    def __init__(
        self,
        job_id: str,
        username: str,
        submitted_ts: str,
        query: str,
        queried_datasets: str,
    ):
        self.job_id = job_id
        self.username = username
        self.submitted_ts = self._get_submitted_ts(submitted_ts)
        self.query = self._get_query(query)
        self.queried_datasets = self._get_queried_datasets(queried_datasets)

    def _get_submitted_ts(self, timestamp: str) -> datetime:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    def _get_query(self, query: str) -> str:
        # Normalize smart single quotes so downstream sqlglot parsing doesn't fail.
        return str(query).replace("\u2018", "'").replace("\u2019", "'")

    def _get_queried_datasets(self, queried_datasets: str) -> List[str]:
        return list(
            {dataset.strip() for dataset in queried_datasets.strip("[]").split(",")}
        )


class DremioDataset:
    resource_id: str
    resource_name: str
    path: List[str]
    # Parent container path set by DremioCatalog: `path` in the casing the walk
    # emitted, `path` itself when no container matched, or [] when several did.
    container_path: Optional[List[str]] = None
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
        self._dataset_response = DremioDatasetResponse.model_validate(dataset_details)
        self.glossary_terms: List[DremioGlossaryTerm] = []
        self.resource_id = self._dataset_response.resource_id
        self.resource_name = self._dataset_response.table_name
        self.path = self._dataset_response.path
        # Community-edition views can omit LOCATION_ID — preserve the
        # legacy "" default rather than carrying None forward.
        self.location_id = self._dataset_response.location_id or ""
        self.columns = self._dataset_response.columns
        self.sql_definition = self._dataset_response.view_definition

        # Safe defaults — populated conditionally below depending on edition and type
        self.default_schema: Optional[str] = None
        self.created: str = ""
        self.format_type: Optional[str] = None
        self.parents: Optional[List[str]] = None

        if self.sql_definition:
            self.dataset_type = DremioDatasetType.VIEW
            self.default_schema = api_operations.get_context_for_vds(
                resource_id=self.resource_id
            )
        else:
            self.dataset_type = DremioDatasetType.TABLE

        self.owner = self._dataset_response.owner
        self.owner_type = self._dataset_response.owner_type

        if api_operations.edition in (
            DremioEdition.ENTERPRISE,
            DremioEdition.CLOUD,
        ):
            self.created = self._dataset_response.created or ""
            self.format_type = self._dataset_response.format_type

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
    # Declared without a default so subclasses can't silently inherit
    # the wrong subtype.
    subclass: ClassVar[DatasetContainerSubTypes]
    container_name: str
    location_id: Optional[str]
    path: List[str]
    description: Optional[str]

    def __init__(
        self,
        container_name: str,
        location_id: Optional[str],
        path: List[str],
        api_operations: DremioAPIOperations,
    ):
        self.container_name = container_name
        self.location_id = location_id
        self.path = path

        self.description = (
            api_operations.get_description_for_resource(
                resource_id=location_id,
            )
            if location_id
            else ""
        )


class DremioSourceContainer(DremioContainer):
    subclass: ClassVar[DatasetContainerSubTypes] = (
        DatasetContainerSubTypes.DREMIO_SOURCE
    )
    dremio_source_type: str
    root_path: Optional[str]
    database_name: Optional[str]

    def __init__(
        self,
        container_name: str,
        location_id: Optional[str],
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
    subclass: ClassVar[DatasetContainerSubTypes] = DatasetContainerSubTypes.DREMIO_SPACE


class DremioFolder(DremioContainer):
    subclass: ClassVar[DatasetContainerSubTypes] = (
        DatasetContainerSubTypes.DREMIO_FOLDER
    )
    # Stamped by the recursive catalog walk in DremioAPIOperations so
    # DremioAspects._create_browse_paths_containers picks the right
    # top-level prefix ("Spaces" vs "Sources").
    root_container_type: Optional[str] = None


_EMITTED_CONTAINER_TYPES = frozenset(
    (
        DremioEntityContainerType.SOURCE,
        DremioEntityContainerType.SPACE,
        DremioEntityContainerType.FOLDER,
    )
)


class DremioCatalog:
    api: DremioAPIOperations
    edition: DremioEdition

    def __init__(self, api: DremioAPIOperations):
        self.api = api
        self.edition = api.edition
        self._container_responses: Optional[List[DremioContainerResponse]] = None
        # TABLE_SCHEMA can case folders differently from the catalog walk.
        self._container_paths = DremioContainerPathIndex()
        # Warn once per schema path, not once per dataset.
        self._unresolved_container_paths: Set[str] = set()

    def _all_containers(self) -> List[DremioContainerResponse]:
        # One catalog walk per run, shared by the source map, container emission
        # and dataset parent resolution.
        if self._container_responses is None:
            self._container_responses = list(self.api.get_all_containers())
            for container in self._container_responses:
                if container.container_type in _EMITTED_CONTAINER_TYPES:
                    self._container_paths.add(container.full_path)
        return self._container_responses

    def get_datasets(self) -> Iterator[DremioDataset]:
        """Get all Dremio datasets (tables and views) as an iterator."""
        self._all_containers()
        for dataset_details in self.api.get_all_tables_and_columns():
            dremio_dataset = DremioDataset(
                dataset_details=dataset_details,
                api_operations=self.api,
            )
            dremio_dataset.container_path = self._resolve_container_path(
                dremio_dataset.path
            )

            yield dremio_dataset

    def _resolve_container_path(self, path: List[str]) -> List[str]:
        if not path:
            return path
        resolution = self._container_paths.resolve(path)
        if resolution.path is not None:
            return resolution.path
        ambiguous = resolution.match == DremioContainerMatch.AMBIGUOUS
        joined = ".".join(path)
        if joined not in self._unresolved_container_paths:
            self._unresolved_container_paths.add(joined)
            if ambiguous:
                message = (
                    "Schema path matches containers that differ only by case; "
                    "its datasets get no container"
                )
            else:
                message = (
                    "Schema path matches no emitted container; its datasets point "
                    "at a container that was not emitted this run"
                )
            self.api.report.warning(
                title="Dataset container not resolved",
                message=message,
                context=joined,
            )
        # Several candidates means any choice could be wrong.
        return [] if ambiguous else path

    def get_containers(self) -> Iterator[DremioContainer]:
        """Get all containers (sources, spaces, folders) as an iterator."""
        for container in self._all_containers():
            dremio_container = self._to_container(container)
            if dremio_container is not None:
                yield dremio_container

    def _to_container(
        self, container: DremioContainerResponse
    ) -> Optional[DremioContainer]:
        container_type = container.container_type
        if container_type == DremioEntityContainerType.SOURCE:
            return DremioSourceContainer(
                container_name=container.name,
                location_id=container.id,
                path=container.path or [],
                api_operations=self.api,
                dremio_source_type=container.source_type or "",
                root_path=container.root_path,
                database_name=container.database_name,
            )
        if container_type == DremioEntityContainerType.SPACE:
            return DremioSpace(
                container_name=container.name,
                location_id=container.id,
                path=container.path or [],
                api_operations=self.api,
            )
        if container_type == DremioEntityContainerType.FOLDER:
            folder = DremioFolder(
                container_name=container.name,
                location_id=container.id,
                path=container.path or [],
                api_operations=self.api,
            )
            if container.root_container_type is not None:
                folder.root_container_type = container.root_container_type
            return folder
        return None

    def get_sources(self) -> Iterator[DremioSourceContainer]:
        """Get all Dremio source containers (external data connections) as an iterator."""
        for container in self.get_containers():
            if isinstance(container, DremioSourceContainer):
                yield container

    def get_glossary_terms(self) -> Iterator[DremioGlossaryTerm]:
        """Get all unique glossary terms (tags) from datasets."""
        glossary_terms_seen = set()

        for dataset in self.get_datasets():
            for glossary_term in dataset.glossary_terms:
                if glossary_term not in glossary_terms_seen:
                    glossary_terms_seen.add(glossary_term)
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

    def get_queries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Iterator[DremioQuery]:
        """Get all valid Dremio queries for lineage analysis.

        Optional start_time/end_time override the config-level time window. This
        is used by the stateful time-window handler to advance start_time to the
        previous run's end_time, so only new job history is re-processed.
        """
        for query in self.api.extract_all_queries(
            start_time=start_time, end_time=end_time
        ):
            if not self.is_valid_query(query):
                continue
            yield DremioQuery(
                job_id=query["job_id"],
                username=query["user_name"],
                submitted_ts=query["submitted_ts"],
                query=query["query"],
                queried_datasets=query["queried_datasets"],
            )
