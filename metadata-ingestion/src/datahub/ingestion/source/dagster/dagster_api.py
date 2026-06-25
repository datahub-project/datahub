"""Raw GraphQL client for the Dagster webserver.

We deliberately do not use ``dagster_graphql.DagsterGraphQLClient``: it is a
run-management wrapper and does not expose arbitrary metadata queries. Issuing raw
GraphQL keeps OSS and Dagster+ on a single code path (only the URL and the
``Dagster-Cloud-Api-Token`` header differ) and avoids pulling the heavy ``dagster``
runtime into the connector.
"""

import logging
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.dagster.config import DagsterSourceConfig
from datahub.ingestion.source.dagster.data_classes import (
    DagsterAsset,
    DagsterAssetMetadata,
    DagsterColumn,
    DagsterJob,
    DagsterLink,
    DagsterOwner,
    DagsterRepository,
    DagsterTag,
)
from datahub.ingestion.source.dagster.queries import REPOSITORIES_QUERY

logger = logging.getLogger(__name__)


class DagsterGraphQLError(Exception):
    """Raised when the Dagster GraphQL endpoint returns errors."""


class DagsterGraphQLClient:
    def __init__(self, config: DagsterSourceConfig) -> None:
        self.config = config
        self.endpoint = self._build_endpoint(config)

        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset({"POST"}),
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if config.is_cloud and config.token is not None:
            # Programmatic header injection — never via os.environ.
            self.session.headers.update(
                {"Dagster-Cloud-Api-Token": config.token.get_secret_value()}
            )

    @staticmethod
    def _build_endpoint(config: DagsterSourceConfig) -> str:
        if config.is_cloud and config.deployment:
            return f"{config.host}/{config.deployment}/graphql"
        return f"{config.host}/graphql"

    def _execute(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        response = self.session.post(
            self.endpoint,
            json={"query": query, "variables": variables or {}},
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise DagsterGraphQLError(str(payload["errors"]))
        return payload["data"]

    def test_connection(self) -> None:
        """Raise if the endpoint is unreachable or auth fails."""
        self._execute("query { version }")

    def get_repositories(self) -> List[DagsterRepository]:
        data = self._execute(REPOSITORIES_QUERY)
        result = data["repositoriesOrError"]
        typename = result.get("__typename")
        if typename == "PythonError":
            raise DagsterGraphQLError(result.get("message", "Unknown Dagster error"))
        if typename != "RepositoryConnection":
            raise DagsterGraphQLError(f"Unexpected response type: {typename}")

        return [self._parse_repository(node) for node in result.get("nodes", [])]

    # --- parsing helpers: raw GraphQL -> dataclasses ---

    @staticmethod
    def _parse_owners(
        raw_owners: Optional[List[Dict[str, Any]]],
    ) -> List[DagsterOwner]:
        owners: List[DagsterOwner] = []
        for owner in raw_owners or []:
            if owner.get("email"):
                owners.append(DagsterOwner(email=owner["email"]))
            elif owner.get("team"):
                owners.append(DagsterOwner(team=owner["team"]))
        return owners

    @staticmethod
    def _parse_tags(raw_tags: Optional[List[Dict[str, Any]]]) -> List[DagsterTag]:
        return [
            DagsterTag(key=tag["key"], value=tag.get("value") or None)
            for tag in raw_tags or []
            if tag.get("key")
        ]

    @staticmethod
    def _parse_metadata(
        raw_entries: Optional[List[Dict[str, Any]]],
    ) -> DagsterAssetMetadata:
        metadata = DagsterAssetMetadata()
        for entry in raw_entries or []:
            typename = entry.get("__typename")
            label = entry.get("label", "")
            if typename == "MarkdownMetadataEntry" and entry.get("mdStr"):
                metadata.custom_properties[label] = entry["mdStr"]
            elif typename == "UrlMetadataEntry" and entry.get("url"):
                metadata.links.append(
                    DagsterLink(url=entry["url"], description=label or entry["url"])
                )
            elif typename == "CodeReferencesMetadataEntry":
                for ref in entry.get("codeReferences") or []:
                    if ref.get("url"):
                        metadata.links.append(
                            DagsterLink(
                                url=ref["url"], description=label or "source code"
                            )
                        )
            elif typename == "TableSchemaMetadataEntry":
                schema = entry.get("schema") or {}
                columns = [
                    DagsterColumn(
                        name=col["name"],
                        native_type=col.get("type", ""),
                        description=col.get("description"),
                        nullable=(col.get("constraints") or {}).get("nullable", True),
                    )
                    for col in schema.get("columns") or []
                ]
                if columns:
                    metadata.columns = columns
            else:
                value = DagsterGraphQLClient._scalar_metadata_value(entry)
                if value is not None and label:
                    metadata.custom_properties[label] = value
        return metadata

    @staticmethod
    def _scalar_metadata_value(entry: Dict[str, Any]) -> Optional[str]:
        for key in ("text", "path", "jsonString", "intRepr", "floatRepr"):
            if entry.get(key) is not None:
                return str(entry[key])
        if entry.get("boolValue") is not None:
            return str(entry["boolValue"])
        return None

    @staticmethod
    def _key_paths(raw_keys: Optional[List[Dict[str, Any]]]) -> List[List[str]]:
        return [key["path"] for key in raw_keys or [] if key.get("path")]

    def _parse_asset(self, raw: Dict[str, Any]) -> DagsterAsset:
        return DagsterAsset(
            key=raw["assetKey"]["path"],
            group_name=raw.get("groupName"),
            op_names=raw.get("opNames") or [],
            job_names=raw.get("jobNames") or [],
            description=raw.get("description"),
            compute_kind=raw.get("computeKind"),
            kinds=raw.get("kinds") or [],
            owners=self._parse_owners(raw.get("owners")),
            tags=self._parse_tags(raw.get("tags")),
            upstream_keys=self._key_paths(raw.get("dependencyKeys")),
            downstream_keys=self._key_paths(raw.get("dependedByKeys")),
            metadata=self._parse_metadata(raw.get("metadataEntries")),
        )

    def _parse_repository(self, raw: Dict[str, Any]) -> DagsterRepository:
        location_name = (raw.get("location") or {}).get("name", "")
        jobs = [
            DagsterJob(
                name=job["name"],
                location_name=location_name,
                description=job.get("description"),
                owners=self._parse_owners(job.get("owners")),
                tags=self._parse_tags(job.get("tags")),
            )
            for job in raw.get("pipelines") or []
        ]
        assets = [self._parse_asset(asset) for asset in raw.get("assetNodes") or []]
        return DagsterRepository(
            name=raw["name"],
            location_name=location_name,
            jobs=jobs,
            assets=assets,
        )
