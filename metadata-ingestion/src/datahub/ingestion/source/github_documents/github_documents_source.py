"""Ingest documents from a GitHub repository."""

from __future__ import annotations

import logging
import os
from typing import Dict, Iterable, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.github_documents.github_api import (
    GitHubApiClient,
    collect_intermediate_directories,
    make_dir_source_id,
    make_file_source_id,
    normalize_document_id,
    resolve_parent_dir_source_id,
)
from datahub.ingestion.source.github_documents.github_documents_config import (
    DocumentImportMode,
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import DocumentStateClass
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)


@platform_name("GitHub")
@config_class(GitHubDocumentsSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class GitHubDocumentsSource(StatefulIngestionSourceBase, TestableSource):
    """
    Ingest markdown and text files from a GitHub repository as DataHub Document entities.

    Preserves repository folder structure as parent-child document relationships. By default
    documents are ingested as native (editable) documents; set ``document_import_mode`` to
    ``EXTERNAL`` for read-only references back to GitHub.
    """

    platform = "github"

    def __init__(self, config: GitHubDocumentsSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = GitHubDocumentsSourceReport()
        self.client = GitHubApiClient(config.github_token.get_secret_value())
        self._source_id_to_urn: Dict[str, str] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GitHubDocumentsSource":
        config = GitHubDocumentsSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        owner_repo = self.config.repository
        branch = self.config.branch
        path_prefix = self.config.path_prefix.strip("/")
        extensions = self.config.file_extensions

        files = self.client.list_matching_files(
            owner_repo, branch, path_prefix, extensions
        )
        if not files:
            logger.info("No matching files found in %s (branch=%s)", owner_repo, branch)
            return

        commit_sha = self.client.get_latest_commit_sha(owner_repo, branch)
        dir_paths = sorted(
            collect_intermediate_directories(files, path_prefix),
            key=lambda path: path.count("/"),
        )

        for dir_path in dir_paths:
            yield from self._emit_folder_document(
                owner_repo=owner_repo,
                branch=branch,
                dir_path=dir_path,
                path_prefix=path_prefix,
            )

        for file in files:
            yield from self._emit_file_document(
                owner_repo=owner_repo,
                branch=branch,
                path_prefix=path_prefix,
                file_path=file.path,
                commit_sha=commit_sha,
            )

    def _emit_folder_document(
        self,
        owner_repo: str,
        branch: str,
        dir_path: str,
        path_prefix: str,
    ) -> Iterable[MetadataWorkUnit]:
        source_id = make_dir_source_id(owner_repo, dir_path)
        parent_source_id = resolve_parent_dir_source_id(
            owner_repo, dir_path, path_prefix
        )
        parent_urn = self._resolve_parent_urn(parent_source_id)

        title = os.path.basename(dir_path.rstrip("/")) or dir_path
        doc_id = normalize_document_id(source_id)
        custom_properties = {
            "import_source": "github",
            "github_repo": owner_repo,
            "github_branch": branch,
            "github_directory_path": dir_path,
            "is_folder_document": "true",
            "import_source_id": source_id,
        }

        doc = self._build_document(
            doc_id=doc_id,
            title=title,
            text="",
            owner_repo=owner_repo,
            branch=branch,
            github_path=dir_path,
            parent_document=parent_urn,
            custom_properties=custom_properties,
        )
        self._source_id_to_urn[source_id] = doc.urn
        self.report.folders_processed += 1
        yield from doc.as_workunits()

    def _emit_file_document(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        file_path: str,
        commit_sha: str,
    ) -> Iterable[MetadataWorkUnit]:
        content = self.client.fetch_file_content(owner_repo, file_path, branch)
        if content is None:
            self.report.files_skipped += 1
            return

        source_id = make_file_source_id(owner_repo, file_path)
        parent_source_id = resolve_parent_dir_source_id(
            owner_repo, file_path, path_prefix
        )
        parent_urn = self._resolve_parent_urn(parent_source_id)

        title = os.path.basename(file_path)
        doc_id = normalize_document_id(source_id)
        custom_properties = {
            "import_source": "github",
            "github_repo": owner_repo,
            "github_branch": branch,
            "github_file_path": file_path,
            "github_commit_sha": commit_sha,
            "import_source_id": source_id,
        }

        doc = self._build_document(
            doc_id=doc_id,
            title=title,
            text=content,
            owner_repo=owner_repo,
            branch=branch,
            github_path=file_path,
            parent_document=parent_urn,
            custom_properties=custom_properties,
        )
        self._source_id_to_urn[source_id] = doc.urn
        self.report.files_processed += 1
        yield from doc.as_workunits()

    def _resolve_parent_urn(self, parent_source_id: Optional[str]) -> Optional[str]:
        if parent_source_id:
            parent_urn = self._source_id_to_urn.get(parent_source_id)
            if parent_urn:
                return parent_urn
            logger.warning(
                "Parent source id %s was not found; falling back to configured root parent",
                parent_source_id,
            )
        return self.config.parent_document_urn

    def _build_document(
        self,
        doc_id: str,
        title: str,
        text: str,
        owner_repo: str,
        branch: str,
        github_path: str,
        parent_document: Optional[str],
        custom_properties: Dict[str, str],
    ) -> Document:
        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            return Document.create_document(
                id=doc_id,
                title=title,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                custom_properties=custom_properties,
                parent_document=parent_document,
                show_in_global_context=self.config.show_in_global_context,
            )

        external_url = (
            f"https://github.com/{owner_repo}/blob/{branch}/{github_path.lstrip('/')}"
        )
        return Document.create_external_document(
            id=doc_id,
            title=title,
            platform=self.platform,
            external_url=external_url,
            external_id=github_path,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            custom_properties=custom_properties,
            parent_document=parent_document,
            show_in_global_context=self.config.show_in_global_context,
        )

    def get_report(self) -> GitHubDocumentsSourceReport:
        return self.report

    @classmethod
    def test_connection(cls, config_dict: dict) -> TestConnectionReport:
        try:
            config = GitHubDocumentsSourceConfig.parse_obj(config_dict)
        except Exception as exc:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {exc}",
            )

        try:
            client = GitHubApiClient(config.github_token.get_secret_value())
            client.list_matching_files(
                config.repository,
                config.branch,
                config.path_prefix.strip("/"),
                config.file_extensions[:1] or [".md"],
            )
            return TestConnectionReport(basic_connectivity="PASS")
        except Exception as exc:
            return TestConnectionReport(
                basic_connectivity="FAIL",
                internal_failure=True,
                internal_failure_reason=(
                    f"Failed to connect to GitHub repository: {exc}. "
                    "Verify repository, branch, token, and network access."
                ),
            )
