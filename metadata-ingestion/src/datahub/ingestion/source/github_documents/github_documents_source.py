"""Ingest documents from a GitHub repository."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from functools import partial
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.github_documents.github_api import (
    GitHubApiClient,
    StaticTokenProvider,
    collect_intermediate_directories,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
    normalize_document_id,
    resolve_parent_dir_source_id,
)
from datahub.ingestion.source.github_documents.github_documents_config import (
    GitHubDocumentsSourceConfig,
)
from datahub.ingestion.source.github_documents.github_documents_report import (
    GitHubDocumentsSourceReport,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentStateClass,
)
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)

EXTRACTION_ALGO_VERSION = "1"
LAST_EXPORTED_CONTENT_HASH_KEY = "last_exported_content_hash"


def compute_file_content_hash(content: str) -> str:
    """Hash raw file body for change detection (plain markdown/text, no frontmatter)."""
    hash_input = json.dumps(
        {"body": content, "algo_version": EXTRACTION_ALGO_VERSION},
        sort_keys=True,
    )
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


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

    GitHub files are imported as plain text/markdown. Document metadata lives in DataHub
    aspects and customProperties, not in repo file headers.
    """

    platform = "github"

    def __init__(self, config: GitHubDocumentsSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report: GitHubDocumentsSourceReport = GitHubDocumentsSourceReport()
        self.client = GitHubApiClient(
            StaticTokenProvider(config.github_token.get_secret_value())
        )
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            state_provider=self.state_provider,
            report=self.report,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            platform=self.platform,
        )
        self._source_id_to_urn: Dict[str, str] = {}
        self._repo_root_source_id: Optional[str] = (
            None
            if config.parent_document_urn or not config.create_repo_root_document
            else make_repo_source_id(config.repository)
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GitHubDocumentsSource":
        config = GitHubDocumentsSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(self):
        # Manual stale-removal handler supports registering skipped unchanged files.
        return [AutoStaleEntityRemovalProcessor]

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(auto_stale_entity_removal, self.stale_entity_removal_handler),
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        owner_repo = self.config.repository
        branch = self.config.branch
        path_prefix = self.config.path_prefix.strip("/")
        extensions = self.config.file_extensions

        files, tree_truncated = self.client.list_matching_files(
            owner_repo, branch, path_prefix, extensions
        )
        if tree_truncated:
            self.report.tree_truncated = True
            self.report.warning(
                title="github-tree-truncated",
                message=(
                    f"GitHub returned a truncated tree for {owner_repo}@{branch}; "
                    "only a subset of files will be imported. "
                    "Narrow path_prefix or split across multiple ingestion sources."
                ),
                context=f"repository={owner_repo}, branch={branch}",
            )
        if not files:
            logger.info("No matching files found in %s (branch=%s)", owner_repo, branch)
            return

        if len(files) > self.config.max_files:
            total_matches = len(files)
            files = files[: self.config.max_files]
            self.report.files_truncated_by_limit = True
            self.report.warning(
                title="github-files-truncated",
                message=(
                    f"Found {total_matches} matching files in {owner_repo}@{branch} but "
                    f"max_files is {self.config.max_files}; only the first "
                    f"{self.config.max_files} will be imported."
                ),
                context=f"repository={owner_repo}, branch={branch}",
            )

        commit_sha = self.client.get_latest_commit_sha(owner_repo, branch)

        if self._repo_root_source_id:
            yield from self._emit_repo_root_document(
                owner_repo=owner_repo, branch=branch
            )

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
                blob_sha=file.sha,
            )

    def _emit_repo_root_document(
        self, owner_repo: str, branch: str
    ) -> Iterable[MetadataWorkUnit]:
        source_id = self._repo_root_source_id
        if not source_id:
            return

        title = owner_repo.split("/")[-1] if "/" in owner_repo else owner_repo
        doc_id = normalize_document_id(source_id)
        custom_properties = {
            "import_source": "github",
            "github_repo": owner_repo,
            "github_branch": branch,
            "is_repo_root_document": "true",
            "import_source_id": source_id,
        }

        doc = self._build_document(
            doc_id=doc_id,
            title=title,
            text="",
            owner_repo=owner_repo,
            branch=branch,
            github_path="",
            parent_document=None,
            custom_properties=custom_properties,
        )
        self._source_id_to_urn[source_id] = str(doc.urn)
        self.report.folders_processed += 1
        yield from doc.as_workunits()

    def _emit_folder_document(
        self,
        owner_repo: str,
        branch: str,
        dir_path: str,
        path_prefix: str,
    ) -> Iterable[MetadataWorkUnit]:
        source_id = make_dir_source_id(owner_repo, dir_path)
        parent_source_id = resolve_parent_dir_source_id(
            owner_repo,
            dir_path,
            path_prefix,
            repo_root_source_id=self._repo_root_source_id,
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
        self._source_id_to_urn[source_id] = str(doc.urn)
        self.report.folders_processed += 1
        yield from doc.as_workunits()

    def _emit_file_document(
        self,
        owner_repo: str,
        branch: str,
        path_prefix: str,
        file_path: str,
        commit_sha: str,
        blob_sha: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        content = self.client.fetch_file_content(owner_repo, file_path, branch)
        if content is None:
            self.report.files_skipped += 1
            return

        source_id = make_file_source_id(owner_repo, file_path)
        doc_id = normalize_document_id(source_id)
        document_urn = f"urn:li:document:{doc_id}"
        content_hash = compute_file_content_hash(content)

        if self._should_skip_unchanged_file(document_urn, content_hash):
            logger.info("Skipping unchanged file document: %s", file_path)
            self.report.files_skipped_unchanged += 1
            self._register_document_for_stale_removal(document_urn)
            return

        parent_source_id = resolve_parent_dir_source_id(
            owner_repo,
            file_path,
            path_prefix,
            repo_root_source_id=self._repo_root_source_id,
        )
        parent_urn = self._resolve_parent_urn(parent_source_id)

        title = os.path.basename(file_path)
        custom_properties = {
            "import_source": "github",
            "github_repo": owner_repo,
            "github_branch": branch,
            "github_file_path": file_path,
            "github_commit_sha": commit_sha,
            "import_source_id": source_id,
            "content_hash": content_hash,
            "extraction_algo_version": EXTRACTION_ALGO_VERSION,
        }
        if blob_sha:
            custom_properties["github_blob_sha"] = blob_sha

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
        self._source_id_to_urn[source_id] = str(doc.urn)
        self.report.files_processed += 1
        yield from doc.as_workunits()

    def _should_skip_unchanged_file(self, document_urn: str, content_hash: str) -> bool:
        if not self.ctx.graph:
            return False

        try:
            document_info = self.ctx.graph.get_aspect(document_urn, DocumentInfoClass)
        except Exception as exc:
            logger.debug(
                "Could not load existing document %s for change detection: %s",
                document_urn,
                exc,
            )
            return False

        if not document_info or not document_info.customProperties:
            return False

        props = document_info.customProperties
        stored_hash = props.get("content_hash")
        exported_hash = props.get(LAST_EXPORTED_CONTENT_HASH_KEY)
        return content_hash in (stored_hash, exported_hash)

    def _register_document_for_stale_removal(self, document_urn: str) -> None:
        self.stale_entity_removal_handler.add_entity_to_state("document", document_urn)

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

    def _build_github_external_url(
        self, owner_repo: str, branch: str, github_path: str
    ) -> str:
        return (
            f"https://github.com/{owner_repo}/blob/{branch}/{github_path.lstrip('/')}"
        )

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
        external_url = self._build_github_external_url(owner_repo, branch, github_path)
        external_id = github_path or owner_repo

        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            doc = Document.create_document(
                id=doc_id,
                title=title,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                custom_properties=custom_properties,
                parent_document=parent_document,
                show_in_global_context=self.config.show_in_global_context,
            )
            doc.set_source(
                "NATIVE",
                external_url=external_url,
                external_id=external_id,
            )
        else:
            doc = Document.create_external_document(
                id=doc_id,
                title=title,
                platform=self.platform,
                external_url=external_url,
                external_id=external_id,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                custom_properties=custom_properties,
                parent_document=parent_document,
                show_in_global_context=self.config.show_in_global_context,
            )

        self._attach_platform_instance(doc, owner_repo)
        return doc

    def _attach_platform_instance(self, doc: Document, owner_repo: str) -> None:
        doc._set_aspect(
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(self.platform, owner_repo),
            )
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
            client = GitHubApiClient(
                StaticTokenProvider(config.github_token.get_secret_value())
            )
            client.list_matching_files(
                config.repository,
                config.branch,
                config.path_prefix.strip("/"),
                config.file_extensions[:1] or [".md"],
            )
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )
        except Exception as exc:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=str(exc)
                ),
                internal_failure=True,
                internal_failure_reason=(
                    f"Failed to connect to GitHub repository: {exc}. "
                    "Verify repository, branch, token, and network access."
                ),
            )
