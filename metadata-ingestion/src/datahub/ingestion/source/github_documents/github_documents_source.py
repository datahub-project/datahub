"""Ingest documents from a GitHub repository."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from functools import cached_property, partial
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.ingestion.reporting.datahub_ingestion_run_summary_provider import (
    DatahubIngestionRunSummaryProvider,
)
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
from datahub.ingestion.source.github_documents.github_hierarchy import (
    GitHubHierarchyExtractor,
)
from datahub.ingestion.source.github_documents.okf_bundles import (
    RESERVED_FILENAMES,
    discover_bundles,
    enclosing_bundle,
)
from datahub.ingestion.source.github_documents.okf_cleanup import clear_okf_metadata
from datahub.ingestion.source.github_documents.okf_frontmatter import (
    parse_okf_frontmatter,
)
from datahub.ingestion.source.github_documents.okf_mapping import (
    MAPPING_VERSION,
    OkfLookupCache,
    map_okf,
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
    BrowsePathEntryClass,
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentStateClass,
    ExecutionRequestInputClass,
    OkfBundleClass,
    OkfRevisionClass,
    StatusClass,
)
from datahub.metadata.urns import DocumentUrn
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)

EXTRACTION_ALGO_VERSION = "2"
LAST_EXPORTED_CONTENT_HASH_KEY = "last_exported_content_hash"
PROP_IMPORT_SOURCE_ID = "import_source_id"
PROP_GITHUB_FILE_PATH = "github_file_path"
PROP_GITHUB_DIRECTORY_PATH = "github_directory_path"
_DOCUMENT_URN_PREFIX = "urn:li:document:"


def compute_file_content_hash(content: str) -> str:
    """Hash document text plus the extraction version for the sync-back contract.

    content_hash must equal this hash of documentInfo.contents.text. Cloud sync-back
    uses this function from a released OSS package; source interpretation metadata
    belongs in source_content_hash, never in this hash.
    """
    return hashlib.sha256(
        json.dumps(
            {"body": content, "algo_version": EXTRACTION_ALGO_VERSION}, sort_keys=True
        ).encode("utf-8")
    ).hexdigest()


def compute_source_hash(
    content: str, *, bundle: Optional[OkfBundleClass], enable_okf: bool
) -> str:
    """Hash the complete source file and its applicable OKF interpretation."""
    if not enable_okf or bundle is None:
        return compute_file_content_hash(content)
    return hashlib.sha256(
        json.dumps(
            {
                "body": content,
                "algo_version": EXTRACTION_ALGO_VERSION,
                "okf_mapping_version": MAPPING_VERSION,
                "okf_bundle": bundle.to_obj(),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _looks_like_concept(file_path: str, content: str) -> bool:
    path = PurePosixPath(file_path)
    if path.suffix.lower() != ".md" or path.name in RESERVED_FILENAMES:
        return False
    # A throwaway report: the real parse, with its warnings, happens on import.
    frontmatter = parse_okf_frontmatter(
        content, path=file_path, report=GitHubDocumentsSourceReport()
    )
    concept_type = frontmatter.values.get("type") if frontmatter else None
    return isinstance(concept_type, str) and bool(concept_type.strip())


@platform_name("GitHub")
@config_class(GitHubDocumentsSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class GitHubDocumentsSource(StatefulIngestionSourceBase, TestableSource):
    """
    Ingest markdown and text files from a GitHub repository as DataHub Document entities.

    Preserves repository folder structure as parent-child document relationships. By default
    documents are ingested as native (editable) documents; set ``document_import_mode`` to
    ``EXTERNAL`` for read-only references back to GitHub.

    Markdown concepts inside OKF bundles are mapped to OKF and native aspects
    when enable_okf is set. Other files retain their original text.
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
        # Maps used to reconstruct hierarchical browse paths. Documents are
        # emitted parent-before-child, so by the time a child is processed all
        # of its ancestors are already present in these maps.
        self._source_id_to_title: Dict[str, str] = {}
        self._source_id_to_parent: Dict[str, Optional[str]] = {}
        self._repo_root_source_id: Optional[str] = (
            None
            if config.parent_document_urn or not config.create_repo_root_document
            else make_repo_source_id(config.repository)
        )
        # Cache of import_source_id -> existing document URN (sync-back round-trip).
        self._resolved_source_id_urns: Dict[str, Optional[str]] = {}
        self._soft_deleted_cache: Optional[Set[str]] = None
        self._okf_bundles: Dict[str, OkfBundleClass] = {}
        self._okf_index_contents: Dict[str, Optional[str]] = {}
        self._okf_unread_index_dirs: Set[PurePosixPath] = set()
        self._okf_lookup_cache = OkfLookupCache()

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

        commit_sha = self.client.get_latest_commit_sha(owner_repo, branch)
        if not commit_sha or commit_sha == "unknown":
            raise RuntimeError(
                f"Could not resolve branch {branch!r} in {owner_repo} "
                "to a commit snapshot"
            )
        self._okf_lookup_cache = OkfLookupCache()
        files, tree_truncated = self.client.list_matching_files(
            owner_repo, commit_sha, path_prefix, extensions
        )
        if tree_truncated:
            self.report.tree_truncated = True
            self.report.warning(
                title="github-tree-truncated",
                message=(
                    "GitHub returned a truncated tree; "
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
                    "Found more matching files than max_files allows; "
                    "only the first batch will be imported."
                ),
                context=f"repository={owner_repo}, branch={branch}, "
                f"total_matches={total_matches}, max_files={self.config.max_files}",
            )

        self._okf_index_contents.clear()
        self._okf_unread_index_dirs.clear()
        self._okf_bundles = {}
        if self.config.enable_okf:

            def fetch_index(path: str) -> Optional[str]:
                content = self.client.fetch_file_content(owner_repo, path, commit_sha)
                if content is None and self.client.last_api_error:
                    # The tree lists this index, so a failed request is not a
                    # confirmed non-bundle. (An oversized index is permanent and
                    # is treated as no bundle.) The normal file fetch retries it.
                    self._okf_unread_index_dirs.add(PurePosixPath(path).parent)
                elif content is not None:
                    self._okf_index_contents[path] = content
                return content

            self._okf_bundles = discover_bundles(
                repo=owner_repo,
                file_paths=[file.path for file in files],
                index_paths=self.client.index_file_paths,
                explicit_roots=self.config.okf_bundle_roots,
                fetch=fetch_index,
                report=self.report,
            )

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
            PROP_IMPORT_SOURCE_ID: source_id,
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
        self._register_hierarchy(source_id, title, parent_source_id=None)
        self._source_id_to_urn[source_id] = str(doc.urn)
        self._attach_browse_path(doc, source_id)
        self.report.folders_processed += 1
        yield from doc.as_workunits()

    def _emit_folder_document(
        self,
        owner_repo: str,
        branch: str,
        dir_path: str,
        path_prefix: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a metadata-only folder document for a GitHub directory.

        Folder bodies are not synced from GitHub files. DataHub may hold richer
        folder content than GitHub (a deliberate DH ⊇ GH split).
        """
        dir_source_id = make_dir_source_id(owner_repo, dir_path)
        parent_source_id = resolve_parent_dir_source_id(
            owner_repo,
            dir_path,
            path_prefix,
            repo_root_source_id=self._repo_root_source_id,
        )
        parent_urn = self._resolve_parent_urn(parent_source_id)

        title = os.path.basename(dir_path.rstrip("/")) or dir_path
        doc_id, _document_urn = self._resolve_document_identity(
            dir_source_id, owner_repo, dir_path, is_directory=True
        )

        # Folders are metadata-only on import. Re-emitting DocumentInfo for an
        # existing folder (including DataHub-native URNs stamped by sync-back)
        # would UPSERT empty text and replace title/status/customProperties.
        if self._resolved_source_id_urns.get(dir_source_id):
            doc = Document(urn=DocumentUrn(doc_id))
            self._attach_platform_instance(doc, owner_repo)
        else:
            doc = self._build_document(
                doc_id=doc_id,
                title=title,
                text="",
                owner_repo=owner_repo,
                branch=branch,
                github_path=dir_path,
                parent_document=parent_urn,
                custom_properties={
                    "import_source": "github",
                    "github_repo": owner_repo,
                    "github_branch": branch,
                    "github_directory_path": dir_path,
                    "is_folder_document": "true",
                    PROP_IMPORT_SOURCE_ID: dir_source_id,
                },
            )
        doc._set_aspect(StatusClass(removed=False))
        self._register_hierarchy(
            dir_source_id, title, parent_source_id=parent_source_id
        )
        self._source_id_to_urn[dir_source_id] = str(doc.urn)
        self._attach_browse_path(doc, dir_source_id)
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
        content = (
            self._okf_index_contents[file_path]
            if file_path in self._okf_index_contents
            else self.client.fetch_file_content(owner_repo, file_path, commit_sha)
        )
        if content is None:
            self.report.files_skipped += 1
            return

        source_id = make_file_source_id(owner_repo, file_path)
        doc_id, document_urn = self._resolve_document_identity(
            source_id, owner_repo, file_path
        )
        bundle = enclosing_bundle(file_path, self._okf_bundles)
        if self._under_unread_okf_index(file_path) and _looks_like_concept(
            file_path, content
        ):
            # Without its index the file's bundle is unknown. Importing a concept
            # as plain text would clear its OKF state, so keep the previous import.
            self.report.files_skipped += 1
            self._register_document_for_stale_removal(document_urn)
            return
        source_hash = compute_source_hash(
            content, enable_okf=self.config.enable_okf, bundle=bundle
        )

        filename = PurePosixPath(file_path).name
        frontmatter = None
        if (
            self.config.enable_okf
            and bundle
            and filename not in RESERVED_FILENAMES
            and PurePosixPath(file_path).suffix.lower() == ".md"
        ):
            frontmatter = parse_okf_frontmatter(
                content, path=file_path, report=self.report
            )

        concept_type = frontmatter.values.get("type") if frontmatter else None
        is_okf_concept = isinstance(concept_type, str) and bool(concept_type.strip())
        if self._should_skip_unchanged_file(
            document_urn,
            source_hash,
            compute_file_content_hash(content),
            is_okf_concept=is_okf_concept,
        ):
            logger.info("Skipping unchanged file document: %s", file_path)
            self.report.files_skipped_unchanged += 1
            self._register_document_for_stale_removal(document_urn)
            yield from self._resurrect_unchanged_document(document_urn, owner_repo)
            if not is_okf_concept:
                yield from clear_okf_metadata(
                    document_urn, ctx=self.ctx, report=self.report
                )
            return

        parent_source_id = resolve_parent_dir_source_id(
            owner_repo,
            file_path,
            path_prefix,
            repo_root_source_id=self._repo_root_source_id,
        )
        parent_urn = self._resolve_parent_urn(parent_source_id)

        title = os.path.basename(file_path)
        okf_mapping = None
        text = content
        if frontmatter is not None and bundle is not None:
            okf_mapping = map_okf(
                frontmatter,
                document_urn=document_urn,
                bundle=bundle,
                file_path=file_path,
                revision=OkfRevisionClass(
                    ref=branch,
                    commit=commit_sha if commit_sha != "unknown" else None,
                    blobSha=blob_sha,
                ),
                attribution_source=self._okf_attribution_source,
                ctx=self.ctx,
                report=self.report,
                lookup_cache=self._okf_lookup_cache,
            )
            if okf_mapping is not None:
                text = frontmatter.body
                title = okf_mapping.title or title
                self.report.okf_files_processed += 1
        custom_properties = {
            "import_source": "github",
            "github_repo": owner_repo,
            "github_branch": branch,
            "github_file_path": file_path,
            "github_commit_sha": commit_sha,
            PROP_IMPORT_SOURCE_ID: source_id,
            "content_hash": compute_file_content_hash(text),
            "source_content_hash": source_hash,
            "extraction_algo_version": EXTRACTION_ALGO_VERSION,
        }
        if blob_sha:
            custom_properties["github_blob_sha"] = blob_sha

        doc = self._build_document(
            doc_id=doc_id,
            title=title,
            text=text,
            owner_repo=owner_repo,
            branch=branch,
            github_path=file_path,
            parent_document=parent_urn,
            custom_properties=custom_properties,
        )
        # GitHub is source of truth for import: if the file still exists, clear
        # Status.removed so a previously soft-deleted DataHub doc is resurrected.
        # The unchanged-file short-circuit above resurrects too, so this does not
        # depend on whether the content happened to change.
        doc._set_aspect(StatusClass(removed=False))
        if okf_mapping is not None:
            for aspect in okf_mapping.aspects:
                doc._set_aspect(aspect)
        self._register_hierarchy(source_id, title, parent_source_id=parent_source_id)
        self._source_id_to_urn[source_id] = str(doc.urn)
        self._attach_browse_path(doc, source_id)
        self.report.files_processed += 1
        yield from doc.as_workunits()
        if okf_mapping is not None:
            for index, patch in enumerate(okf_mapping.patches):
                yield MetadataWorkUnit(
                    id=f"{document_urn}-okf-{patch.aspectName}-{index}",
                    mcp_raw=patch,
                )
        else:
            yield from clear_okf_metadata(
                document_urn, ctx=self.ctx, report=self.report
            )

    @cached_property
    def _okf_attribution_source(self) -> Optional[str]:
        if self.ctx.run_id.startswith("urn:li:dataHubExecutionRequest:"):
            if self.ctx.graph:
                try:
                    execution = self.ctx.graph.get_aspect(
                        self.ctx.run_id, ExecutionRequestInputClass
                    )
                    if execution and execution.source:
                        return execution.source.ingestionSource
                except Exception as exc:
                    logger.debug("Could not resolve OKF ingestion source: %s", exc)
            return None
        if self.ctx.pipeline_config:
            # Use the same identity as the CLI ingestion reporter.
            key = DatahubIngestionRunSummaryProvider.generate_unique_key(
                self.ctx.pipeline_config
            )
            return f"urn:li:dataHubIngestionSource:cli-{datahub_guid(key)}"
        return None

    def _under_unread_okf_index(self, file_path: str) -> bool:
        path = PurePosixPath(file_path)
        return any(
            directory == PurePosixPath(".") or path.is_relative_to(directory)
            for directory in self._okf_unread_index_dirs
        )

    def _should_skip_unchanged_file(
        self,
        document_urn: str,
        source_hash: str,
        file_text_hash: str,
        *,
        is_okf_concept: bool = False,
    ) -> bool:
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
        stored_hash = props.get("source_content_hash")
        exported_hash = props.get(LAST_EXPORTED_CONTENT_HASH_KEY)
        if source_hash == stored_hash:
            return True
        # A merged sync-back write must be re-imported so the file takes over
        # attribution of the values DataHub wrote into it.
        return not is_okf_concept and file_text_hash == exported_hash

    def _resolve_document_identity(
        self, source_id: str, owner_repo: str, path: str, *, is_directory: bool = False
    ) -> Tuple[str, str]:
        """Return ``(doc_id, document_urn)`` for a GitHub file or directory.

        Prefers an existing document already stamped with this
        ``import_source_id`` (e.g. created in DataHub and synced back) so
        re-import does not mint a second URN and soft-delete the original via
        stale entity removal.
        """
        canonical_doc_id = normalize_document_id(source_id)
        canonical_urn = f"{_DOCUMENT_URN_PREFIX}{canonical_doc_id}"
        existing_urn = self._find_existing_document_urn(
            source_id, owner_repo, path, canonical_urn, is_directory=is_directory
        )
        if existing_urn and existing_urn.startswith(_DOCUMENT_URN_PREFIX):
            return existing_urn[len(_DOCUMENT_URN_PREFIX) :], existing_urn
        return canonical_doc_id, canonical_urn

    def _search_documents_by_properties(
        self, extra_filters: List[Dict[str, Any]]
    ) -> List[str]:
        """Search documents by customProperties, tolerating older servers.

        ``include_hidden_lifecycle_stages`` is what surfaces documents parked in a
        hideInSearch stage (e.g. ``urn:li:lifecycleStageType:DRAFT``). Servers that
        predate the flag reject it outright, which is a permanent condition rather
        than a transient one, so degrade once instead of failing every run.
        """
        assert self.ctx.graph is not None
        if not self.report.hidden_lifecycle_search_unsupported:
            try:
                return list(
                    self.ctx.graph.get_urns_by_filter(
                        entity_types=["document"],
                        status=RemovedStatusFilter.ALL,
                        include_hidden_lifecycle_stages=True,
                        extraFilters=extra_filters,
                    )
                )
            except ValueError as exc:
                self.report.hidden_lifecycle_search_unsupported = True
                self.report.warning(
                    title="Search flag unsupported by this DataHub server",
                    message=(
                        "includeHiddenLifecycleStages is unavailable, so documents "
                        "in hidden lifecycle stages (for example drafts) cannot be "
                        "matched when reusing document identity. Upgrade GMS to "
                        "reuse their URNs instead of minting new ones."
                    ),
                    exc=exc,
                )
        return list(
            self.ctx.graph.get_urns_by_filter(
                entity_types=["document"],
                status=RemovedStatusFilter.ALL,
                extraFilters=extra_filters,
            )
        )

    def _find_existing_document_urn(
        self,
        source_id: str,
        owner_repo: str,
        path: str,
        canonical_urn: str,
        *,
        is_directory: bool,
    ) -> Optional[str]:
        if source_id in self._resolved_source_id_urns:
            return self._resolved_source_id_urns[source_id]
        if not self.ctx.graph:
            self._resolved_source_id_urns[source_id] = None
            return None

        # Fast path: classic GitHub imports already live at the canonical URN
        # with this import_source_id. Avoid a search round-trip per file.
        try:
            aspect = self.ctx.graph.get_aspect(canonical_urn, DocumentInfoClass)
            if (
                aspect
                and aspect.customProperties
                and aspect.customProperties.get(PROP_IMPORT_SOURCE_ID) == source_id
            ):
                self._resolved_source_id_urns[source_id] = canonical_urn
                return canonical_urn
        except Exception as exc:
            # Counted, not just logged: a persistent condition here (expired token,
            # permission denied) silently degrades every file to the slow path, and
            # operators read the report rather than executor logs.
            self.report.identity_fast_path_errors += 1
            logger.debug(
                "Could not load canonical document %s for identity short-circuit: %s",
                canonical_urn,
                exc,
            )

        # Slow path: DataHub-native URNs stamped by sync-back (import_source_id
        # on a non-canonical URN). Pure GitHub imports set import_source_id on
        # first ingest; DataHub-created docs get it on first successful sync-back.
        try:
            matches = self._search_documents_by_properties(
                [
                    {
                        "field": "customProperties",
                        "condition": "EQUAL",
                        "values": [f"{PROP_IMPORT_SOURCE_ID}={source_id}"],
                    }
                ]
            )
        except Exception as exc:
            # Reported as a failure, not a warning: StaleEntityRemovalHandler skips
            # soft-deletion entirely when the source has failures. Without that,
            # falling back to the canonical URN would mint a duplicate and reap the
            # real document (and its children). The URN we would need to pass to
            # add_urn_to_skip() is exactly what this lookup failed to determine.
            self.report.failure(
                title="Document identity lookup failed",
                message=(
                    "Could not search for an existing document for this GitHub "
                    "path. Stale entity removal is skipped for this run so no "
                    "document is soft-deleted; re-run once search is reachable."
                ),
                context=f"path={path}, source_id={source_id}",
                exc=exc,
            )
            self._resolved_source_id_urns[source_id] = None
            return None

        if not matches:
            # Fallback for documents whose import_source_id predates this scheme:
            # match the stamped GitHub path instead. The key differs by kind: a
            # folder document carries github_directory_path and no
            # github_file_path, so querying the file key for a directory could
            # never match.
            path_property = (
                PROP_GITHUB_DIRECTORY_PATH if is_directory else PROP_GITHUB_FILE_PATH
            )
            try:
                matches = self._search_documents_by_properties(
                    [
                        {
                            "field": "customProperties",
                            "condition": "EQUAL",
                            "values": [f"{path_property}={path}"],
                        },
                        {
                            "field": "customProperties",
                            "condition": "EQUAL",
                            "values": [f"github_repo={owner_repo}"],
                        },
                    ]
                )
            except Exception as exc:
                self.report.failure(
                    title="Document identity lookup failed",
                    message=(
                        "Fallback github_file_path search failed for this GitHub "
                        "path. Stale entity removal is skipped for this run so no "
                        "document is soft-deleted; re-run once search is reachable."
                    ),
                    context=f"path={path}, source_id={source_id}",
                    exc=exc,
                )
                self._resolved_source_id_urns[source_id] = None
                return None

        chosen = self._prefer_existing_document_urn(
            matches, canonical_urn, path=path, source_id=source_id
        )
        if chosen and chosen != canonical_urn:
            logger.info(
                "Reusing existing document %s for GitHub path %s (source_id=%s)",
                chosen,
                path,
                source_id,
            )
        self._resolved_source_id_urns[source_id] = chosen
        return chosen

    def _prefer_existing_document_urn(
        self,
        matches: List[str],
        canonical_urn: str,
        *,
        path: str,
        source_id: str,
    ) -> Optional[str]:
        """Prefer a DataHub-native (non-canonical) URN so children are preserved."""
        if not matches:
            return None
        unique = sorted(dict.fromkeys(matches))
        if len(unique) > 1:
            self.report.warning(
                title="Ambiguous document identity for GitHub path",
                message=(
                    "More than one DataHub document claims this GitHub path. One was "
                    "reused; the others were left in place this run. Merge or delete "
                    "the duplicates, or rename them so their titles map to distinct "
                    "file names."
                ),
                context=f"path={path}, source_id={source_id}, candidates={unique}",
            )
        non_canonical = [urn for urn in unique if urn != canonical_urn]
        chosen = non_canonical[0] if non_canonical else unique[0]
        for urn in unique:
            if urn != chosen:
                self._register_document_for_stale_removal(urn)
        return chosen

    def _register_document_for_stale_removal(self, document_urn: str) -> None:
        self.stale_entity_removal_handler.add_entity_to_state("document", document_urn)

    def _register_hierarchy(
        self, source_id: str, title: str, parent_source_id: Optional[str]
    ) -> None:
        """Record a document's title and parent link for browse-path building."""
        self._source_id_to_title[source_id] = title
        self._source_id_to_parent[source_id] = parent_source_id

    def _browse_path_prefix_entries(self) -> List[BrowsePathEntryClass]:
        """Build the leading browse-path entries shared by all documents.

        Order (top-most first): configured parent document, organization, then
        repository. The synthetic repository entry is only added when no
        repo-root document exists; otherwise that document already provides a
        (clickable) repository entry in each descendant's ancestor chain and
        the repo-root document itself must not list itself as a parent.
        """
        entries: List[BrowsePathEntryClass] = []

        if self.config.parent_document_urn:
            entries.append(
                BrowsePathEntryClass(
                    id=self.config.parent_document_urn,
                    urn=self.config.parent_document_urn,
                )
            )

        owner_repo = self.config.repository
        org_name, _, repo_name = owner_repo.partition("/")

        if self.config.include_organization_in_browse_path and org_name:
            entries.append(BrowsePathEntryClass(id=org_name))

        if (
            self.config.include_repository_in_browse_path
            and repo_name
            and self._repo_root_source_id is None
        ):
            entries.append(BrowsePathEntryClass(id=repo_name))

        return entries

    def _attach_browse_path(self, doc: Document, source_id: str) -> None:
        """Attach a BrowsePathsV2 aspect describing the document's ancestry.

        Browse paths are a navigation enhancement, not core document data, so any
        failure here is logged and the document is emitted without one rather
        than failing ingestion.
        """
        try:
            browse_path = GitHubHierarchyExtractor.build_browse_path_v2(
                source_id=source_id,
                parent_links=self._source_id_to_parent,
                titles=self._source_id_to_title,
                urns=self._source_id_to_urn,
                prefix_entries=self._browse_path_prefix_entries(),
            )
            if browse_path:
                doc._set_aspect(browse_path)
        except Exception as exc:
            self.report.warning(
                title="Browse path generation failed",
                message=(
                    "Failed to build the browse path for a document; it was "
                    "emitted without one. Hierarchical navigation may be "
                    "incomplete for the affected document."
                ),
                context=f"source_id={source_id}",
                exc=exc,
            )

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

    def _soft_deleted_urns(self, owner_repo: str) -> Set[str]:
        """URNs this repo previously imported that are currently soft-deleted.

        Loaded with a single search so the unchanged-file path can resurrect a
        document without paying a Status lookup per file.
        """
        if self._soft_deleted_cache is not None:
            return self._soft_deleted_cache

        urns: Set[str] = set()
        if self.ctx.graph:
            try:
                urns = set(
                    self.ctx.graph.get_urns_by_filter(
                        entity_types=["document"],
                        platform=self.platform,
                        platform_instance=make_dataplatform_instance_urn(
                            self.platform, owner_repo
                        ),
                        status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                    )
                )
            except Exception as exc:
                # Non-fatal: only costs us resurrection of documents whose file came
                # back to GitHub unchanged. Import itself is unaffected.
                logger.debug("Could not list soft-deleted documents: %s", exc)
        self._soft_deleted_cache = urns
        return urns

    def _resurrect_unchanged_document(
        self, document_urn: str, owner_repo: str
    ) -> Iterable[MetadataWorkUnit]:
        """Clear Status.removed for an unchanged file that exists in GitHub again.

        Without this, deleting a file, letting stale removal soft-delete its
        document, then restoring the file with identical content would leave the
        document soft-deleted forever, since the unchanged-file short-circuit
        returns before any Status is emitted.
        """
        if document_urn not in self._soft_deleted_urns(owner_repo):
            return
        self.report.documents_resurrected += 1
        yield MetadataChangeProposalWrapper(
            entityUrn=document_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

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
