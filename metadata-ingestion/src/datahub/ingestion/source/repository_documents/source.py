from __future__ import annotations

import hashlib
import json
import logging
import os
from abc import abstractmethod
from functools import cached_property, partial
from pathlib import PurePosixPath
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.ingestion.reporting.datahub_ingestion_run_summary_provider import (
    DatahubIngestionRunSummaryProvider,
)
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.repository_documents.client import (
    RepositoryDocumentsClient,
)
from datahub.ingestion.source.repository_documents.config import (
    RepositoryDocumentsSourceConfigBase,
)
from datahub.ingestion.source.repository_documents.hierarchy import (
    RepositoryHierarchyExtractor,
)
from datahub.ingestion.source.repository_documents.okf_bundles import (
    RESERVED_FILENAMES,
    discover_bundles,
    enclosing_bundle,
)
from datahub.ingestion.source.repository_documents.okf_cleanup import clear_okf_metadata
from datahub.ingestion.source.repository_documents.okf_frontmatter import (
    OkfFrontmatter,
    is_okf_concept,
    parse_okf_frontmatter,
    read_okf_frontmatter,
)
from datahub.ingestion.source.repository_documents.okf_mapping import (
    MAPPING_VERSION,
    OkfLookupCache,
    OkfMapping,
    map_okf,
)
from datahub.ingestion.source.repository_documents.paths import (
    collect_intermediate_directories,
    make_dir_source_id,
    make_file_source_id,
    make_repo_source_id,
    make_source_prefix,
    normalize_document_id,
    property_key,
    resolve_parent_dir_source_id,
)
from datahub.ingestion.source.repository_documents.report import (
    RepositoryDocumentsSourceReport,
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
_DOCUMENT_URN_PREFIX = "urn:li:document:"

ConfigT = TypeVar("ConfigT", bound=RepositoryDocumentsSourceConfigBase)


def _property_filter(key: str, value: str) -> Dict[str, Any]:
    """A search filter matching documents whose custom property ``key`` is ``value``."""
    return {
        "field": "customProperties",
        "condition": "EQUAL",
        "values": [f"{key}={value}"],
    }


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
    # Not reported: the real parse, with its warnings, happens on import.
    return read_okf_frontmatter(content).frontmatter is not None


class RepositoryDocumentsSourceBase(
    StatefulIngestionSourceBase, TestableSource, Generic[ConfigT]
):
    """
    Shared import logic for documents sources backed by a git host.

    Subclasses parametrize it with their config class, set ``platform`` and
    supply the API client and file URLs. The
    platform name also prefixes source ids and the ``<platform>_*`` custom
    properties, so documents from different hosts never share an identity.

    Hooks for subclasses. Each host source must define ``platform``,
    ``platform_display_name``, ``_parse_config``, ``_make_client``,
    ``_repository_url`` and ``_build_external_url``. These may also be
    overridden, and keep their signatures across releases:

    - ``_create_client``: build the client from the source instance rather
      than from the config alone.
    - ``_map_okf``: adjust the metadata mapped from OKF frontmatter.
    - ``_build_document``: adjust the document built for a file or folder.

    Other underscore methods are internal and may change without notice.
    """

    platform: ClassVar[str]
    # Human-readable host name used in report messages.
    platform_display_name: ClassVar[str]
    config: ConfigT

    def __init__(self, config: ConfigT, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report: RepositoryDocumentsSourceReport = RepositoryDocumentsSourceReport()
        self.client: RepositoryDocumentsClient = self._create_client()
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
            else make_repo_source_id(
                config.repository, source_prefix=self._source_prefix
            )
        )
        # Cache of import_source_id -> existing document URN (sync-back round-trip).
        self._resolved_source_id_urns: Dict[str, Optional[str]] = {}
        self._last_checkpoint_urns: Optional[Set[str]] = None
        self._soft_deleted_cache: Optional[Set[str]] = None
        self._okf_bundles: Dict[str, OkfBundleClass] = {}
        self._okf_index_contents: Dict[str, Optional[str]] = {}
        self._okf_unread_index_dirs: Set[PurePosixPath] = set()
        self._okf_lookup_cache = OkfLookupCache()

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "RepositoryDocumentsSourceBase[ConfigT]":
        return cls(cls._parse_config(config_dict), ctx)

    @classmethod
    @abstractmethod
    def _parse_config(cls, config_dict: dict) -> ConfigT:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _make_client(cls, config: ConfigT) -> RepositoryDocumentsClient:
        raise NotImplementedError

    def _create_client(self) -> RepositoryDocumentsClient:
        return self._make_client(self.config)

    @abstractmethod
    def _repository_url(self, owner_repo: str) -> str:
        """Canonical repository URL: no branch, no trailing slash."""
        raise NotImplementedError

    @abstractmethod
    def _build_external_url(
        self, owner_repo: str, branch: str, path: str, *, is_directory: bool
    ) -> str:
        raise NotImplementedError

    @property
    def _source_prefix(self) -> str:
        """Prefix of every source id. A configured ``platform_instance`` is
        part of it, so sources on different instances never share an id."""
        return make_source_prefix(self.platform, self.config.platform_instance)

    def _instance_name(self, owner_repo: str) -> str:
        """The document's data platform instance: the configured
        ``platform_instance``, or the repository when none is set."""
        return self.config.platform_instance or owner_repo

    def _prop(self, name: str) -> str:
        return property_key(self.platform, name)

    def _file_source_id(self, file_path: str) -> str:
        return make_file_source_id(
            self.config.repository, file_path, source_prefix=self._source_prefix
        )

    def _dir_source_id(self, dir_path: str) -> str:
        return make_dir_source_id(
            self.config.repository, dir_path, source_prefix=self._source_prefix
        )

    def _parent_source_id(self, path: str) -> Optional[str]:
        """The source id of the folder (or repository root) holding ``path``."""
        return resolve_parent_dir_source_id(
            self.config.repository,
            path,
            self.config.path_prefix.strip("/"),
            repo_root_source_id=self._repo_root_source_id,
            source_prefix=self._source_prefix,
        )

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

        commit_sha = self._resolve_commit_sha(self.client, owner_repo, branch)
        self._okf_lookup_cache = OkfLookupCache()
        files, tree_truncated = self.client.list_matching_files(
            owner_repo, commit_sha, path_prefix, extensions
        )
        if tree_truncated:
            self.report.tree_truncated = True
            # Files missing from a partial listing must not be treated as deleted
            # from the repository.
            self._keep_previous_documents()
            self.report.warning(
                title="Repository tree truncated",
                message=(
                    "The git host returned a truncated file tree; only a subset of "
                    "files will be imported and documents imported by earlier runs "
                    "are kept. Narrow path_prefix or split across multiple "
                    "ingestion sources."
                ),
                context=f"platform={self.platform}, repository={owner_repo}, "
                f"branch={branch}, api_error={self.client.last_api_error}",
            )
        if not files:
            logger.info("No matching files found in %s (branch=%s)", owner_repo, branch)
            return

        if len(files) > self.config.max_files:
            total_matches = len(files)
            files = files[: self.config.max_files]
            self.report.files_truncated_by_limit = True
            self._keep_previous_documents()
            self.report.warning(
                title="Matching files truncated by max_files",
                message=(
                    "Found more matching files than max_files allows; "
                    "only the first batch will be imported and documents "
                    "imported by earlier runs are kept."
                ),
                context=f"platform={self.platform}, repository={owner_repo}, "
                f"branch={branch}, total_matches={total_matches}, "
                f"max_files={self.config.max_files}",
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
                location=self._repository_url(owner_repo),
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
            )

        for file in files:
            yield from self._emit_file_document(
                owner_repo=owner_repo,
                branch=branch,
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
        custom_properties = self._base_custom_properties(
            owner_repo=owner_repo,
            branch=branch,
            source_id=source_id,
            extra={"is_repo_root_document": "true"},
        )

        doc = self._build_document(
            doc_id=doc_id,
            title=title,
            text="",
            owner_repo=owner_repo,
            branch=branch,
            repo_path="",
            parent_document=None,
            custom_properties=custom_properties,
            is_directory=True,
        )
        self._register_document(
            doc, source_id, title, parent_source_id=None, is_directory=True
        )
        yield from doc.as_workunits()

    def _emit_folder_document(
        self,
        owner_repo: str,
        branch: str,
        dir_path: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a metadata-only folder document for a repository directory.

        Folder bodies are not synced from repository files. DataHub may hold
        richer folder content than the repository (a deliberate DH ⊇ GH split).
        """
        dir_source_id = self._dir_source_id(dir_path)
        parent_source_id = self._parent_source_id(dir_path)
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
                repo_path=dir_path,
                parent_document=parent_urn,
                custom_properties=self._base_custom_properties(
                    owner_repo=owner_repo,
                    branch=branch,
                    source_id=dir_source_id,
                    extra={
                        self._prop("directory_path"): dir_path,
                        "is_folder_document": "true",
                    },
                ),
                is_directory=True,
            )
        doc._set_aspect(StatusClass(removed=False))
        self._register_document(
            doc, dir_source_id, title, parent_source_id, is_directory=True
        )
        yield from doc.as_workunits()

    def _emit_file_document(
        self,
        owner_repo: str,
        branch: str,
        file_path: str,
        commit_sha: str,
        blob_sha: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        content = (
            self._okf_index_contents[file_path]
            if file_path in self._okf_index_contents
            else self.client.fetch_file_content(owner_repo, file_path, commit_sha)
        )
        source_id = self._file_source_id(file_path)
        if content is None:
            self._skip_unreadable_file(owner_repo, file_path, source_id)
            return

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

        frontmatter = self._okf_frontmatter(file_path, content, bundle)
        if self._should_skip_unchanged_file(
            document_urn,
            source_hash,
            compute_file_content_hash(content),
            is_okf_concept=is_okf_concept(frontmatter),
        ):
            logger.info("Skipping unchanged file document: %s", file_path)
            self.report.files_skipped_unchanged += 1
            self._register_document_for_stale_removal(document_urn)
            yield from self._resurrect_unchanged_document(document_urn)
            if not is_okf_concept(frontmatter):
                yield from clear_okf_metadata(
                    document_urn, ctx=self.ctx, report=self.report
                )
            return

        parent_source_id = self._parent_source_id(file_path)
        parent_urn = self._resolve_parent_urn(parent_source_id)

        title = os.path.basename(file_path)
        okf_mapping = None
        text = content
        if frontmatter is not None and bundle is not None:
            okf_mapping = self._map_okf(
                frontmatter,
                document_urn=document_urn,
                bundle=bundle,
                file_path=file_path,
                revision=OkfRevisionClass(
                    ref=branch, commit=commit_sha, blobSha=blob_sha
                ),
            )
            if okf_mapping is not None:
                text = frontmatter.body
                title = okf_mapping.title or title
                self.report.okf_files_processed += 1
        doc = self._build_document(
            doc_id=doc_id,
            title=title,
            text=text,
            owner_repo=owner_repo,
            branch=branch,
            repo_path=file_path,
            parent_document=parent_urn,
            custom_properties=self._file_custom_properties(
                owner_repo=owner_repo,
                branch=branch,
                file_path=file_path,
                source_id=source_id,
                commit_sha=commit_sha,
                blob_sha=blob_sha,
                text=text,
                source_hash=source_hash,
            ),
        )
        # The repository is source of truth for import: if the file still exists, clear
        # Status.removed so a previously soft-deleted DataHub doc is resurrected.
        # The unchanged-file short-circuit above resurrects too, so this does not
        # depend on whether the content happened to change.
        doc._set_aspect(StatusClass(removed=False))
        if okf_mapping is not None:
            for aspect in okf_mapping.aspects:
                doc._set_aspect(aspect)
        self._register_document(
            doc, source_id, title, parent_source_id, is_directory=False
        )
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

    def _skip_unreadable_file(
        self, owner_repo: str, file_path: str, source_id: str
    ) -> None:
        """Count and report a file whose content could not be read.

        A failed request says nothing about the file, so keep the previous
        import rather than let stale removal delete it. Without a graph match,
        fall back to the canonical URN only if the last run imported it: a file
        never imported has nothing to keep.
        """
        self.report.files_skipped += 1
        api_error = self.client.last_api_error
        if api_error:
            canonical_urn = f"{_DOCUMENT_URN_PREFIX}{normalize_document_id(source_id)}"
            existing_urn = self._find_existing_document_urn(
                source_id, owner_repo, file_path, canonical_urn, is_directory=False
            ) or (
                canonical_urn
                if canonical_urn in self._previous_document_urns()
                else None
            )
            if existing_urn:
                self._register_document_for_stale_removal(existing_urn)
        self.report.warning(
            title="Repository file not read",
            message=(
                "A file could not be read and was skipped. Files that failed "
                "to download keep their previous import; files that are too "
                "large or not text are not imported."
            ),
            context=f"platform={self.platform}, repository={owner_repo}, "
            f"path={file_path}, "
            f"error={api_error or 'file too large or not readable as text'}",
        )

    def _okf_frontmatter(
        self, file_path: str, content: str, bundle: Optional[OkfBundleClass]
    ) -> Optional[OkfFrontmatter]:
        """The file's OKF concept frontmatter, or None when it is not a concept."""
        path = PurePosixPath(file_path)
        if (
            not self.config.enable_okf
            or bundle is None
            or path.name in RESERVED_FILENAMES
            or path.suffix.lower() != ".md"
        ):
            return None
        return parse_okf_frontmatter(content, path=file_path, report=self.report)

    def _base_custom_properties(
        self,
        *,
        owner_repo: str,
        branch: str,
        source_id: str,
        extra: Dict[str, str],
    ) -> Dict[str, str]:
        return {
            "import_source": self.platform,
            self._prop("repo"): owner_repo,
            self._prop("branch"): branch,
            **extra,
            PROP_IMPORT_SOURCE_ID: source_id,
        }

    def _file_custom_properties(
        self,
        *,
        owner_repo: str,
        branch: str,
        file_path: str,
        source_id: str,
        commit_sha: str,
        blob_sha: Optional[str],
        text: str,
        source_hash: str,
    ) -> Dict[str, str]:
        properties = self._base_custom_properties(
            owner_repo=owner_repo,
            branch=branch,
            source_id=source_id,
            extra={
                self._prop("file_path"): file_path,
                self._prop("commit_sha"): commit_sha,
            },
        )
        properties.update(
            {
                "content_hash": compute_file_content_hash(text),
                "source_content_hash": source_hash,
                "extraction_algo_version": EXTRACTION_ALGO_VERSION,
            }
        )
        if blob_sha:
            properties[self._prop("blob_sha")] = blob_sha
        return properties

    def _register_document(
        self,
        doc: Document,
        source_id: str,
        title: str,
        parent_source_id: Optional[str],
        *,
        is_directory: bool,
    ) -> None:
        """Record an emitted document for its descendants and attach its browse path."""
        self._register_hierarchy(source_id, title, parent_source_id=parent_source_id)
        self._source_id_to_urn[source_id] = str(doc.urn)
        self._attach_browse_path(doc, source_id)
        if is_directory:
            self.report.folders_processed += 1
        else:
            self.report.files_processed += 1

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
        """Return ``(doc_id, document_urn)`` for a repository file or directory.

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

        # Fast path: classic imports already live at the canonical URN
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
        # on a non-canonical URN). Pure repository imports set import_source_id on
        # first ingest; DataHub-created docs get it on first successful sync-back.
        try:
            matches = self._search_documents_by_properties(
                [_property_filter(PROP_IMPORT_SOURCE_ID, source_id)]
            )
            if not matches:
                # Fallback for documents whose import_source_id predates this
                # scheme: match the stamped repository path instead. The key
                # differs by kind: a folder document carries
                # <platform>_directory_path and no <platform>_file_path, so
                # querying the file key for a directory could never match.
                path_property = self._prop(
                    "directory_path" if is_directory else "file_path"
                )
                matches = self._search_documents_by_properties(
                    [
                        _property_filter(path_property, path),
                        _property_filter(self._prop("repo"), owner_repo),
                    ]
                )
                matches = [
                    urn
                    for urn in matches
                    if self._is_legacy_or_same_import(urn, source_id)
                ]
        except Exception as exc:
            # Reported as a failure, not a warning: StaleEntityRemovalHandler skips
            # soft-deletion entirely when the source has failures. Without that,
            # falling back to the canonical URN would mint a duplicate and reap the
            # real document (and its children). The URN we would need to pass to
            # add_urn_to_skip() is exactly what this lookup failed to determine.
            self.report.failure(
                title="Document identity lookup failed",
                message=(
                    "Could not search for an existing document for this repository "
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
                "Reusing existing document %s for repository path %s (source_id=%s)",
                chosen,
                path,
                source_id,
            )
        self._resolved_source_id_urns[source_id] = chosen
        return chosen

    def _is_legacy_or_same_import(self, document_urn: str, source_id: str) -> bool:
        """Whether a path match may be reused: a document stamped before
        ``import_source_id`` existed, or one with this source id.

        A document stamped with another source id belongs to another source,
        such as the same repository path on another instance, or this source
        before its ``platform_instance`` changed. Reusing it would make the two
        overwrite each other.
        """
        assert self.ctx.graph is not None
        try:
            info = self.ctx.graph.get_aspect(document_urn, DocumentInfoClass)
        except Exception as exc:
            # Keep the match, as before this check: minting a second document
            # would let stale removal delete the one the path already has.
            self.report.warning(
                title="Could not check an existing document's source",
                message=(
                    "A document matching this repository path could not be read "
                    "to check which import created it, so it was reused. If it "
                    "belongs to another source, the two will overwrite each other."
                ),
                context=f"document={document_urn}, source_id={source_id}",
                exc=exc,
            )
            return True
        stamped = (
            (info.customProperties or {}).get(PROP_IMPORT_SOURCE_ID) if info else None
        )
        return stamped is None or stamped == source_id

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
                title="Ambiguous document identity for repository path",
                message=(
                    "More than one DataHub document claims this repository path. One was "
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

    def _previous_document_urns(self) -> Set[str]:
        if self._last_checkpoint_urns is None:
            handler = self.stale_entity_removal_handler
            last_checkpoint = (
                self.state_provider.get_last_checkpoint(
                    handler.job_id, GenericCheckpointState
                )
                if handler.is_checkpointing_enabled()
                and not handler._ignore_old_state()
                else None
            )
            self._last_checkpoint_urns = (
                set(last_checkpoint.state.urns) if last_checkpoint else set()
            )
        return self._last_checkpoint_urns

    def _keep_previous_documents(self) -> None:
        """Carry the last run's documents into this run's state.

        Used when the file listing is partial, so stale removal cannot tell a
        deleted file from one that was not listed.
        """
        for urn in self._previous_document_urns():
            self.stale_entity_removal_handler.add_entity_to_state("", urn)

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
        # rpartition: GitLab projects can sit under nested groups (a/b/project).
        org_name, _, repo_name = owner_repo.rpartition("/")

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
            browse_path = RepositoryHierarchyExtractor.build_browse_path_v2(
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

    def _build_document(
        self,
        doc_id: str,
        title: str,
        text: str,
        owner_repo: str,
        branch: str,
        repo_path: str,
        parent_document: Optional[str],
        custom_properties: Dict[str, str],
        is_directory: bool = False,
    ) -> Document:
        external_url = self._build_external_url(
            owner_repo, branch, repo_path, is_directory=is_directory
        )
        external_id = repo_path or owner_repo

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

    def _soft_deleted_urns(self) -> Set[str]:
        """URNs this repo previously imported that are currently soft-deleted.

        Loaded with a single search so the unchanged-file path can resurrect a
        document without paying a Status lookup per file.
        """
        if self._soft_deleted_cache is not None:
            return self._soft_deleted_cache

        owner_repo = self.config.repository
        urns: Set[str] = set()
        if self.ctx.graph:
            try:
                urns = set(
                    self.ctx.graph.get_urns_by_filter(
                        entity_types=["document"],
                        platform=self.platform,
                        platform_instance=make_dataplatform_instance_urn(
                            self.platform, self._instance_name(owner_repo)
                        ),
                        status=RemovedStatusFilter.ONLY_SOFT_DELETED,
                        # A platform_instance is shared by every repository on
                        # it, so narrow to this repository's documents.
                        extraFilters=[_property_filter(self._prop("repo"), owner_repo)],
                    )
                )
            except Exception as exc:
                # Import itself is unaffected; only documents whose file came
                # back unchanged stay soft-deleted this run.
                self.report.warning(
                    title="Could not list soft-deleted documents",
                    message=(
                        "Documents whose file was restored to the repository "
                        "unchanged stay soft-deleted this run. They are restored "
                        "on a later run once search is reachable, or when the "
                        "file changes."
                    ),
                    context=f"repository={owner_repo}",
                    exc=exc,
                )
        self._soft_deleted_cache = urns
        return urns

    def _resurrect_unchanged_document(
        self, document_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Clear Status.removed for an unchanged file that exists in the repository again.

        Without this, deleting a file, letting stale removal soft-delete its
        document, then restoring the file with identical content would leave the
        document soft-deleted forever, since the unchanged-file short-circuit
        returns before any Status is emitted.
        """
        if document_urn not in self._soft_deleted_urns():
            return
        self.report.documents_resurrected += 1
        yield MetadataChangeProposalWrapper(
            entityUrn=document_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

    def _map_okf(
        self,
        frontmatter: OkfFrontmatter,
        *,
        document_urn: str,
        bundle: OkfBundleClass,
        file_path: str,
        revision: OkfRevisionClass,
    ) -> Optional[OkfMapping]:
        """Map a file's OKF frontmatter to DataHub metadata.

        Override to adjust the mapping, for example to filter or add owners.
        Wrapping the module-level ``map_okf`` does not work: this method is
        the only call site.
        """
        return map_okf(
            frontmatter,
            document_urn=document_urn,
            bundle=bundle,
            file_path=file_path,
            revision=revision,
            attribution_source=self._okf_attribution_source,
            ctx=self.ctx,
            report=self.report,
            lookup_cache=self._okf_lookup_cache,
        )

    def _attach_platform_instance(self, doc: Document, owner_repo: str) -> None:
        doc._set_aspect(
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self._instance_name(owner_repo)
                ),
            )
        )

    @staticmethod
    def _resolve_commit_sha(
        client: RepositoryDocumentsClient, owner_repo: str, branch: str
    ) -> str:
        """The commit the branch points at, so a run reads one consistent snapshot."""
        commit_sha = client.get_latest_commit_sha(owner_repo, branch)
        if not commit_sha:
            detail = client.last_api_error or "branch not found"
            raise RuntimeError(
                f"Could not resolve branch {branch!r} in {owner_repo} "
                f"to a commit snapshot: {detail}"
            )
        return commit_sha

    def get_report(self) -> RepositoryDocumentsSourceReport:
        return self.report

    @classmethod
    def test_connection(cls, config_dict: dict) -> TestConnectionReport:
        try:
            config = cls._parse_config(config_dict)
        except Exception as exc:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {exc}",
            )

        try:
            client = cls._make_client(config)
            # Resolve the branch the same way ingestion does, so a tag or a
            # missing branch fails here rather than on the first run.
            commit_sha = cls._resolve_commit_sha(
                client, config.repository, config.branch
            )
            client.list_matching_files(
                config.repository,
                commit_sha,
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
                    f"Failed to connect to {cls.platform_display_name} repository: "
                    f"{exc}. Verify repository, branch, token, and network access."
                ),
            )
