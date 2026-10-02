"""DataHub Entity Embeddings source.

Generates ``semanticContent`` embeddings for every entity type the server has
enabled for semantic search, except documents (owned by ``datahub-documents``):

1. Discovers the enabled entity types from the server (appConfig) and their
   searchable fields from the server's entity registry.
2. Scrolls each type with only the aspects that hold searchable fields, skipping
   entities whose platform is not allowed by ``platform_pattern``.
3. Renders those fields as markdown (see ``text_builder``), and re-embeds the
   entity only when that text or the embedding configuration changed.
4. Emits ``semanticContent`` through the pipeline's sink, or a skip marker when
   the entity has no indexable text (as ``datahub-documents`` does).

An entity that is later excluded (type or platform) keeps its last embeddings;
one whose text becomes empty gets a skip marker, which drops this model's entry.

Adding an entity type to semantic search is a server-side change only
(``ELASTICSEARCH_SEMANTIC_SEARCH_ENTITIES`` plus the ``semanticContent`` aspect
in the registry); this source picks it up on its next run.

Reading the entity registry requires the Manage System Operations privilege.
"""

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessor
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.source.datahub_documents.document_chunking_state_handler import (
    DocumentChunkingStateHandler,
)
from datahub.ingestion.source.datahub_documents.document_indexing_lock import (
    DocumentIndexingLock,
)
from datahub.ingestion.source.datahub_documents.text_partitioner import TextPartitioner
from datahub.ingestion.source.datahub_entity_embeddings.config import (
    AUTO_ENTITY_TYPES,
    DataHubEntityEmbeddingsSourceConfig,
)
from datahub.ingestion.source.datahub_entity_embeddings.text_builder import (
    EntityTextBuilder,
    EntityTextSpec,
    extract_values,
    parse_registry,
    urn_entity_type,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    DataHubConnectionConfig,
    DocumentChunkingSourceConfig,
    get_processing_config_fingerprint,
    get_semantic_search_config,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
    compute_source_text_sha256,
)
from datahub.ingestion.workunit_processors.auto_workunits_reporter import (
    AutoWorkunitsReporterProcessor,
)
from datahub.metadata.schema_classes import SemanticContentClass

logger = logging.getLogger(__name__)

REGISTRY_SPECS_PATH = "/openapi/v1/registry/models/entity/specifications"
SCROLL_PATH = "/openapi/v3/entity/scroll"
DOCUMENT_ENTITY_TYPE = "document"
SIBLINGS_ASPECT = "siblings"
DATA_PLATFORM_INSTANCE_ASPECT = "dataPlatformInstance"
_PLATFORM_URN = re.compile(r"urn:li:dataPlatform:([^,()]+)")
# Part of every content hash: bump it when the rendered text changes shape so all
# entities are re-embedded with the new text.
TEXT_BUILDER_VERSION = "entity_text_v1"


class _RunStopped(Exception):
    """Raised to stop the run cleanly (limits reached); the checkpoint still commits."""


class _RunAborted(Exception):
    """Raised to abort the run after a systemic failure (already reported)."""


@dataclass
class DataHubEntityEmbeddingsReport(StatefulIngestionReport):
    entity_types_embedded: List[str] = field(default_factory=list)
    entity_types_skipped: Dict[str, str] = field(default_factory=dict)
    num_entities_scanned: Dict[str, int] = field(default_factory=dict)
    num_entities_embedded: Dict[str, int] = field(default_factory=dict)
    num_entities_unchanged: Dict[str, int] = field(default_factory=dict)
    num_entities_empty: Dict[str, int] = field(default_factory=dict)
    num_entities_not_indexable: Dict[str, int] = field(default_factory=dict)
    num_entities_filtered: Dict[str, int] = field(default_factory=dict)
    num_entities_failed: Dict[str, int] = field(default_factory=dict)
    num_chunks_created: int = 0
    num_embeddings_generated: int = 0
    num_state_entries_pruned: int = 0
    lock_skipped_run: bool = False
    time_budget_reached: bool = False
    entity_limit_reached: bool = False

    def count(self, counter: Dict[str, int], entity_type: str) -> None:
        counter[entity_type] = counter.get(entity_type, 0) + 1


@platform_name("DataHubEntityEmbeddings", id="datahub-entity-embeddings")
@support_status(SupportStatus.ALPHA)
@config_class(DataHubEntityEmbeddingsSourceConfig)
class DataHubEntityEmbeddingsSource(StatefulIngestionSourceBase):
    """Generate semantic search embeddings for any DataHub entity type."""

    def __init__(
        self, ctx: PipelineContext, config: DataHubEntityEmbeddingsSourceConfig
    ):
        super().__init__(
            cast(StatefulIngestionConfigBase[StatefulIngestionConfig], config), ctx
        )
        self.config = config
        self.report: DataHubEntityEmbeddingsReport = DataHubEntityEmbeddingsReport()

        self.state_handler: Optional[DocumentChunkingStateHandler] = (
            DocumentChunkingStateHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )
            if self.state_provider.is_stateful_ingestion_configured()
            else None
        )

        if self.ctx.graph:
            self.graph = self.ctx.graph
        else:
            token = self.config.datahub.token
            self.graph = DataHubGraph(
                config=DatahubClientConfig(
                    server=self.config.datahub.server,
                    token=token.get_secret_value() if token else None,
                )
            )

        self.text_builder = EntityTextBuilder(self.config.text)
        self.text_partitioner = TextPartitioner()
        self.chunking_source = DocumentChunkingSource(
            ctx=ctx,
            config=DocumentChunkingSourceConfig(
                datahub=DataHubConnectionConfig(),
                chunking=self.config.chunking,
                embedding=self.config.embedding,
                # Limits are enforced here (max_entities_per_run), not by raising.
                max_documents=-1,
            ),
            standalone=False,
            graph=self.graph,
        )

        self.lock: Optional[DocumentIndexingLock] = None
        if self.config.locking.enabled:
            self.lock = DocumentIndexingLock(
                graph=self.graph,
                lock_id=self.config.locking.lock_id
                or self._default_lock_id(self.ctx.pipeline_name),
                run_id=self.ctx.run_id,
                ttl_seconds=self.config.locking.lock_ttl_seconds,
                renewal_interval_seconds=self.config.locking.lock_renewal_interval_seconds,
            )

        self._specs: Dict[str, EntityTextSpec] = {}
        self._reference_names: Dict[str, Optional[str]] = {}
        self._started_at = time.monotonic()
        self._num_embedded = 0
        self._consecutive_failures = 0

    @staticmethod
    def _default_lock_id(pipeline_name: Optional[str]) -> str:
        name = pipeline_name or "default"
        prefix = "urn:li:dataHubIngestionSource:"
        if name.startswith(prefix):
            name = name[len(prefix) :]
        return "entity-embeddings-lock-" + re.sub(r"[^A-Za-z0-9_.-]", "_", name)

    def get_allowed_workunit_processors(
        self,
    ) -> List[Union[str, Type[WorkunitProcessor]]]:
        # Only semanticContent may be written to entities that other sources own;
        # the default processors would add aspects to them, e.g. an empty
        # browsePathsV2 that replaces a container's real browse path.
        return [AutoWorkunitsReporterProcessor]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        if self.lock is not None and not self.lock.acquire():
            self.report.lock_skipped_run = True
            self.report.warning(
                title="Run skipped (lock held)",
                message="Another run of this pipeline is in progress; this run exited "
                "without processing.",
                log=False,
            )
            return
        try:
            yield from self._run()
        except _RunAborted as e:
            logger.error(f"Aborting run: {e}")
        except Exception as e:
            self.report.failure(
                title="Entity embeddings run failed",
                message="The run failed before all entity types were processed.",
                exc=e,
            )
        finally:
            if self.lock is not None:
                self.lock.release()

    def _run(self) -> Iterable[MetadataWorkUnit]:
        server_config = get_semantic_search_config(self.graph)
        if not server_config.enabled:
            self.report.warning(
                title="Semantic search disabled",
                message="Semantic search is disabled on the server; nothing to embed.",
            )
            return
        if self.chunking_source.get_model_embedding_key() is None:
            self.report.failure(
                title="No embedding model",
                message="No embedding provider/model could be resolved from the server "
                "or the recipe; nothing can be embedded.",
            )
            return

        try:
            self._specs = parse_registry(
                self.graph._paginate_offset(
                    f"{self.graph._gms_server}{REGISTRY_SPECS_PATH}"
                )
            )
        except OperationalError as e:
            if _http_status(e) != 403:
                raise
            self.report.failure(
                title="Missing privilege",
                message="Reading the entity registry requires the Manage System "
                "Operations privilege (MANAGE_SYSTEM_OPERATIONS); grant it to the "
                "user or service account of the token.",
                exc=e,
            )
            return
        entity_types = self.resolve_entity_types(server_config.enabled_entities)
        self.report.entity_types_embedded = entity_types
        logger.info(f"Embedding entity types: {entity_types}")

        seen: Set[str] = set()
        completed = True
        try:
            for entity_type in entity_types:
                yield from self._process_entity_type(entity_type, seen)
        except _RunStopped as e:
            completed = False
            self.report.info(
                title="Run stopped early",
                message="A run limit was reached; the next run continues from here.",
                context=str(e),
            )
        if completed:
            self._prune_state(seen)

    def resolve_entity_types(self, server_enabled: List[str]) -> List[str]:
        """Entity types to embed, recording why every other candidate is skipped."""
        registry_names = {name.lower(): name for name in self._specs}
        enabled = {
            registry_names.get(t.strip().lower(), t.strip())
            for t in server_enabled
            if t.strip()
        }
        requested = [t for t in self.config.entity_types if t != AUTO_ENTITY_TYPES]
        if AUTO_ENTITY_TYPES in self.config.entity_types:
            requested = sorted(enabled) + requested
        excluded = {t.lower() for t in self.config.exclude_entity_types}

        resolved: List[str] = []
        for requested_type in dict.fromkeys(requested):
            entity_type = registry_names.get(requested_type.lower(), requested_type)
            spec = self._specs.get(entity_type)
            reason: Optional[str] = None
            expected = True
            if entity_type == DOCUMENT_ENTITY_TYPE:
                reason = "embedded by the datahub-documents source"
            elif entity_type.lower() in excluded:
                reason = "excluded by exclude_entity_types"
            elif spec is None:
                reason, expected = "not in the server's entity registry", False
            elif entity_type not in enabled:
                reason, expected = (
                    "not enabled for semantic search on the server "
                    "(elasticsearch.entityIndex.semanticSearch.enabledEntities)",
                    False,
                )
            elif not spec.has_semantic_content:
                reason, expected = (
                    "the semanticContent aspect is not registered for this entity "
                    "type (add it with a registry plugin)",
                    False,
                )
            elif spec.search_group not in self.config.search_groups:
                reason = (
                    f"search group '{spec.search_group}' is not in search_groups "
                    "(operational types are opt-in)"
                )
            elif not spec.fields:
                reason = "no searchable text fields"
            if reason is None:
                if entity_type not in resolved:
                    resolved.append(entity_type)
                continue
            self.report.entity_types_skipped[entity_type] = reason
            if expected:
                self.report.info(
                    title="Entity type skipped",
                    message="Entity type not embedded by this source.",
                    context=f"{entity_type}: {reason}",
                    log=False,
                )
            else:
                self.report.warning(
                    title="Entity type skipped",
                    message="Entity type requested for semantic search cannot be embedded.",
                    context=f"{entity_type}: {reason}",
                )
        return resolved

    def _scroll(
        self, entity_type: str, aspects: List[str]
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        url = f"{self.graph._gms_server}{SCROLL_PATH}"
        scroll_id: Optional[str] = None
        while True:
            params: Dict[str, Any] = {
                "count": self.config.scroll_batch_size,
                "query": "*",
            }
            if scroll_id:
                params["scrollId"] = scroll_id
            response = self.graph._post_generic(
                url, {"entities": [entity_type], "aspects": aspects}, params=params
            )
            entities = response.get("entities")
            # Treating a malformed response as an empty catalog would prune all state.
            if not isinstance(entities, list):
                raise ValueError(
                    f"Scroll response for {entity_type} has no 'entities' list"
                )
            for entity in entities:
                urn = entity.get("urn")
                if urn:
                    yield urn, _aspect_values(entity)
            scroll_id = response.get("scrollId")
            if not scroll_id or not entities:
                return

    def _process_entity_type(
        self, entity_type: str, seen: Set[str]
    ) -> Iterable[MetadataWorkUnit]:
        spec = self._specs[entity_type]
        report = self.report
        report.num_entities_scanned.setdefault(entity_type, 0)
        aspects_to_fetch = list(spec.aspects)
        if self.config.text.include_siblings and SIBLINGS_ASPECT in spec.all_aspects:
            aspects_to_fetch.append(SIBLINGS_ASPECT)
        if (
            DATA_PLATFORM_INSTANCE_ASPECT in spec.all_aspects
            and DATA_PLATFORM_INSTANCE_ASPECT not in aspects_to_fetch
        ):
            aspects_to_fetch.append(DATA_PLATFORM_INSTANCE_ASPECT)
        for urn, aspects in self._scroll(entity_type, aspects_to_fetch):
            self._check_limits()
            if self.lock is not None:
                self.lock.heartbeat()
            report.count(report.num_entities_scanned, entity_type)
            platform = entity_platform(urn, aspects)
            if platform is not None and not self.config.platform_pattern.allowed(
                platform
            ):
                # Not added to `seen`, so its state is pruned and it is embedded
                # again if its platform is allowed later.
                report.count(report.num_entities_filtered, entity_type)
                continue
            seen.add(urn)

            text = self.text_builder.build(
                spec,
                urn,
                aspects,
                self._resolve_reference,
                self._fetch_siblings(urn, aspects),
            )
            content_hash = self._content_hash(text)
            if not self._needs_embedding(urn, content_hash):
                report.count(report.num_entities_unchanged, entity_type)
                continue

            workunits = self._embed(entity_type, urn, text)
            if workunits is None:
                continue
            yield from workunits
            # A skip marker is deterministic for this text, so it is recorded too and
            # only retried when the text changes.
            self._record(urn, content_hash)
            if not text.strip():
                report.count(report.num_entities_empty, entity_type)
            elif _is_skip_marker(workunits):
                report.count(report.num_entities_not_indexable, entity_type)
            else:
                report.count(report.num_entities_embedded, entity_type)
                self._num_embedded += 1
                if self.config.index_delay_seconds > 0:
                    time.sleep(self.config.index_delay_seconds)

    def _check_limits(self) -> None:
        budget = self.config.time_budget_seconds
        if budget is not None and time.monotonic() - self._started_at >= budget:
            self.report.time_budget_reached = True
            raise _RunStopped(f"time_budget_seconds={budget} reached")
        limit = self.config.max_entities_per_run
        if limit > 0 and self._num_embedded >= limit:
            self.report.entity_limit_reached = True
            raise _RunStopped(f"max_entities_per_run={limit} reached")

    def _embed(
        self, entity_type: str, urn: str, text: str
    ) -> Optional[List[MetadataWorkUnit]]:
        """The entity's workunits (embeddings or a skip marker), or None if it failed."""
        try:
            if not text.strip():
                workunits = [
                    self.chunking_source.build_skip_marker_workunit(urn, "EMPTY_TEXT")
                ]
            else:
                elements = self.text_partitioner.partition_text(text)
                workunits = (
                    list(
                        self.chunking_source.process_elements_inline(
                            document_urn=urn,
                            elements=elements,
                            source_text_sha256=compute_source_text_sha256(text),
                        )
                    )
                    if elements
                    else [
                        self.chunking_source.build_skip_marker_workunit(
                            urn, "NO_INDEXABLE_CONTENT"
                        )
                    ]
                )
            if not workunits:
                raise ValueError("No semanticContent was produced for the entity")
        except Exception as e:
            self.report.count(self.report.num_entities_failed, entity_type)
            self._consecutive_failures += 1
            self.report.warning(
                title="Embedding failed",
                message="Failed to embed an entity; it is retried on the next run.",
                context=urn,
                exc=e,
            )
            if self._consecutive_failures >= self.config.max_consecutive_failures:
                self.report.failure(
                    title="Too many consecutive embedding failures",
                    message="Aborting the run: the embedding provider keeps failing.",
                    context=f"{self._consecutive_failures} consecutive failures",
                    exc=e,
                )
                raise _RunAborted("too many consecutive embedding failures") from e
            return None
        if not _is_skip_marker(workunits):
            self._consecutive_failures = 0
        return workunits

    def _content_hash(self, text: str) -> str:
        # Only the chunking source's config has the embedding settings resolved from
        # the server; this source's config is usually empty.
        processing = self.chunking_source.config
        payload = {
            "text": text,
            "builder": TEXT_BUILDER_VERSION,
            "config": get_processing_config_fingerprint(
                processing.chunking, processing.embedding
            ),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def _needs_embedding(self, urn: str, content_hash: str) -> bool:
        if (
            not self.config.incremental.enabled
            or self.config.incremental.force_reprocess
        ):
            return True
        if (
            self.state_handler is None
            or not self.state_handler.is_checkpointing_enabled()
        ):
            return True
        return self.state_handler.get_document_hash(urn) != content_hash

    def _record(self, urn: str, content_hash: str) -> None:
        if (
            self.state_handler is not None
            and self.state_handler.is_checkpointing_enabled()
        ):
            self.state_handler.update_document_state(
                urn, content_hash, datetime.now(timezone.utc).isoformat()
            )

    def _prune_state(self, seen: Set[str]) -> None:
        """Forget entities that no longer exist or are no longer embedded by this source."""
        if (
            self.state_handler is None
            or not self.state_handler.is_checkpointing_enabled()
        ):
            return
        state = self.state_handler.get_current_state()
        if state is None:
            return
        stale = [urn for urn in state.document_state if urn not in seen]
        for urn in stale:
            del state.document_state[urn]
        self.report.num_state_entries_pruned = len(stale)

    def _resolve_reference(self, urn: str) -> Optional[str]:
        if urn in self._reference_names:
            return self._reference_names[urn]
        name: Optional[str] = None
        entity_type = urn_entity_type(urn)
        spec = self._specs.get(entity_type) if entity_type else None
        name_fields = spec.name_fields if spec else []
        if name_fields:
            try:
                aspects = _aspect_values(
                    self.graph.get_entity_raw(
                        urn, aspects=list(dict.fromkeys(f.aspect for f in name_fields))
                    ).get("aspects")
                    or {}
                )
                for name_field in name_fields:
                    values = extract_values(
                        aspects.get(name_field.aspect), name_field.path
                    )
                    name = next((v for v in values if isinstance(v, str) and v), None)
                    if name:
                        break
            except Exception as e:
                logger.debug(f"Could not resolve the name of {urn}: {e}")
        self._reference_names[urn] = name
        return name

    def _fetch_siblings(
        self, urn: str, aspects: Dict[str, Any]
    ) -> List[Tuple[EntityTextSpec, Dict[str, Any]]]:
        if not self.config.text.include_siblings:
            return []
        siblings = (aspects.get(SIBLINGS_ASPECT) or {}).get("siblings") or []
        result: List[Tuple[EntityTextSpec, Dict[str, Any]]] = []
        for sibling_urn in siblings:
            if not isinstance(sibling_urn, str) or sibling_urn == urn:
                continue
            sibling_type = urn_entity_type(sibling_urn)
            spec = self._specs.get(sibling_type) if sibling_type else None
            if spec is None or not spec.aspects:
                continue
            try:
                raw = self.graph.get_entity_raw(sibling_urn, aspects=spec.aspects)
            except Exception as e:
                logger.debug(f"Could not fetch sibling {sibling_urn} of {urn}: {e}")
                continue
            sibling_aspects = _aspect_values(raw.get("aspects") or {})
            # Siblings reference each other; drop the back-reference to avoid loops.
            sibling_aspects.pop(SIBLINGS_ASPECT, None)
            if sibling_aspects:
                result.append((spec, sibling_aspects))
        return result

    def get_report(self) -> SourceReport:
        chunking_report = self.chunking_source.report
        self.report.num_chunks_created = chunking_report.num_chunks_created
        self.report.num_embeddings_generated = chunking_report.num_embeddings_generated
        return self.report

    def close(self) -> None:
        if self.lock is not None:
            self.lock.release()
        super().close()


def entity_platform(urn: str, aspects: Dict[str, Any]) -> Optional[str]:
    """Platform name of an entity: its dataPlatformInstance, else the platform in its URN."""
    platform = (aspects.get(DATA_PLATFORM_INSTANCE_ASPECT) or {}).get("platform")
    match = _PLATFORM_URN.search(
        platform if isinstance(platform, str) and platform else urn
    )
    return match.group(1) if match else None


def _is_skip_marker(workunits: List[MetadataWorkUnit]) -> bool:
    return all(
        isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, SemanticContentClass)
        and wu.metadata.aspect.skipReason is not None
        for wu in workunits
    )


def _http_status(error: OperationalError) -> Optional[int]:
    response = getattr(error.__cause__, "response", None)
    return getattr(response, "status_code", None)


def _aspect_values(entity: Dict[str, Any]) -> Dict[str, Any]:
    """Map aspect name -> aspect value from OpenAPI v3 or Rest.li entity payloads."""
    return {
        name: payload["value"]
        for name, payload in entity.items()
        if isinstance(payload, dict) and isinstance(payload.get("value"), dict)
    }
