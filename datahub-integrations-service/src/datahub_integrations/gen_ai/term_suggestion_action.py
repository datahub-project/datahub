import enum
import itertools
import threading
from datetime import timedelta

import cachetools
import cachetools.keys
import datahub.metadata.schema_classes as models
import pydantic
from datahub.configuration.common import ConfigEnum, ConfigModel
from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.ratelimiter import RateLimiter
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from loguru import logger
from typing_extensions import Self, assert_never

from datahub_integrations.gen_ai.description_context import ShellEntityError
from datahub_integrations.gen_ai.router import (
    SuggestedTerm,
    SuggestedTerms,
    _suggest_terms_batch,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import (
    GlossaryUniverseConfig,
    fetch_glossary_info,
)

_APPLY_TO_TOP_N_DATASETS = 10000
_MAX_GQL_BATCH_SIZE = 10000
_TERMS_ALGO_VERSION = 1

_SCHEMA_FIELD_ENTITY_TYPE = "SCHEMA_FIELD"
_DATASET_ENTITY_TYPE = "DATASET"
_ALLOWED_ENTITY_TYPES = [_DATASET_ENTITY_TYPE, _SCHEMA_FIELD_ENTITY_TYPE]


class AutomationApplyType(ConfigEnum):
    PROPOSE = enum.auto()
    APPLY = enum.auto()


class CardinalityType(ConfigEnum):
    SINGLE = enum.auto()
    MULTIPLE = enum.auto()


class TermEntitySelector(ConfigModel):
    entity_types_enabled: list[str] = [_SCHEMA_FIELD_ENTITY_TYPE]
    platforms: list[str] | None = None
    containers: list[str] | None = None

    @pydantic.validator("entity_types_enabled")
    def validate_entity_types_enabled(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one entity type must be enabled")
        for entity_type in v:
            if entity_type not in _ALLOWED_ENTITY_TYPES:
                raise ValueError(
                    f"Entity type {entity_type} is not allowed. Allowed entity types: {_ALLOWED_ENTITY_TYPES}"
                )
        return v

    @pydantic.validator("platforms", "containers")
    def validate_filter_lists(cls, v: list[str] | None) -> list[str] | None:
        # If the filter list is empty, treat it as if there was no filter to begin with.
        return v or None

    def is_asset_enabled(self) -> bool:
        return _DATASET_ENTITY_TYPE in self.entity_types_enabled

    def is_schema_field_enabled(self) -> bool:
        return _SCHEMA_FIELD_ENTITY_TYPE in self.entity_types_enabled


class TermSuggestionActionConfig(TermEntitySelector):
    glossary_term_urns: list[str] = []
    glossary_node_urns: list[str] = []

    cardinality: CardinalityType = CardinalityType.MULTIPLE
    recommendation_action: AutomationApplyType = AutomationApplyType.PROPOSE

    custom_instructions: str | None = None

    new_urn_interval: timedelta = timedelta(days=1)
    max_urns_per_minute: int = 4


class TermSuggestionAction(Action):
    def __init__(self, config: TermSuggestionActionConfig, ctx: PipelineContext):
        self.config = config
        self.ctx = ctx

        assert self.ctx.graph is not None, "Graph client is required"
        self.bulk_suggester = BulkTermSuggester(
            graph=self.ctx.graph.graph, config=self.config
        )
        (
            self.bulk_suggester_thread,
            self.shutdown_event,
        ) = self.bulk_suggester.run_forever_background()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Self:
        config = TermSuggestionActionConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def act(self, event: EventEnvelope) -> None:
        if not self.bulk_suggester_thread.is_alive():
            # TODO: This requires that an event get processed before the death of
            # the background thread is actually noticed.
            raise SystemExit("Main bulk term suggester thread died")

    # TODO: Add reporting.
    # def get_report(self) -> ActionStageReport:
    #     return super().get_report()

    def close(self) -> None:
        logger.info(
            "Shutting down bulk term suggester, waiting for background thread to finish..."
        )
        self.shutdown_event.set()
        self.bulk_suggester_thread.join()
        return super().close()


@cachetools.cached(
    cache=cachetools.TTLCache(ttl=timedelta(minutes=15).total_seconds(), maxsize=1),
    key=lambda _graph, **kwargs: cachetools.keys.hashkey(
        **{k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}
    ),
)
def _get_top_datasets(
    graph: DataHubGraph,
    *,
    platforms: list[str] | None,
    containers: list[str] | None,
) -> set[str]:
    # TODO: This is a pretty hacky way to get the top N datasets - it relies on
    # us having usage stats in our backend, since otherwise the order entities
    # are returned is pretty much random.

    top_urns = set(
        itertools.islice(
            graph.get_urns_by_filter(
                entity_types=[DatasetUrn.ENTITY_TYPE],
                batch_size=min(_MAX_GQL_BATCH_SIZE, _APPLY_TO_TOP_N_DATASETS),
                platform=platforms,
                container=containers,
            ),
            _APPLY_TO_TOP_N_DATASETS,
        )
    )

    sampled_urns = LossyList[str]()
    sampled_urns.extend(top_urns)
    logger.debug(f'Dataset "universe" under consideration: {sampled_urns}')
    return top_urns


def get_urns_to_process(
    graph: DataHubGraph, config: TermEntitySelector, max_items: int
) -> list[str]:
    assert 0 < max_items <= _APPLY_TO_TOP_N_DATASETS

    # TODO: Ideally we could collapse this into a single query.
    matching_urns = itertools.chain(
        # Get entities that have not been processed at all.
        graph.get_urns_by_filter(
            entity_types=[DatasetUrn.ENTITY_TYPE],
            batch_size=min(_MAX_GQL_BATCH_SIZE, _APPLY_TO_TOP_N_DATASETS),
            platform=config.platforms,
            container=config.containers,
            extraFilters=[
                {
                    "field": "glossaryTermsVersion",
                    "values": [f"{_TERMS_ALGO_VERSION}"],
                    "condition": "EXISTS",
                    "negated": True,
                }
            ],
        ),
        # Get entities that were processed but the version is old.
        graph.get_urns_by_filter(
            entity_types=[DatasetUrn.ENTITY_TYPE],
            batch_size=min(_MAX_GQL_BATCH_SIZE, _APPLY_TO_TOP_N_DATASETS),
            platform=config.platforms,
            container=config.containers,
            extraFilters=[
                {
                    "field": "glossaryTermsVersion",
                    "values": [f"{_TERMS_ALGO_VERSION}"],
                    "condition": "LESS_THAN",
                }
            ],
        ),
        # TODO: Maybe also include entities that haven't been processed in a while?
        # Can implement this by filtering on `glossaryTermsLastInferredAt`.
    )

    # Restrict to urns that are also in the top datasets.
    top_urns = _get_top_datasets(
        graph,
        platforms=config.platforms,
        containers=config.containers,
    )

    # Subtle: by using itertools islice + chain, we can avoid making the second get_urns_by_filter call
    # if the first call returns enough urns.
    return list(
        itertools.islice((urn for urn in matching_urns if urn in top_urns), max_items)
    )


class BulkTermSuggester:
    def __init__(self, graph: DataHubGraph, config: TermSuggestionActionConfig):
        self.graph = graph
        self.config = config

    def run_forever_background(self) -> tuple[threading.Thread, threading.Event]:
        shutdown_event = threading.Event()
        thread = threading.Thread(
            target=self._thread_worker,
            args=(shutdown_event,),
            name="bulk-term-suggestion-worker",
            daemon=True,
        )
        thread.start()
        return thread, shutdown_event

    def _thread_worker(self, shutdown_event: threading.Event) -> None:
        logger.info("Bulk term suggester worker thread started")

        rate_limiter = RateLimiter(
            max_calls=self.config.max_urns_per_minute,
            period=timedelta(minutes=1).total_seconds(),
        )

        while not shutdown_event.is_set():
            urns = self.find_urns()

            if not urns:
                logger.info("No urns to process right now")

            else:
                for urn in urns:
                    with rate_limiter:
                        self.process_urns([urn])

                    if shutdown_event.is_set():
                        logger.info(
                            "Shutdown event set while processing urns, exiting without processing all urns"
                        )
                        return

                logger.debug(f"Finished processing {len(urns)} urns")

            # This is effectively a time.sleep, but also allows it to early exit
            # if the shutdown event is set.
            logger.info(
                f"Waiting for {self.config.new_urn_interval.total_seconds()} seconds before searching for new urns"
            )
            if shutdown_event.wait(
                timeout=self.config.new_urn_interval.total_seconds()
            ):
                break

        logger.info("Bulk term suggester got shutdown signal")

    def find_urns(self) -> list[str]:
        return get_urns_to_process(
            self.graph, config=self.config, max_items=_APPLY_TO_TOP_N_DATASETS
        )

    def process_urns(self, urns: list[str]) -> None:
        glossary_universe = GlossaryUniverseConfig(
            glossary_terms=self.config.glossary_term_urns,
            glossary_nodes=self.config.glossary_node_urns,
        )

        # Fetch glossary info once per batch for efficiency
        glossary_info = fetch_glossary_info(
            graph_client=self.graph, universe=glossary_universe
        )

        # Process URNs individually to avoid batch failures affecting all entities
        for urn in urns:
            try:
                suggestions = _suggest_terms_batch(
                    graph=self.graph,
                    entity_urns=[urn],
                    glossary_info=glossary_info,
                    custom_instructions=self.config.custom_instructions,
                )
                # TODO add plumbing to include confidence scores

                if urn in suggestions:
                    self.update_entity(urn, suggestions[urn])
                else:
                    # URN was processed but no suggestions returned
                    # Still mark as processed to prevent infinite retry
                    self._mark_entity_as_processed(urn)

            except ShellEntityError as e:
                logger.info(f"Skipping shell entity {urn}: {e}")
                # CRITICAL: Mark shell entities as processed to prevent infinite retry
                self._mark_entity_as_processed(urn)

            except Exception as e:
                # For other exceptions, don't mark as processed to allow retry
                # This preserves retry behavior for network issues, temporary failures, etc.
                logger.exception(f"Failed to process urn {urn}: {e}")

    def update_entity(self, urn: str, suggestion: SuggestedTerms) -> None:
        # Apply the cardinality config.
        table_terms = self.apply_cardinality(suggestion.table_terms)
        all_column_terms = {
            column_name: self.apply_cardinality(column_terms)
            for column_name, column_terms in (suggestion.column_terms or {}).items()
        }

        # Update the entity's inferred properties.
        self.graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=models.EntityInferenceMetadataClass(
                    # TODO: Replace this with a patch.
                    glossaryTermsInference=models.InferenceGroupMetadataClass(
                        lastInferredAt=get_sys_time(),
                        version=_TERMS_ALGO_VERSION,
                    ),
                ),
            ),
            async_flag=False,
        )

        # TODO: do we need to filter out terms that already exist or have already been proposed?

        if self.config.is_asset_enabled():
            self.apply_or_propose_terms(urn, column_name=None, terms=table_terms)
        if self.config.is_schema_field_enabled():
            for column_name, column_terms in all_column_terms.items():
                self.apply_or_propose_terms(urn, column_name, column_terms)

    def _mark_entity_as_processed(self, urn: str) -> None:
        """Mark entity as processed by updating glossaryTermsVersion to prevent infinite retries.

        This should be called for entities that cannot be processed (e.g., shell entities)
        to prevent them from being selected again in future processing cycles.
        """
        self.graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=models.EntityInferenceMetadataClass(
                    # TODO: Replace this with a patch.
                    glossaryTermsInference=models.InferenceGroupMetadataClass(
                        lastInferredAt=get_sys_time(),
                        version=_TERMS_ALGO_VERSION,
                    ),
                ),
            ),
            async_flag=False,
        )

    def apply_cardinality(
        self, terms: list[SuggestedTerm] | None
    ) -> list[SuggestedTerm]:
        if not terms:
            return []

        match self.config.cardinality:
            case CardinalityType.MULTIPLE:
                return terms
            case CardinalityType.SINGLE:
                # Select the highest confidence term.
                sorted_terms = sorted(
                    terms, key=lambda x: x.confidence_score, reverse=True
                )
                return [sorted_terms[0]]
            case _:
                assert_never(self.config.cardinality)

    def apply_or_propose_terms(
        self, urn: str, column_name: str | None, terms: list[SuggestedTerm] | None
    ) -> None:
        item = urn
        if column_name:
            item = f"{urn} field {column_name}"

        if not terms:
            logger.debug(f"No terms for {item}")
            return

        try:
            if self.config.recommendation_action == AutomationApplyType.APPLY:
                term_urns = [term.urn for term in terms]
                logger.debug(f"Applying terms for {item}: {term_urns}")
                self.apply_terms(urn, column_name, term_urns)

            elif self.config.recommendation_action == AutomationApplyType.PROPOSE:
                logger.debug(f"Proposing terms for {item}: {terms}")
                self.propose_terms(urn, column_name, terms)

            else:
                assert_never(self.config.recommendation_action)
        except Exception as e:
            logger.error(f"Error applying/proposing terms for {item}: {e}")

    def apply_terms(self, urn: str, column_name: str | None, terms: list[str]) -> None:
        variables = {
            "dataset": urn,
            "columnName": column_name,
            "subResourceType": "DATASET_FIELD" if column_name is not None else None,
            "terms": terms,
        }

        self.graph.execute_graphql(
            """\
mutation($dataset: String!, $columnName: String, $subResourceType: SubResourceType, $terms: [String!]!) {
  addTerms(input: {
    resourceUrn: $dataset,
    termUrns: $terms,
    subResourceType: $subResourceType,
    subResource: $columnName
  })
}
""",
            variables=variables,
        )

    def propose_terms(
        self, urn: str, column_name: str | None, terms: list[SuggestedTerm]
    ) -> None:
        ts = get_sys_time()

        for term in terms:
            variables = {
                "dataset": urn,
                "columnName": column_name,
                "subResourceType": "DATASET_FIELD" if column_name is not None else None,
                "term": term.urn,
                "algoVersion": _TERMS_ALGO_VERSION,
                "confidenceLevel": _get_confidence_level(term.confidence_score),
                "ts": ts,
            }

            self.graph.execute_graphql(
                """\
mutation ($dataset: String!, $columnName: String, $subResourceType: SubResourceType, $term: String!, $algoVersion: Int!, $confidenceLevel: Int, $ts: Long!) {
  proposeTerm(
    input: {
      termUrn: $term
      resourceUrn: $dataset
      subResource: $columnName
      subResourceType: $subResourceType
      origin: INFERRED
      inferenceMetadata: {
        version: $algoVersion
        confidenceLevel: $confidenceLevel
        lastInferredAt: $ts
      }
    }
  )
}
""",
                variables=variables,
            )


def _get_confidence_level(confidence_score: float) -> int:
    # Confidence score is 1-10, with 10 being the highest confidence.
    # The confidence level is 0-2, with 2 being the highest confidence.
    # Mapping:
    # 10 -> HIGH (2)
    # 9 -> MEDIUM (1)
    # 1-8 -> LOW (0)

    if confidence_score > 9:
        return 2
    elif confidence_score > 8:
        return 1
    else:
        return 0
