package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentContributor;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentProjector;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * V3 update indices strategy implementation for UpdateIndicesService. This handles the new v3
 * mapping approach with multi-entity indices.
 */
@Slf4j
public class UpdateIndicesV3Strategy implements UpdateIndicesStrategy {

  private final EntityIndexVersionConfiguration v3Config;
  private final ElasticSearchService elasticSearchService;
  private final V3SearchDocumentProjector searchDocumentProjector;
  private final TimeseriesAspectService timeseriesAspectService;
  private final MultiEntityMappingsBuilder mappingsBuilder;
  @Nullable private final TimeseriesWriteThrottleCache timeseriesThrottleCache;
  private static final Set<String> STRATEGY_OWNED_DOCUMENT_FIELDS =
      MappingConstants.STRATEGY_OWNED_ROOT_FIELDS;

  private final EntityDocumentIdHasher entityDocumentIdHasher;
  private final List<V3SearchDocumentContributor> documentContributors;
  private final boolean v2Enabled;
  @Nullable private final SemanticSearchConfiguration semanticSearchConfiguration;

  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache) {
    this(
        v3Config,
        elasticSearchService,
        searchDocumentTransformer,
        timeseriesAspectService,
        timeseriesThrottleCache,
        new Sha256UrnEntityDocumentIdHasher(),
        List.of(),
        false);
  }

  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache,
      @Nonnull EntityDocumentIdHasher entityDocumentIdHasher,
      @Nonnull List<V3SearchDocumentContributor> documentContributors) {
    this(
        v3Config,
        elasticSearchService,
        searchDocumentTransformer,
        timeseriesAspectService,
        timeseriesThrottleCache,
        entityDocumentIdHasher,
        documentContributors,
        false);
  }

  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache,
      @Nonnull EntityDocumentIdHasher entityDocumentIdHasher,
      @Nonnull List<V3SearchDocumentContributor> documentContributors,
      boolean v2Enabled) {
    this(
        v3Config,
        elasticSearchService,
        searchDocumentTransformer,
        timeseriesAspectService,
        timeseriesThrottleCache,
        entityDocumentIdHasher,
        documentContributors,
        v2Enabled,
        null);
  }

  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache,
      @Nonnull EntityDocumentIdHasher entityDocumentIdHasher,
      @Nonnull List<V3SearchDocumentContributor> documentContributors,
      boolean v2Enabled,
      @Nullable SemanticSearchConfiguration semanticSearchConfiguration) {
    this.v3Config = v3Config;
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentProjector = new V3SearchDocumentProjector(searchDocumentTransformer);
    this.timeseriesAspectService = timeseriesAspectService;
    this.timeseriesThrottleCache = timeseriesThrottleCache;
    this.entityDocumentIdHasher = entityDocumentIdHasher;
    this.documentContributors =
        documentContributors == null ? List.of() : List.copyOf(documentContributors);
    this.v2Enabled = v2Enabled;
    this.semanticSearchConfiguration = semanticSearchConfiguration;
    try {
      this.mappingsBuilder =
          new MultiEntityMappingsBuilder(
              com.linkedin.metadata.config.search.EntityIndexConfiguration.builder()
                  .v3(v3Config)
                  .build());
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize V3 mappings builder", e);
    }
  }

  @Override
  public void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled) {

    if (groupedEvents.isEmpty()) {
      return;
    }

    TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary =
        timeseriesThrottleCache != null ? timeseriesThrottleCache.newSummary() : null;

    log.debug("Processing {} URN groups with V3 unified batch optimization", groupedEvents.size());

    // Process each group of events for the same URN
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      Urn urn = entry.getKey();
      List<MCLItem> urnEvents = entry.getValue();

      log.debug("Processing {} events for URN: {} with V3 unified batch", urnEvents.size(), urn);

      // V3 optimization: single operation per URN regardless of aspect count
      processUrnBatch(opContext, urn, urnEvents, structuredPropertiesHookEnabled, throttleSummary);
    }

    if (throttleSummary != null) {
      throttleSummary.logIfSuppressed();
    }
  }

  public void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue) {
    try {
      log.debug("Updating V3 index mappings for structured property change: {}", urn);

      if (Constants.STRUCTURED_PROPERTY_ENTITY_NAME.equals(entitySpec.getName())
          && Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspectSpec.getName())) {

        StructuredPropertyDefinition newDefinition =
            new StructuredPropertyDefinition(((RecordTemplate) newValue).data().copy());

        // Apply the mapping for the full set of currently-declared entity types on every
        // definition upsert, not just types newly added since oldValue. applyMappings is an
        // idempotent put_mapping, so re-saving a property re-applies (and thereby repairs) any
        // mapping a previous attempt failed to write — the only convergence path when the
        // structured-property system-update machinery is disabled (the default). Entity types
        // removed from the definition are handled by the dedicated removal path, not here.
        if (!newDefinition.getEntityTypes().isEmpty()) {

          // V3 uses the same approach as V2 but with V3 mappings and index convention
          log.info(
              "V3 structured property mapping update for {} - updating indices", newDefinition);

          elasticSearchService
              .buildReindexConfigsWithNewStructProp(opContext, urn, newDefinition)
              .forEach(
                  reindexState -> {
                    // Isolate failures per index: the property may declare multiple entity
                    // types, and one failing index must not prevent the mapping update from
                    // reaching the remaining declared entity types' indexes.
                    try {
                      log.info(
                          "Applying new V3 structured property {} to index {}",
                          newDefinition,
                          reindexState.name());
                      elasticSearchService
                          .getIndexBuilder(reindexState.name())
                          .applyMappings(opContext, reindexState, false);
                    } catch (Exception e) {
                      log.error(
                          "Failed to apply V3 structured property {} mapping to index {}",
                          urn,
                          reindexState.name(),
                          e);
                    }
                  });
        }
      }
    } catch (Exception e) {
      log.error("Issue with updating V3 index mappings for structured property change", e);
    }
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext) {
    return mappingsBuilder.getIndexMappings(opContext);
  }

  @Override
  public Collection<MappingsBuilder.IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    return mappingsBuilder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);
  }

  @Override
  public boolean isEnabled() {
    return v3Config.isEnabled();
  }

  /**
   * Processes all events for a single URN in V3 optimized fashion. Either deletes the entire entity
   * or performs a single upsert.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   */
  private void processUrnBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      boolean structuredPropertiesHookEnabled,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    try {
      processUrnBatchUnchecked(
          opContext, urn, events, structuredPropertiesHookEnabled, throttleSummary);
    } catch (RuntimeException e) {
      if (v2Enabled) {
        log.error(
            "V3 search write failed for URN {} while V2 dual-write is enabled; skipping V3 document",
            urn,
            e);
        return;
      }
      throw e;
    }
  }

  private void processUrnBatchUnchecked(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      boolean structuredPropertiesHookEnabled,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    log.debug("V3 unified batch processing for URN: {} with {} events", urn, events.size());

    // Check if any event is a key aspect deletion - if so, delete the entire document
    boolean hasKeyAspectDeletion =
        events.stream()
            .anyMatch(
                event -> {
                  try {
                    // Only check for key aspect deletion if this is actually a DELETE event
                    if (event.getChangeType() == ChangeType.DELETE) {
                      Pair<EntitySpec, AspectSpec> specPair =
                          UpdateIndicesUtil.extractSpecPair(event);
                      return UpdateIndicesUtil.isDeletingKey(specPair);
                    }
                    return false;
                  } catch (Exception e) {
                    log.error(
                        "Error checking key aspect deletion for event {}: {}",
                        event.getAspectName(),
                        e.getMessage(),
                        e);
                    return false;
                  }
                });

    String docId = entityDocumentIdHasher.documentId(opContext, urn);
    if (hasKeyAspectDeletion) {
      String indexKey = v3IndexKey(events.get(0).getEntitySpec());
      elasticSearchService.deleteDocumentBySearchGroup(opContext, indexKey, docId);
      log.debug(
          "V3 deleted entire document for URN: {} from index key: {} due to key aspect deletion",
          urn,
          indexKey);
      return;
    }

    // Build the combined V3 document with _aspect structure
    ObjectNode combinedDocument = buildV3SearchDocument(opContext, urn, events, throttleSummary);

    // As on V2, the structured property mapping hook runs even when no aspect changed
    if (structuredPropertiesHookEnabled) {
      for (MCLItem event : events) {
        try {
          EntitySpec entitySpec = event.getEntitySpec();
          AspectSpec aspectSpec = event.getAspectSpec();
          updateIndexMappings(
              opContext,
              event.getUrn(),
              entitySpec,
              aspectSpec,
              event.getRecordTemplate(),
              event.getPreviousRecordTemplate());
        } catch (RuntimeException e) {
          log.error(
              "Error updating V3 index mappings for aspect {} of URN {}",
              event.getAspectName(),
              urn,
              e);
        }
      }
    }

    if (combinedDocument == null) {
      log.debug("V3 combined document is empty for URN: {}, skipping update", urn);
      return;
    }

    // Write the combined document to Elasticsearch
    if (events.isEmpty()) {
      log.warn("V3 upsert attempted but no events available for URN: {}", urn);
      return;
    }

    String indexKey = v3IndexKey(events.get(0).getEntitySpec());

    String finalDocument = combinedDocument.toString();

    elasticSearchService.upsertDocumentBySearchGroup(opContext, indexKey, finalDocument, docId);
    log.debug(
        "V3 upserted combined document for URN: {} to index key: {} with {} aspects",
        urn,
        indexKey,
        events.size());

    // Append runIds to search document so rollback/list runs can find touched URNs (MAE path)
    List<String> distinctRunIds =
        events.stream()
            .filter(e -> e.getSystemMetadata() != null && e.getSystemMetadata().hasRunId())
            .map(e -> e.getSystemMetadata().getRunId())
            .distinct()
            .collect(Collectors.toList());
    for (String runId : distinctRunIds) {
      elasticSearchService.appendRunIdBySearchGroup(opContext, indexKey, docId, urn, runId);
    }
  }

  /**
   * Builds a V3 search document by combining multiple aspects into a single document with _aspect
   * structure.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   * @return the combined V3 document or null if no aspects to process
   */
  private ObjectNode buildV3SearchDocument(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    EntitySpec entitySpec = events.get(0).getEntitySpec();
    String entityType = entitySpec.getName();
    ObjectNode combinedDocument = searchDocumentProjector.newEntityDocument(urn, entitySpec);

    boolean hasAnyAspects = false;
    List<BatchProjection> projections = new ArrayList<>();
    Set<String> changedRootFields = new HashSet<>();

    for (CoalescedAspectEvent coalesced : coalesceAspectEvents(events)) {
      MCLItem event = coalesced.event();
      try {
        if (isUnchanged(coalesced)) {
          opContext
              .getMetricUtils()
              .ifPresent(
                  metricUtils ->
                      metricUtils.increment(this.getClass(), "search_diff_no_changes_detected", 1));
          // Unchanged, so there is nothing to diff against
          searchDocumentProjector
              .projectAspect(opContext, event, null)
              .ifPresent(unchanged -> projections.add(new BatchProjection(unchanged, true)));
          continue;
        }
        AspectSpec aspectSpec = event.getAspectSpec();
        String aspectName = aspectSpec.getName();

        // Throttle timeseries aspects for entity index writes
        if (aspectSpec.isTimeseries() && timeseriesThrottleCache != null) {
          long eventTimeMs =
              event.getAuditStamp() != null
                  ? event.getAuditStamp().getTime()
                  : System.currentTimeMillis();
          if (timeseriesThrottleCache.shouldThrottle(
              event.getEntitySpec().getName(), urn.toString(), aspectName, eventTimeMs)) {
            if (timeseriesThrottleCache.isEntityIndexEnabled()) {
              if (throttleSummary != null) {
                throttleSummary.recordSuppressed(
                    TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
              }
              continue;
            }
            if (timeseriesThrottleCache.isObserveEnabled() && throttleSummary != null) {
              throttleSummary.recordObserved();
            }
          }
        }

        // Each aspect's fields go to the document root and under _aspects.<aspect>; structured
        // properties go to the root only. In V3, timeseries aspects are treated the same as other
        // versioned aspects.
        Optional<V3SearchDocumentProjector.ProjectedAspect> projectedAspect =
            searchDocumentProjector.projectAspect(opContext, event, coalesced.baseline());
        if (projectedAspect.isPresent()) {
          projections.add(new BatchProjection(projectedAspect.get(), false));
          projectedAspect.get().rootFields().fieldNames().forEachRemaining(changedRootFields::add);
          hasAnyAspects = true;

          // recordWrite is handled by UpdateIndicesService after all strategies have processed
          if (aspectSpec.isTimeseries() && throttleSummary != null) {
            throttleSummary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
          }

          if (aspectSpec.isTimeseries()) {
            log.debug(
                "V3 included timeseries aspect {} in combined document for URN: {}",
                aspectName,
                urn);
          } else {
            log.debug(
                "V3 included versioned aspect {} in combined document for URN: {}",
                aspectName,
                urn);
          }
        }

      } catch (Exception e) {
        log.error(
            "Error processing aspect {} for URN {}: {}",
            event.getAspectName(),
            urn,
            e.getMessage(),
            e);
      }
    }

    if (!hasAnyAspects) {
      return null;
    }

    // Aspects the batch left unchanged are not rewritten, but they keep their place in the order
    // for the root fields a changed aspect writes, so a shared root field ends with the value of
    // the last aspect in the batch that sets it
    for (BatchProjection projection : projections) {
      searchDocumentProjector.applyProjection(
          combinedDocument,
          projection.unchanged()
              ? sharedRootValues(projection.projected(), changedRootFields)
              : keepBatchRootValues(combinedDocument, projection.projected()));
    }

    liftSemanticEmbeddingsToRoot(opContext, urn, entityType, events, combinedDocument);

    applyDocumentContributors(opContext, urn, combinedDocument);
    return combinedDocument;
  }

  /**
   * The event that writes an aspect, the aspect value to diff it against, and whether the aspect
   * must be written even when that value equals the last one.
   */
  private record CoalescedAspectEvent(
      @Nonnull MCLItem event, @Nullable RecordTemplate baseline, boolean mustWrite) {}

  private static boolean isForceIndexing(@Nonnull MCLItem event) {
    SystemMetadata systemMetadata = event.getSystemMetadata();
    return systemMetadata != null
        && systemMetadata.getProperties() != null
        && Boolean.parseBoolean(systemMetadata.getProperties().get(Constants.FORCE_INDEXING_KEY));
  }

  /** An aspect's projection, and whether the batch left the aspect unchanged. */
  private record BatchProjection(
      @Nonnull V3SearchDocumentProjector.ProjectedAspect projected, boolean unchanged) {}

  /**
   * One event per aspect, in order of first appearance: the last event in the batch, diffed against
   * the first previous value of the batch, as V2 does when it coalesces a batch. That value is what
   * the index held before the batch, or, when the batch restated the aspect, the value it held
   * right after that, so removal nulls cover every field the batch dropped; diffing each event
   * against its own predecessor would lose the nulls of all but the last event under {@code
   * _aspects.<aspect>}, which each event replaces. An aspect the batch created has no baseline: the
   * index held none of its values, and a null for one could only clear a root field another aspect
   * supplies. An aspect the batch created or forced is always written.
   *
   * <p>Two aspects can project the same root field (e.g. corpuser displayName from CorpUserInfo and
   * CorpUserEditableInfo). Root fields are last-write-wins, so user-edited override aspects are
   * applied after their ingested base aspect, as the fork does when it rebuilds a document.
   */
  @Nonnull
  private static List<CoalescedAspectEvent> coalesceAspectEvents(@Nonnull List<MCLItem> events) {
    Map<String, MCLItem> lastEventByAspect = new LinkedHashMap<>();
    Map<String, RecordTemplate> baselineByAspect = new HashMap<>();
    // Aspects the batch must write even if their value looks unchanged: the index may not hold the
    // first event's value (it had no previous value), or an event forces indexing
    Set<String> mustWrite = new HashSet<>();
    Set<String> created = new HashSet<>();
    for (MCLItem event : events) {
      String aspectName = event.getAspectName();
      if (!lastEventByAspect.containsKey(aspectName) && event.getPreviousRecordTemplate() == null) {
        mustWrite.add(aspectName);
        if (event.getChangeType() != ChangeType.RESTATE) {
          created.add(aspectName);
        }
      }
      if (isForceIndexing(event)) {
        mustWrite.add(aspectName);
      }
      // A restate carries no previous value, so a later event's previous value is the baseline
      if (baselineByAspect.get(aspectName) == null && !created.contains(aspectName)) {
        baselineByAspect.put(aspectName, event.getPreviousRecordTemplate());
      }
      lastEventByAspect.put(aspectName, event);
    }
    return lastEventByAspect.entrySet().stream()
        .sorted(Comparator.comparing(entry -> isEditableOverrideAspect(entry.getKey())))
        .map(
            entry ->
                new CoalescedAspectEvent(
                    entry.getValue(),
                    baselineByAspect.get(entry.getKey()),
                    mustWrite.contains(entry.getKey())))
        .collect(Collectors.toList());
  }

  /**
   * Skips an aspect whose value did not change, as V2 does outside forced indexing. Rewriting it
   * would let an unchanged base aspect overwrite the root value of its user-edited override.
   */
  private static boolean isUnchanged(@Nonnull CoalescedAspectEvent coalesced) {
    MCLItem event = coalesced.event();
    return !coalesced.mustWrite()
        && event.getChangeType() != ChangeType.DELETE
        && coalesced.baseline() != null
        && event.getRecordTemplate() != null
        && coalesced.baseline().data() != null
        && coalesced.baseline().data().equals(event.getRecordTemplate().data());
  }

  /**
   * A null root value means the aspect dropped the field. When an aspect applied earlier in the
   * batch still sets it, as when a user-edited displayName is removed while the ingested one stays,
   * the earlier value is kept; the null still clears the field under {@code _aspects.<aspect>}.
   */
  @Nonnull
  private static V3SearchDocumentProjector.ProjectedAspect keepBatchRootValues(
      @Nonnull ObjectNode document, @Nonnull V3SearchDocumentProjector.ProjectedAspect projected) {
    ObjectNode rootFields = projected.rootFields().deepCopy();
    List<String> setEarlier = new ArrayList<>();
    rootFields
        .fieldNames()
        .forEachRemaining(
            name -> {
              if (rootFields.get(name).isNull() && document.hasNonNull(name)) {
                setEarlier.add(name);
              }
            });
    rootFields.remove(setEarlier);
    return new V3SearchDocumentProjector.ProjectedAspect(
        projected.aspectName(), rootFields, projected.aspectFields(), projected.rootOnly());
  }

  /**
   * The root values of an unchanged aspect, limited to the root fields a changed aspect of the
   * batch writes. Its other root fields and its {@code _aspects.<aspect>} entry are not rewritten,
   * so an unchanged aspect cannot overwrite a value another aspect set in an earlier batch.
   */
  @Nonnull
  private static V3SearchDocumentProjector.ProjectedAspect sharedRootValues(
      @Nonnull V3SearchDocumentProjector.ProjectedAspect unchanged,
      @Nonnull Set<String> changedRootFields) {
    ObjectNode rootFields = JsonNodeFactory.instance.objectNode();
    unchanged
        .rootFields()
        .fieldNames()
        .forEachRemaining(
            name -> {
              if (changedRootFields.contains(name) && unchanged.rootFields().hasNonNull(name)) {
                rootFields.set(name, unchanged.rootFields().get(name).deepCopy());
              }
            });
    return new V3SearchDocumentProjector.ProjectedAspect(
        unchanged.aspectName(), rootFields, JsonNodeFactory.instance.objectNode(), true);
  }

  // DataHub names user-override aspects with "editable"/"Editable" (corpUserEditableInfo,
  // corpGroupEditableInfo, editableDatasetProperties, editableSchemaMetadata, ...). These overrides
  // conventionally take precedence over their ingested base aspect, so they are applied last when
  // resolving a shared root convenience field.
  private static boolean isEditableOverrideAspect(@Nullable final String aspectName) {
    return aspectName != null && aspectName.toLowerCase(Locale.ROOT).contains("editable");
  }

  private void liftSemanticEmbeddingsToRoot(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityType,
      @Nonnull List<MCLItem> events,
      @Nonnull ObjectNode combinedDocument) {
    if (!SemanticEmbeddingMappings.isEnabledForEntity(semanticSearchConfiguration, entityType)) {
      return;
    }
    JsonNode aspects = combinedDocument.get(MappingConstants.ASPECTS_FIELD_NAME);
    if (aspects != null && aspects.isObject() && aspects.has("semanticContent")) {
      JsonNode semanticContentNode = aspects.get("semanticContent");
      if (semanticContentNode instanceof ObjectNode semanticContent) {
        for (String field :
            List.of(
                SemanticEmbeddingMappings.EMBEDDINGS_FIELD,
                SemanticEmbeddingMappings.SKIP_REASON_FIELD,
                SemanticEmbeddingMappings.SKIPPED_AT_FIELD)) {
          if (semanticContent.has(field)) {
            combinedDocument.set(field, semanticContent.get(field));
            semanticContent.remove(field);
          }
        }
      }
    }
    boolean shouldStamp =
        events.stream()
            .map(MCLItem::getAspectName)
            .anyMatch(SemanticDocumentProvenance::isStampEligibleAspect);
    if (!shouldStamp) {
      return;
    }
    // Combined V3 docs keep embed text under _aspects, so stamp as semanticContent (fetch both
    // sides) regardless of which content aspect arrived first in the batch.
    SemanticDocumentProvenance.stampResolvedTextSha256(
        opContext, urn, entityType, "semanticContent", combinedDocument);
  }

  private void applyDocumentContributors(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull ObjectNode document) {
    for (V3SearchDocumentContributor contributor : documentContributors) {
      ObjectNode extras = JsonNodeFactory.instance.objectNode();
      contributor.contribute(opContext, urn, extras);
      extras
          .fields()
          .forEachRemaining(
              entry -> {
                String fieldName = entry.getKey();
                if (STRATEGY_OWNED_DOCUMENT_FIELDS.contains(fieldName) || document.has(fieldName)) {
                  throw new IllegalStateException(
                      "V3 search document contributor attempted to overwrite field '"
                          + fieldName
                          + "' for "
                          + urn);
                }
                document.set(fieldName, entry.getValue());
              });
    }
  }

  @Nonnull
  private String v3IndexKey(@Nonnull EntitySpec entitySpec) {
    return V3IndexKeys.resolve(entitySpec);
  }
}
