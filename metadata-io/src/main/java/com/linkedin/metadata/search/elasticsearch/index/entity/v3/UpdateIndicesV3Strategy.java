package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.service.UpdateIndicesService.UPDATE_CHANGE_TYPES;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.RetryableIndexingException;
import com.linkedin.metadata.service.SearchIndexFailureClassifier;
import com.linkedin.metadata.service.TimeseriesIndexWriter;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.indices.PutMappingRequest;

/**
 * V3 update indices strategy implementation for UpdateIndicesService. This handles the new v3
 * mapping approach with multi-entity indices.
 *
 * <p>Baseline and catch-up writes write the complete document. Catch-up and a baseline batch that
 * does not carry every stored aspect hydrate primary storage first; a complete-entity baseline
 * batch is the stored state. Live writes project only the aspects carried by their MCL batch and
 * merge that patch into the document, creating it when absent, without hydrating storage or reading
 * the existing source.
 *
 * <p>No write carries an external version. Live and catch-up events for one URN share a change-log
 * partition, so they reach the index in order and the later one wins. An upgrade fill names its
 * target index and reads primary storage outside that order, so it only creates documents that are
 * absent: a live write or tombstone already in the index is never overwritten by an older snapshot.
 * A partial document a live merge creates ahead of the fill, and a fill-read race that leaves a
 * stale document, are repaired by catch-up, whose window starts when the fill is planned. A
 * baseline that names no target is operator maintenance of the alias and replaces documents.
 *
 * <p>A hard delete removes the document. While a fill could still bring the entity back, the index
 * it writes holds a tombstone instead, which catch-up removes once it completes.
 */
@Slf4j
public class UpdateIndicesV3Strategy implements UpdateIndicesStrategy {

  private final AtomicLong drainedSuccessfulV3Writes = new AtomicLong();
  private final AtomicLong drainedStaleVersionRejected = new AtomicLong();

  // The MAE search client shares two pooled connections between the bulk processor and these
  // puts. One put at a time leaves bulk writes a connection, and the attempts fit well inside
  // the Kafka max.poll.interval.ms default of five minutes.
  private static final int PUT_MAPPING_MAX_ATTEMPTS = 2;
  private static final int PUT_MAPPING_SOCKET_TIMEOUT_MILLIS = 60_000;
  private static final int PUT_MAPPING_DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS = 10_000;
  private static final long PUT_MAPPING_RETRY_BACKOFF_MILLIS = 1_000L;
  private static final long PUT_MAPPING_BUDGET_MILLIS = 150_000L;

  /**
   * Structured-property field ids each V3 index has acknowledged in this JVM. Filled only by
   * acknowledged puts, never by reading mappings, so a failed put is sent again by the next batch.
   */
  private final Map<String, Set<String>> appliedStructuredPropertyFields =
      new ConcurrentHashMap<>();

  private final Object structuredPropertyMappingLock = new Object();

  private static final int MAX_READ_ATTEMPTS = 3;
  private static final String PRIMARY_HYDRATION_FAILURE_METRIC = "v3_primary_hydration_read_failed";
  private static final String TIMESERIES_HYDRATION_FAILURE_METRIC =
      "v3_timeseries_hydration_read_failed";
  private static final String EXISTING_SOURCE_FAILURE_METRIC = "v3_existing_source_read_failed";
  private static final String LEGACY_SYSTEM_METADATA_FIELD = "_systemmetadata";
  private static final Set<String> EXISTING_TIMESERIES_ROOT_EXCLUDED_FIELDS =
      Set.of(
          V3SearchDocumentProjector.SYSTEM_METADATA_FIELD,
          LEGACY_SYSTEM_METADATA_FIELD,
          MappingConstants.ASPECTS_FIELD_NAME,
          V3SearchDocumentProjector.URN_FIELD,
          V3SearchDocumentProjector.ENTITY_TYPE_FIELD,
          V3SearchDocumentProjector.TOMBSTONE_FIELD,
          V3IndexOnlyFieldMerger.RUN_ID_FIELD,
          V3IndexOnlyFieldMerger.SYSTEM_INDEX_MODIFIED_FIELD);

  private final EntityIndexVersionConfiguration v3Config;
  private final ElasticSearchService elasticSearchService;
  private final V3SearchDocumentProjector searchDocumentProjector;
  private final V3IndexOnlyFieldMerger indexOnlyFieldMerger;
  private final TimeseriesAspectService timeseriesAspectService;
  private final MultiEntityMappingsBuilder mappingsBuilder;
  @Nullable private final TimeseriesWriteThrottleCache timeseriesThrottleCache;
  private static final Set<String> STRATEGY_OWNED_DOCUMENT_FIELDS =
      MappingConstants.STRATEGY_OWNED_ROOT_FIELDS;

  private final EntityDocumentIdHasher entityDocumentIdHasher;
  private final List<V3SearchDocumentContributor> documentContributors;
  private final boolean v2Enabled;
  private final String idHashAlgo;
  @Nullable private final SemanticSearchConfiguration semanticSearchConfiguration;
  @Nullable private final IncrementalReindexWriteTargets incrementalReindexWriteTargets;
  // Accepted for builder parity with V2; V3 does not read it.
  @Nullable private final StructuredPropertyMappingCache structuredPropertyMappingCache;
  @Nullable private final List<V3SearchIndexSkip> searchIndexSkips;

  @Builder
  public UpdateIndicesV3Strategy(
      @Nonnull EntityIndexVersionConfiguration v3Config,
      @Nonnull ElasticSearchService elasticSearchService,
      @Nonnull SearchDocumentTransformer searchDocumentTransformer,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      final boolean v2Enabled,
      @Nullable String idHashAlgo,
      @Nullable TimeseriesWriteThrottleCache timeseriesThrottleCache,
      @Nullable SearchClientShim<?> searchClientShim,
      final boolean vectorProjectionEnabled,
      @Nonnull Map<String, Integer> expectedVectorDimensions,
      @Nonnull EntityDocumentIdHasher entityDocumentIdHasher,
      @Nullable List<V3SearchDocumentContributor> documentContributors,
      @Nullable SemanticSearchConfiguration semanticSearchConfiguration,
      @Nullable IncrementalReindexWriteTargets incrementalReindexWriteTargets,
      @Nullable StructuredPropertyMappingCache structuredPropertyMappingCache,
      @Nullable List<V3SearchIndexSkip> searchIndexSkips) {
    this.v3Config = v3Config;
    this.elasticSearchService = elasticSearchService;
    this.searchDocumentProjector =
        new V3SearchDocumentProjector(
            searchDocumentTransformer,
            vectorProjectionEnabled,
            expectedVectorDimensions == null || expectedVectorDimensions.isEmpty()
                ? configuredVectorDimensions(semanticSearchConfiguration)
                : expectedVectorDimensions,
            semanticEntityNames(semanticSearchConfiguration));
    this.indexOnlyFieldMerger = new V3IndexOnlyFieldMerger();
    this.timeseriesAspectService = timeseriesAspectService;
    this.timeseriesThrottleCache = timeseriesThrottleCache;
    this.entityDocumentIdHasher =
        entityDocumentIdHasher == null
            ? new Sha256UrnEntityDocumentIdHasher()
            : entityDocumentIdHasher;
    this.documentContributors =
        documentContributors == null ? List.of() : List.copyOf(documentContributors);
    this.v2Enabled = v2Enabled;
    this.idHashAlgo = idHashAlgo == null || idHashAlgo.isBlank() ? "MD5" : idHashAlgo;
    this.semanticSearchConfiguration = semanticSearchConfiguration;
    this.incrementalReindexWriteTargets = incrementalReindexWriteTargets;
    this.structuredPropertyMappingCache = structuredPropertyMappingCache;
    this.searchIndexSkips = searchIndexSkips == null ? List.of() : List.copyOf(searchIndexSkips);
    try {
      this.mappingsBuilder =
          new MultiEntityMappingsBuilder(
              com.linkedin.metadata.config.search.EntityIndexConfiguration.builder()
                  .v3(v3Config)
                  .build(),
              searchClientShim,
              ESUtils.KEYWORD_MAXLENGTH,
              List.of());
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize V3 mappings builder", e);
    }
  }

  /**
   * Dimensions for the configured models. A model missing from this map is treated as retired and
   * its vectors are dropped, so fall back to the semantic configuration rather than drop every
   * vector when a caller supplies that configuration alone.
   */
  @Nonnull
  private static Map<String, Integer> configuredVectorDimensions(
      @Nullable SemanticSearchConfiguration semanticSearchConfiguration) {
    if (semanticSearchConfiguration == null || semanticSearchConfiguration.getModels() == null) {
      return Map.of();
    }
    return semanticSearchConfiguration.getModels().entrySet().stream()
        .filter(entry -> entry.getValue() != null)
        .collect(
            Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getVectorDimension()));
  }

  @Nonnull
  private static Set<String> semanticEntityNames(
      @Nullable SemanticSearchConfiguration semanticSearchConfiguration) {
    if (semanticSearchConfiguration == null
        || !semanticSearchConfiguration.isEnabled()
        || semanticSearchConfiguration.getEnabledEntities() == null) {
      return Set.of();
    }
    return Set.copyOf(semanticSearchConfiguration.getEnabledEntities());
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

    final List<PreparedUrnBatch> preparedBatches = new ArrayList<>();
    for (Map.Entry<Urn, List<MCLItem>> entry : groupedEvents.entrySet()) {
      final Urn urn = entry.getKey();
      final List<MCLItem> indexEvents =
          entry.getValue().stream()
              .filter(event -> !shouldSkipSearchIndexUpdate(event))
              .collect(Collectors.toList());

      if (indexEvents.isEmpty()) {
        continue;
      }

      log.debug("Processing {} events for URN: {} with V3 unified batch", indexEvents.size(), urn);

      prepareUrnBatch(opContext, urn, indexEvents).ifPresent(preparedBatches::add);
    }

    final Map<Urn, Map<String, EnvelopedAspect>> latestTimeseriesByUrn =
        hydrateLatestTimeseriesAspects(opContext, preparedBatches);

    // A fill or catch-up checkpoints once this returns, so those writes have to land first. A
    // delete mirrored into an index a fill is building has to land too: nothing replays it. An
    // ordinary live batch is left queued. The next batch does not read what this one wrote.
    // The drain still runs when processing throws, so a fill's earlier writes are not left for a
    // later batch to account. The original processing failure still propagates, with any drain
    // failure attached as suppressed.
    final boolean drainWrites = preparedBatches.stream().anyMatch(this::requiresDurableDrain);
    RuntimeException processingFailure = null;
    try {
      // The timeseries index is not versioned with the entity index. V2 writes it when that
      // strategy is enabled; otherwise this strategy writes the same documents.
      if (!v2Enabled) {
        writeSharedTimeseriesIndex(opContext, groupedEvents, throttleSummary);
      }
      // One cluster-state update for every structured-property definition in the batch. Doing this
      // per definition, and then per index, holds the consumer long enough that later events in
      // the same partition (a dataset profile, for one) are not indexed before readers give up.
      applyCoalescedStructuredPropertyMappings(
          opContext, preparedBatches, structuredPropertiesHookEnabled);
      for (PreparedUrnBatch preparedBatch : preparedBatches) {
        processUrnBatch(
            opContext,
            preparedBatch,
            latestTimeseriesByUrn.getOrDefault(preparedBatch.urn(), Collections.emptyMap()),
            throttleSummary);
      }
      if (throttleSummary != null) {
        throttleSummary.logIfSuppressed();
      }
    } catch (RuntimeException e) {
      processingFailure = e;
    }

    if (drainWrites) {
      try {
        flushPendingWrites();
      } catch (RuntimeException drainFailure) {
        if (processingFailure == null) {
          throw drainFailure;
        }
        // A retryable drain failure means the buffered writes did not land and a redelivery can
        // recover them, so prefer it over a non-retryable processing failure. Otherwise it would be
        // attached as a suppressed exception and missed — the MAE transports detect retryability by
        // walking the cause chain, not the suppressed list.
        if (drainFailure instanceof RetryableIndexingException
            && !(processingFailure instanceof RetryableIndexingException)) {
          drainFailure.addSuppressed(processingFailure);
          throw drainFailure;
        }
        processingFailure.addSuppressed(drainFailure);
      }
    }
    if (processingFailure != null) {
      throw processingFailure;
    }
  }

  /**
   * Upserts each timeseries update into the shared timeseries index. Entity-index throttling is
   * applied later and does not suppress this write; only the timeseries-index throttle does.
   */
  private void writeSharedTimeseriesIndex(
      @Nonnull final OperationContext opContext,
      @Nonnull final Map<Urn, List<MCLItem>> groupedEvents,
      @Nullable final TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {
    for (List<MCLItem> urnEvents : groupedEvents.values()) {
      for (MCLItem event : urnEvents) {
        if (event.getAspectSpec() == null
            || !event.getAspectSpec().isTimeseries()
            || !UPDATE_CHANGE_TYPES.contains(event.getChangeType())) {
          continue;
        }
        if (suppressTimeseriesIndexWrite(event, throttleSummary)) {
          continue;
        }
        TimeseriesIndexWriter.upsert(opContext, timeseriesAspectService, idHashAlgo, event);
        if (throttleSummary != null
            && timeseriesThrottleCache != null
            && timeseriesThrottleCache.isEnabled()) {
          throttleSummary.recordWritten(
              TimeseriesWriteThrottleCache.ThrottleTarget.TIMESERIES_INDEX);
        }
      }
    }
  }

  private boolean suppressTimeseriesIndexWrite(
      @Nonnull final MCLItem event,
      @Nullable final TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {
    if (timeseriesThrottleCache == null) {
      return false;
    }
    boolean timeseriesIndexEnabled = timeseriesThrottleCache.isTimeseriesIndexEnabled();
    boolean observeEnabled = timeseriesThrottleCache.isObserveEnabled();
    if (!timeseriesIndexEnabled && !observeEnabled) {
      return false;
    }

    long eventTimeMs =
        event.getAuditStamp() == null
            ? System.currentTimeMillis()
            : event.getAuditStamp().getTime();
    boolean throttled =
        timeseriesThrottleCache.shouldThrottle(
            event.getEntitySpec().getName(),
            event.getUrn().toString(),
            event.getAspectName(),
            eventTimeMs);
    if (throttled && timeseriesIndexEnabled) {
      if (throttleSummary != null) {
        throttleSummary.recordSuppressed(
            TimeseriesWriteThrottleCache.ThrottleTarget.TIMESERIES_INDEX);
      }
      return true;
    }
    if (throttled && observeEnabled && throttleSummary != null) {
      throttleSummary.recordObserved();
    }
    return false;
  }

  /**
   * A baseline fill and a catch-up replay are checkpointed when the batch returns, so their bulk
   * writes are drained first. A live delete copied into an index a fill is building is drained too,
   * because that copy is the only record of the delete. Any other live write stays queued.
   */
  private boolean requiresDurableDrain(@Nonnull final PreparedUrnBatch preparedBatch) {
    if (preparedBatch.baselineFill() || preparedBatch.catchUpReplay()) {
      return true;
    }
    return carriesDelete(preparedBatch)
        && incrementalReindexWriteTargets != null
        && incrementalReindexWriteTargets.fillTargetFor(preparedBatch.indexName()).isPresent();
  }

  private void flushPendingWrites() {
    BulkProcessorFlushResult result = BulkProcessorFlushResult.empty();
    for (ESBulkProcessor bulkProcessor : elasticSearchService.getEsWriteDAO().bulkProcessors()) {
      result = result.plus(bulkProcessor.flushAndAwait());
    }
    if (result.hasFailures()) {
      final String detail =
          String.format(
              "unexpectedItemFailures=%d, transportFailedOperations=%d, incompleteOperations=%d,"
                  + " timedOut=%s",
              result.unexpectedItemFailures(),
              result.transportFailedOperations(),
              result.incompleteOperations(),
              result.timedOut());
      if (result.unexpectedItemFailures() > 0) {
        // Poison: item-level rejections (mapping errors, non-stale conflicts) will not succeed on
        // retry. Do not escalate for redelivery — a bounded-retry consumer would just re-fail and
        // head-of-line-block the batch. Fail loudly; the MAE transports keep at-most-once and
        // commit past it, while batch jobs (LoadIndices) still fail the whole run.
        throw new IllegalStateException(
            "V3 search writes hit non-retryable item failures before rebuilding documents ("
                + detail
                + ")",
            result.flushException());
      }
      // Transient (transport error / timeout / incomplete drain): a bounded redelivery can
      // succeed, so surface it as retryable. The MAE transports escalate this to redelivery;
      // batch jobs (LoadIndices) fail the run and are re-run.
      throw new RetryableIndexingException(
          "V3 search writes failed transiently before rebuilding documents (" + detail + ")",
          result.flushException() != null
              ? result.flushException()
              : new IllegalStateException("bulk drain did not complete: " + detail));
    }
    // Intermediate drains consume the bulk processor's windowed flush stats; accumulate them so
    // run-level reports (LoadIndices) can add them to the final flush instead of losing them.
    drainedSuccessfulV3Writes.addAndGet(result.successfulV3Writes());
    drainedStaleVersionRejected.addAndGet(result.staleVersionRejected());
    log.debug("Drained pending V3 search writes: {}", result);
  }

  /** Totals accumulated from intermediate {@link #flushPendingWrites()} drains. */
  public record DrainedWriteStats(long successfulV3Writes, long staleVersionRejected) {}

  /** Returns and resets the accumulated intermediate drain totals. */
  @Nonnull
  public DrainedWriteStats consumeDrainedWriteStats() {
    return new DrainedWriteStats(
        drainedSuccessfulV3Writes.getAndSet(0), drainedStaleVersionRejected.getAndSet(0));
  }

  /**
   * Puts every structured-property definition in the batch onto every V3 index in one request.
   *
   * <p>The definition written for a URN is that URN's surviving event. A catch-up replay carries
   * definitions that are already on the indices it is rebuilding, and a delete carries no
   * definition. Re-saving a definition puts its mapping again: that re-apply is the only repair
   * when the structured-property system-update machinery is disabled (the default).
   *
   * <p>The body is only the shared {@code structuredProperties} object. Put mapping merges it, so
   * each index keeps its other fields. The indices are named explicitly; a {@code *index_v3}
   * wildcard also matches rebuild backing indices. One rejected index fails the whole request, so
   * that falls back to one {@code applyMappings} per index.
   */
  private void applyCoalescedStructuredPropertyMappings(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<PreparedUrnBatch> preparedBatches,
      final boolean structuredPropertiesHookEnabled) {
    if (!structuredPropertiesHookEnabled) {
      return;
    }
    final List<Pair<Urn, StructuredPropertyDefinition>> definitions =
        structuredPropertyDefinitions(preparedBatches);
    if (definitions.isEmpty()) {
      return;
    }
    final Map<String, Object> mappingBody =
        buildStructuredPropertyMappingBody(opContext, definitions);
    if (mappingBody.isEmpty()) {
      return;
    }
    final Set<String> fieldIds = StructuredPropertyMappingCache.fieldNames(mappingBody);
    if (fieldIds.isEmpty() || missingFieldIndices(opContext, fieldIds).isEmpty()) {
      return;
    }
    synchronized (structuredPropertyMappingLock) {
      // Another thread may have applied the same fields while this one waited.
      final List<String> indexNames = missingFieldIndices(opContext, fieldIds);
      if (indexNames.isEmpty()) {
        return;
      }
      putStructuredPropertyMappings(
          opContext, definitions.size(), indexNames, mappingBody, fieldIds);
    }
  }

  /**
   * The combined body for {@code definitions}. One definition that cannot be projected must not
   * keep the rest of the batch from becoming searchable, so a failure retries each definition on
   * its own and merges the bodies of those that build.
   */
  @Nonnull
  private Map<String, Object> buildStructuredPropertyMappingBody(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<Pair<Urn, StructuredPropertyDefinition>> definitions) {
    try {
      return structuredPropertyMappingBody(opContext, definitions);
    } catch (RuntimeException e) {
      log.error(
          "Failed to build V3 structured property mappings for {} definitions; building each one",
          definitions.size(),
          e);
    }
    final Map<String, Object> mergedFields = new LinkedHashMap<>();
    Map<String, Object> structuredPropertiesTemplate = null;
    for (Pair<Urn, StructuredPropertyDefinition> definition : definitions) {
      try {
        final Map<String, Object> body =
            structuredPropertyMappingBody(opContext, List.of(definition));
        final Map<String, Object> structuredProperties = structuredPropertiesOf(body);
        if (structuredProperties == null) {
          continue;
        }
        if (structuredPropertiesTemplate == null) {
          structuredPropertiesTemplate = structuredProperties;
        }
        mergedFields.putAll(structuredPropertyFields(structuredProperties));
      } catch (RuntimeException e) {
        log.error(
            "Failed to build V3 structured property mapping for {}; skipping it",
            definition.getKey(),
            e);
      }
    }
    if (structuredPropertiesTemplate == null || mergedFields.isEmpty()) {
      return Map.of();
    }
    final Map<String, Object> structuredProperties = new HashMap<>(structuredPropertiesTemplate);
    structuredProperties.put("properties", mergedFields);
    return Map.of(
        "properties", Map.of(Constants.STRUCTURED_PROPERTY_MAPPING_FIELD, structuredProperties));
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private static Map<String, Object> structuredPropertiesOf(
      @Nonnull final Map<String, Object> body) {
    if (body.get("properties") instanceof Map<?, ?> properties
        && properties.get(Constants.STRUCTURED_PROPERTY_MAPPING_FIELD) instanceof Map<?, ?> value) {
      return (Map<String, Object>) value;
    }
    return null;
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private static Map<String, Object> structuredPropertyFields(
      @Nonnull final Map<String, Object> structuredProperties) {
    return structuredProperties.get("properties") instanceof Map<?, ?> fields
        ? (Map<String, Object>) fields
        : Map.of();
  }

  /** V3 entity indices that have not acknowledged every one of {@code fieldIds}. */
  @Nonnull
  private List<String> missingFieldIndices(
      @Nonnull final OperationContext opContext, @Nonnull final Set<String> fieldIds) {
    return mappingTargetIndexNames(opContext).stream()
        .filter(
            indexName ->
                !appliedStructuredPropertyFields
                    .getOrDefault(indexName, Set.of())
                    .containsAll(fieldIds))
        .collect(Collectors.toList());
  }

  /**
   * The V3 aliases plus the separate index a fill in flight is building behind one. That index is
   * not reachable through its alias yet, and a definition saved mid-fill would otherwise be missing
   * from the mapping the fill's documents are written under.
   */
  @Nonnull
  private List<String> mappingTargetIndexNames(@Nonnull final OperationContext opContext) {
    final List<String> aliases = v3EntityIndexNames(opContext);
    if (incrementalReindexWriteTargets == null) {
      return aliases;
    }
    final Set<String> targets = new LinkedHashSet<>(aliases);
    for (String alias : aliases) {
      incrementalReindexWriteTargets.fillTargetFor(alias).ifPresent(targets::add);
    }
    return List.copyOf(targets);
  }

  private void recordAppliedFields(
      @Nonnull final Collection<String> indexNames, @Nonnull final Set<String> fieldIds) {
    for (String indexName : indexNames) {
      appliedStructuredPropertyFields.merge(
          indexName,
          Set.copyOf(fieldIds),
          (applied, added) -> {
            final Set<String> merged = new HashSet<>(applied);
            merged.addAll(added);
            return Set.copyOf(merged);
          });
    }
  }

  /**
   * One put naming every index in {@code indexNames}. A transport failure, 429 or 5xx is tried once
   * more while the budget allows, then left unrecorded for the next batch. Any other failure, such
   * as a 4xx mapping rejection, can come from one bad index failing the whole request, so each
   * index is then applied on its own.
   */
  private void putStructuredPropertyMappings(
      @Nonnull final OperationContext opContext,
      final int definitionCount,
      @Nonnull final List<String> indexNames,
      @Nonnull final Map<String, Object> mappingBody,
      @Nonnull final Set<String> fieldIds) {
    final SearchClientShim<?> searchClient =
        elasticSearchService.getIndexBuilder(indexNames.get(0)).getSearchClient();
    final RequestOptions options = putMappingRequestOptions(searchClient);
    final long deadline = System.currentTimeMillis() + PUT_MAPPING_BUDGET_MILLIS;
    final long attemptMillis =
        (long) options.getRequestConfig().getConnectionRequestTimeout()
            + PUT_MAPPING_SOCKET_TIMEOUT_MILLIS;
    for (int attempt = 1; ; attempt++) {
      try {
        log.info(
            "Applying {} V3 structured property definition(s) to {} indices, attempt {}",
            definitionCount,
            indexNames.size(),
            attempt);
        searchClient.putIndexMapping(
            opContext,
            new PutMappingRequest(indexNames.toArray(String[]::new)).source(mappingBody),
            options);
        recordAppliedFields(indexNames, fieldIds);
        return;
      } catch (Exception e) {
        if (!isRetryablePutMappingFailure(e)) {
          log.error(
              "V3 structured property mapping update rejected for {} definitions; applying per index",
              definitionCount,
              e);
          applyStructuredPropertyMappingsPerIndex(opContext, indexNames, mappingBody, fieldIds);
          return;
        }
        final boolean outOfBudget =
            System.currentTimeMillis() + PUT_MAPPING_RETRY_BACKOFF_MILLIS + attemptMillis
                > deadline;
        if (attempt >= PUT_MAPPING_MAX_ATTEMPTS || outOfBudget) {
          log.error(
              "V3 structured property mapping update failed after {} attempt(s) for {} definitions"
                  + " on indices {}; the next batch with these definitions sends it again",
              attempt,
              definitionCount,
              indexNames,
              e);
          return;
        }
        log.warn(
            "V3 structured property mapping update attempt {} failed; retrying in {}ms",
            attempt,
            PUT_MAPPING_RETRY_BACKOFF_MILLIS,
            e);
        try {
          Thread.sleep(PUT_MAPPING_RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private static boolean isRetryablePutMappingFailure(@Nonnull final Exception failure) {
    final OptionalInt httpStatus = SearchIndexFailureClassifier.findHttpStatus(failure);
    if (httpStatus.isPresent()) {
      return SearchIndexFailureClassifier.isRetryableHttpStatus(httpStatus.getAsInt());
    }
    Throwable current = failure;
    while (current != null) {
      if (current instanceof IOException || current instanceof TimeoutException) {
        return true;
      }
      current = current.getCause() == current ? null : current.getCause();
    }
    return false;
  }

  /**
   * A request config replaces the client's defaults rather than merging with them, so the connect
   * and connection-request timeouts are set alongside the longer socket timeout.
   */
  @Nonnull
  private static RequestOptions putMappingRequestOptions(
      @Nonnull final SearchClientShim<?> searchClient) {
    Integer connectionRequestTimeout = null;
    final SearchClientShim.ShimConfiguration shimConfiguration =
        searchClient.getShimConfiguration();
    if (shimConfiguration != null) {
      connectionRequestTimeout = shimConfiguration.getConnectionRequestTimeout();
    }
    if (connectionRequestTimeout == null || connectionRequestTimeout <= 0) {
      connectionRequestTimeout = PUT_MAPPING_DEFAULT_CONNECTION_REQUEST_TIMEOUT_MILLIS;
    }
    return RequestOptions.DEFAULT.toBuilder()
        .setRequestConfig(
            RequestConfig.custom()
                .setConnectTimeout(RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS)
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .setSocketTimeout(PUT_MAPPING_SOCKET_TIMEOUT_MILLIS)
                .build())
        .build();
  }

  /**
   * The surviving structured-property definition of each URN. Catch-up events and deletes are
   * skipped, matching the per-event mapping hook this replaces.
   */
  @Nonnull
  private static List<Pair<Urn, StructuredPropertyDefinition>> structuredPropertyDefinitions(
      @Nonnull final List<PreparedUrnBatch> preparedBatches) {
    final Map<Urn, StructuredPropertyDefinition> definitionsByUrn = new LinkedHashMap<>();
    for (PreparedUrnBatch preparedBatch : preparedBatches) {
      for (MCLItem event : preparedBatch.coalescedEvents().finalEvents()) {
        if (isCatchUpBackfillEvent(event) || event.getRecordTemplate() == null) {
          continue;
        }
        final EntitySpec entitySpec = event.getEntitySpec();
        final AspectSpec aspectSpec = event.getAspectSpec();
        if (entitySpec == null
            || aspectSpec == null
            || !Constants.STRUCTURED_PROPERTY_ENTITY_NAME.equals(entitySpec.getName())
            || !Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspectSpec.getName())) {
          continue;
        }
        final StructuredPropertyDefinition definition;
        try {
          definition = new StructuredPropertyDefinition(event.getRecordTemplate().data().copy());
        } catch (CloneNotSupportedException e) {
          log.error("Failed to copy structured property definition for URN {}", event.getUrn(), e);
          continue;
        }
        if (definition.getEntityTypes() == null || definition.getEntityTypes().isEmpty()) {
          continue;
        }
        definitionsByUrn.put(event.getUrn(), definition);
      }
    }
    return definitionsByUrn.entrySet().stream()
        .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  /**
   * The {@code structuredProperties} object for these definitions, wrapped as a put-mapping body.
   * Empty when none of the definitions name a searchable entity type.
   */
  @Nonnull
  private Map<String, Object> structuredPropertyMappingBody(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<Pair<Urn, StructuredPropertyDefinition>> definitions) {
    final Collection<MappingsBuilder.IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(opContext, definitions);
    if (mappings.isEmpty()) {
      return Map.of();
    }
    final Object properties = mappings.iterator().next().getMappings().get("properties");
    if (!(properties instanceof Map<?, ?> propertyMap)) {
      return Map.of();
    }
    final Object structuredProperties =
        propertyMap.get(Constants.STRUCTURED_PROPERTY_MAPPING_FIELD);
    if (!(structuredProperties instanceof Map<?, ?> structuredPropertyMap)) {
      return Map.of();
    }
    final Object fields = structuredPropertyMap.get("properties");
    if (!(fields instanceof Map<?, ?> fieldMap) || fieldMap.isEmpty()) {
      return Map.of();
    }
    final Map<String, Object> bodyProperties = new HashMap<>();
    bodyProperties.put(Constants.STRUCTURED_PROPERTY_MAPPING_FIELD, structuredProperties);
    final Map<String, Object> body = new HashMap<>();
    body.put("properties", bodyProperties);
    return body;
  }

  @Nonnull
  private static List<String> v3EntityIndexNames(@Nonnull final OperationContext opContext) {
    return V3IndexKeys.groupEntitySpecs(opContext.getEntityRegistry()).keySet().stream()
        .map(
            indexKey ->
                opContext
                    .getSearchContext()
                    .getIndexConvention()
                    .getEntityIndexNameV3(opContext, indexKey))
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * One {@code applyMappings} per index of the same body the combined request would have sent. A
   * failure on one index does not stop the others, and only the indices that succeed are recorded.
   */
  private void applyStructuredPropertyMappingsPerIndex(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<String> indexNames,
      @Nonnull final Map<String, Object> mappingBody,
      @Nonnull final Set<String> fieldIds) {
    for (String indexName : indexNames) {
      try {
        final Map<String, Object> currentMappings = currentIndexMappings(opContext, indexName);
        final ReindexConfig indexState =
            ReindexConfig.builder()
                .name(indexName)
                .exists(true)
                .currentMappings(currentMappings)
                .targetMappings(mappingBody)
                .build();
        elasticSearchService.getIndexBuilder(indexName).applyMappings(opContext, indexState, false);
        recordAppliedFields(List.of(indexName), fieldIds);
      } catch (Exception e) {
        log.error("Failed to apply V3 structured property mappings to index {}", indexName, e);
      }
    }
  }

  /**
   * The index's own mapping, so a field that is already there is not classified as new and a
   * definition whose type changed is not put in place. An unreadable mapping falls back to an empty
   * one, which treats every field as an addition.
   */
  @Nonnull
  private Map<String, Object> currentIndexMappings(
      @Nonnull final OperationContext opContext, @Nonnull final String indexName) {
    try {
      final Map<String, Object> mapping =
          elasticSearchService.indexMappingSource(opContext, indexName);
      if (mapping != null && !mapping.isEmpty()) {
        return mapping;
      }
    } catch (Exception e) {
      log.warn(
          "Could not read the mapping of V3 index {}; treating every field as new", indexName, e);
    }
    return Map.of("properties", Map.of());
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
        // mapping a previous attempt failed to write, without waiting for the next
        // system-update to add it. Entity types removed from the definition are handled by the
        // dedicated removal path, not here.
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

  @Override
  public boolean fillsNewIndicesFromPrimaryStorage() {
    return true;
  }

  /** Resolves where one URN's events are written and which write path they take. */
  private Optional<PreparedUrnBatch> prepareUrnBatch(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final List<MCLItem> events) {
    final CoalescedEvents coalescedEvents = coalesceFinalEvents(events);
    final List<MCLItem> effectiveEvents = coalescedEvents.finalEvents();
    if (effectiveEvents.isEmpty()) {
      return Optional.empty();
    }

    final EntitySpec entitySpec = effectiveEvents.get(0).getEntitySpec();
    // Explicit searchGroup consolidates entities into a shared V3 index; unset resolves to the
    // entity-named index, matching V2's per-entity splits.
    final String searchGroup = V3IndexKeys.resolve(entitySpec);

    final String docId = entityDocumentIdHasher.documentId(opContext, urn);
    // An MCL side effect joined to this URN carries no backfill mode. It must not turn a fill into
    // a live merge, nor a complete fill into one that hydrates. A live event does carry no mode
    // either, and one sharing the batch is projected with the fill; catch-up repairs the alias.
    final List<MCLItem> classifiedEvents =
        effectiveEvents.stream()
            .filter(event -> backfillMode(event).isPresent())
            .collect(Collectors.toList());
    final boolean baselineFill =
        !classifiedEvents.isEmpty()
            && classifiedEvents.stream().allMatch(UpdateIndicesV3Strategy::isBaselineBackfillEvent);
    final boolean completeBaselineFill =
        baselineFill
            && classifiedEvents.stream()
                .allMatch(UpdateIndicesV3Strategy::isCompleteEntityFillEvent);
    final boolean catchUpReplay =
        classifiedEvents.stream().anyMatch(UpdateIndicesV3Strategy::isCatchUpBackfillEvent);
    final boolean hasKeyAspectDeletion =
        effectiveEvents.stream()
            .filter(
                event ->
                    ChangeType.DELETE.equals(event.getChangeType())
                        && !isCatchUpBackfillEvent(event))
            .map(UpdateIndicesUtil::extractSpecPair)
            .anyMatch(UpdateIndicesUtil::isDeletingKey);
    // A replay (the catch-up step, or operator LoadIndices in catch-up mode) describes rows read
    // from a snapshot of primary storage. The document is rebuilt from primary storage as it
    // stands now, and an entity that is gone from it is removed whatever the replayed row says.
    final Map<String, AspectSpec> searchableTimeseriesAspectSpecs =
        getSearchableTimeseriesAspectSpecs(entitySpec);
    final Optional<String> fillTarget =
        baselineFill ? fillTargetIndex(classifiedEvents) : Optional.empty();
    return Optional.of(
        new PreparedUrnBatch(
            urn,
            events,
            coalescedEvents,
            entitySpec,
            searchGroup,
            fillTarget.orElseGet(() -> v3IndexName(opContext, searchGroup)),
            docId,
            baselineFill,
            fillTarget.isPresent(),
            completeBaselineFill,
            hasKeyAspectDeletion,
            catchUpReplay,
            searchableTimeseriesAspectSpecs));
  }

  /**
   * The index an upgrade fill rebuilds into, named by every baseline event in the batch. It is the
   * next physical index the alias does not point at yet, or the alias itself for a fill in place.
   * Such a fill only creates absent documents. A baseline event without the header is operator
   * LoadIndices maintenance of the alias, which replaces documents. One baseline event without the
   * header downgrades the whole batch the same way: create-only would drop that event.
   */
  private static Optional<String> fillTargetIndex(@Nonnull final List<MCLItem> events) {
    final Set<String> targets = new HashSet<>();
    for (MCLItem event : events) {
      final String target =
          event.getMetadataChangeLog().getHeaders().get(BackfillHeaders.TARGET_INDEX_HEADER_NAME);
      if (target == null || target.isEmpty()) {
        return Optional.empty();
      }
      targets.add(target);
    }
    return targets.size() == 1 ? targets.stream().findFirst() : Optional.empty();
  }

  private Map<Urn, Map<String, EnvelopedAspect>> hydrateLatestTimeseriesAspects(
      @Nonnull final OperationContext opContext,
      @Nonnull final List<PreparedUrnBatch> preparedBatches) {
    final Map<TimeseriesHydrationGroup, Set<Urn>> urnsByHydrationGroup = new LinkedHashMap<>();
    for (PreparedUrnBatch preparedBatch : preparedBatches) {
      if ((!preparedBatch.baselineFill() && !preparedBatch.catchUpReplay())
          || preparedBatch.searchableTimeseriesAspectSpecs().isEmpty()) {
        continue;
      }
      final TimeseriesHydrationGroup hydrationGroup =
          new TimeseriesHydrationGroup(
              preparedBatch.entitySpec().getName(),
              Set.copyOf(preparedBatch.searchableTimeseriesAspectSpecs().keySet()));
      urnsByHydrationGroup
          .computeIfAbsent(hydrationGroup, ignored -> new LinkedHashSet<>())
          .add(preparedBatch.urn());
    }

    final Map<Urn, Map<String, EnvelopedAspect>> latestTimeseriesByUrn = new LinkedHashMap<>();
    for (Map.Entry<TimeseriesHydrationGroup, Set<Urn>> entry : urnsByHydrationGroup.entrySet()) {
      final TimeseriesHydrationGroup hydrationGroup = entry.getKey();
      final Set<Urn> urns = entry.getValue();
      final Map<Urn, Map<String, EnvelopedAspect>> hydratedGroup =
          readWithRetry(
              opContext,
              TIMESERIES_HYDRATION_FAILURE_METRIC,
              "hydrate latest timeseries aspects for entity type " + hydrationGroup.entityType(),
              () ->
                  Optional.ofNullable(
                          timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
                              opContext, urns, hydrationGroup.aspectNames(), null))
                      .orElse(Collections.emptyMap()));
      latestTimeseriesByUrn.putAll(hydratedGroup);
    }
    return latestTimeseriesByUrn;
  }

  private void processUrnBatch(
      @Nonnull final OperationContext opContext,
      @Nonnull final PreparedUrnBatch preparedBatch,
      @Nonnull final Map<String, EnvelopedAspect> latestTimeseriesAspects,
      @Nullable final TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {
    try {
      processUrnBatchUnchecked(opContext, preparedBatch, latestTimeseriesAspects, throttleSummary);
    } catch (RuntimeException e) {
      // Live dual-write: one URN's V3 failure must not stall V2, and the entity's next event
      // rebuilds its document. A replay is different: nothing comes back for a URN it drops. A
      // baseline fill is the only pass over the historical corpus, and a catch-up replay is the
      // only pass over its window; the loader counts a URN as processed once this returns, and
      // the system-update catch-up step checkpoints after producing the event, so neither sends
      // it again. A later baseline cannot repair the hole either, since it does not overwrite the
      // live documents already in the index. The failure propagates instead: the loader retries the
      // batch, splitting it down to the single event, and otherwise fails the run with its
      // checkpoint intact; the MAE consumer redelivers a retryable failure and fails loudly on a
      // non-retryable one. A removal that failed retryably is not skipped either: no later event
      // removes the entity or the aspect again, on the alias or in the index a rebuild is filling,
      // so the consumer must redeliver it. A removal that failed for good is skipped like any other
      // live write: failing the batch would only drop the other entities' writes with it. A failed
      // aspect read is redelivered too, including a live upsert while V2 is dual-written: nothing
      // else writes that aspect, so skipping it leaves the document missing it. A transient index
      // write stays skipped for a live upsert, because the entity's next event rebuilds it.
      final boolean replay = preparedBatch.baselineFill() || preparedBatch.catchUpReplay();
      final boolean redeliverableRemoval =
          carriesDelete(preparedBatch) && e instanceof RetryableIndexingException;
      final boolean failedAspectRead = hasCause(e, RemoteInvocationException.class);
      if (v2Enabled && !replay && !redeliverableRemoval && !failedAspectRead) {
        log.error(
            "V3 search write failed for URN {} while V2 dual-write is enabled; skipping V3 document",
            preparedBatch.urn(),
            e);
        return;
      }
      throw e;
    }
  }

  private void processUrnBatchUnchecked(
      @Nonnull final OperationContext opContext,
      @Nonnull final PreparedUrnBatch preparedBatch,
      @Nonnull final Map<String, EnvelopedAspect> latestTimeseriesAspects,
      @Nullable final TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {
    final Urn urn = preparedBatch.urn();
    final List<MCLItem> events = preparedBatch.originalEvents();
    final CoalescedEvents coalescedEvents = preparedBatch.coalescedEvents();
    final List<MCLItem> effectiveEvents = coalescedEvents.finalEvents();
    final EntitySpec entitySpec = preparedBatch.entitySpec();
    if (!preparedBatch.baselineFill() && !preparedBatch.catchUpReplay()) {
      processLiveUrnBatch(opContext, preparedBatch, throttleSummary);
      return;
    }
    // A fill into a target index rebuilds the whole document from primary storage and writes it
    // only if the index holds none. There is no document to merge with, and reading one back would
    // cost the V3 cluster a round trip per URN. Maintenance of the alias and catch-up replace the
    // document, so they keep the index-only fields it already holds.
    final Optional<Map<String, Object>> existingSource =
        preparedBatch.targetFill()
            ? Optional.empty()
            : loadExistingV3DocumentSource(
                opContext, preparedBatch.searchGroup(), preparedBatch.docId(), urn);

    log.debug("V3 unified batch processing for URN: {} with {} events", urn, events.size());

    // A complete baseline fill batch carries every stored aspect of the entity: the loader read
    // the rows hydration would read. Treat them as the stored state rather than reading them a
    // second time from primary storage.
    final Map<String, SystemAspect> latestStoredAspects =
        preparedBatch.completeBaselineFill()
            ? storedAspectsFromEvents(effectiveEvents)
            : hydrateLatestStoredAspects(opContext, urn, entitySpec);
    // Remove only when the key aspect is truly absent from primary storage; hydration reads
    // PRIMARY, so absence is authoritative. A catch-up replay's row was read before the entity was
    // hard-deleted, and rebuilding from an empty hydration would otherwise bring back what the
    // live DELETE removed as a bare document that surfaces as a blank search hit.
    if ((preparedBatch.hasKeyAspectDeletion() || preparedBatch.catchUpReplay())
        && !latestStoredAspects.containsKey(entitySpec.getKeyAspectName())) {
      writeTombstone(opContext, preparedBatch, existingSource);
      return;
    }

    V3DocumentBuildResult buildResult =
        buildV3SearchDocument(
            opContext,
            urn,
            effectiveEvents,
            preparedBatch.searchableTimeseriesAspectSpecs(),
            latestTimeseriesAspects,
            latestStoredAspects,
            existingSource,
            !preparedBatch.completeBaselineFill(),
            throttleSummary);

    if (buildResult == null) {
      log.debug("V3 combined document is empty for URN: {}, skipping update", urn);
      return;
    }
    ObjectNode combinedDocument = buildResult.document();

    List<String> distinctRunIds = getDistinctRunIds(events);
    indexOnlyFieldMerger.merge(
        combinedDocument, existingSource, distinctRunIds, buildResult.latestSystemMetadata());
    String finalDocument = combinedDocument.toString();

    writeV3Document(
        opContext,
        new V3SearchDocumentWrite(
            preparedBatch.indexName(),
            preparedBatch.docId(),
            urn,
            finalDocument,
            preparedBatch.targetFill()
                ? V3SearchDocumentWrite.Kind.CREATE
                : V3SearchDocumentWrite.Kind.REPLACE),
        carriesDelete(preparedBatch));
    log.debug(
        "V3 wrote combined document for URN: {} to search group: {} with {} aspects",
        urn,
        preparedBatch.searchGroup(),
        effectiveEvents.size());
  }

  private void processLiveUrnBatch(
      @Nonnull final OperationContext opContext,
      @Nonnull final PreparedUrnBatch preparedBatch,
      @Nullable final TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {
    if (preparedBatch.hasKeyAspectDeletion()) {
      writeTombstone(opContext, preparedBatch, Optional.empty());
      return;
    }

    final ObjectNode document =
        searchDocumentProjector.newEntityDocument(
            preparedBatch.urn(), preparedBatch.entitySpec(), opContext.getEntityRegistry());
    boolean hasProjection = false;
    for (List<MCLItem> aspectEvents :
        UpdateIndicesUtil.groupUpdatesByAspect(preparedBatch.originalEvents()).values()) {
      final MCLItem event = aspectEvents.get(aspectEvents.size() - 1);
      // A throttled timeseries event is omitted from the partial update. The entity document keeps
      // the value it already has; an earlier event in this batch is not a substitute for it.
      if (event.getAspectSpec().isTimeseries() && timeseriesThrottleCache != null) {
        long eventTimeMs =
            event.getAuditStamp() == null
                ? System.currentTimeMillis()
                : event.getAuditStamp().getTime();
        if (timeseriesThrottleCache.shouldThrottle(
            event.getEntitySpec().getName(),
            preparedBatch.urn().toString(),
            event.getAspectName(),
            eventTimeMs)) {
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
      final RecordTemplate previous = aspectEvents.get(0).getPreviousRecordTemplate();
      final Optional<V3SearchDocumentProjector.ProjectedAspect> projection;
      try {
        projection = searchDocumentProjector.projectAspect(opContext, event, previous);
      } catch (RetryableIndexingException e) {
        // A failed aspect read. Redeliver it; skipping the aspect commits the offset past a
        // document this event never writes again.
        throw e;
      } catch (RuntimeException e) {
        // A deterministic projection failure is not retryable, and failing the URN here drops
        // every later URN in the MCL batch. The listener commits that batch, so those documents
        // are never written. Skip the aspect; the rest of this URN and the batch still land.
        log.error(
            "V3 search projection failed for aspect {} of URN {}; skipping this aspect",
            event.getAspectName(),
            preparedBatch.urn(),
            e);
        continue;
      }
      if (projection.isPresent()) {
        searchDocumentProjector.applyProjection(document, projection.get());
        hasProjection = true;
        if (event.getAspectSpec().isTimeseries() && throttleSummary != null) {
          throttleSummary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
        }
      }
    }
    // The hard-delete placeholder stays in the index with removed=true. A partial update leaves
    // any field it omits, so the key upsert that recreates the entity has to clear it. Other
    // aspects must not: a soft-deleted entity is also removed=true, and a later tag update has
    // to leave that flag in place.
    final boolean restoresKey = liveBatchRestoresKey(preparedBatch);
    if (restoresKey) {
      clearHardDeleteMarker(document);
    }
    if (!hasProjection && !restoresKey) {
      return;
    }
    liftSemanticEmbeddingsToRoot(preparedBatch.entitySpec().getName(), document);
    applyDocumentContributors(opContext, preparedBatch.urn(), document);
    // The partial document must not carry runId: an update replaces an array, which would drop
    // runs already stored. The script appends this batch's runs after the document write.
    indexOnlyFieldMerger.stampIndexModified(document);
    final boolean carriesDelete = carriesDelete(preparedBatch);
    // An in-place fill creates into this same alias. A versioned write can leave a partial
    // document, because catch-up replays that row and replaces it. A timeseries-only batch is not
    // in primary storage, so catch-up never sees it, and a document created here would make the
    // fill skip the entity.
    final boolean updateOnly = timeseriesOnlyDuringInPlaceFill(preparedBatch);
    writeV3Document(
        opContext,
        new V3SearchDocumentWrite(
            preparedBatch.indexName(),
            preparedBatch.docId(),
            preparedBatch.urn(),
            document.toString(),
            updateOnly
                ? V3SearchDocumentWrite.Kind.MERGE_EXISTING
                : V3SearchDocumentWrite.Kind.MERGE),
        carriesDelete);
    appendLiveRunIds(opContext, preparedBatch, carriesDelete, updateOnly);
  }

  /**
   * Appends this batch's run ids on every index the live merge just wrote. Same document-id routing
   * as the merge, and queued after it, so the append sees that document. In a next index a fill is
   * building, the append only updates a document the fill already wrote, like the mirrored merge. A
   * timeseries-only write during an in-place fill does the same on the alias: an upsert would
   * create the partial document the fill then skips.
   */
  private void appendLiveRunIds(
      @Nonnull final OperationContext opContext,
      @Nonnull final PreparedUrnBatch preparedBatch,
      final boolean carriesDelete,
      final boolean updateOnly) {
    final List<String> runIds = getDistinctRunIds(preparedBatch.originalEvents());
    if (runIds.isEmpty()) {
      return;
    }
    if (updateOnly) {
      appendRunIdsToExistingDocument(
          opContext, preparedBatch.indexName(), preparedBatch.docId(), runIds);
    } else {
      for (String runId : runIds) {
        elasticSearchService.appendRunIdBySearchGroup(
            opContext,
            preparedBatch.searchGroup(),
            preparedBatch.docId(),
            preparedBatch.urn(),
            runId);
      }
    }
    if (incrementalReindexWriteTargets == null) {
      return;
    }
    incrementalReindexWriteTargets
        .oldBackingIndexFor(preparedBatch.indexName())
        .ifPresent(
            indexName -> {
              if (updateOnly) {
                appendRunIdsToExistingDocument(opContext, indexName, preparedBatch.docId(), runIds);
              } else {
                for (String runId : runIds) {
                  elasticSearchService.appendRunIdByIndexName(
                      opContext, indexName, preparedBatch.docId(), preparedBatch.urn(), runId);
                }
              }
            });
    if (carriesDelete) {
      incrementalReindexWriteTargets
          .fillTargetFor(preparedBatch.indexName())
          .ifPresent(
              indexName ->
                  appendRunIdsToExistingDocument(
                      opContext, indexName, preparedBatch.docId(), runIds));
    }
  }

  private void appendRunIdsToExistingDocument(
      @Nonnull final OperationContext opContext,
      @Nonnull final String indexName,
      @Nonnull final String docId,
      @Nonnull final List<String> runIds) {
    for (String runId : runIds) {
      elasticSearchService.appendRunIdToExistingDocument(opContext, indexName, docId, runId);
    }
  }

  /**
   * True when this alias is the index an in-place fill is creating and the batch holds only
   * timeseries aspects. Creating a document here would block that fill, and catch-up never sees the
   * batch.
   */
  private boolean timeseriesOnlyDuringInPlaceFill(@Nonnull final PreparedUrnBatch preparedBatch) {
    if (incrementalReindexWriteTargets == null
        || preparedBatch.originalEvents().isEmpty()
        || incrementalReindexWriteTargets
            .activeFillTargetFor(preparedBatch.indexName())
            .filter(preparedBatch.indexName()::equals)
            .isEmpty()) {
      return false;
    }
    return preparedBatch.originalEvents().stream()
        .allMatch(event -> event.getAspectSpec() != null && event.getAspectSpec().isTimeseries());
  }

  /** The batch's surviving key event creates or updates the entity, rather than deleting it. */
  private static boolean liveBatchRestoresKey(@Nonnull final PreparedUrnBatch preparedBatch) {
    final String keyAspectName = preparedBatch.entitySpec().getKeyAspectName();
    return preparedBatch.coalescedEvents().finalEvents().stream()
        .anyMatch(
            event ->
                keyAspectName.equals(event.getAspectName())
                    && !ChangeType.DELETE.equals(event.getChangeType()));
  }

  /**
   * Clears the hard-delete placeholder. A status aspect in this batch already wrote {@code removed}
   * (a soft delete or an undelete), so that value is left as projected.
   */
  private static void clearHardDeleteMarker(@Nonnull final ObjectNode document) {
    if (!document.has(V3SearchDocumentProjector.REMOVED_FIELD)) {
      document.put(V3SearchDocumentProjector.REMOVED_FIELD, false);
    }
    // A null in the partial document removes the field. Absent would leave the old true in place.
    document.putNull(V3SearchDocumentProjector.TOMBSTONE_FIELD);
  }

  private void writeTombstone(
      @Nonnull final OperationContext opContext,
      @Nonnull final PreparedUrnBatch preparedBatch,
      @Nonnull final Optional<Map<String, Object>> existingSource) {
    final ObjectNode tombstoneDocument =
        searchDocumentProjector.newTombstoneDocument(
            preparedBatch.urn(), preparedBatch.entitySpec());
    indexOnlyFieldMerger.merge(
        tombstoneDocument,
        existingSource,
        getDistinctRunIds(preparedBatch.originalEvents()),
        List.of());
    applyDocumentContributors(opContext, preparedBatch.urn(), tombstoneDocument);
    final V3SearchDocumentWrite.Kind kind = removalKind(preparedBatch);
    writeV3Document(
        opContext,
        new V3SearchDocumentWrite(
            preparedBatch.indexName(),
            preparedBatch.docId(),
            preparedBatch.urn(),
            tombstoneDocument.toString(),
            kind),
        carriesDelete(preparedBatch));
    log.debug(
        "V3 wrote {} for URN: {} to search group: {} due to absent key aspect",
        kind,
        preparedBatch.urn(),
        preparedBatch.searchGroup());
  }

  /**
   * A removed entity's document is deleted, unless a fill could still bring it back: the fill
   * creates documents only where the index holds none, so the index it writes keeps the hard-delete
   * placeholder instead. That is a fill's target index, or the alias while it is filled in place.
   * Catch-up starts only once the fill is done, so its replays delete, and it removes the
   * placeholders when it completes. A copy of a delete into a next index a fill is building becomes
   * a placeholder too, see {@link V3SearchDocumentWrite#redirectedToFill}.
   */
  private V3SearchDocumentWrite.Kind removalKind(@Nonnull final PreparedUrnBatch preparedBatch) {
    final boolean filledInPlace =
        incrementalReindexWriteTargets != null
            && incrementalReindexWriteTargets
                .activeFillTargetFor(preparedBatch.indexName())
                .filter(preparedBatch.indexName()::equals)
                .isPresent();
    return preparedBatch.targetFill() || filledInPlace
        ? V3SearchDocumentWrite.Kind.TOMBSTONE
        : V3SearchDocumentWrite.Kind.DELETE;
  }

  /**
   * Writes to the live index and, while the index's alias has a swapped-out backing index awaiting
   * rollback, repeats the identical write there, so the old backing index stays a faithful copy the
   * alias can be swapped back to. The rollback copy never fails the live write; a missed copy only
   * degrades rollback fidelity.
   *
   * <p>A write that carries a delete is also repeated on the index a rebuild is filling for the
   * alias, while that fill is in flight: the alias still serves the old index, and catch-up
   * re-derives its window from primary storage, which holds nothing for a removed entity or aspect.
   * The fill creates only absent documents, so a delete is mirrored as a tombstone that keeps the
   * entity deleted. A mirrored merge only updates a document the fill already wrote, so it cannot
   * leave a partial one for the fill to skip. Unlike the rollback copy, a mirrored delete that
   * cannot be handed to the bulk processor fails the write as retryable: nothing replays a hard
   * delete, so the consumer has to redeliver it.
   */
  private void writeV3Document(
      @Nonnull final OperationContext opContext,
      @Nonnull final V3SearchDocumentWrite write,
      final boolean carriesDelete) {
    elasticSearchService.writeV3SearchDocument(opContext, write);
    if (incrementalReindexWriteTargets == null) {
      return;
    }
    incrementalReindexWriteTargets
        .oldBackingIndexFor(write.indexName())
        .ifPresent(oldBackingIndex -> writeRollbackCopy(opContext, write, oldBackingIndex));
    if (carriesDelete) {
      incrementalReindexWriteTargets
          .fillTargetFor(write.indexName())
          .ifPresent(fillTarget -> writeFillTargetCopy(opContext, write, fillTarget));
    }
  }

  private void writeFillTargetCopy(
      @Nonnull final OperationContext opContext,
      @Nonnull final V3SearchDocumentWrite write,
      @Nonnull final String fillTarget) {
    try {
      elasticSearchService.writeV3SearchDocument(opContext, write.redirectedToFill(fillTarget));
    } catch (RuntimeException e) {
      throw new RetryableIndexingException(
          String.format(
              "Mirroring a delete into fill target '%s' failed for index '%s', urn '%s'",
              fillTarget, write.indexName(), write.urn()),
          e);
    }
  }

  private static boolean carriesDelete(@Nonnull final PreparedUrnBatch preparedBatch) {
    return preparedBatch.originalEvents().stream()
        .anyMatch(event -> event.getChangeType() == ChangeType.DELETE);
  }

  private void writeRollbackCopy(
      @Nonnull final OperationContext opContext,
      @Nonnull final V3SearchDocumentWrite write,
      @Nonnull final String oldBackingIndex) {
    try {
      elasticSearchService.writeV3SearchDocument(opContext, write.redirectedTo(oldBackingIndex));
      incrementalReindexWriteTargets.recordDualWriteStarted(write.indexName());
    } catch (RuntimeException e) {
      log.error(
          "Rollback dual-write to '{}' failed for index '{}', urn '{}'",
          oldBackingIndex,
          write.indexName(),
          write.urn(),
          e);
    }
  }

  private static String v3IndexName(
      @Nonnull final OperationContext opContext, @Nonnull final String searchGroup) {
    return opContext
        .getSearchContext()
        .getIndexConvention()
        .getEntityIndexNameV3(opContext, searchGroup);
  }

  private static CoalescedEvents coalesceFinalEvents(@Nonnull final List<MCLItem> events) {
    final Map<String, MCLItem> finalEventsByAspect = new LinkedHashMap<>();
    final Map<String, RecordTemplate> earliestPreviousByAspect = new LinkedHashMap<>();
    for (MCLItem event : events) {
      final String aspectName = event.getAspectName();
      if (!earliestPreviousByAspect.containsKey(aspectName)) {
        earliestPreviousByAspect.put(aspectName, event.getPreviousRecordTemplate());
      }
      finalEventsByAspect.remove(aspectName);
      finalEventsByAspect.put(aspectName, event);
    }
    return new CoalescedEvents(
        new ArrayList<>(finalEventsByAspect.values()), earliestPreviousByAspect);
  }

  private static List<String> getDistinctRunIds(@Nonnull final List<MCLItem> events) {
    return events.stream()
        .filter(e -> e.getSystemMetadata() != null && e.getSystemMetadata().hasRunId())
        .map(e -> e.getSystemMetadata().getRunId())
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Builds a V3 search document by hydrating latest primary-storage aspect state and overlaying the
   * current batch's changed aspects into a single document with _aspect structure.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param events the events for this URN
   * @return the combined V3 document or null if no aspects to process
   */
  private V3DocumentBuildResult buildV3SearchDocument(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> events,
      @Nonnull Map<String, AspectSpec> searchableTimeseriesAspectSpecs,
      @Nonnull Map<String, EnvelopedAspect> latestTimeseriesAspects,
      @Nonnull Map<String, SystemAspect> latestStoredAspects,
      @Nonnull Optional<Map<String, Object>> existingSource,
      final boolean overlayEvents,
      @Nullable TimeseriesWriteThrottleCache.ThrottleSummary throttleSummary) {

    final EntitySpec entitySpec = events.get(0).getEntitySpec();
    final String entityType = entitySpec.getName();
    final ObjectNode combinedDocument =
        searchDocumentProjector.newEntityDocument(urn, entitySpec, opContext.getEntityRegistry());
    final Set<String> deletedAspectNames = getDeletedAspectNames(events);
    // Honor a live DELETE only when primary hydration confirms the aspect is gone. If the aspect
    // is still present in primary, the DELETE is stale — it was re-ingested afterwards — and
    // masking it would erase the field from the rebuilt document (the key-aspect case is decided
    // the same way by the tombstone check before this method). A
    // version comparison is not usable here: aspect versions restart after a full delete, so
    // presence in primary is the authoritative signal, matching the catch-up path where
    // hydration always wins. Timeseries aspects are absent from latestStoredAspects and keep
    // their delete masking.
    deletedAspectNames.removeIf(latestStoredAspects::containsKey);
    final List<SystemMetadata> latestSystemMetadata = new ArrayList<>();

    boolean hasAnyAspects = false;
    final Set<String> projectedTimeseriesAspectNames = new LinkedHashSet<>();
    hasAnyAspects |=
        applyLatestTimeseriesAspects(
            opContext,
            urn,
            entitySpec,
            searchableTimeseriesAspectSpecs,
            latestTimeseriesAspects,
            deletedAspectNames,
            combinedDocument,
            latestSystemMetadata,
            projectedTimeseriesAspectNames);
    hasAnyAspects |=
        applyExistingTimeseriesProjections(
            opContext,
            searchableTimeseriesAspectSpecs,
            projectedTimeseriesAspectNames,
            deletedAspectNames,
            existingSource,
            combinedDocument);
    hasAnyAspects |=
        applyLatestStoredAspects(
            opContext,
            latestStoredAspects,
            deletedAspectNames,
            combinedDocument,
            latestSystemMetadata);

    // When the batch itself was the stored state, its aspects were applied above in stored-aspect
    // order; overlaying them again in event order would let page order decide root-field winners.
    for (MCLItem event : overlayEvents ? events : Collections.<MCLItem>emptyList()) {
      AspectSpec aspectSpec = event.getAspectSpec();
      String aspectName = aspectSpec.getName();

      if (ChangeType.DELETE.equals(event.getChangeType())) {
        continue;
      }
      if (isCatchUpBackfillEvent(event)) {
        continue;
      }
      // A delayed live event can carry an older aspect value than what authoritative PRIMARY
      // hydration just read; overlaying it would regress the field in the whole document written
      // here. Skip the overlay only when the stored aspect is
      // verifiably newer — a lower system-metadata version within the same incarnation, or an
      // emit timestamp before the stored aspect's creation (which catches a hard delete + recreate
      // where per-aspect versions restart). Missing signals fall through to overlay so
      // read-your-writes holds. A delayed UPSERT for an aspect already hard-deleted from PRIMARY
      // (absent from hydration entirely) can still transiently reappear until the aspect's next
      // event; robustly suppressing that needs a primary-storage deletion high-water mark that
      // does not exist yet (tracked separately).
      if (!aspectSpec.isTimeseries()
          && isOlderThanStored(event, latestStoredAspects.get(aspectName))) {
        log.debug(
            "V3 skipped overlay of stale aspect {} for URN: {} (primary has a newer version)",
            aspectName,
            urn);
        continue;
      }

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

      Optional<V3SearchDocumentProjector.ProjectedAspect> projectedAspect =
          searchDocumentProjector.projectAspect(
              opContext, event, event.getPreviousRecordTemplate());
      if (projectedAspect.isPresent()) {
        searchDocumentProjector.applyProjection(combinedDocument, projectedAspect.get());
        hasAnyAspects = true;

        // recordWrite is handled by UpdateIndicesService after all strategies have processed
        if (aspectSpec.isTimeseries() && throttleSummary != null) {
          throttleSummary.recordWritten(TimeseriesWriteThrottleCache.ThrottleTarget.ENTITY_INDEX);
        }

        if (aspectSpec.isTimeseries()) {
          log.debug(
              "V3 included timeseries aspect {} in combined document for URN: {}", aspectName, urn);
        } else {
          log.debug(
              "V3 included versioned aspect {} in combined document for URN: {}", aspectName, urn);
        }
      }
    }

    if (!hasAnyAspects && deletedAspectNames.isEmpty()) {
      return null;
    }

    liftSemanticEmbeddingsToRoot(entityType, combinedDocument);

    applyDocumentContributors(opContext, urn, combinedDocument);
    return new V3DocumentBuildResult(combinedDocument, latestSystemMetadata);
  }

  private void liftSemanticEmbeddingsToRoot(
      @Nonnull String entityType, @Nonnull ObjectNode combinedDocument) {
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
  }

  private void applyDocumentContributors(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final ObjectNode document) {
    for (V3SearchDocumentContributor contributor : documentContributors) {
      final ObjectNode extras = JsonNodeFactory.instance.objectNode();
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

  /**
   * True when the event is older than the hydrated stored aspect. Uses the system-metadata version
   * when both sides carry one (precise within a single entity incarnation), and additionally treats
   * the event as stale when its emit timestamp is strictly before the stored aspect's creation
   * time. The timestamp guard catches a hard delete + recreate, where per-aspect versions restart
   * from 1 and a delayed pre-delete event can carry a numerically-higher version than the recreated
   * aspect — its emit time still precedes the recreated aspect's creation. Missing or zero signals
   * fall through so read-your-writes is preserved.
   */
  private static boolean isOlderThanStored(
      @Nonnull final MCLItem event, @Nullable final SystemAspect storedAspect) {
    if (storedAspect == null) {
      return false;
    }
    final Optional<Long> storedVersion = storedAspect.getSystemMetadataVersion();
    final Optional<Long> eventVersion =
        Optional.ofNullable(event.getSystemMetadata())
            .filter(SystemMetadata::hasVersion)
            .map(SystemMetadata::getVersion)
            .flatMap(UpdateIndicesV3Strategy::parseVersion);
    if (storedVersion.isPresent()
        && eventVersion.isPresent()
        && eventVersion.get() < storedVersion.get()) {
      return true;
    }
    final long eventEmitTime =
        event.getSystemMetadata() != null ? event.getSystemMetadata().getLastObserved() : 0L;
    final long storedCreatedTime =
        storedAspect.getCreatedOn() != null ? storedAspect.getCreatedOn().getTime() : 0L;
    return eventEmitTime > 0 && storedCreatedTime > 0 && eventEmitTime < storedCreatedTime;
  }

  // systemMetadata.version is a free-form string; treat a non-numeric event version as "no version"
  // so a malformed value falls through to the timestamp guard instead of dropping the write with a
  // NumberFormatException. The stored side is guarded the same way in SystemAspect.
  private static Optional<Long> parseVersion(@Nonnull final String version) {
    try {
      return Optional.of(Long.parseLong(version));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private Map<String, SystemAspect> hydrateLatestStoredAspects(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final EntitySpec entitySpec) {
    final Set<String> aspectNames =
        entitySpec.getAspectSpecs().stream()
            .filter(aspectSpec -> !aspectSpec.isTimeseries())
            .map(AspectSpec::getName)
            .collect(Collectors.toSet());
    if (aspectNames.isEmpty()) {
      return Collections.emptyMap();
    }

    // Force PRIMARY: this hydration is the authoritative source for a full-document REPLACE, so a
    // read pool replica lagging behind PRIMARY (EBEAN_READ_POOL_ENABLED /
    // CASSANDRA_READ_POOL_ENABLED)
    // could omit a recently-written aspect and drop it from the rebuilt document, which a fill then
    // keeps, since it never overwrites a document it already wrote. Only re-wrap when
    // the context is not already PRIMARY (the default), so the common path keeps the same context
    // instance; this read takes no row locks.
    final boolean alreadyPrimary =
        opContext.getPrimaryStorageContext() == null
            || opContext.getPrimaryStorageContext().getReadPreference() == ReadPreference.PRIMARY;
    final OperationContext primaryContext =
        alreadyPrimary ? opContext : opContext.withReadPreference(ReadPreference.PRIMARY);
    return readWithRetry(
        primaryContext,
        PRIMARY_HYDRATION_FAILURE_METRIC,
        "hydrate latest primary-storage aspects for " + urn,
        () ->
            Optional.ofNullable(
                    primaryContext
                        .getAspectRetriever()
                        .getLatestSystemAspects(primaryContext, Map.of(urn, aspectNames)))
                .orElse(Collections.emptyMap())
                .getOrDefault(urn, Collections.emptyMap()));
  }

  // DataHub names user-override aspects with "editable"/"Editable" (corpUserEditableInfo,
  // corpGroupEditableInfo, editableDatasetProperties, editableSchemaMetadata, ...). These overrides
  // conventionally take precedence over their ingested base aspect, so they are applied last when
  // resolving a shared root convenience field.
  private static boolean isEditableOverrideAspect(@Nonnull final String aspectName) {
    return aspectName.toLowerCase(Locale.ROOT).contains("editable");
  }

  private boolean applyLatestStoredAspects(
      @Nonnull final OperationContext opContext,
      @Nonnull final Map<String, SystemAspect> latestAspects,
      @Nonnull final Set<String> deletedAspectNames,
      @Nonnull final ObjectNode combinedDocument,
      @Nonnull final List<SystemMetadata> latestSystemMetadata) {
    boolean hasAnyAspects = false;
    // Two stored aspects can project the same root convenience field (e.g. corpuser displayName
    // from CorpUserInfo and CorpUserEditableInfo, or dataset deprecated); applyProjection is
    // last-write-wins on those root fields while per-aspect values stay under _aspects.<name>.
    // Apply the user-editable override aspect last so it wins the shared root field over its
    // ingested base aspect (DataHub's *Editable* aspects are the user override that conventionally
    // takes precedence for display/search), and break ties by ascending aspect name. This makes
    // the root winner deterministic across runs instead of depending on the hydration map's
    // arbitrary iteration order, without the plain-alphabetical order that would (for the
    // corpUserEditableInfo/corpUserInfo pair) let the base aspect win.
    final List<SystemAspect> orderedAspects = new ArrayList<>(latestAspects.values());
    orderedAspects.sort(
        Comparator.comparing(
                (SystemAspect aspect) -> isEditableOverrideAspect(aspect.getAspectName()))
            .thenComparing(SystemAspect::getAspectName));
    for (SystemAspect systemAspect : orderedAspects) {
      if (deletedAspectNames.contains(systemAspect.getAspectName())) {
        continue;
      }
      if (systemAspect.getSystemMetadata() != null) {
        latestSystemMetadata.add(systemAspect.getSystemMetadata());
      }
      Optional<V3SearchDocumentProjector.ProjectedAspect> projectedAspect =
          searchDocumentProjector.projectSystemAspect(opContext, systemAspect);
      if (projectedAspect.isPresent()) {
        searchDocumentProjector.applyProjection(combinedDocument, projectedAspect.get());
        hasAnyAspects = true;
      }
    }
    return hasAnyAspects;
  }

  private boolean applyLatestTimeseriesAspects(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final EntitySpec entitySpec,
      @Nonnull final Map<String, AspectSpec> aspectSpecs,
      @Nonnull final Map<String, EnvelopedAspect> latestAspects,
      @Nonnull final Set<String> deletedAspectNames,
      @Nonnull final ObjectNode combinedDocument,
      @Nonnull final List<SystemMetadata> latestSystemMetadata,
      @Nonnull final Set<String> projectedAspectNames) {
    if (aspectSpecs.isEmpty()) {
      return false;
    }

    boolean hasAnyAspects = false;
    for (Map.Entry<String, EnvelopedAspect> entry : latestAspects.entrySet()) {
      if (deletedAspectNames.contains(entry.getKey())) {
        continue;
      }
      final AspectSpec aspectSpec = aspectSpecs.get(entry.getKey());
      if (aspectSpec == null) {
        continue;
      }
      final EnvelopedAspect envelopedAspect = entry.getValue();
      final RecordTemplate recordTemplate =
          GenericRecordUtils.deserializeAspect(
              envelopedAspect.getAspect().getValue(),
              envelopedAspect.getAspect().getContentType(),
              aspectSpec);
      if (envelopedAspect.getSystemMetadata() != null) {
        latestSystemMetadata.add(envelopedAspect.getSystemMetadata());
      }
      Optional<V3SearchDocumentProjector.ProjectedAspect> projectedAspect =
          searchDocumentProjector.projectStoredAspect(
              opContext,
              urn,
              entitySpec,
              aspectSpec,
              recordTemplate,
              envelopedAspect.getSystemMetadata());
      if (projectedAspect.isPresent()) {
        searchDocumentProjector.applyProjection(combinedDocument, projectedAspect.get());
        projectedAspectNames.add(entry.getKey());
        hasAnyAspects = true;
      }
    }
    return hasAnyAspects;
  }

  private boolean applyExistingTimeseriesProjections(
      @Nonnull final OperationContext opContext,
      @Nonnull final Map<String, AspectSpec> aspectSpecs,
      @Nonnull final Set<String> projectedAspectNames,
      @Nonnull final Set<String> deletedAspectNames,
      @Nonnull final Optional<Map<String, Object>> existingSource,
      @Nonnull final ObjectNode combinedDocument) {
    if (aspectSpecs.isEmpty() || existingSource.isEmpty()) {
      return false;
    }

    final JsonNode existingDocument = opContext.getObjectMapper().valueToTree(existingSource.get());
    final JsonNode existingAspects = existingDocument.get(MappingConstants.ASPECTS_FIELD_NAME);
    if (existingAspects == null || !existingAspects.isObject()) {
      return false;
    }

    boolean hasAnyAspects = false;
    for (String aspectName : aspectSpecs.keySet()) {
      if (projectedAspectNames.contains(aspectName) || deletedAspectNames.contains(aspectName)) {
        continue;
      }
      final JsonNode existingAspect = existingAspects.get(aspectName);
      if (!(existingAspect instanceof ObjectNode existingAspectFields)) {
        continue;
      }
      final ObjectNode canonicalAspectFields = existingAspectFields.deepCopy();
      final JsonNode legacySystemMetadata =
          canonicalAspectFields.remove(LEGACY_SYSTEM_METADATA_FIELD);
      if (!canonicalAspectFields.has(V3SearchDocumentProjector.SYSTEM_METADATA_FIELD)
          && legacySystemMetadata != null) {
        canonicalAspectFields.set(
            V3SearchDocumentProjector.SYSTEM_METADATA_FIELD, legacySystemMetadata);
      }
      final ObjectNode rootFields = canonicalAspectFields.deepCopy();
      rootFields.remove(EXISTING_TIMESERIES_ROOT_EXCLUDED_FIELDS);
      searchDocumentProjector.applyProjection(
          combinedDocument,
          new V3SearchDocumentProjector.ProjectedAspect(
              aspectName, rootFields, canonicalAspectFields, false));
      hasAnyAspects = true;
    }
    return hasAnyAspects;
  }

  private static Map<String, AspectSpec> getSearchableTimeseriesAspectSpecs(
      @Nonnull final EntitySpec entitySpec) {
    return entitySpec.getAspectSpecs().stream()
        .filter(AspectSpec::isTimeseries)
        .filter(aspectSpec -> !aspectSpec.getSearchableFieldSpecs().isEmpty())
        .collect(
            Collectors.toMap(
                AspectSpec::getName,
                aspectSpec -> aspectSpec,
                (first, ignored) -> first,
                LinkedHashMap::new));
  }

  private Optional<Map<String, Object>> loadExistingV3DocumentSource(
      @Nonnull final OperationContext opContext,
      @Nonnull final String searchGroup,
      @Nonnull final String docId,
      @Nonnull final Urn urn) {
    return readWithRetry(
        opContext,
        EXISTING_SOURCE_FAILURE_METRIC,
        "read existing V3 document source for " + urn,
        () ->
            Optional.ofNullable(
                    elasticSearchService.getV3DocumentSourceBySearchGroup(
                        opContext, searchGroup, docId))
                .orElse(Optional.empty()));
  }

  private <T> T readWithRetry(
      @Nonnull final OperationContext opContext,
      @Nonnull final String failureMetric,
      @Nonnull final String description,
      @Nonnull final ReadOperation<T> operation) {
    for (int attempt = 1; attempt <= MAX_READ_ATTEMPTS; attempt++) {
      try {
        return operation.execute();
      } catch (Exception e) {
        opContext
            .getMetricUtils()
            .ifPresent(metricUtils -> metricUtils.increment(getClass(), failureMetric, 1));
        if (hasCause(e, InterruptedException.class)) {
          Thread.currentThread().interrupt();
          throw new RetryableIndexingException("Interrupted while attempting to " + description, e);
        }
        if (!isTransientReadFailure(e)) {
          throw propagateReadFailure(description, e);
        }
        if (attempt == MAX_READ_ATTEMPTS) {
          throw new RetryableIndexingException(
              "Failed to " + description + " after " + MAX_READ_ATTEMPTS + " attempts", e);
        }
        log.warn(
            "Failed to {} on attempt {}/{}, retrying", description, attempt, MAX_READ_ATTEMPTS, e);
      }
    }
    throw new IllegalStateException(
        "Retry loop exited unexpectedly while attempting to " + description);
  }

  private static boolean isTransientReadFailure(@Nonnull final Throwable failure) {
    // Transport-level retryability (HTTP 429/5xx + connection failures) is shared with the
    // write-time index-existence check so both agree on what a redelivery can recover; the SQL
    // cases below are specific to primary-storage-backed reads on this path.
    return SearchIndexFailureClassifier.isRetryableTransportFailure(failure)
        || hasCause(failure, SQLTransientException.class)
        || hasCause(failure, SQLRecoverableException.class);
  }

  private static boolean hasCause(
      @Nonnull final Throwable failure, @Nonnull final Class<? extends Throwable> causeType) {
    Throwable current = failure;
    while (current != null) {
      if (causeType.isInstance(current)) {
        return true;
      }
      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
    }
    return false;
  }

  private static RuntimeException propagateReadFailure(
      @Nonnull final String description, @Nonnull final Exception failure) {
    if (failure instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    return new IllegalStateException("Failed to " + description, failure);
  }

  private static Set<String> getDeletedAspectNames(@Nonnull final List<MCLItem> events) {
    return events.stream()
        .filter(
            event ->
                ChangeType.DELETE.equals(event.getChangeType()) && !isCatchUpBackfillEvent(event))
        .map(MCLItem::getAspectName)
        .collect(Collectors.toSet());
  }

  boolean shouldSkipSearchIndexUpdate(@Nonnull MCLItem event) {
    for (V3SearchIndexSkip skip : searchIndexSkips) {
      if (skip.skip(event)) {
        return true;
      }
    }
    return false;
  }

  /** The backfill mode header, or empty when the event is not part of a fill or a replay. */
  @Nonnull
  private static Optional<String> backfillMode(@Nonnull final MCLItem event) {
    if (event.getMetadataChangeLog() == null || event.getMetadataChangeLog().getHeaders() == null) {
      return Optional.empty();
    }
    final String mode =
        event.getMetadataChangeLog().getHeaders().get(BackfillHeaders.MODE_HEADER_NAME);
    return mode == null || mode.isEmpty() ? Optional.empty() : Optional.of(mode);
  }

  private static boolean isBaselineBackfillEvent(@Nonnull final MCLItem event) {
    return event.getMetadataChangeLog() != null
        && event.getMetadataChangeLog().getHeaders() != null
        && BackfillHeaders.MODE_BASELINE.equalsIgnoreCase(
            event
                .getMetadataChangeLog()
                .getHeaders()
                .getOrDefault(BackfillHeaders.MODE_HEADER_NAME, ""));
  }

  /** A baseline fill event whose batch carries every stored aspect of its entity. */
  private static boolean isCompleteEntityFillEvent(@Nonnull final MCLItem event) {
    return event.getMetadataChangeLog() != null
        && event.getMetadataChangeLog().getHeaders() != null
        && Boolean.parseBoolean(
            event
                .getMetadataChangeLog()
                .getHeaders()
                .get(BackfillHeaders.COMPLETE_ENTITY_HEADER_NAME));
  }

  /**
   * The batch's events as the entity's stored state, so they flow through the same projection as
   * hydrated aspects: override aspects applied last and {@code systemCreated} stamped from the key
   * aspect, exactly as a read from primary storage would produce.
   */
  private static Map<String, SystemAspect> storedAspectsFromEvents(
      @Nonnull final List<MCLItem> events) {
    final Map<String, SystemAspect> stored = new HashMap<>();
    for (MCLItem event : events) {
      final AspectSpec aspectSpec = event.getAspectSpec();
      if (aspectSpec.isTimeseries() || ChangeType.DELETE.equals(event.getChangeType())) {
        continue;
      }
      final EntityAspect row =
          EntityAspect.builder()
              .urn(event.getUrn().toString())
              .aspect(aspectSpec.getName())
              .version(ASPECT_LATEST_VERSION)
              .build();
      final EntityAspect.EntitySystemAspect aspect =
          EntityAspect.EntitySystemAspect.builder()
              .forUpdate(event.getEntitySpec(), aspectSpec, row);
      aspect.setRecordTemplate(event.getRecordTemplate());
      aspect.setSystemMetadata(event.getSystemMetadata());
      aspect.setAuditStamp(event.getAuditStamp());
      stored.put(aspectSpec.getName(), aspect);
    }
    return stored;
  }

  private static boolean isCatchUpBackfillEvent(@Nonnull final MCLItem event) {
    return event.getMetadataChangeLog() != null
        && event.getMetadataChangeLog().getHeaders() != null
        && BackfillHeaders.MODE_CATCH_UP.equalsIgnoreCase(
            event
                .getMetadataChangeLog()
                .getHeaders()
                .getOrDefault(BackfillHeaders.MODE_HEADER_NAME, ""));
  }

  @FunctionalInterface
  private interface ReadOperation<T> {
    T execute() throws Exception;
  }

  private record V3DocumentBuildResult(
      @Nonnull ObjectNode document, @Nonnull List<SystemMetadata> latestSystemMetadata) {}

  private record CoalescedEvents(
      @Nonnull List<MCLItem> finalEvents,
      @Nonnull Map<String, RecordTemplate> earliestPreviousByAspect) {}

  private record PreparedUrnBatch(
      @Nonnull Urn urn,
      @Nonnull List<MCLItem> originalEvents,
      @Nonnull CoalescedEvents coalescedEvents,
      @Nonnull EntitySpec entitySpec,
      @Nonnull String searchGroup,
      @Nonnull String indexName,
      @Nonnull String docId,
      boolean baselineFill,
      boolean targetFill,
      boolean completeBaselineFill,
      boolean hasKeyAspectDeletion,
      boolean catchUpReplay,
      @Nonnull Map<String, AspectSpec> searchableTimeseriesAspectSpecs) {}

  private record TimeseriesHydrationGroup(
      @Nonnull String entityType, @Nonnull Set<String> aspectNames) {}
}
