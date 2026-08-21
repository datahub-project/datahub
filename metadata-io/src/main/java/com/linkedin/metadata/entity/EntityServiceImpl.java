package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.TransactionContext.DEFAULT_MAX_TRANSACTION_RETRY;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.*;
import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;
import static com.linkedin.metadata.utils.PegasusUtils.getDataTemplateClassFromSchema;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;
import static com.linkedin.metadata.utils.metrics.ExceptionUtils.collectMetrics;
import static com.linkedin.metadata.utils.metrics.MetricUtils.BATCH_SIZE_ATTR;
import static io.datahubproject.metadata.context.SystemTelemetryContext.EVENT_SOURCE_KEY;
import static io.datahubproject.metadata.context.SystemTelemetryContext.SOURCE_IP_KEY;
import static io.datahubproject.metadata.context.SystemTelemetryContext.TELEMETRY_TRACE_KEY;

import com.datahub.util.RecordUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.common.urn.VersionedUrnUtils;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.utils.DefaultAspectsUtil;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.dao.throttle.APIThrottle;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.lock.EntityWriteLock;
import com.linkedin.metadata.entity.lock.NoOpEntityWriteLock;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import com.linkedin.metadata.entity.validation.AspectDeletionRequest;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.cache.SyncGraphInvalidationBatch;
import com.linkedin.metadata.graph.cache.service.EntityGraphSyncInvalidationSupport;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityApiUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.SyncSearchIndexUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import jakarta.persistence.EntityNotFoundException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * A class specifying create, update, and read operations against metadata entities and aspects by
 * primary key (urn).
 *
 * <p>This interface is meant to abstract away the storage concerns of these pieces of metadata,
 * permitting any underlying storage system to be used in materializing GMS domain objects, which
 * are implemented using Pegasus {@link RecordTemplate}s.
 *
 * <p>Internal versioning semantics =============================
 *
 * <p>The latest version of any aspect is set to 0 for efficient retrieval; in most cases the latest
 * state of an aspect will be the only fetched.
 *
 * <p>As such, 0 is treated as a special number. Once an aspect is no longer the latest, versions
 * will increment monotonically, starting from 1. Thus, the second-to-last version of an aspect will
 * be equal to total # versions of the aspect - 1.
 *
 * <p>For example, if there are 5 instances of a single aspect, the latest will have version 0, and
 * the second-to-last will have version 4. The "true" latest version of an aspect is always equal to
 * the highest stored version of a given aspect + 1.
 *
 * <p>Note that currently, implementations of this interface are responsible for producing Metadata
 * Change Log on ingestion using {@link #produceMCLAsync(OperationContext, MetadataChangeLog mcl)}.
 *
 * <p>TODO: Consider whether we can abstract away virtual versioning semantics to subclasses of this
 * class.
 */
@Slf4j
public class EntityServiceImpl implements EntityService<ChangeItemImpl> {

  /**
   * As described above, the latest version of an aspect should <b>always</b> take the value 0, with
   * monotonically increasing version incrementing as usual once the latest version is replaced.
   */
  protected final AspectDao aspectDao;

  @VisibleForTesting @Getter private final EventProducer producer;
  private RetentionService<ChangeItemImpl> retentionService;
  // Post-commit retention path only; NO_OP keeps the existing sync-DELETE behavior unchanged.
  private RetentionBuffer retentionBuffer = RetentionBuffer.NO_OP;
  private final Boolean alwaysEmitChangeLog;
  private final Boolean cdcModeChangeLog;
  @Nullable @Getter private SearchIndicesService updateIndicesService;
  private final PreProcessHooks preProcessHooks;
  protected static final int MAX_KEYS_PER_QUERY = 500;
  protected static final int MCP_SIDE_EFFECT_KAFKA_BATCH_SIZE = 500;

  private final Integer ebeanMaxTransactionRetry;
  private final boolean enableBrowseV2;
  // When true, retention runs after upsert commit (best-effort). When false, legacy in-tx path.
  private final boolean postCommitRetentionEnabled;
  private final com.linkedin.metadata.utils.metrics.MetricUtils metricUtils;

  // Pre-transaction write gate for the OL + scoped-retry mode. Default no-op; the real backend
  // (e.g. Hazelcast) is injected by EntityWriteLockFactory via setEntityWriteLock. Serializes
  // concurrent writers on the base URNs OFF the DB connection before the transaction opens, so a
  // hot key queues in the lock backend instead of thrashing CAS and pinning a pooled connection.
  // volatile: assigned once post-construction by setEntityWriteLock (Spring startup) before any
  // request thread reads it; volatile publishes that write safely to serving threads.
  @Nonnull private volatile EntityWriteLock entityWriteLock = new NoOpEntityWriteLock();

  @Getter
  private final Map<Set<ThrottleType>, ThrottleEvent> throttleEvents = new ConcurrentHashMap<>();

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      @Nonnull final PreProcessHooks preProcessHooks,
      @Nonnull final EntityServiceConfiguration entityServiceConfiguration,
      @javax.annotation.Nullable
          final com.linkedin.metadata.utils.metrics.MetricUtils metricUtils) {

    this.aspectDao = aspectDao;
    this.producer = producer;
    this.alwaysEmitChangeLog = entityServiceConfiguration.isAlwaysEmitChangeLog();
    this.cdcModeChangeLog = entityServiceConfiguration.isCdcModeChangeLog();
    this.preProcessHooks = preProcessHooks;
    ebeanMaxTransactionRetry =
        entityServiceConfiguration.getRetry() != null
            ? entityServiceConfiguration.getRetry()
            : DEFAULT_MAX_TRANSACTION_RETRY;
    this.enableBrowseV2 = entityServiceConfiguration.isEnableBrowseV2();
    this.postCommitRetentionEnabled = entityServiceConfiguration.isPostCommitRetentionEnabled();
    this.metricUtils = metricUtils;
    log.info("EntityService cdcModeChangeLog is {}", this.cdcModeChangeLog);
  }

  /**
   * Injects the pre-transaction write gate (OL + scoped mode). Defaults to a no-op; wired by {@code
   * EntityWriteLockFactory} to the configured backend.
   */
  public void setEntityWriteLock(@Nonnull final EntityWriteLock entityWriteLock) {
    this.entityWriteLock = entityWriteLock;
  }

  /** Shared no-op handle for the non-scoped path — avoids null-guarding every gate call site. */
  private static final EntityWriteLock.LockHandle NOOP_WRITE_GATE = () -> {};

  /**
   * Acquires the pre-transaction write gate on {@code gateKeys} when the gate is engaged (see
   * {@link #writeGateEngaged()}), otherwise returns a no-op handle. Keys are per-{@code (urn,
   * aspect)} — the actual CAS conflict unit — so cross-aspect writers on the same URN do not
   * serialize; see {@link #writeGateKey}. Use with try-with-resources; {@link
   * EntityWriteLock.LockHandle#close()} never throws. Centralizes the engaged check so ingest and
   * delete share one gate seam.
   */
  @Nonnull
  private EntityWriteLock.LockHandle acquireWriteGate(
      @Nonnull final OperationContext opContext, @Nonnull final Collection<String> gateKeys) {
    if (writeGateEngaged()) {
      return entityWriteLock.acquire(opContext, gateKeys);
    }
    return NOOP_WRITE_GATE;
  }

  /**
   * Whether a real pre-transaction write gate will serialize these writes: optimistic-locking mode
   * (the gate targets CAS thrash, not the FOR UPDATE path) with a non-no-op backend. Independent of
   * scoped retry — serializing hot-key writers off-connection helps under full-batch retry too.
   * Single source of truth so taking the gate and skipping the now-redundant DAO advisory lock
   * never disagree.
   *
   * <p>This is a static (config-level) decision, evaluated before {@code acquire()} runs. The gate
   * is best-effort: on acquire timeout or a Hazelcast outage it degrades to lockless CAS (not to
   * the advisory lock). So when the gate is engaged the advisory lock is skipped unconditionally —
   * it is NOT a per-write fallback for a failed gate acquire. CAS remains the correctness guard in
   * all cases; a degraded gate simply means that write gets plain OL behavior (bounded thrash). The
   * factory warns at startup when both the gate and the advisory are enabled.
   */
  private boolean writeGateEngaged() {
    return aspectDao.isOptimisticLockingEnabled() && entityWriteLock.isActive();
  }

  // Write-gate key separator. URNs are "urn:..." and aspect names are alphanumeric PDL identifiers,
  // neither of which contains '|', so "<urn>|<aspect>" is an unambiguous composite key. Matches the
  // DAO advisory's ADVISORY_LOCK_KEY_SEP so both aspect-level lock systems share one convention.
  private static final char WRITE_GATE_KEY_SEP = '|';

  /**
   * The write-gate key for one {@code (urn, aspect)} — the CAS conflict unit. CAS and {@code FOR
   * UPDATE} contend on the {@code (urn, aspect, version)} row, so two writers on the same URN but
   * different aspects share no row and must not share a gate key (URN-level keys would
   * over-serialize the common cross-aspect case).
   */
  static String writeGateKey(@Nonnull String urn, @Nonnull String aspectName) {
    return urn + WRITE_GATE_KEY_SEP + aspectName;
  }

  /** The write-gate keys a batch touches: one per {@code (urn, aspect)} written. */
  static List<String> writeGateKeys(@Nonnull Map<String, Set<String>> urnAspects) {
    return urnAspects.entrySet().stream()
        .flatMap(e -> e.getValue().stream().map(a -> writeGateKey(e.getKey(), a)))
        .collect(Collectors.toList());
  }

  /**
   * Function to perform an upsert of the content of a ChangeMCP using selective updates on columns
   *
   * @param changeMCP incoming change request
   * @param latestAspect the latest version of the aspect in-memory, it may not bethe latest version
   *     in the datastore latestAspect.getDatabaseAspect() - this is the latest version in the
   *     datastore
   * @return the system aspect to be persisted to the datastore
   */
  static SystemAspect applyUpsert(
      ChangeMCP changeMCP,
      SystemAspect latestAspect,
      @Nonnull List<com.linkedin.metadata.aspect.SystemAspectValidator> systemAspectValidators,
      @Nullable com.linkedin.metadata.config.AspectSizeValidationConfiguration validationConfig,
      @Nullable io.datahubproject.metadata.context.OperationContext opContext) {

    try {
      // This is the proposed version for this MCP, it can never be 0 (even if stored with row
      // version 0)
      long rowNextVersion = Math.max(1, changeMCP.getNextAspectVersion());

      // Incoming change's system metadata & increment
      SystemMetadata changeSystemMetadata =
          new SystemMetadata(changeMCP.getSystemMetadata().copy().data());
      changeSystemMetadata.setVersion(String.valueOf(rowNextVersion));
      if (rowNextVersion == 1) {
        // First version, we copy over modified audit stamp from where we have set it in initial
        // MCPItem generation
        changeSystemMetadata.setAspectCreated(
            changeMCP.getSystemMetadata().getAspectModified(), SetMode.IGNORE_NULL);
      }
      changeMCP.setSystemMetadata(changeSystemMetadata);

      if (latestAspect != null && latestAspect.getDatabaseAspect().isPresent()) {
        // update existing model

        String previousRunId =
            latestAspect
                .getDatabaseAspect()
                .map(ReadItem::getSystemMetadata)
                .map(dbSysMeta -> dbSysMeta.getRunId(GetMode.NULL))
                .orElse(null);

        // From the latest in-memory state (which may be changed from datastore)
        SystemMetadata latestSystemMetadata = latestAspect.getSystemMetadata();

        // Set the "last run id" to be the run id provided with the new system metadata. This
        // will be
        // stored in index
        latestSystemMetadata.setLastRunId(previousRunId, SetMode.REMOVE_IF_NULL);
        latestSystemMetadata.setLastObserved(
            changeSystemMetadata.getLastObserved(), SetMode.IGNORE_NULL);
        latestSystemMetadata.setRunId(changeSystemMetadata.getRunId(), SetMode.REMOVE_IF_NULL);
        latestSystemMetadata.setSchemaVersion(
            changeSystemMetadata.getSchemaVersion(), SetMode.IGNORE_NULL);

        if (!DataTemplateUtil.areEqual(
            latestAspect.getRecordTemplate(), changeMCP.getRecordTemplate())) {

          // update aspect, version, and audit info
          latestAspect.setRecordTemplate(changeMCP.getRecordTemplate());
          latestSystemMetadata.setVersion(changeSystemMetadata.getVersion());
          latestAspect.setAuditStamp(changeMCP.getAuditStamp());
          latestSystemMetadata.setAspectModified(
              changeSystemMetadata.getAspectModified(), SetMode.IGNORE_NULL);
          latestSystemMetadata.setAspectCreated(
              changeSystemMetadata.getAspectCreated(), SetMode.IGNORE_NULL);
        } else {
          // Do not increment version with the incoming change (match existing version)
          long matchVersion =
              Optional.ofNullable(latestSystemMetadata.getVersion())
                  .map(Long::valueOf)
                  .orElse(rowNextVersion);
          changeMCP.setNextAspectVersion(matchVersion);
          changeSystemMetadata.setVersion(String.valueOf(matchVersion));
          latestSystemMetadata.setVersion(String.valueOf(matchVersion));
        }

        // update previous - based on database aspect, populates MCL
        latestAspect.getDatabaseAspect().ifPresent(changeMCP::setPreviousSystemAspect);

        return latestAspect;
      } else {
        // insert
        return EbeanSystemAspect.builder()
            .systemAspectValidators(systemAspectValidators)
            .validationConfig(validationConfig)
            .operationContext(opContext)
            .forInsert(
                changeMCP.getUrn(),
                changeMCP.getAspectName(),
                changeMCP.getEntitySpec(),
                changeMCP.getAspectSpec(),
                changeMCP.getRecordTemplate(),
                changeMCP.getSystemMetadata(),
                changeMCP.getAuditStamp());
      }
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public void setUpdateIndicesService(@Nullable SearchIndicesService updateIndicesService) {
    this.updateIndicesService = updateIndicesService;
  }

  public ThrottleControl handleThrottleEvent(ThrottleEvent throttleEvent) {
    final Set<ThrottleType> activeEvents = throttleEvent.getActiveThrottles();
    // store throttle event
    throttleEvents.put(activeEvents, throttleEvent);

    return ThrottleControl.builder()
        // clear throttle event
        .callback(clearThrottle -> throttleEvents.remove(clearThrottle.getDisabledThrottles()))
        .build();
  }

  @Override
  public RecordTemplate getLatestAspect(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String aspectName) {
    log.debug("Invoked getLatestAspect with urn {}, aspect {}", urn, aspectName);
    return getAspect(opContext, urn, aspectName, ASPECT_LATEST_VERSION);
  }

  /**
   * Retrieves the latest aspects corresponding to a batch of {@link Urn}s based on a provided set
   * of aspect names.
   *
   * @param urns set of urns to fetch aspects for
   * @param aspectNames aspects to fetch for each urn in urns set
   * @return a map of provided {@link Urn} to a List containing the requested aspects.
   */
  @Override
  public Map<Urn, List<RecordTemplate>> getLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults =
        getLatestAspect(opContext, urns, aspectNames, false);

    // Fetch from db and populate urn -> aspect map.
    final Map<Urn, List<RecordTemplate>> urnToAspects = new HashMap<>();

    // Each urn should have some result, regardless of whether aspects are found in the DB.
    for (Urn urn : urns) {
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
    }

    if (alwaysIncludeKeyAspect) {
      // Add "key" aspects for each urn. TODO: Replace this with a materialized key aspect.
      urnToAspects
          .keySet()
          .forEach(
              key -> {
                final RecordTemplate keyAspect =
                    EntityApiUtils.buildKeyAspect(opContext.getEntityRegistry(), key);
                urnToAspects.get(key).add(keyAspect);
              });
    }

    List<SystemAspect> systemAspects =
        EntityUtils.toSystemAspects(
            opContext, opContext.getRetrieverContext(), batchGetResults.values());

    systemAspects.stream()
        // for now, don't add the key aspect here we have already added it above
        .filter(
            systemAspect ->
                !opContext
                    .getKeyAspectName(systemAspect.getUrn())
                    .equals(systemAspect.getAspectName()))
        .forEach(
            systemAspect ->
                urnToAspects
                    .computeIfAbsent(systemAspect.getUrn(), u -> new ArrayList<>())
                    .add(systemAspect.getRecordTemplate()));

    return urnToAspects;
  }

  @Nonnull
  @Override
  public Map<String, RecordTemplate> getLatestAspectsForUrn(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames,
      boolean forUpdate) {
    Map<EntityAspectIdentifier, EntityAspect> batchGetResults =
        getLatestAspect(opContext, new HashSet<>(Arrays.asList(urn)), aspectNames, forUpdate);

    return EntityUtils.toSystemAspects(
            opContext, opContext.getRetrieverContext(), batchGetResults.values())
        .stream()
        .map(
            systemAspect -> Pair.of(systemAspect.getAspectName(), systemAspect.getRecordTemplate()))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Retrieves an aspect having a specific {@link Urn}, name, & version.
   *
   * <p>Note that once we drop support for legacy aspect-specific resources, we should make this a
   * protected method. Only visible for backwards compatibility.
   *
   * @param urn an urn associated with the requested aspect
   * @param aspectName name of the aspect requested
   * @param version specific version of the aspect being requests
   * @return the {@link RecordTemplate} representation of the requested aspect object, or null if
   *     one cannot be found
   */
  @Nullable
  @Override
  public RecordTemplate getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      long version) {
    return getAspectVersionPair(opContext, urn, aspectName, version).getFirst();
  }

  public Pair<RecordTemplate, Long> getAspectVersionPair(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      long version) {

    log.debug(
        "Invoked getAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    version = calculateVersionNumber(opContext, urn, aspectName, version);
    final EntityAspectIdentifier primaryKey =
        new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect =
        Optional.ofNullable(aspectDao.getAspect(opContext, primaryKey));

    return Pair.of(
        EntityUtils.toSystemAspect(
                opContext, opContext.getRetrieverContext(), maybeAspect.orElse(null), false)
            .map(SystemAspect::getRecordTemplate)
            .orElse(null),
        version);
  }

  /**
   * Retrieves the latest aspects for the given urn as dynamic aspect objects (Without having to
   * define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urn urn of entity to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Nullable
  @Override
  public EntityResponse getEntityV2(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException {
    return getEntitiesV2(
            opContext, entityName, Collections.singleton(urn), aspectNames, alwaysIncludeKeyAspect)
        .get(urn);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @WithSpan
  @Override
  public Map<Urn, EntityResponse> getEntitiesV2(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException {
    return getLatestEnvelopedAspects(opContext, urns, aspectNames, alwaysIncludeKeyAspect)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> EntityUtils.toEntityResponse(entry.getKey(), entry.getValue())));
  }

  /**
   * Retrieves the aspects for the given set of urns and versions as dynamic aspect objects (Without
   * having to define union objects)
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Override
  public Map<Urn, EntityResponse> getEntitiesVersionedV2(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException {
    return getVersionedEnvelopedAspects(
            opContext, versionedUrns, aspectNames, alwaysIncludeKeyAspect)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> EntityUtils.toEntityResponse(entry.getKey(), entry.getValue())));
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EntityAspect.EntitySystemAspect} object
   */
  @Override
  public Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {

    return getEnvelopedVersionedAspects(
        opContext,
        urns.stream()
            .map(
                urn ->
                    Map.entry(
                        urn,
                        aspectNames.stream()
                            .map(aspectName -> Map.entry(aspectName, 0L))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
        alwaysIncludeKeyAspect);
  }

  @Override
  public Map<Urn, List<EnvelopedAspect>> getEnvelopedVersionedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, Map<String, Long>> urnAspectVersions,
      boolean alwaysIncludeKeyAspect) {

    // we will always need to fetch latest aspects in case the requested version is version 0 being
    // requested with version != 0
    Map<Urn, Map<String, Set<Long>>> withLatest =
        urnAspectVersions.entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().entrySet().stream()
                            .map(
                                aspectEntry ->
                                    Map.entry(
                                        aspectEntry.getKey(),
                                        Stream.of(0L, aspectEntry.getValue())
                                            .collect(Collectors.toSet())))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<Urn, List<EnvelopedAspect>> latestResult =
        getEnvelopedVersionedAspectsInternal(opContext, withLatest, alwaysIncludeKeyAspect);

    return latestResult.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                a ->
                    a.getValue().stream()
                        .filter(
                            v ->
                                matchVersion(v, urnAspectVersions.get(a.getKey()).get(v.getName())))
                        .collect(Collectors.toList())));
  }

  private static boolean matchVersion(
      @Nonnull EnvelopedAspect envelopedAspect, @Nullable Long expectedVersion) {
    if (expectedVersion == null) {
      return true;
    }
    if (Objects.equals(envelopedAspect.getVersion(GetMode.NULL), expectedVersion)) {
      return true;
    }
    if (envelopedAspect.hasSystemMetadata()
        && envelopedAspect.getSystemMetadata().hasVersion()
        && envelopedAspect.getSystemMetadata().getVersion() != null) {
      return Objects.equals(
          Long.parseLong(envelopedAspect.getSystemMetadata().getVersion()), expectedVersion);
    }

    return false;
  }

  private Map<Urn, List<EnvelopedAspect>> getEnvelopedVersionedAspectsInternal(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, Map<String, Set<Long>>> urnAspectVersions,
      boolean alwaysIncludeKeyAspect) {
    final Set<EntityAspectIdentifier> dbKeys =
        urnAspectVersions.entrySet().stream()
            .flatMap(
                entry -> {
                  Urn urn = entry.getKey();
                  return entry.getValue().entrySet().stream()
                      .flatMap(
                          aspectNameVersion ->
                              aspectNameVersion.getValue().stream()
                                  .map(
                                      version ->
                                          new EntityAspectIdentifier(
                                              urn.toString(),
                                              aspectNameVersion.getKey(),
                                              version)));
                })
            .collect(Collectors.toSet());

    return getCorrespondingAspects(opContext, dbKeys, alwaysIncludeKeyAspect);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized
   *     string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  @Override
  public Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Set<VersionedUrn> versionedUrns,
      @Nonnull Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect)
      throws URISyntaxException {

    Map<String, Map<String, Long>> urnAspectVersionMap =
        versionedUrns.stream()
            .collect(
                Collectors.toMap(
                    versionedUrn -> versionedUrn.getUrn().toString(),
                    versionedUrn ->
                        VersionedUrnUtils.convertVersionStamp(versionedUrn.getVersionStamp())));

    // Cover full/partial versionStamp
    final Set<EntityAspectIdentifier> dbKeys =
        urnAspectVersionMap.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .map(
                entry ->
                    aspectNames.stream()
                        .filter(aspectName -> entry.getValue().containsKey(aspectName))
                        .map(
                            aspectName ->
                                new EntityAspectIdentifier(
                                    entry.getKey(), aspectName, entry.getValue().get(aspectName)))
                        .collect(Collectors.toList()))
            .flatMap(List::stream)
            .collect(Collectors.toSet());

    // Cover empty versionStamp
    dbKeys.addAll(
        urnAspectVersionMap.entrySet().stream()
            .filter(entry -> entry.getValue().isEmpty())
            .map(
                entry ->
                    aspectNames.stream()
                        .map(
                            aspectName ->
                                new EntityAspectIdentifier(entry.getKey(), aspectName, 0L))
                        .collect(Collectors.toList()))
            .flatMap(List::stream)
            .collect(Collectors.toSet()));

    return getCorrespondingAspects(opContext, dbKeys, alwaysIncludeKeyAspect);
  }

  private Map<Urn, List<EnvelopedAspect>> getCorrespondingAspects(
      @Nonnull OperationContext opContext,
      Set<EntityAspectIdentifier> dbKeys,
      boolean alwaysIncludeKeyAspect) {

    Set<Urn> urns =
        dbKeys.stream().map(dbKey -> UrnUtils.getUrn(dbKey.getUrn())).collect(Collectors.toSet());

    final Map<EntityAspectIdentifier, EnvelopedAspect> envelopedAspectMap =
        getEnvelopedAspects(opContext, dbKeys);

    // Group result by Urn
    final Map<String, List<EnvelopedAspect>> urnToAspects =
        envelopedAspectMap.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    entry -> entry.getKey().getUrn(),
                    Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

    final Map<Urn, List<EnvelopedAspect>> result = new HashMap<>();
    for (Urn urn : urns) {
      List<EnvelopedAspect> aspects =
          urnToAspects.getOrDefault(urn.toString(), Collections.emptyList());

      EnvelopedAspect keyAspect =
          EntityUtils.getKeyEnvelopedAspect(urn, opContext.getEntityRegistry());
      // Add key aspect if it does not exist in the returned aspects
      if (alwaysIncludeKeyAspect
          && (aspects.isEmpty()
              || aspects.stream()
                  .noneMatch(aspect -> keyAspect.getName().equals(aspect.getName())))) {
        result.put(
            urn, ImmutableList.<EnvelopedAspect>builder().addAll(aspects).add(keyAspect).build());
      } else {
        result.put(urn, aspects);
      }
    }
    return result;
  }

  /**
   * Retrieves the latest aspect for the given urn as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urn urn to fetch
   * @param aspectName name of the aspect to fetch
   * @return {@link EnvelopedAspect} object, or null if one cannot be found
   */
  @Override
  public EnvelopedAspect getLatestEnvelopedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName)
      throws Exception {
    return getLatestEnvelopedAspects(opContext, ImmutableSet.of(urn), ImmutableSet.of(aspectName))
        .getOrDefault(urn, Collections.emptyList())
        .stream()
        .filter(envelopedAspect -> envelopedAspect.getName().equals(aspectName))
        .findFirst()
        .orElse(null);
  }

  /** Retrieves an {@link VersionedAspect}, or null if one cannot be found. */
  @Nullable
  @Override
  public VersionedAspect getVersionedAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long version) {

    log.debug(
        "Invoked getVersionedAspect with urn: {}, aspectName: {}, version: {}",
        urn,
        aspectName,
        version);

    VersionedAspect result = new VersionedAspect();

    Pair<RecordTemplate, Long> aspectRecord =
        getAspectVersionPair(opContext, urn, aspectName, version);
    if (aspectRecord.getFirst() == null) {
      return null;
    }

    Aspect resultAspect = new Aspect();

    RecordUtils.setSelectedRecordTemplateInUnion(resultAspect, aspectRecord.getFirst());
    result.setAspect(resultAspect);
    result.setVersion(aspectRecord.getSecond());

    return result;
  }

  /**
   * Retrieves a list of all aspects belonging to an entity of a particular type, sorted by urn.
   *
   * <p>Note that once we drop support for legacy 'getAllDataPlatforms' endpoint, we can drop
   * support for this unless otherwise required. Only visible for backwards compatibility.
   *
   * @param entityName name of the entity type the aspect belongs to, e.g. 'dataset'
   * @param aspectName name of the aspect requested, e.g. 'ownership'
   * @param start the starting index of the returned aspects, used in pagination
   * @param count the count of the aspects to be returned, used in pagination
   * @return a {@link ListResult} of {@link RecordTemplate}s representing the requested aspect.
   */
  @Nonnull
  @Override
  public ListResult<RecordTemplate> listLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      @Nullable Integer count) {

    log.debug(
        "Invoked listLatestAspects with entityName: {}, aspectName: {}, start: {}, count: {}",
        entityName,
        aspectName,
        start,
        count);

    final ListResult<String> aspectMetadataList =
        aspectDao.listLatestAspectMetadata(opContext, entityName, aspectName, start, count);

    List<EntityAspect> entityAspects = new ArrayList<>();
    for (int i = 0; i < aspectMetadataList.getValues().size(); i++) {
      EntityAspect entityAspect = new EntityAspect();
      entityAspect.setUrn(
          aspectMetadataList.getMetadata().getExtraInfos().get(i).getUrn().toString());
      entityAspect.setAspect(aspectName);
      entityAspect.setMetadata(aspectMetadataList.getValues().get(i));
      entityAspects.add(entityAspect);
    }

    return new ListResult<>(
        EntityUtils.toSystemAspects(opContext, opContext.getRetrieverContext(), entityAspects)
            .stream()
            .map(SystemAspect::getRecordTemplate)
            .collect(Collectors.toList()),
        aspectMetadataList.getMetadata(),
        aspectMetadataList.getNextStart(),
        aspectMetadataList.isHasNext(),
        aspectMetadataList.getTotalCount(),
        aspectMetadataList.getTotalPageCount(),
        aspectMetadataList.getPageSize());
  }

  /**
   * Common batch-like pattern used primarily in tests.
   *
   * @param entityUrn the entity urn
   * @param pairList list of aspects in pairs of aspect name and record template
   * @param auditStamp audit stamp
   * @param systemMetadata system metadata
   * @return update result
   */
  @Override
  public List<UpdateAspectResult> ingestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      List<Pair<String, RecordTemplate>> pairList,
      @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    List<? extends MCPItem> items =
        pairList.stream()
            .map(
                pair ->
                    ChangeItemImpl.builder()
                        .urn(entityUrn)
                        .aspectName(pair.getKey())
                        .recordTemplate(pair.getValue())
                        .systemMetadata(systemMetadata)
                        .auditStamp(auditStamp)
                        .build(opContext.getAspectRetriever()))
            .collect(Collectors.toList());
    return ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(opContext),
        !cdcModeChangeLog,
        true);
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link
   * com.linkedin.mxe.MetadataChangeLog}.
   *
   * @param aspectsBatch aspects to write
   * @param emitMCL whether a {@link com.linkedin.mxe.MetadataChangeLog} should be emitted in
   *     correspondence upon successful update
   * @return the {@link RecordTemplate} representation of the written aspect object
   */
  @Override
  public List<UpdateAspectResult> ingestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull final AspectsBatch aspectsBatch,
      boolean emitMCL,
      boolean overwrite) {

    // Skip DB timer for empty batch
    if (aspectsBatch.getItems().size() == 0) {
      return Collections.emptyList();
    }

    // Handle throttling
    APIThrottle.evaluate(opContext, new HashSet<>(throttleEvents.values()), false);

    IngestAspectsResult ingestResults = ingestAspectsToLocalDB(opContext, aspectsBatch, overwrite);

    // Produce MCLs & run side effects
    List<MetadataChangeLog> mcls =
        ingestResults.getUpdateAspectResults().stream()
            .map(UpdateAspectResult::toMCL)
            .collect(Collectors.toList());

    List<UpdateAspectResult> updateAspectResults;

    List<MCLEmitResult> mclEmitResults;
    try {
      if (!cdcModeChangeLog && emitMCL) {
        mclEmitResults = produceMCLAsync(opContext, mcls);
      } else {
        // This results in pre-process being called here that may be potentially out-of-order.
        // when the CDC record is consumed, produceMCLAsync is called in the CDC order and
        // will result in pre-process being called again potentially overwriting this first
        // preprocess result.
        mclEmitResults =
            mcls.stream()
                .map(mcl -> Pair.of(preprocessEvent(opContext, mcl), mcl))
                .map(
                    preprocessResult ->
                        MCLEmitResult.builder()
                            .emitted(false)
                            .processedMCL(preprocessResult.getFirst())
                            .mclFuture(null)
                            .metadataChangeLog(preprocessResult.getSecond())
                            .build())
                .collect(Collectors.toList());
      }
      updateAspectResults =
          IntStream.range(0, ingestResults.getUpdateAspectResults().size())
              .mapToObj(
                  i -> {
                    UpdateAspectResult updateAspectResult =
                        ingestResults.getUpdateAspectResults().get(i);
                    MCLEmitResult mclEmitResult = mclEmitResults.get(i);
                    return updateAspectResult.toBuilder()
                        .mclFuture(mclEmitResult.getMclFuture())
                        .processedMCL(mclEmitResult.isProcessedMCL())
                        .build();
                  })
              .collect(Collectors.toList());

      // Produce FailedMCPs for tracing
      produceFailedMCPs(opContext, ingestResults);

      invalidateEntityGraphCacheOnSyncIngest(
          opContext, aspectsBatch, ingestResults.getUpdateAspectResults());
    } finally {
      // Retention is best-effort cleanup keyed only on the committed upsert results, not on MCL
      // output. Run it in a finally so a failure emitting MCLs / running side effects — all of
      // which happen after the upsert has already committed — cannot skip the prune (legacy in-tx
      // retention ran before MCL work; post-commit must not regress that). applyRetentionPostCommit
      // never throws (its own outer try/catch), so it cannot mask an in-flight exception here.
      applyRetentionPostCommit(opContext, ingestResults.getUpdateAspectResults());
    }

    return updateAspectResults;
  }

  private void invalidateEntityGraphCacheOnSyncIngest(
      @Nonnull OperationContext opContext,
      @Nonnull AspectsBatch aspectsBatch,
      @Nonnull List<UpdateAspectResult> updateResults) {
    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncIngestBatch(
            opContext, preProcessHooks, aspectsBatch, updateResults);
    if (!batch.isEmpty()) {
      opContext.getEntityGraphCache().invalidateOnSyncBatch(batch);
    }
  }

  /**
   * Builds retention contexts from upsert results. Previous version existence is implied by
   * oldValue != null.
   *
   * @param updatedLatestAspects when non-null (legacy in-transaction path), restricts retention to
   *     urn/aspect pairs actually present in the post-upsert latest-aspect snapshot; when null
   *     (post-commit path), no such restriction is applied since that snapshot isn't available
   *     outside the transaction.
   */
  private List<RetentionService.RetentionContext> buildRetentionContexts(
      List<UpdateAspectResult> upsertResults,
      @Nullable Map<String, Map<String, SystemAspect>> updatedLatestAspects) {
    return upsertResults.stream()
        .filter(
            r ->
                updatedLatestAspects == null
                    || (updatedLatestAspects.containsKey(r.getUrn().toString())
                        && updatedLatestAspects
                            .get(r.getUrn().toString())
                            .containsKey(r.getRequest().getAspectName())))
        .filter(
            r -> {
              RecordTemplate oldAspect = r.getOldValue();
              return oldAspect != r.getNewValue() && oldAspect != null;
            })
        .map(
            r ->
                RetentionService.RetentionContext.builder()
                    .urn(r.getUrn())
                    .aspectName(r.getRequest().getAspectName())
                    .maxVersion(Optional.of(r.getMaxVersion()))
                    .build())
        .collect(Collectors.toList());
  }

  /**
   * Best-effort retention applied AFTER the upsert transaction commits. Failures are logged and
   * metric'd; they never throw, never retry, never poison the ingest batch. Only runs when
   * postCommitRetentionEnabled is true.
   *
   * <p>When a {@link RetentionBuffer} is wired and {@link RetentionBuffer#defersApply()} returns
   * true, retention keys are coalesced into the buffer for a background {@code RetentionDrainer} to
   * apply asynchronously. Otherwise (no buffer, i.e. {@link RetentionBuffer#NO_OP}), this falls
   * back to the original synchronous DELETE loop below.
   */
  @VisibleForTesting
  void applyRetentionPostCommit(
      @Nonnull OperationContext opContext, @Nonnull List<UpdateAspectResult> upsertResults) {
    if (!postCommitRetentionEnabled) {
      return;
    }
    if (retentionService == null) {
      log.warn("Retention service is missing!");
      return;
    }
    // Outer safety net: nothing in this method may propagate. The upsert has already committed,
    // so a retention bug must never fail the ingest call.
    try {
      List<RetentionService.RetentionContext> retentionBatch =
          // null updatedLatestAspects: post-commit has no in-tx snapshot; oldValue!=null filter
          // below is the sole eligibility gate (intentional — see buildRetentionContexts javadoc).
          buildRetentionContexts(upsertResults, null);
      if (retentionBatch.isEmpty()) {
        return;
      }

      if (retentionBuffer.defersApply()) {
        for (RetentionService.RetentionContext ctx : retentionBatch) {
          retentionBuffer.enqueue(
              opContext, ctx.getUrn(), ctx.getAspectName(), ctx.getMaxVersion().orElse(0L));
        }
        return;
      }

      opContext.withSpan(
          "retentionPostCommit",
          () -> {
            // Per-context try/catch: one bad DELETE must not abort the rest of the cleanup.
            for (RetentionService.RetentionContext ctx : retentionBatch) {
              try {
                retentionService.applyRetentionWithPolicyDefaults(opContext, List.of(ctx));
              } catch (Exception e) {
                // TODO: no retention_dlq table yet; do not claim DLQ. FMCP is for failed MCPs
                // only, not retention prune. For now: log + metric only.
                log.warn(
                    "Post-commit retention failed for urn={} aspect={}; recorded metric only (no"
                        + " DLQ yet). Upsert already committed; no data loss.",
                    ctx.getUrn(),
                    ctx.getAspectName(),
                    e);
                opContext
                    .getMetricUtils()
                    .ifPresent(
                        m ->
                            m.increment(
                                EntityServiceImpl.class, "post_commit_retention_failed", 1));
              }
            }
          },
          BATCH_SIZE_ATTR,
          String.valueOf(retentionBatch.size()));
    } catch (Exception e) {
      log.warn(
          "Post-commit retention batch failed unexpectedly; upsert already committed; no data"
              + " loss.",
          e);
      opContext
          .getMetricUtils()
          .ifPresent(m -> m.increment(EntityServiceImpl.class, "post_commit_retention_failed", 1));
    }
  }

  /**
   * Process post-commit MCPSideEffects
   *
   * @param mcls mcls generated
   */
  private void processPostCommitMCLSideEffects(
      @Nonnull OperationContext opContext, List<MetadataChangeLog> mcls) {
    log.debug("Considering {} MCLs post commit side effects.", mcls.size());
    List<MCLItem> batch =
        mcls.stream()
            .map(mcl -> MCLItemImpl.builder().build(mcl, opContext.getAspectRetriever()))
            .collect(Collectors.toList());

    try (Stream<MCPItem> sideEffectStream =
        AspectsBatch.applyPostMCPSideEffects(opContext, batch, opContext.getRetrieverContext())) {
      applyPostCommitMcpSideEffects(opContext, sideEffectStream.collect(Collectors.toList()));
    }
  }

  /**
   * Applies post-commit MCP side effects. Upserts go through async MCP produce (consumed later).
   * Deletes are applied synchronously via {@link #deleteAspect} — ChangeType.DELETE is not a valid
   * async MCP ingest change type end-to-end ({@link MCPItem#CHANGE_TYPES} / consumer sync ingest).
   */
  @VisibleForTesting
  void applyPostCommitMcpSideEffects(
      @Nonnull OperationContext opContext, @Nonnull List<MCPItem> sideEffects) {
    if (sideEffects.isEmpty()) {
      return;
    }

    List<MCPItem> deletes = new ArrayList<>();
    List<MCPItem> nonDeletes = new ArrayList<>();
    for (MCPItem item : sideEffects) {
      if (ChangeType.DELETE.equals(item.getChangeType())) {
        deletes.add(item);
      } else {
        nonDeletes.add(item);
      }
    }

    for (MCPItem delete : deletes) {
      try {
        boolean hardDelete =
            EntityUtils.shouldHardDeleteAspect(opContext, delete.getUrn(), delete.getAspectName());
        deleteAspect(
            opContext, delete.getUrn().toString(), delete.getAspectName(), Map.of(), hardDelete);
      } catch (Exception e) {
        log.error(
            "Failed post-commit side-effect delete for urn={} aspect={}: {}",
            delete.getUrn(),
            delete.getAspectName(),
            e.getMessage(),
            e);
      }
    }

    if (nonDeletes.isEmpty()) {
      return;
    }

    Iterable<List<MCPItem>> iterable =
        () -> Iterators.partition(nonDeletes.iterator(), MCP_SIDE_EFFECT_KAFKA_BATCH_SIZE);
    StreamSupport.stream(iterable.spliterator(), false)
        .forEach(
            chunk -> {
              long count =
                  ingestProposalAsync(
                          opContext,
                          AspectsBatchImpl.builder()
                              .items(chunk)
                              .retrieverContext(opContext.getRetrieverContext())
                              .build(opContext))
                      .count();
              log.debug("Generated {} MCP SideEffects for async processing", count);
            });
  }

  /**
   * Checks whether there is an actual update to the aspect by applying the updateLambda If there is
   * an update, push the new version into the local DB. Otherwise, do not push the new version, but
   * just update the system metadata.
   *
   * @param inputBatch Collection of the following: an urn associated with the new aspect, name of
   *     the aspect being inserted, and a function to apply to the latest version of the aspect to
   *     get the updated version
   * @return Details about the new and old version of the aspect
   */
  @Nonnull
  private IngestAspectsResult ingestAspectsToLocalDB(
      @Nonnull OperationContext opContext,
      @Nonnull final AspectsBatch inputBatch,
      boolean overwrite) {

    return opContext.withSpan(
        "ingestAspectsToLocalDB",
        () -> {
          try {
            // Clear pending deletions at start of each request to ensure fresh state
            opContext.clearPendingDeletions();

            if (inputBatch.containsDuplicateAspects()) {
              log.warn("Batch contains duplicates: {}", inputBatch.duplicateAspects());
              opContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils ->
                          metricUtils.increment(
                              EntityServiceImpl.class, "batch_with_duplicate", 1));
            }

            // Write gate: in scoped mode, serialize concurrent writers on the base URNs BEFORE the
            // transaction opens (off the DB connection with the Hazelcast backend), held across all
            // internal retries and released when the try-with-resources closes. No-op otherwise.
            // Parents only — derived aspects rely on CAS.
            final IngestAspectsResult result;
            try (EntityWriteLock.LockHandle writeGate =
                acquireWriteGate(opContext, writeGateKeys(inputBatch.getUrnAspectsMap()))) {
              result =
                  aspectDao
                      .runInTransactionWithRetry(
                          opContext,
                          (txContext) -> {
                            // Additive gate: our scoped/branch-keyed retry runs ONLY when both
                            // optimistic locking and scoped retry are enabled. Otherwise fall
                            // through
                            // to the full-batch path (the optimistic-locking base): each MCP is
                            // persisted via the throwing ingestAspectToLocalDB, and an
                            // OptimisticLockConflictException bubbles to runInTransactionWithRetry,
                            // which re-runs the whole batch. With OL off this else-branch is
                            // byte-identical to the legacy behavior.
                            if (aspectDao.isOptimisticLockingEnabled()
                                && aspectDao.isScopedRetryEnabled()) {
                              // Generate default aspects within the transaction (they are
                              // re-calculated
                              // on
                              // retry)
                              AspectsBatch batchWithDefaults =
                                  DefaultAspectsUtil.withAdditionalChanges(
                                      opContext, inputBatch, this, enableBrowseV2);

                              // First pass over the full batch: read latest -> compute versions ->
                              // run
                              // hooks -> CAS persist, collecting a per-MCP BatchWriteResult.
                              ScopedComputeResult attempt =
                                  computeAndPersistWithinTransaction(
                                      opContext, txContext, batchWithDefaults, overwrite, Set.of());

                              // No changes, return (all no-ops / filtered) — unchanged fast path.
                              if (attempt.changeMCPs.isEmpty()) {
                                opContext
                                    .getMetricUtils()
                                    .ifPresent(
                                        metricUtils ->
                                            metricUtils.increment(
                                                EntityServiceImpl.class, "batch_empty", 1));
                                return TransactionResult.ingestAspectsRollback();
                              }

                              final List<UpdateAspectResult> upsertResults =
                                  new ArrayList<>(attempt.upsertResults);
                              final List<Pair<ChangeMCP, Set<AspectValidationException>>>
                                  failedUpsertResults =
                                      new ArrayList<>(attempt.failedUpsertResults);
                              Map<String, Map<String, SystemAspect>> updatedLatestAspects =
                                  attempt.updatedLatestAspects;

                              // Stage 2 — scoped (branch-keyed) retry. Gated on optimistic locking:
                              // when
                              // OL is off, ingestAspectToLocalDB never produces CONFLICT, so this
                              // branch
                              // is never entered and the legacy path stays byte-identical. On
                              // CONFLICT,
                              // recompute ONLY the conflicted URNs' branch (re-read the winner's
                              // row
                              // within this open READ_COMMITTED txn, re-run that URN's hooks,
                              // re-CAS)
                              // rather than regenerating and re-CAS-ing the whole batch.
                              if (aspectDao.isOptimisticLockingEnabled()
                                  && attempt.batchWriteResult.hasConflicts()) {
                                BatchWriteResult batchWriteResult = attempt.batchWriteResult;
                                Map<Pair<Urn, String>, Set<Urn>> derivedToParents =
                                    attempt.derivedToParents;

                                // (urn, aspect) pairs already committed in this transaction. A
                                // multi-aspect URN can have one aspect CONFLICT while a sibling
                                // already
                                // COMMITTED in the same pass; because the sub-batch is scoped by
                                // URN, that
                                // committed sibling would otherwise be re-persisted on retry. The
                                // version
                                // no-op guard does NOT catch it (withTraceId stamps a fresh traceId
                                // each
                                // pass, so systemMetadata differs and it looks like a real write) →
                                // duplicate UpdateAspectResult/MCL + a spurious extra version bump.
                                // Skipping already-committed keys avoids the double-commit; per-URN
                                // atomicity still holds because everything commits in one
                                // transaction.
                                //
                                // Both base and derived committed keys are held: on a branch retry
                                // the
                                // parent re-derives its children, and re-persisting an
                                // already-committed
                                // (urn,aspect) — whose content is typically unchanged — would emit
                                // a
                                // duplicate MCL / spurious version bump (the fresh traceId defeats
                                // the
                                // content no-op guard). KNOWN LIMITATION: in the exotic case where
                                // a
                                // re-derivation would change a derived child (e.g. a VersionSet
                                // whose
                                // previous-latest moved under a concurrent race), skipping it
                                // leaves the
                                // old derived row stale. Rare; documented; a content-aware skip is
                                // the
                                // future fix.
                                final Set<Pair<Urn, String>> committedKeys = new HashSet<>();
                                committedKeys.addAll(committedKeysOf(attempt.batchWriteResult));

                                // (urn, aspect) pairs already recorded as failed. Dedups failures
                                // across scoped-retry passes: a terminally validation-failing
                                // aspect
                                // on a URN whose sibling also conflicts is re-included in every
                                // URN-scoped retry sub-batch and re-fails each pass, so without
                                // this
                                // guard it would be dead-lettered once per pass on the consumer
                                // path.
                                // Seeded from the first pass's failures.
                                final Set<Pair<Urn, String>> seenFailedKeys =
                                    new HashSet<>(failedKeysOf(failedUpsertResults));

                                int scopedAttempt = 0;
                                while (batchWriteResult.hasConflicts()
                                    && scopedAttempt < SCOPED_RETRY_MAX_ATTEMPTS) {
                                  // Exact branch scoping: a conflict on any node (base or derived)
                                  // recomputes its whole branch — root + entire derived subtree —
                                  // regenerated together; unrelated branches are untouched.
                                  final Set<Urn> recomputeUrns =
                                      branchScopedRecompute(batchWriteResult, derivedToParents);
                                  if (recomputeUrns.isEmpty()) {
                                    break;
                                  }

                                  final int recomputeCount = recomputeUrns.size();
                                  opContext
                                      .getMetricUtils()
                                      .ifPresent(
                                          metricUtils -> {
                                            metricUtils.increment(
                                                EntityServiceImpl.class,
                                                "optimistic_lock_scoped_retry",
                                                1);
                                            metricUtils.increment(
                                                EntityServiceImpl.class,
                                                "optimistic_lock_scoped_retry_urns",
                                                recomputeCount);
                                          });

                                  // Re-run the conflicted URNs through the SAME compute path. Never
                                  // hand-roll per-item re-versioning: build a sub-batch from the
                                  // original
                                  // input items filtered to the recompute URNs and reuse
                                  // toUpsertBatchItems / incrementBatchVersion. Derived (Layer-2)
                                  // items
                                  // are intentionally dropped and re-derived from the freshly
                                  // re-read
                                  // base state on each pass. Already-committed sibling aspects are
                                  // skipped
                                  // inside the persist loop via committedKeys.
                                  final AspectsBatch subBatch =
                                      AspectsBatchImpl.builder()
                                          .items(
                                              filterItemsForRecompute(
                                                  batchWithDefaults.getItems(), recomputeUrns))
                                          .retrieverContext(batchWithDefaults.getRetrieverContext())
                                          .build(opContext);

                                  ScopedComputeResult retry =
                                      computeAndPersistWithinTransaction(
                                          opContext, txContext, subBatch, overwrite, committedKeys);

                                  // Committed URNs stay committed (they are not in the sub-batch);
                                  // only
                                  // the newly resolved results are spliced in.
                                  upsertResults.addAll(retry.upsertResults);
                                  appendNewFailedResults(
                                      failedUpsertResults,
                                      retry.failedUpsertResults,
                                      seenFailedKeys);
                                  updatedLatestAspects =
                                      AspectsBatch.merge(
                                          updatedLatestAspects, retry.updatedLatestAspects);
                                  committedKeys.addAll(committedKeysOf(retry.batchWriteResult));
                                  batchWriteResult = retry.batchWriteResult;
                                  derivedToParents = retry.derivedToParents;
                                  scopedAttempt++;
                                }

                                if (batchWriteResult.hasConflicts()) {
                                  // Scoped retries exhausted (rare — the pre-transaction write gate
                                  // serializes hot-key writers, so a URN rarely loses this many CAS
                                  // races
                                  // in one request). Deliberate fallback: throw once so the write
                                  // is
                                  // re-driven by the OUTER whole-batch retry with backoff and the
                                  // DB
                                  // connection released, and — on final failure — routed to the
                                  // existing
                                  // FMCP dead-letter path by the async ingest consumer. No
                                  // commitAndContinue
                                  // has run yet, so this rolls back the whole transaction cleanly
                                  // with no
                                  // torn per-URN write. We intentionally do NOT commit the good
                                  // branches +
                                  // DLQ only the unresolved ones here: that would require undoing
                                  // any
                                  // torn branch (parent CAS-written, derived still conflicting)
                                  // without a
                                  // savepoint, which isn't possible in one transaction. In Mode C
                                  // the gate serializes PARENTS, so an exhaustion here is almost
                                  // always a torn DERIVED case (the case that must roll the whole
                                  // txn back), so commit-good/DLQ-unresolved (design step 4/5) is a
                                  // Mode B (per-branch atomic commit unit) feature, not safe here.
                                  //
                                  // KNOWN LIMITATION (lock-across-backoff): this
                                  // throw propagates to the DAO's runInTransactionWithRetry, whose
                                  // outer retry sleeps (sleepBeforeRetry) between attempts WHILE
                                  // the
                                  // write gate acquired in ingestAspectsToLocalDB is still held,
                                  // blocking other writers on these URNs during the backoff. Not
                                  // fixed here: both candidate fixes carry untestable risk — (a)
                                  // commit-good/DLQ-unresolved is unsafe in Mode C (torn derived,
                                  // above); (b) releasing the gate around each outer-retry backoff
                                  // requires hoisting the retry loop out of the DAO into this
                                  // method
                                  // or passing the gate into the DAO to release around
                                  // sleepBeforeRetry — both perturb the shared retry path used by
                                  // the
                                  // OL-base and PL modes and cannot be validated without ITs.
                                  final Set<Urn> unresolved = batchWriteResult.conflictedUrns();
                                  final int attempts = scopedAttempt;
                                  opContext
                                      .getMetricUtils()
                                      .ifPresent(
                                          metricUtils ->
                                              metricUtils.increment(
                                                  EntityServiceImpl.class,
                                                  "optimistic_lock_scoped_retry_exhausted",
                                                  1));
                                  throw new OptimisticLockConflictException(
                                      "Optimistic lock conflict unresolved after "
                                          + attempts
                                          + " scoped retries for urns="
                                          + unresolved);
                                }
                              }

                              // Effectively-final view of the (possibly merged) latest-aspect map
                              // for the
                              // retention lambda below.
                              final Map<String, Map<String, SystemAspect>>
                                  finalUpdatedLatestAspects = updatedLatestAspects;

                              if (!upsertResults.isEmpty()) {
                                // commit upserts prior to retention or kafka send, if supported by
                                // impl
                                if (txContext != null) {
                                  try {
                                    txContext.commitAndContinue();
                                  } catch (RejectedExecutionException e) {
                                    log.warn(
                                        "Post-commit cache notification failed (executor terminated),"
                                            + " cache may serve stale data until TTL expiry",
                                        e);
                                    opContext
                                        .getMetricUtils()
                                        .ifPresent(
                                            metricUtils ->
                                                metricUtils.increment(
                                                    EntityServiceImpl.class,
                                                    "post_commit_notify_rejected",
                                                    1));
                                  } catch (EntityNotFoundException e) {
                                    if (e.getMessage() != null
                                        && e.getMessage().contains("No rows updated")) {
                                      log.warn(
                                          "Ignoring no rows updated condition for metadata update",
                                          e);
                                      opContext
                                          .getMetricUtils()
                                          .ifPresent(
                                              metricUtils ->
                                                  metricUtils.increment(
                                                      EntityServiceImpl.class,
                                                      "no_rows_updated",
                                                      1));
                                      return TransactionResult.rollback();
                                    }
                                    throw e;
                                  }
                                }

                                // Retention optimization and tx. In-tx retention runs ONLY when
                                // post-commit retention is OFF; when it is ON,
                                // applyRetentionPostCommit
                                // applies retention after commit. Gating here mirrors the legacy
                                // branch
                                // so the scoped path does not apply retention twice for the same
                                // upsert
                                // results.
                                if (!postCommitRetentionEnabled && retentionService != null) {
                                  opContext.withSpan(
                                      "retentionService",
                                      () -> {
                                        List<RetentionService.RetentionContext> retentionBatch =
                                            buildRetentionContexts(
                                                upsertResults, finalUpdatedLatestAspects);
                                        retentionService.applyRetentionWithPolicyDefaults(
                                            opContext, retentionBatch);
                                      },
                                      BATCH_SIZE_ATTR,
                                      String.valueOf(upsertResults.size()));
                                } else if (!postCommitRetentionEnabled) {
                                  log.warn("Retention service is missing!");
                                }
                              } else {
                                opContext
                                    .getMetricUtils()
                                    .ifPresent(
                                        metricUtils ->
                                            metricUtils.increment(
                                                EntityServiceImpl.class,
                                                "batch_empty_transaction",
                                                1));
                                // This includes no-op batches. i.e. patch removing non-existent
                                // items
                                log.debug("Empty transaction detected");
                                if (txContext != null) {
                                  txContext.rollback();
                                }
                              }

                              // Force flush span processing for DUE Exports
                              Optional.ofNullable(opContext.getSystemTelemetryContext())
                                  .map(SystemTelemetryContext::getUsageSpanExporter)
                                  .ifPresent(SpanProcessor::forceFlush);

                              return TransactionResult.of(
                                  IngestAspectsResult.builder()
                                      .updateAspectResults(upsertResults)
                                      .failedUpdateAspectResults(failedUpsertResults)
                                      .build());
                            } else {
                              // Full-batch retry path (optimistic-locking base + legacy). Restored
                              // verbatim from the pre-scoped persist loop: each successful
                              // ChangeMCP is
                              // persisted via the throwing ingestAspectToLocalDB. On the OL path a
                              // CONFLICT throws OptimisticLockConflictException, which propagates
                              // to
                              // runInTransactionWithRetry to re-run the whole batch; with OL off
                              // this is
                              // byte-identical to the legacy path.
                              AspectsBatch batchWithDefaults =
                                  DefaultAspectsUtil.withAdditionalChanges(
                                      opContext, inputBatch, this, enableBrowseV2);

                              final Map<String, Set<String>> urnAspects =
                                  batchWithDefaults.getUrnAspectsMap();

                              // Opt-in per-(urn, aspect) write serialization (Postgres advisory
                              // lock), taken before any row locks so this write serializes against
                              // a
                              // concurrent hard-delete. Skipped when the pre-transaction write gate
                              // is engaged — the gate already serializes the same (urn, aspect)
                              // keys
                              // off-connection, so the advisory would be a redundant round trip.
                              if (!writeGateEngaged()) {
                                aspectDao.lockAspectsForWrite(opContext, urnAspects);
                              }

                              // Write-intent read: pin primary. The DAO uses SELECT FOR UPDATE in
                              // legacy mode and skips the row lock in optimistic mode, where CAS
                              // detects concurrent changes.
                              final Map<String, Map<String, SystemAspect>> batchAspects =
                                  aspectDao.getLatestAspects(opContext, urnAspects, true);
                              final Map<String, Map<String, SystemAspect>> updatedLatestAspects;

                              // read #2 (potentially)
                              final Map<String, Map<String, Long>> nextVersions =
                                  EntityUtils.calculateNextVersions(
                                      opContext, txContext, aspectDao, batchAspects, urnAspects);

                              // 1. Convert patches to full upserts
                              // 2. Run any entity/aspect level hooks
                              Pair<Map<String, Set<String>>, List<ChangeMCP>> updatedItems =
                                  batchWithDefaults.toUpsertBatchItems(
                                      opContext,
                                      batchAspects,
                                      nextVersions,
                                      (changeMCP, systemAspect) ->
                                          applyUpsert(
                                              changeMCP,
                                              systemAspect,
                                              aspectDao.getSystemAspectValidators(),
                                              aspectDao.getValidationConfig(),
                                              opContext));

                              // Fetch additional information if needed
                              final List<ChangeMCP> changeMCPs;

                              if (!updatedItems.getFirst().isEmpty()) {
                                // These items are new items from side effects
                                Map<String, Set<String>> sideEffects = updatedItems.getFirst();

                                final Map<String, Map<String, Long>> updatedNextVersions;

                                Map<String, Map<String, SystemAspect>> newLatestAspects =
                                    aspectDao.getLatestAspects(opContext, sideEffects, true);

                                // merge
                                updatedLatestAspects =
                                    AspectsBatch.merge(batchAspects, newLatestAspects);

                                Map<String, Map<String, Long>> newNextVersions =
                                    EntityUtils.calculateNextVersions(
                                        opContext,
                                        txContext,
                                        aspectDao,
                                        updatedLatestAspects,
                                        updatedItems.getFirst());
                                // merge
                                updatedNextVersions =
                                    AspectsBatch.merge(nextVersions, newNextVersions);

                                changeMCPs =
                                    updatedItems.getSecond().stream()
                                        .peek(
                                            changeMCP -> {
                                              // Add previous version to each side-effect
                                              if (sideEffects
                                                  .getOrDefault(
                                                      changeMCP.getUrn().toString(),
                                                      Collections.emptySet())
                                                  .contains(changeMCP.getAspectName())) {

                                                AspectsBatch.incrementBatchVersion(
                                                    changeMCP,
                                                    updatedLatestAspects,
                                                    updatedNextVersions,
                                                    (mcp, sysAspect) ->
                                                        applyUpsert(
                                                            mcp,
                                                            sysAspect,
                                                            aspectDao.getSystemAspectValidators(),
                                                            aspectDao.getValidationConfig(),
                                                            opContext));
                                              }
                                            })
                                        .collect(Collectors.toList());
                              } else {
                                changeMCPs = updatedItems.getSecond();
                                updatedLatestAspects = batchAspects;
                              }

                              // No changes, return
                              if (changeMCPs.isEmpty()) {
                                opContext
                                    .getMetricUtils()
                                    .ifPresent(
                                        metricUtils ->
                                            metricUtils.increment(
                                                EntityServiceImpl.class, "batch_empty", 1));
                                return TransactionResult.ingestAspectsRollback();
                              }

                              // do final pre-commit checks with previous aspect value
                              ValidationExceptionCollection exceptions =
                                  AspectsBatch.validatePreCommit(
                                      opContext,
                                      changeMCPs,
                                      opContext.getRetrieverContext(),
                                      opContext);

                              List<Pair<ChangeMCP, Set<AspectValidationException>>>
                                  failedUpsertResults = new ArrayList<>();
                              if (exceptions.hasFatalExceptions()) {
                                // IF this is a client request/API request we fail the
                                // `transaction
                                // batch`
                                if (opContext.getRequestContext() != null) {
                                  opContext
                                      .getMetricUtils()
                                      .ifPresent(
                                          metricUtils ->
                                              metricUtils.increment(
                                                  EntityServiceImpl.class,
                                                  "batch_request_validation_exception",
                                                  1));
                                  collectMetrics(
                                      opContext.getMetricUtils().orElse(null), exceptions);
                                  throw new ValidationException(exceptions);
                                }

                                opContext
                                    .getMetricUtils()
                                    .ifPresent(
                                        metricUtils ->
                                            metricUtils.increment(
                                                EntityServiceImpl.class,
                                                "batch_consumer_validation_exception",
                                                1));
                                log.error(
                                    "mce-consumer batch exceptions: {}",
                                    collectMetrics(
                                        opContext.getMetricUtils().orElse(null), exceptions));
                                failedUpsertResults =
                                    exceptions
                                        .streamExceptions(changeMCPs.stream())
                                        .map(
                                            writeItem ->
                                                Pair.of(
                                                    writeItem,
                                                    exceptions.get(
                                                        Pair.of(
                                                            writeItem.getUrn(),
                                                            writeItem.getAspectName()))))
                                        .collect(Collectors.toList());
                              }

                              // Database Upsert successfully validated results
                              log.debug(
                                  "Ingesting aspects batch to database: {}",
                                  AspectsBatch.toAbbreviatedString(changeMCPs, 2048));

                              List<UpdateAspectResult> upsertResults =
                                  exceptions
                                      .streamSuccessful(changeMCPs.stream())
                                      .map(
                                          writeItem -> {

                                            /*
                                              Latest aspect after possible in-memory mutation
                                            */
                                            final SystemAspect latestAspect =
                                                updatedLatestAspects
                                                    .getOrDefault(
                                                        writeItem.getUrn().toString(), Map.of())
                                                    .get(writeItem.getAspectName());

                                            // eliminate unneeded writes within a batch if the
                                            // latest
                                            // aspect
                                            // doesn't match this ChangeMCP
                                            if (latestAspect != null
                                                && !Objects.equals(
                                                    latestAspect.getSystemMetadata().getVersion(),
                                                    writeItem.getSystemMetadata().getVersion())) {
                                              log.debug(
                                                  "Skipping obsolete write: urn: {} aspect: {} version: {}",
                                                  writeItem.getUrn(),
                                                  writeItem.getAspectName(),
                                                  writeItem.getSystemMetadata().getVersion());
                                              return null;
                                            }

                                            /*
                                              This condition is specifically for an older conditional write ingestAspectIfNotPresent()
                                              overwrite is always true otherwise
                                            */
                                            if (overwrite
                                                || latestAspect == null
                                                || latestAspect.getDatabaseAspect().isEmpty()) {
                                              try {
                                                return Optional.ofNullable(
                                                        ingestAspectToLocalDB(
                                                            opContext,
                                                            txContext,
                                                            writeItem,
                                                            latestAspect))
                                                    .map(
                                                        optResult ->
                                                            optResult.toBuilder()
                                                                .request(writeItem)
                                                                .build())
                                                    .orElse(null);
                                              } catch (
                                                  com.linkedin.metadata.entity.validation
                                                          .AspectSizeExceededException
                                                      e) {
                                                // Convert to AspectValidationException for
                                                // uniform
                                                // batch handling
                                                AspectValidationException validationException =
                                                    AspectValidationException.forItem(
                                                        writeItem,
                                                        String.format(
                                                            "Aspect size validation failed at %s: %d bytes exceeds threshold of %d bytes",
                                                            e.getValidationPoint(),
                                                            e.getActualSize(),
                                                            e.getThreshold()),
                                                        e);

                                                // API requests: fail entire batch immediately
                                                // Kafka consumers: collect exception and continue
                                                if (opContext.getRequestContext() != null) {
                                                  ValidationExceptionCollection sizeExceptions =
                                                      ValidationExceptionCollection.newCollection();
                                                  sizeExceptions.addException(validationException);
                                                  throw new ValidationException(sizeExceptions);
                                                } else {
                                                  exceptions.addException(validationException);
                                                  return null; // Exclude from successful results
                                                }
                                              }
                                            }

                                            return null;
                                          })
                                      .filter(Objects::nonNull)
                                      .collect(Collectors.toList());

                              if (!upsertResults.isEmpty()) {
                                // commit upserts prior to retention or kafka send, if supported
                                // by impl
                                if (txContext != null) {
                                  try {
                                    txContext.commitAndContinue();
                                  } catch (RejectedExecutionException e) {
                                    log.warn(
                                        "Post-commit cache notification failed (executor terminated),"
                                            + " cache may serve stale data until TTL expiry",
                                        e);
                                    opContext
                                        .getMetricUtils()
                                        .ifPresent(
                                            metricUtils ->
                                                metricUtils.increment(
                                                    EntityServiceImpl.class,
                                                    "post_commit_notify_rejected",
                                                    1));
                                  } catch (EntityNotFoundException e) {
                                    if (e.getMessage() != null
                                        && e.getMessage().contains("No rows updated")) {
                                      log.warn(
                                          "Ignoring no rows updated condition for metadata update",
                                          e);
                                      opContext
                                          .getMetricUtils()
                                          .ifPresent(
                                              metricUtils ->
                                                  metricUtils.increment(
                                                      EntityServiceImpl.class,
                                                      "no_rows_updated",
                                                      1));
                                      return TransactionResult.rollback();
                                    }
                                    throw e;
                                  }
                                }

                                // Retention optimization and tx (legacy path when flag OFF)
                                if (!postCommitRetentionEnabled && retentionService != null) {
                                  opContext.withSpan(
                                      "retentionService",
                                      () -> {
                                        List<RetentionService.RetentionContext> retentionBatch =
                                            buildRetentionContexts(
                                                upsertResults, updatedLatestAspects);
                                        retentionService.applyRetentionWithPolicyDefaults(
                                            opContext, retentionBatch);
                                      },
                                      BATCH_SIZE_ATTR,
                                      String.valueOf(upsertResults.size()));
                                } else if (!postCommitRetentionEnabled) {
                                  log.warn("Retention service is missing!");
                                }
                              } else {
                                opContext
                                    .getMetricUtils()
                                    .ifPresent(
                                        metricUtils ->
                                            metricUtils.increment(
                                                EntityServiceImpl.class,
                                                "batch_empty_transaction",
                                                1));
                                // This includes no-op batches. i.e. patch removing non-existent
                                // items
                                log.debug("Empty transaction detected");
                                if (txContext != null) {
                                  txContext.rollback();
                                }
                              }

                              // Force flush span processing for DUE Exports
                              Optional.ofNullable(opContext.getSystemTelemetryContext())
                                  .map(SystemTelemetryContext::getUsageSpanExporter)
                                  .ifPresent(SpanProcessor::forceFlush);

                              return TransactionResult.of(
                                  IngestAspectsResult.builder()
                                      .updateAspectResults(upsertResults)
                                      .failedUpdateAspectResults(failedUpsertResults)
                                      .build());
                            }
                          },
                          inputBatch,
                          ebeanMaxTransactionRetry)
                      .stream()
                      .reduce(IngestAspectsResult.EMPTY, IngestAspectsResult::combine);
            }

            return result;
          } finally {
            // Process pending deletions whether transaction succeeded or failed.
            // For DELETE remediation, this is where actual deletion happens.
            // Must run in finally block to ensure deletions are processed even when
            // validation failures occur.
            List<Object> pendingDeletionsRaw = opContext.getPendingDeletions();
            if (!pendingDeletionsRaw.isEmpty()) {
              // Cast to AspectDeletionRequest - all objects added by validators should be of this
              // type
              List<AspectDeletionRequest> pendingDeletions =
                  pendingDeletionsRaw.stream()
                      .filter(obj -> obj instanceof AspectDeletionRequest)
                      .map(obj -> (AspectDeletionRequest) obj)
                      .collect(java.util.stream.Collectors.toList());
              if (!pendingDeletions.isEmpty()) {
                processPendingDeletions(opContext, pendingDeletions);
              }
            }

            // Always cleanup pending deletions to ensure fresh state for next operation
            opContext.clearPendingDeletions();
          }
        },
        BATCH_SIZE_ATTR,
        String.valueOf(inputBatch.getItems().size()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "ingestAspectsToLocalDB"));
  }

  /**
   * Processes pending aspect deletions collected during validation.
   *
   * <p>Called after database transaction commits to execute proper EntityService-level deletions
   * for oversized aspects. Ensures all side effects are handled: database deletion, Elasticsearch
   * index updates, graph edge cleanup, and consumer hook invocation.
   *
   * @param opContext operation context
   * @param deletions list of deletion requests collected during validation
   */
  private void processPendingDeletions(
      @Nonnull OperationContext opContext, @Nonnull List<AspectDeletionRequest> deletions) {

    for (AspectDeletionRequest deletion : deletions) {
      try {
        log.warn(
            "Executing system-level deletion for oversized aspect: urn={}, aspect={}, validationPoint={}, size={} bytes, threshold={} bytes",
            deletion.getUrn(),
            deletion.getAspectName(),
            deletion.getValidationPoint(),
            deletion.getAspectSize(),
            deletion.getThreshold());

        // Create new OperationContext with remediation deletion flag set
        // This prevents circular validation when deleting oversized aspects
        OperationContext remediationContext =
            opContext.toBuilder()
                .validationContext(
                    opContext.getValidationContext().toBuilder()
                        .isRemediationDeletion(true)
                        .build())
                .build(opContext.getSessionActorContext(), false);

        // Call proper deletion through EntityService
        // This handles all side effects: ES indices, graph edges, consumer hooks, system metadata
        this.deleteAspect(
            remediationContext,
            deletion.getUrn().toString(),
            deletion.getAspectName(),
            Map.of(),
            false);

        // Emit success metric
        if (metricUtils != null) {
          metricUtils.incrementMicrometer("aspectSizeValidation.remediationDeletion.success", 1);
        }

      } catch (Exception e) {
        log.error(
            "Failed to delete oversized aspect: urn={}, aspect={}",
            deletion.getUrn(),
            deletion.getAspectName(),
            e);

        // Emit failure metric
        if (metricUtils != null) {
          metricUtils.incrementMicrometer("aspectSizeValidation.remediationDeletion.failure", 1);
        }

        // Don't throw - continue with other deletions
        // The oversized aspect will remain in database and continue to trigger validation
      }
    }
  }

  public MCLEmitResult produceMCLAsync(@Nonnull OperationContext opContext, MetadataChangeLog mcl) {
    List<MCLEmitResult> mclResults = produceMCLAsync(opContext, List.of(mcl));
    // On failure, a Runtime exception is thrown.
    return mclResults.get(0);
  }

  @Nonnull
  public List<MCLEmitResult> produceMCLAsync(
      @Nonnull OperationContext opContext, List<MetadataChangeLog> mcls) {

    return opContext.withSpan(
        "produceMCLAsync",
        () -> {
          List<MCLEmitResult> mclResults = conditionallyProduceMCLAsync(opContext, mcls);

          // This is now a common function and called from timeseries MCLs as well as versioned
          // MCLs. postCommitSideEffects are not applicable for timeseries MCLs. Calling this
          // here also enables  side effects to be executed for messages that were published
          // even if a failure interrupts the flow.
          processPostCommitMCLSideEffects(
              opContext,
              mclResults.stream()
                  .filter(
                      result -> result.getMclFuture() != null) // Only those that actually got sent
                  .filter(
                      result -> { // only versioned MCLs
                        MetadataChangeLog mcl = result.getMetadataChangeLog();
                        return !opContext
                            .getEntityRegistry()
                            .getEntitySpec(mcl.getEntityType())
                            .getAspectSpec(mcl.getAspectName())
                            .isTimeseries();
                      })
                  .map(MCLEmitResult::getMetadataChangeLog)
                  .collect(Collectors.toList()));
          // join futures messages, capture error state
          List<MCLEmitResult> failedMCLs =
              mclResults.stream()
                  .filter(result -> result.isEmitted() && !result.isProduced())
                  .collect(Collectors.toList());

          if (!failedMCLs.isEmpty()) {
            log.error(
                "Failed to produce MCLs: {}",
                failedMCLs.stream()
                    .map(
                        result -> {
                          MetadataChangeLog mcl = result.getMetadataChangeLog();
                          return mcl.getEntityUrn() + "/" + mcl.getAspectName();
                        })
                    .collect(Collectors.toList()));
            // TODO restoreIndices?
            throw new RuntimeException("Failed to produce MCLs");
          }

          return mclResults;
        },
        BATCH_SIZE_ATTR,
        String.valueOf(mcls.size()));
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link
   * com.linkedin.mxe.MetadataAuditEvent}.
   *
   * <p>This method runs a read -> write atomically in a single transaction, this is to prevent
   * multiple IDs from being created.
   *
   * <p>Note that in general, this should not be used externally. It is currently serving upgrade
   * scripts and is as such public.
   *
   * @param urn an urn associated with the new aspect
   * @param aspectName name of the aspect being inserted
   * @param newValue value of the aspect being inserted
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param systemMetadata
   * @return the {@link RecordTemplate} representation of the written aspect object
   * @deprecated See Conditional Write ChangeType CREATE
   */
  @Nullable
  @Override
  @Deprecated
  public RecordTemplate ingestAspectIfNotPresent(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate newValue,
      @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata) {
    log.debug(
        "Invoked ingestAspectIfNotPresent with urn: {}, aspectName: {}, newValue: {}",
        urn,
        aspectName,
        newValue);

    AspectsBatchImpl aspectsBatch =
        AspectsBatchImpl.builder()
            .one(
                ChangeItemImpl.builder()
                    .urn(urn)
                    .aspectName(aspectName)
                    .recordTemplate(newValue)
                    .systemMetadata(systemMetadata)
                    .auditStamp(auditStamp)
                    .build(opContext.getAspectRetriever()),
                opContext.getRetrieverContext())
            .build(opContext);
    List<UpdateAspectResult> ingested = ingestAspects(opContext, aspectsBatch, true, false);

    return ingested.stream().findFirst().map(UpdateAspectResult::getNewValue).orElse(null);
  }

  /**
   * Wrapper around batch method for single item
   *
   * @param proposal the proposal
   * @param auditStamp an audit stamp representing the time and actor proposing the change
   * @param async a flag to control whether we commit to primary store or just write to proposal log
   *     before returning
   * @return an {@link IngestResult} containing the results
   */
  @Override
  public IngestResult ingestProposal(
      @Nonnull OperationContext opContext,
      MetadataChangeProposal proposal,
      AuditStamp auditStamp,
      final boolean async) {
    return ingestProposal(
            opContext,
            AspectsBatchImpl.builder()
                .mcps(List.of(proposal), auditStamp, opContext.getRetrieverContext())
                .build(opContext),
            async)
        .stream()
        .findFirst()
        .orElse(null);
  }

  /**
   * Ingest a new {@link MetadataChangeProposal}. Note that this method does NOT include any
   * additional aspects or do any enrichment, instead it changes only those which are provided
   * inside the metadata change proposal.
   *
   * <p>Do not use this method directly for creating new entities, as it DOES NOT create an Entity
   * Key aspect in the DB. Instead, use an Entity Client.
   *
   * @param aspectsBatch the proposals to ingest
   * @param async a flag to control whether we commit to primary store or just write to proposal log
   *     before returning
   * @return an {@link IngestResult} containing the results
   */
  @Override
  public List<IngestResult> ingestProposal(
      @Nonnull OperationContext opContext, AspectsBatch aspectsBatch, final boolean async) {
    // Apply MCP observers (pre-transaction metrics collection, external actions).
    // Only on sync path — async MCPs come back via MCE consumer with async=false.
    if (!async) {
      try {
        aspectsBatch.applyMCPObservers(aspectsBatch.getItems());
      } catch (VirtualMachineError e) {
        throw e;
      } catch (Throwable t) {
        // Outermost guard around the observer call site. Inner layers in MCPObserver.apply and
        // AspectsBatch.applyMCPObservers already isolate per-observer failures; anything that
        // leaks here is a non-observer bug (batch wiring, retriever context) — log it as such
        // rather than as an observer failure so it doesn't get triaged to the wrong owner.
        log.warn("MCP observer call site failed; ingest continuing", t);
      }
    }
    Stream<IngestResult> timeseriesIngestResults =
        ingestTimeseriesProposal(opContext, aspectsBatch, async);
    Stream<IngestResult> nonTimeseriesIngestResults =
        async
            ? ingestProposalAsync(opContext, aspectsBatch)
            : ingestProposalSync(opContext, aspectsBatch);

    return Stream.concat(nonTimeseriesIngestResults, timeseriesIngestResults)
        .collect(Collectors.toList());
  }

  /**
   * Timeseries is pass through to MCL, no MCP
   *
   * @param aspectsBatch timeseries upserts batch
   * @return returns ingest proposal result, however was never in the MCP topic
   */
  @VisibleForTesting
  Stream<IngestResult> ingestTimeseriesProposal(
      @Nonnull OperationContext opContext, AspectsBatch aspectsBatch, final boolean async) {

    List<? extends BatchItem> unsupported =
        aspectsBatch.getItems().stream()
            .filter(
                item ->
                    item.getAspectSpec() != null
                        && item.getAspectSpec().isTimeseries()
                        && item.getChangeType() != ChangeType.UPSERT)
            .collect(Collectors.toList());
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException(
          "ChangeType not supported: "
              + unsupported.stream().map(BatchItem::getChangeType).collect(Collectors.toSet()));
    }

    return opContext.withSpan(
        "ingestTimeseriesProposal",
        () -> {
          // Handle throttling
          APIThrottle.evaluate(opContext, new HashSet<>(throttleEvents.values()), true);

          // Create default non-timeseries aspects for timeseries aspects
          List<MCPItem> timeseriesKeyAspects =
              aspectsBatch.getMCPItems().stream()
                  .filter(
                      item -> item.getAspectSpec() != null && item.getAspectSpec().isTimeseries())
                  .map(
                      item ->
                          ChangeItemImpl.builder()
                              .urn(item.getUrn())
                              .aspectName(item.getEntitySpec().getKeyAspectName())
                              .changeType(ChangeType.UPSERT)
                              .entitySpec(item.getEntitySpec())
                              .aspectSpec(item.getEntitySpec().getKeyAspectSpec())
                              .auditStamp(item.getAuditStamp())
                              .systemMetadata(item.getSystemMetadata())
                              .recordTemplate(
                                  EntityApiUtils.buildKeyAspect(
                                      opContext.getEntityRegistry(), item.getUrn()))
                              .build(opContext.getAspectRetriever()))
                  .collect(Collectors.toList());

          if (async) {
            ingestProposalAsync(
                opContext,
                AspectsBatchImpl.builder()
                    .retrieverContext(aspectsBatch.getRetrieverContext())
                    .items(timeseriesKeyAspects)
                    .build(opContext));
          } else {
            ingestProposalSync(
                opContext,
                AspectsBatchImpl.builder()
                    .retrieverContext(aspectsBatch.getRetrieverContext())
                    .items(timeseriesKeyAspects)
                    .build(opContext));
          }

          // Emit timeseries MCLs
          List<Pair<MCPItem, MetadataChangeLog>> timeseriesMCLs =
              aspectsBatch.getItems().stream()
                  .filter(
                      item -> item.getAspectSpec() != null && item.getAspectSpec().isTimeseries())
                  .map(item -> (MCPItem) item)
                  .map(
                      item ->
                          Pair.of(
                              item,
                              constructMCL(
                                  item.getMetadataChangeProposal(),
                                  urnToEntityName(item.getUrn()),
                                  item.getUrn(),
                                  item.getAspectSpec().getName(),
                                  item.getAuditStamp(),
                                  item.getRecordTemplate(),
                                  item.getSystemMetadata(),
                                  null,
                                  null)))
                  .collect(Collectors.toList());

          List<Pair<MCPItem, MCLEmitResult>> timeseriesResults =
              timeseriesMCLs.stream()
                  .map(
                      pair ->
                          Pair.of(pair.getFirst(), produceMCLAsync(opContext, pair.getSecond())))
                  .collect(Collectors.toList());

          return timeseriesResults.stream()
              .filter(pair -> pair.getSecond().isEmitted())
              .map(
                  pair -> {
                    MCPItem item = pair.getFirst();
                    MCLEmitResult mclEmitResult = pair.getSecond();
                    return IngestResult.builder()
                        .urn(item.getUrn())
                        .request(item)
                        .result(
                            UpdateAspectResult.builder()
                                .urn(item.getUrn())
                                .newValue(item.getRecordTemplate())
                                .auditStamp(item.getAuditStamp())
                                .newSystemMetadata(item.getSystemMetadata())
                                .build())
                        .publishedMCL(mclEmitResult.isEmitted())
                        .processedMCL(mclEmitResult.isProcessedMCL())
                        .build();
                  });
        },
        "async",
        String.valueOf(async),
        BATCH_SIZE_ATTR,
        String.valueOf(aspectsBatch.getItems().size()));
  }

  /**
   * For async ingestion of non-timeseries, any change type
   *
   * @param aspectsBatch non-timeseries ingest aspects
   * @return produced items to the MCP topic
   */
  @VisibleForTesting
  Stream<IngestResult> ingestProposalAsync(OperationContext opContext, AspectsBatch aspectsBatch) {
    return opContext.withSpan(
        "ingestProposalAsync",
        () -> {
          List<? extends MCPItem> nonTimeseries =
              aspectsBatch.getMCPItems().stream()
                  .filter(
                      item -> item.getAspectSpec() == null || !item.getAspectSpec().isTimeseries())
                  .collect(Collectors.toList());

          List<Future<?>> futures =
              nonTimeseries.stream()
                  .map(
                      item -> {
                        // When async is turned on, we write to proposal log and return without
                        // waiting
                        return producer.produceMetadataChangeProposal(
                            opContext, item.getUrn(), item);
                      })
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());

          futures.forEach(
              f -> {
                try {
                  f.get();
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });

          return nonTimeseries.stream()
              .map(
                  item ->
                      IngestResult.<MCPItem>builder()
                          .urn(item.getUrn())
                          .request(item)
                          .publishedMCP(true)
                          .build());
        },
        BATCH_SIZE_ATTR,
        String.valueOf(aspectsBatch.getItems().size()));
  }

  @VisibleForTesting
  Stream<IngestResult> ingestProposalSync(
      @Nonnull OperationContext opContext, AspectsBatch aspectsBatch) {

    return opContext.withSpan(
        "ingestProposalSync",
        () -> {
          AspectsBatchImpl nonTimeseries =
              AspectsBatchImpl.builder()
                  .retrieverContext(aspectsBatch.getRetrieverContext())
                  .items(
                      aspectsBatch.getItems().stream()
                          .filter(item -> !item.getAspectSpec().isTimeseries())
                          .collect(Collectors.toList()))
                  .build(opContext);

          List<? extends MCPItem> unsupported =
              nonTimeseries.getMCPItems().stream()
                  .filter(
                      item ->
                          !MCPItem.isValidChangeType(item.getChangeType(), item.getAspectSpec()))
                  .collect(Collectors.toList());
          if (!unsupported.isEmpty()) {
            throw new UnsupportedOperationException(
                "ChangeType not supported: "
                    + unsupported.stream()
                        .map(item -> item.getChangeType())
                        .collect(Collectors.toSet()));
          }

          List<UpdateAspectResult> upsertResults =
              ingestAspects(opContext, nonTimeseries, true, true);

          return upsertResults.stream()
              .map(
                  result -> {
                    ChangeMCP item = result.getRequest();

                    return IngestResult.builder()
                        .urn(item.getUrn())
                        .request(item)
                        .result(result)
                        .publishedMCL(result.getMclFuture() != null)
                        .sqlCommitted(true)
                        .isUpdate(result.getOldValue() != null)
                        .build();
                  });
        },
        BATCH_SIZE_ATTR,
        String.valueOf(aspectsBatch.getItems().size()));
  }

  @Override
  public String batchApplyRetention(
      @Nonnull OperationContext opContext,
      Integer start,
      Integer count,
      Integer attemptWithVersion,
      String aspectName,
      String urn) {
    BulkApplyRetentionArgs args = new BulkApplyRetentionArgs();
    if (start == null) {
      start = 0;
    }
    args.start = start;
    if (count == null) {
      count = 100;
    }
    args.count = count;
    if (attemptWithVersion == null) {
      attemptWithVersion = 21;
    }
    args.attemptWithVersion = attemptWithVersion;
    args.aspectName = aspectName;
    args.urn = urn;
    args.opContext = opContext;
    BulkApplyRetentionResult result = retentionService.batchApplyRetentionEntities(args);
    return result.toString();
  }

  boolean preprocessEvent(
      @Nonnull OperationContext opContext, MetadataChangeLog metadataChangeLog) {
    // Deletes cannot rely on System Metadata being passed through so can't always be determined by
    // system metadata,
    // for all other types of events should use system metadata rather than the boolean param.
    boolean requiresSyncSearchIndexUpdate =
        SyncSearchIndexUtils.requiresSyncSearchIndexUpdate(
            preProcessHooks, opContext, metadataChangeLog.getSystemMetadata());
    boolean syncIndexUpdateHeader =
        metadataChangeLog.getHeaders() != null
            && metadataChangeLog
                .getHeaders()
                .getOrDefault(SYNC_INDEX_UPDATE_HEADER_NAME, "false")
                .equalsIgnoreCase(Boolean.toString(true));
    boolean syncGated = requiresSyncSearchIndexUpdate || syncIndexUpdateHeader;

    if (updateIndicesService != null && syncGated) {
      updateIndicesService.handleChangeEvent(opContext, metadataChangeLog);
      // Drop stale snapshots as soon as secondary storage reflects the write. Batch invalidation
      // at the end of ingestAspects remains as a backstop, but must not be the only path — reads
      // between preprocess and batch completion could otherwise HIT an ACTIVE membership snapshot
      // missing a just-written edge (e.g. corpGroup INCOMING members after addGroupMembers).
      invalidateEntityGraphCacheOnSyncWrite(opContext, metadataChangeLog);
    }
    return syncGated && updateIndicesService != null;
  }

  private void invalidateEntityGraphCacheOnSyncWrite(
      @Nonnull OperationContext opContext, @Nonnull MetadataChangeLog metadataChangeLog) {
    SyncGraphInvalidationBatch batch =
        EntityGraphSyncInvalidationSupport.fromSyncMetadataChangeLog(
            opContext, preProcessHooks, metadataChangeLog);
    if (!batch.isEmpty()) {
      opContext.getEntityGraphCache().invalidateOnSyncBatch(batch);
    }
  }

  @Override
  public Integer getCountAspect(
      @Nonnull OperationContext opContext, @Nonnull String aspectName, @Nullable String urnLike) {
    return aspectDao.countAspect(opContext, aspectName, urnLike);
  }

  @Override
  public Integer countAspect(
      @Nonnull OperationContext opContext,
      @Nonnull RestoreIndicesArgs args,
      @Nonnull Consumer<String> logger) {
    logger.accept(String.format("Args are %s", args));
    return aspectDao.countAspect(opContext, args);
  }

  @Nonnull
  @Override
  public List<RestoreIndicesResult> restoreIndices(
      @Nonnull OperationContext opContext,
      @Nonnull RestoreIndicesArgs args,
      @Nonnull Consumer<String> logger) {

    logger.accept(String.format("Args are %s", args));
    logger.accept(
        String.format(
            "Reading rows %s through %s (0 == infinite) in batches of %s from the aspects table started.",
            args.start, args.start + args.limit, args.batchSize));

    long startTime = System.currentTimeMillis();

    return aspectDao.streamAspectBatches(
        opContext,
        args,
        stream ->
            stream
                .partition(args.batchSize)
                .map(
                    batch -> {
                      long timeSqlQueryMs = System.currentTimeMillis() - startTime;

                      try {
                        List<SystemAspect> systemAspects =
                            EntityUtils.toSystemAspectFromEbeanAspects(
                                opContext,
                                opContext.getRetrieverContext(),
                                batch.collect(Collectors.toList()));

                        RestoreIndicesResult result =
                            restoreIndices(
                                opContext, systemAspects, logger, args.createDefaultAspects());
                        result.timeSqlQueryMs = timeSqlQueryMs;

                        logger.accept("Batch completed.");
                        try {
                          TimeUnit.MILLISECONDS.sleep(args.batchDelayMs);
                        } catch (InterruptedException e) {
                          throw new RuntimeException(
                              "Thread interrupted while sleeping after successful batch migration.");
                        }

                        return result;
                      } catch (Exception e) {
                        log.error("Error processing aspect for restore indices.", e);
                        return null;
                      }
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
  }

  @Nonnull
  @Override
  public List<RestoreIndicesResult> restoreIndices(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> inputAspectNames,
      @Nullable Integer inputBatchSize,
      boolean createDefaultAspects)
      throws RemoteInvocationException, URISyntaxException {
    int batchSize = inputBatchSize != null ? inputBatchSize : 100;

    List<RestoreIndicesResult> results = new LinkedList<>();

    for (List<Urn> urnBatch : Iterables.partition(urns, batchSize)) {

      Map<String, Set<Urn>> byEntityType =
          urnBatch.stream().collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));

      for (Map.Entry<String, Set<Urn>> entityBatch : byEntityType.entrySet()) {
        Set<String> aspectNames =
            inputAspectNames != null
                ? inputAspectNames
                : opContext.getEntityAspectNames(entityBatch.getKey());

        long startTime = System.currentTimeMillis();
        List<SystemAspect> systemAspects =
            EntityUtils.toSystemAspects(
                opContext,
                opContext.getRetrieverContext(),
                getLatestAspect(opContext, entityBatch.getValue(), aspectNames, false).values());
        long timeSqlQueryMs = System.currentTimeMillis() - startTime;

        RestoreIndicesResult result =
            restoreIndices(opContext, systemAspects, s -> {}, createDefaultAspects);
        result.timeSqlQueryMs = timeSqlQueryMs;
        results.add(result);
      }
    }

    return results;
  }

  /**
   * Interface designed to maintain backwards compatibility
   *
   * @param systemAspects
   * @param logger
   * @return
   */
  private RestoreIndicesResult restoreIndices(
      @Nonnull OperationContext opContext,
      List<SystemAspect> systemAspects,
      @Nonnull Consumer<String> logger,
      boolean createDefaultAspects) {
    RestoreIndicesResult result = new RestoreIndicesResult();
    long startTime = System.currentTimeMillis();
    int ignored = 0;
    int rowsMigrated = 0;
    long defaultAspectsCreated = 0;

    LinkedList<Future<?>> futures = new LinkedList<>();

    for (SystemAspect aspect : systemAspects) {
      // 1. Extract an Entity type from the entity Urn
      result.timeGetRowMs = System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      Urn urn;
      try {
        urn = aspect.getUrn();
        result.lastUrn = urn.toString();
      } catch (Exception e) {
        logger.accept(
            String.format(
                "Failed to bind Urn with value %s into Urn object: %s. Ignoring row.",
                aspect.getUrn(), e));
        ignored = ignored + 1;
        continue;
      }
      result.timeUrnMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // 2. Verify that the entity associated with the aspect is found in the registry.
      final String entityName = urn.getEntityType();
      final EntitySpec entitySpec;
      try {
        entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
      } catch (Exception e) {
        logger.accept(
            String.format(
                "Failed to find entity with name %s in Entity Registry: %s. Ignoring row.",
                entityName, e));
        ignored = ignored + 1;
        continue;
      }
      result.timeEntityRegistryCheckMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      final String aspectName = aspect.getAspectName();
      result.lastAspect = aspectName;

      // 3. Verify that the aspect is a valid aspect associated with the entity
      AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
      if (aspectSpec == null) {
        logger.accept(
            String.format(
                "Failed to find aspect with name %s associated with entity named %s",
                aspectName, entityName));
        ignored = ignored + 1;
        continue;
      }
      result.aspectCheckMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // 4. Create record from json aspect
      final RecordTemplate aspectRecord;
      try {
        aspectRecord = aspect.getRecordTemplate();
      } catch (Exception e) {
        logger.accept(
            String.format(
                "Failed to deserialize for entity %s, aspect %s: %s. Ignoring row.",
                entityName, aspectName, e));
        ignored = ignored + 1;
        continue;
      }
      result.createRecordMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // Force indexing to skip diff mode and fix error states
      SystemMetadata latestSystemMetadata = aspect.getSystemMetadata();

      StringMap properties =
          latestSystemMetadata.getProperties() != null
              ? latestSystemMetadata.getProperties()
              : new StringMap();
      properties.put(FORCE_INDEXING_KEY, Boolean.TRUE.toString());
      latestSystemMetadata.setProperties(properties);

      // 5. Produce MAE events for the aspect record
      AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();
      futures.add(
          alwaysProduceMCLAsync(
                  opContext,
                  urn,
                  entityName,
                  aspectName,
                  aspectSpec,
                  null,
                  aspectRecord,
                  null,
                  latestSystemMetadata,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());

      // 6. Ensure default aspects are in existence in SQL
      if (createDefaultAspects) {
        List<MCPItem> keyAspect =
            List.of(
                ChangeItemImpl.builder()
                    .urn(urn)
                    .aspectName(entitySpec.getKeyAspectName())
                    .changeType(ChangeType.UPSERT)
                    .entitySpec(entitySpec)
                    .aspectSpec(entitySpec.getKeyAspectSpec())
                    .auditStamp(auditStamp)
                    .systemMetadata(latestSystemMetadata)
                    .recordTemplate(
                        EntityApiUtils.buildKeyAspect(opContext.getEntityRegistry(), urn))
                    .build(opContext.getAspectRetriever()));
        Stream<IngestResult> defaultAspectsResult =
            ingestProposalSync(
                opContext,
                AspectsBatchImpl.builder()
                    .retrieverContext(opContext.getRetrieverContext())
                    .items(keyAspect)
                    .build(opContext));
        defaultAspectsCreated += defaultAspectsResult.count();
      }

      result.sendMessageMs += System.currentTimeMillis() - startTime;

      rowsMigrated++;
    }
    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    result.ignored = ignored;
    result.rowsMigrated = rowsMigrated;
    result.defaultAspectsCreated = defaultAspectsCreated;

    producer.flush();

    return result;
  }

  @Override
  public void flushEventProducer() {
    producer.flush();
  }

  /**
   * Lists the entity URNs found in storage.
   *
   * @param entityName the name associated with the entity
   * @param start the start offset
   * @param count the count
   */
  @Override
  public ListUrnsResult listUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      final int start,
      @Nullable Integer count) {
    log.debug(
        "Invoked listUrns with entityName: {}, start: {}, count: {}", entityName, start, count);

    // If a keyAspect exists, the entity exists.
    final String keyAspectName =
        opContext.getEntityRegistry().getEntitySpec(entityName).getKeyAspectSpec().getName();
    final ListResult<String> keyAspectList =
        aspectDao.listUrns(opContext, entityName, keyAspectName, start, count);

    final ListUrnsResult result = new ListUrnsResult();
    result.setStart(start);
    result.setCount(keyAspectList.getValues().size());
    result.setTotal(keyAspectList.getTotalCount());

    // Extract urns
    final UrnArray entityUrns = new UrnArray();
    for (String urn : keyAspectList.getValues()) {
      try {
        entityUrns.add(Urn.createFromString(urn));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(
            String.format("Failed to convert urn %s found in db to Urn object.", urn), e);
      }
    }
    result.setEntities(entityUrns);
    return result;
  }

  /**
   * Default implementations. Subclasses should feel free to override if it's more efficient to do
   * so.
   */
  @Override
  public Entity getEntity(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {
    return getEntities(opContext, Collections.singleton(urn), aspectNames, alwaysIncludeKeyAspect)
        .values()
        .stream()
        .findFirst()
        .orElse(null);
  }

  /**
   * Deprecated! Use getEntitiesV2 instead.
   *
   * <p>Retrieves multiple entities.
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Deprecated
  @Override
  public Map<Urn, Entity> getEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {
    log.debug("Invoked getEntities with urns {}, aspects {}", urns, aspectNames);
    if (urns.isEmpty()) {
      return Collections.emptyMap();
    }
    return getSnapshotUnions(opContext, urns, aspectNames, alwaysIncludeKeyAspect)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey, entry -> EntityUtils.toEntity(entry.getValue())));
  }

  @Override
  public Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    return opContext.withSpan(
        "alwaysProduceMCLAsync",
        () -> {
          boolean preprocessed = preprocessEvent(opContext, metadataChangeLog);
          Future<?> future =
              producer.produceMetadataChangeLog(opContext, urn, aspectSpec, metadataChangeLog);
          // TODO Is this trace event ID correct when called in CDC mode?
          Span.current()
              .addEvent(UPDATE_ASPECT_EVENT, mapEventAttributes(metadataChangeLog, opContext));
          return Pair.of(future, preprocessed);
        });
  }

  @VisibleForTesting
  Attributes mapEventAttributes(MetadataChangeLog metadataChangeLog, OperationContext opContext) {
    AttributesBuilder attributesBuilder = Attributes.builder();

    Optional.ofNullable(metadataChangeLog.getSystemMetadata())
        .map(SystemMetadata::getProperties)
        .ifPresent(
            properties -> {
              Optional.ofNullable(properties.get(EVENT_SOURCE_KEY))
                  .ifPresent(eventSource -> attributesBuilder.put(EVENT_SOURCE, eventSource));
              Optional.ofNullable(properties.get(SOURCE_IP_KEY))
                  .ifPresent(sourceIP -> attributesBuilder.put(SOURCE_IP, sourceIP));
              Optional.ofNullable(properties.get(TELEMETRY_TRACE_KEY))
                  .ifPresent(
                      eventSource -> attributesBuilder.put(TELEMETRY_TRACE_ID_ATTR, eventSource));
            });

    mapAspectToUsageEvent(attributesBuilder, metadataChangeLog);

    RequestContext requestContext = opContext.getRequestContext();
    if (requestContext != null) {
      attributesBuilder.put(USER_AGENT_ATTR, requestContext.getUserAgent());
    }

    String actor = metadataChangeLog.getCreated().getActor().toString();
    attributesBuilder.put(USER_ID_ATTR, actor);

    attributesBuilder.put(
        ENTITY_URN_ATTR,
        EntityKeyUtils.getUrnFromEvent(metadataChangeLog, opContext.getEntityRegistry())
            .toString());

    attributesBuilder.put(ENTITY_TYPE_ATTR, metadataChangeLog.getEntityType());

    attributesBuilder.put(ASPECT_NAME_ATTR, metadataChangeLog.getAspectName());

    return attributesBuilder.build();
  }

  /**
   * Right now this is limited to target use cases so the logic is simplified, might make sense to
   * make this entity registry & model driven
   */
  @VisibleForTesting
  void mapAspectToUsageEvent(
      AttributesBuilder attributesBuilder, MetadataChangeLog metadataChangeLog) {
    String aspectName =
        Optional.ofNullable(metadataChangeLog.getAspectName()).orElse(StringUtils.EMPTY);
    ChangeType changeType = metadataChangeLog.getChangeType();
    String eventType;
    if (isCreateOrUpdate(changeType)) {
      switch (aspectName) {
        case ACCESS_TOKEN_KEY_ASPECT_NAME:
          // TODO: Remove case ACCESS_TOKEN_INFO_NAME:
          eventType = DataHubUsageEventType.CREATE_ACCESS_TOKEN_EVENT.getType();
          break;
        case INGESTION_SOURCE_KEY_ASPECT_NAME:
          eventType = DataHubUsageEventType.CREATE_INGESTION_SOURCE_EVENT.getType();
          break;
        case INGESTION_INFO_ASPECT_NAME:
          eventType = DataHubUsageEventType.UPDATE_INGESTION_SOURCE_EVENT.getType();
          break;
        case DATAHUB_POLICY_KEY_ASPECT_NAME:
          eventType = DataHubUsageEventType.CREATE_POLICY_EVENT.getType();
          break;
        case DATAHUB_POLICY_INFO_ASPECT_NAME:
          eventType = DataHubUsageEventType.UPDATE_POLICY_EVENT.getType();
          break;
        case CORP_USER_KEY_ASPECT_NAME:
          eventType = DataHubUsageEventType.CREATE_USER_EVENT.getType();
          break;
        case CORP_USER_CREDENTIALS_ASPECT_NAME:
        case CORP_USER_EDITABLE_INFO_ASPECT_NAME:
        case CORP_USER_INFO_ASPECT_NAME:
        case CORP_USER_SETTINGS_ASPECT_NAME:
        case CORP_USER_STATUS_ASPECT_NAME:
        case GROUP_MEMBERSHIP_ASPECT_NAME:
        case ROLE_MEMBERSHIP_ASPECT_NAME:
        case NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME:
          eventType = DataHubUsageEventType.UPDATE_USER_EVENT.getType();
          break;
        default:
          eventType = DataHubUsageEventType.UPDATE_ASPECT_EVENT.getType();
      }
    } else if (ChangeType.DELETE.equals(changeType)) {
      switch (aspectName) {
        case ACCESS_TOKEN_KEY_ASPECT_NAME:
          eventType = DataHubUsageEventType.REVOKE_ACCESS_TOKEN_EVENT.getType();
          break;
        case DATAHUB_POLICY_KEY_ASPECT_NAME:
          eventType = DataHubUsageEventType.DELETE_POLICY_EVENT.getType();
          break;
        default:
          eventType = DataHubUsageEventType.DELETE_ENTITY_EVENT.getType();
      }
    } else {
      eventType = DataHubUsageEventType.ENTITY_EVENT.getType();
    }
    attributesBuilder.put(EVENT_TYPE_ATTR, eventType);
  }

  private boolean isCreateOrUpdate(ChangeType changeType) {
    return ChangeType.UPSERT.equals(changeType)
        || ChangeType.CREATE.equals(changeType)
        || ChangeType.CREATE_ENTITY.equals(changeType)
        || ChangeType.PATCH.equals(changeType)
        || ChangeType.UPDATE.equals(changeType);
  }

  public List<MCLEmitResult> conditionallyProduceMCLAsync(
      @Nonnull OperationContext opContext, List<MetadataChangeLog> mcls) {
    return mcls.stream()
        .map(
            mcl -> {
              Urn entityUrn = mcl.getEntityUrn();
              AspectSpec aspectSpec =
                  opContext
                      .getEntityRegistry()
                      .getEntitySpec(mcl.getEntityType())
                      .getAspectSpec(mcl.getAspectName());
              return conditionallyProduceMCLAsync(opContext, aspectSpec, mcl);
            })
        .collect(Collectors.toList());
  }

  public MCLEmitResult conditionallyProduceMCLAsync(
      @Nonnull OperationContext opContext,
      AspectSpec aspectSpec,
      MetadataChangeLog metadataChangeLog) {
    SystemMetadata newSystemMetadata = metadataChangeLog.getSystemMetadata();
    Urn entityUrn = metadataChangeLog.getEntityUrn();

    boolean isNoOp = isNoOp(aspectSpec, metadataChangeLog);
    if (!isNoOp || alwaysEmitChangeLog || shouldAspectEmitChangeLog(aspectSpec)) {
      log.info("Producing MCL for ingested aspect {}, urn {}", aspectSpec.getName(), entityUrn);

      log.debug("Serialized MCL event: {}", metadataChangeLog);
      Pair<Future<?>, Boolean> emissionStatus =
          alwaysProduceMCLAsync(opContext, entityUrn, aspectSpec, metadataChangeLog);

      // for tracing propagate properties to system meta
      if (newSystemMetadata != null && metadataChangeLog.getSystemMetadata().hasProperties()) {
        if (!newSystemMetadata.hasProperties()) {
          newSystemMetadata.setProperties(
              metadataChangeLog.getSystemMetadata().getProperties(), SetMode.IGNORE_NULL);
        } else {
          newSystemMetadata
              .getProperties()
              .putAll(metadataChangeLog.getSystemMetadata().getProperties());
        }
      }

      return MCLEmitResult.builder()
          .metadataChangeLog(metadataChangeLog)
          .mclFuture(emissionStatus.getFirst())
          .processedMCL(emissionStatus.getSecond())
          .emitted(emissionStatus.getFirst() != null)
          .build();
    } else {
      log.debug(
          "Skipped producing MCL for ingested aspect {}, urn {}. Aspect has not changed.",
          aspectSpec.getName(),
          entityUrn);
      return MCLEmitResult.builder().metadataChangeLog(metadataChangeLog).emitted(false).build();
    }
  }

  private static boolean isNoOp(AspectSpec aspectSpec, MetadataChangeLog mcl) {

    RecordTemplate oldAspect =
        mcl.getPreviousAspectValue() != null
            ? GenericRecordUtils.deserializeAspect(
                mcl.getPreviousAspectValue().getValue(),
                mcl.getPreviousAspectValue().getContentType(),
                aspectSpec)
            : null;
    RecordTemplate newAspect =
        mcl.getAspect() != null
            ? GenericRecordUtils.deserializeAspect(
                mcl.getAspect().getValue(), mcl.getAspect().getContentType(), aspectSpec)
            : null;
    return SystemMetadataUtils.isNoOp(mcl.getSystemMetadata())
        || ValidationApiUtils.normalizedEqual(oldAspect, newAspect);
  }

  public void produceFailedMCPs(
      @Nonnull OperationContext opContext, @Nonnull IngestAspectsResult ingestAspectsResult) {

    if (!ingestAspectsResult.getFailedUpdateAspectResults().isEmpty()) {
      Span currentSpan = Span.current();
      currentSpan.recordException(
          new IllegalStateException("Batch contains failed aspect validations."));
      currentSpan.setStatus(StatusCode.ERROR, "Batch contains failed aspect validations.");
      currentSpan.setAttribute(MetricUtils.ERROR_TYPE, IllegalStateException.class.getName());

      List<Future<?>> futures =
          ingestAspectsResult.getFailedUpdateAspectResults().stream()
              .map(
                  failedItem ->
                      producer.produceFailedMetadataChangeProposalAsync(
                          opContext, failedItem.getFirst(), new HashSet<>(failedItem.getSecond())))
              .collect(Collectors.toList());

      futures.forEach(
          f -> {
            try {
              f.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  @Override
  public void ingestEntities(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Entity> entities,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata) {
    log.debug("Invoked ingestEntities with entities {}, audit stamp {}", entities, auditStamp);
    Streams.zip(
            entities.stream(),
            systemMetadata.stream(),
            (a, b) -> new Pair<Entity, SystemMetadata>(a, b))
        .forEach(pair -> ingestEntity(opContext, pair.getFirst(), auditStamp, pair.getSecond()));
  }

  @Override
  public SystemMetadata ingestEntity(
      @Nonnull OperationContext opContext, Entity entity, AuditStamp auditStamp) {
    SystemMetadata generatedSystemMetadata = createDefaultSystemMetadata();
    ingestEntity(opContext, entity, auditStamp, generatedSystemMetadata);
    return generatedSystemMetadata;
  }

  @Override
  public void ingestEntity(
      @Nonnull OperationContext opContext,
      @Nonnull Entity entity,
      @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata) {
    log.debug(
        "Invoked ingestEntity with entity {}, audit stamp {} systemMetadata {}",
        entity,
        auditStamp,
        systemMetadata.toString());
    ingestSnapshotUnion(opContext, entity.getValue(), auditStamp, systemMetadata);
  }

  @Nonnull
  protected Map<Urn, Snapshot> getSnapshotUnions(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {
    return getSnapshotRecords(opContext, urns, aspectNames, alwaysIncludeKeyAspect)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> EntityUtils.toSnapshotUnion(entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, RecordTemplate> getSnapshotRecords(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {
    return getLatestAspectUnions(opContext, urns, aspectNames, alwaysIncludeKeyAspect)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> toSnapshotRecord(opContext, entry.getKey(), entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, List<UnionTemplate>> getLatestAspectUnions(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean alwaysIncludeKeyAspect) {
    return this.getLatestAspects(opContext, urns, aspectNames, alwaysIncludeKeyAspect)
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(aspectRecord -> toAspectUnion(opContext, entry.getKey(), aspectRecord))
                        .collect(Collectors.toList())));
  }

  private void ingestSnapshotUnion(
      @Nonnull OperationContext opContext,
      @Nonnull final Snapshot snapshotUnion,
      @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    final RecordTemplate snapshotRecord =
        RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = com.datahub.util.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final List<Pair<String, RecordTemplate>> aspectRecordsToIngest =
        NewModelUtils.getAspectsFromSnapshot(snapshotRecord);

    log.debug("Ingesting entity urn {} with system metadata {}", urn, systemMetadata.toString());

    AspectsBatchImpl aspectsBatch =
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                aspectRecordsToIngest.stream()
                    .map(
                        pair ->
                            ChangeItemImpl.builder()
                                .urn(urn)
                                .aspectName(pair.getKey())
                                .recordTemplate(pair.getValue())
                                .auditStamp(auditStamp)
                                .systemMetadata(systemMetadata)
                                .build(opContext.getAspectRetriever()))
                    .collect(Collectors.toList()))
            .build(opContext);

    ingestAspects(opContext, aspectsBatch, true, true);
  }

  protected RecordTemplate toSnapshotRecord(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final List<UnionTemplate> aspectUnionTemplates) {
    final String entityName = urnToEntityName(urn);
    final EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);
    return com.datahub.util.ModelUtils.newSnapshot(
        getDataTemplateClassFromSchema(entitySpec.getSnapshotSchema(), RecordTemplate.class),
        urn,
        aspectUnionTemplates);
  }

  protected UnionTemplate toAspectUnion(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final RecordTemplate aspectRecord) {
    final EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(urnToEntityName(urn));
    final TyperefDataSchema aspectSchema = entitySpec.getAspectTyperefSchema();
    if (aspectSchema == null) {
      throw new RuntimeException(
          String.format(
              "Aspect schema for %s is null: v4 operation is not supported on this entity registry",
              entitySpec.getName()));
    }
    return com.datahub.util.ModelUtils.newAspectUnion(
        getDataTemplateClassFromSchema(entitySpec.getAspectTyperefSchema(), UnionTemplate.class),
        aspectRecord);
  }

  @Override
  public void setRetentionService(RetentionService<ChangeItemImpl> retentionService) {
    this.retentionService = retentionService;
  }

  /**
   * Not part of the {@link EntityService} interface (avoids a circular dependency between
   * metadata-service/services and metadata-io, where {@link RetentionBuffer} lives). Wired by
   * {@code EntityServiceFactory} via {@code ObjectProvider.getIfAvailable()}; a null (no buffer
   * bean) is normalized to {@link RetentionBuffer#NO_OP} so callers never see null.
   */
  public void setRetentionBuffer(@Nullable RetentionBuffer retentionBuffer) {
    this.retentionBuffer = retentionBuffer != null ? retentionBuffer : RetentionBuffer.NO_OP;
  }

  @Override
  public void setWritable(boolean canWrite) {
    log.debug("Setting writable to {}", canWrite);
    aspectDao.setWritable(canWrite);
  }

  @Override
  public RollbackRunResult rollbackRun(
      @Nonnull OperationContext opContext,
      List<AspectRowSummary> aspectRows,
      String runId,
      boolean hardDelete) {
    return rollbackWithConditions(
        opContext, aspectRows, Collections.singletonMap("runId", runId), hardDelete, false);
  }

  @Override
  public RollbackRunResult rollbackWithConditions(
      @Nonnull OperationContext opContext,
      List<AspectRowSummary> aspectRows,
      Map<String, String> conditions,
      boolean hardDelete,
      boolean preProcessHooks) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    List<RollbackResult> removedAspectResults = new ArrayList<>();
    AtomicInteger rowsDeletedFromEntityDeletion = new AtomicInteger(0);

    List<Future<?>> futures =
        aspectRows.stream()
            .map(
                aspectToRemove -> {
                  RollbackResult result =
                      deleteAspectWithoutMCL(
                          opContext,
                          aspectToRemove.getUrn(),
                          aspectToRemove.getAspectName(),
                          conditions,
                          hardDelete);
                  if (result != null) {
                    Optional<AspectSpec> aspectSpec =
                        opContext
                            .getEntityRegistryContext()
                            .getAspectSpec(result.entityName, result.aspectName);
                    if (!aspectSpec.isPresent()) {
                      log.error(
                          "Issue while rolling back: unknown aspect {} for entity {}",
                          result.entityName,
                          result.aspectName);
                      return null;
                    }

                    rowsDeletedFromEntityDeletion.addAndGet(result.additionalRowsAffected);
                    removedAspects.add(aspectToRemove);
                    removedAspectResults.add(result);
                    if (!cdcModeChangeLog) {
                      return alwaysProduceMCLAsync(
                              opContext,
                              result.getUrn(),
                              result.getEntityName(),
                              result.getAspectName(),
                              aspectSpec.get(),
                              result.getOldValue(),
                              result.getNewValue(),
                              result.getOldSystemMetadata(),
                              result.getNewSystemMetadata(),
                              // TODO: use properly attributed audit stamp.
                              createSystemAuditStamp(),
                              result.getChangeType())
                          .getFirst();
                    } else {
                      return null; // CDC Consumer will emit the MCL. Nothing to wait on here.
                    }
                  }

                  return null;
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    futures.forEach(
        f -> {
          try {
            f.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });

    return new RollbackRunResult(
        removedAspects, rowsDeletedFromEntityDeletion.get(), removedAspectResults);
  }

  @Override
  public RollbackRunResult deleteUrn(@Nonnull OperationContext opContext, Urn urn) {
    // No write gate is taken here. It is acquired inside deleteAspectWithoutMCL (the shared
    // DB-delete primitive), which scopes the lock to just the DB transaction and keeps the async
    // MCL
    // emission below (a Kafka round-trip) OUT from under the lock — previously this whole body ran
    // under the gate, holding a Hazelcast lock across the Kafka wait.
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    List<RollbackResult> removedAspectResults = new ArrayList<>();
    Integer rowsDeletedFromEntityDeletion = 0;

    final EntitySpec spec =
        opContext.getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    String keyAspectName = opContext.getKeyAspectName(urn);

    RollbackResult result =
        deleteAspectWithoutMCL(
            opContext, urn.toString(), keyAspectName, Collections.emptyMap(), true);

    if (result != null) {
      AspectRowSummary summary = new AspectRowSummary();
      summary.setUrn(urn.toString());
      summary.setKeyAspect(true);
      summary.setAspectName(keyAspectName);
      summary.setVersion(0);
      long aspectTime =
          result.getOldSystemMetadata() != null
                  && result.getOldSystemMetadata().getAspectCreated() != null
              ? result.getOldSystemMetadata().getAspectCreated().getTime()
              : System.currentTimeMillis();
      summary.setTimestamp(aspectTime);

      rowsDeletedFromEntityDeletion = result.additionalRowsAffected;
      removedAspects.add(summary);
      removedAspectResults.add(result);

      Future<?> future = null;
      if (!cdcModeChangeLog) {
        future =
            alwaysProduceMCLAsync(
                    opContext,
                    result.getUrn(),
                    result.getEntityName(),
                    result.getAspectName(),
                    keySpec,
                    result.getOldValue(),
                    result.getNewValue(),
                    result.getOldSystemMetadata(),
                    result.getNewSystemMetadata(),
                    opContext.getAuditStamp(),
                    result.getChangeType())
                .getFirst();
      }

      if (future != null) {
        try {
          future.get();
          Optional.ofNullable(opContext.getSystemTelemetryContext())
              .map(SystemTelemetryContext::getUsageSpanExporter)
              .ifPresent(SpanProcessor::forceFlush);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return new RollbackRunResult(
        removedAspects, rowsDeletedFromEntityDeletion, removedAspectResults);
  }

  @Override
  public Set<Urn> exists(
      @Nonnull OperationContext opContext,
      @Nonnull final Collection<Urn> urns,
      @Nullable String aspectName,
      boolean includeSoftDeleted,
      boolean forUpdate) {
    final Set<EntityAspectIdentifier> dbKeys =
        urns.stream()
            .map(
                urn ->
                    new EntityAspectIdentifier(
                        urn.toString(),
                        aspectName == null
                            ? opContext
                                .getEntityRegistry()
                                .getEntitySpec(urn.getEntityType())
                                .getKeyAspectSpec()
                                .getName()
                            : aspectName,
                        ASPECT_LATEST_VERSION))
            .collect(Collectors.toSet());
    final Map<EntityAspectIdentifier, EntityAspect> aspects =
        aspectDao.batchGet(opContext, dbKeys, forUpdate);
    final Set<String> existingUrnStrings =
        aspects.values().stream()
            .filter(Objects::nonNull)
            .map(EntityAspect::getUrn)
            .collect(Collectors.toSet());

    Set<Urn> existing =
        urns.stream()
            .filter(urn -> existingUrnStrings.contains(urn.toString()))
            .collect(Collectors.toSet());

    if (includeSoftDeleted) {
      return existing;
    } else {
      // Additionally exclude status.removed == true
      Map<Urn, List<RecordTemplate>> statusResult =
          getLatestAspects(opContext, existing, Set.of(STATUS_ASPECT_NAME), false);
      return existing.stream()
          .filter(
              urn ->
                  // key aspect is always returned, make sure to only consider the status aspect
                  statusResult.getOrDefault(urn, List.of()).stream()
                      .filter(
                          aspect -> STATUS_ASPECT_NAME.equalsIgnoreCase(aspect.schema().getName()))
                      .noneMatch(aspect -> ((Status) aspect).isRemoved()))
          .collect(Collectors.toSet());
    }
  }

  /** Does not emit MCL */
  @VisibleForTesting
  @Nullable
  RollbackResult deleteAspectWithoutMCL(
      @Nonnull OperationContext opContext,
      String urn,
      String aspectName,
      @Nonnull Map<String, String> conditions,
      boolean hardDelete) {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    // Validate pre-conditions before running queries
    Urn entityUrn = UrnUtils.getUrn(urn);

    // Runs simple validations
    MCPItem deleteItem =
        DeleteItemImpl.builder()
            .urn(entityUrn)
            .aspectName(aspectName)
            .auditStamp(auditStamp)
            .build(opContext.getAspectRetriever());

    // Delete validation hooks
    ValidationExceptionCollection exceptions =
        AspectsBatch.validateProposed(
            opContext, List.of(deleteItem), opContext.getRetrieverContext(), opContext);
    if (!exceptions.isEmpty()) {
      throw new ValidationException(
          collectMetrics(opContext.getMetricUtils().orElse(null), exceptions).toString());
    }

    // Hard delete wipes all aspects in one shot; capture propertyDefinition before deleteUrn so
    // PropertyDefinitionDeleteSideEffect can scroll ES and emit PATCH REMOVE MCPs (see
    // docs/api/tutorials/structured-properties.md).
    final PropertyDefinitionBeforeHardDelete propertyDefinitionBeforeHardDelete =
        new PropertyDefinitionBeforeHardDelete();

    // Gate the shared DB-delete primitive at the (urn, aspect) conflict unit, off the DB
    // connection,
    // BEFORE opening the delete transaction; release after commit/rollback. (scoped mode only;
    // no-op
    // otherwise.) A normal delete touches one aspect, so it locks just (urn, aspectName). A
    // hard-delete of the KEY aspect wipes the whole entity (deleteUrn), so it must serialize
    // against
    // ANY aspect write on this URN — it locks the entity's full aspect key-set (wide).
    // Delete↔upsert
    // safety is key-set overlap, not a permanent URN-wide lock on every ingest. The async MCL
    // emission callers run after this returns stays OUT from under the lock.
    final Collection<String> gateKeys =
        (hardDelete && aspectName.equals(opContext.getKeyAspectName(entityUrn)))
            ? opContext.getEntityAspectNames(entityUrn).stream()
                .map(a -> writeGateKey(urn, a))
                .collect(Collectors.toList())
            : List.of(writeGateKey(urn, aspectName));
    final RollbackResult result;
    try (EntityWriteLock.LockHandle writeGate = acquireWriteGate(opContext, gateKeys)) {
      result =
          aspectDao
              .runInTransactionWithRetry(
                  opContext,
                  (txContext) -> {
                    Integer additionalRowsDeleted = 0;

                    // 1. Fetch the latest existing version of the aspect.
                    SystemAspect latest = null;
                    try {
                      latest = aspectDao.getLatestAspect(opContext, urn, aspectName, false);
                    } catch (EntityNotFoundException e) {
                      log.debug("Delete non-existing aspect. urn {} aspect {}", urn, aspectName);
                      opContext
                          .getMetricUtils()
                          .ifPresent(
                              metricUtils ->
                                  metricUtils.increment(
                                      EntityServiceImpl.class, "delete_nonexisting", 1));
                    }

                    // 1.1 If no latest exists, skip this aspect
                    if (latest == null) {
                      return TransactionResult.rollback();
                    }

                    // 2. Compare the match conditions, if they don't match, ignore.
                    SystemMetadata latestSystemMetadata = latest.getSystemMetadata();
                    if (!filterMatch(latestSystemMetadata, conditions)) {
                      return TransactionResult.rollback();
                    }

                    // 3. Check if this is a key aspect
                    Boolean isKeyAspect = opContext.getKeyAspectName(entityUrn).equals(aspectName);

                    // 4. Fetch all preceding aspects, that match
                    List<SystemAspect> aspectsToDelete = new ArrayList<>();
                    Pair<Long, Long> versionRange =
                        aspectDao.getVersionRange(opContext, urn, aspectName);
                    if (versionRange.getFirst() == null
                        || versionRange.getSecond() == null
                        || versionRange.getFirst() < 0
                        || versionRange.getSecond() < 0) {
                      log.debug(
                          "Delete skipped due to empty version range. urn {} aspect {}",
                          urn,
                          aspectName);
                      return TransactionResult.rollback();
                    }
                    long minVersion = versionRange.getFirst();
                    long maxVersion = versionRange.getSecond();

                    EntityAspect.EntitySystemAspect survivingAspect = null;

                    boolean filterMatch = true;
                    while (maxVersion > minVersion && filterMatch) {
                      EntityAspect.EntitySystemAspect candidateAspect =
                          (EntityAspect.EntitySystemAspect)
                              EntityUtils.toSystemAspect(
                                      opContext,
                                      opContext.getRetrieverContext(),
                                      aspectDao.getAspect(opContext, urn, aspectName, maxVersion),
                                      true)
                                  .orElse(null);
                      SystemMetadata previousSysMetadata =
                          candidateAspect != null ? candidateAspect.getSystemMetadata() : null;
                      filterMatch =
                          previousSysMetadata != null
                              && filterMatch(previousSysMetadata, conditions);
                      if (filterMatch) {
                        aspectsToDelete.add(candidateAspect);
                      } else if (candidateAspect == null) {
                        // potential gap
                        filterMatch = true;
                      } else {
                        survivingAspect = candidateAspect;
                      }
                      maxVersion = maxVersion - 1;
                    }

                    // Delete validation hooks
                    ValidationExceptionCollection preCommitExceptions =
                        AspectsBatch.validatePreCommit(
                            opContext,
                            aspectsToDelete.stream()
                                .map(
                                    toDelete ->
                                        DeleteItemImpl.builder()
                                            .urn(toDelete.getUrn())
                                            .aspectName(toDelete.getAspectName())
                                            .auditStamp(auditStamp)
                                            .build(opContext.getAspectRetriever()))
                                .collect(Collectors.toList()),
                            opContext.getRetrieverContext(),
                            opContext);
                    if (!preCommitExceptions.isEmpty()) {
                      throw new ValidationException(
                          collectMetrics(
                                  opContext.getMetricUtils().orElse(null), preCommitExceptions)
                              .toString());
                    }

                    // 5. Apply deletes and fix up latest row
                    aspectsToDelete.forEach(
                        aspect ->
                            aspectDao.deleteAspect(
                                opContext,
                                aspect.getUrn(),
                                aspect.getAspectName(),
                                aspect.getVersion()));

                    if (survivingAspect != null) {
                      // if there was a surviving aspect, copy its information into the latest row
                      // eBean does not like us updating a pkey column (version) for the surviving
                      // aspect
                      // as a result we copy information from survivingAspect to latest and delete
                      // survivingAspect
                      latest.setRecordTemplate(survivingAspect.getRecordTemplate());
                      latest.setSystemMetadata(survivingAspect.getSystemMetadata());
                      latest.setAuditStamp(survivingAspect.getAuditStamp());

                      Optional<EntityAspect> survivingResult =
                          aspectDao.updateAspect(opContext, txContext, latest);

                      // metrics
                      aspectDao.incrementWriteMetrics(
                          opContext,
                          aspectName,
                          1,
                          survivingResult.map(r -> r.getMetadata().getBytes().length).orElse(0));

                      if (survivingAspect.getVersion() > 0) {
                        aspectDao.deleteAspect(
                            opContext,
                            survivingAspect.getUrn(),
                            survivingAspect.getAspectName(),
                            survivingAspect.getVersion());
                      }
                    } else {
                      if (isKeyAspect) {
                        if (hardDelete) {
                          // If this is the key aspect, delete the entity entirely.
                          // If Using CDCs, need to ensure key aspect is the deleted last.
                          if (STRUCTURED_PROPERTY_ENTITY_NAME.equals(entityUrn.getEntityType())) {
                            try {
                              SystemAspect definitionAspect =
                                  aspectDao.getLatestAspect(
                                      opContext,
                                      urn,
                                      STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                                      false);
                              propertyDefinitionBeforeHardDelete.definition =
                                  definitionAspect.getRecordTemplate();
                              propertyDefinitionBeforeHardDelete.metadata =
                                  definitionAspect.getSystemMetadata();
                            } catch (EntityNotFoundException e) {
                              log.debug(
                                  "No {} aspect to capture before hard delete of {}",
                                  STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                                  urn);
                            }
                          }
                          additionalRowsDeleted = aspectDao.deleteUrn(opContext, txContext, urn);
                        } else if (deleteItem
                            .getEntitySpec()
                            .hasAspect(Constants.STATUS_ASPECT_NAME)) {
                          // Soft delete: set removed=true.
                          final Status statusAspect = new Status();
                          statusAspect.setRemoved(true);

                          final MetadataChangeProposal gmce = new MetadataChangeProposal();
                          gmce.setEntityUrn(entityUrn);
                          gmce.setChangeType(ChangeType.UPSERT);
                          gmce.setEntityType(entityUrn.getEntityType());
                          gmce.setAspectName(Constants.STATUS_ASPECT_NAME);
                          gmce.setAspect(GenericRecordUtils.serializeAspect(statusAspect));

                          // Re-entrant gate acquire: this nested ingest re-acquires the write gate
                          // for the SAME urn on the SAME thread that already holds it (taken in
                          // deleteAspectWithoutMCL). The Hazelcast IMap lock is re-entrant per
                          // (thread, key), so this does not self-deadlock; the no-op backend is
                          // trivially re-entrant too.
                          //
                          // KNOWN LIMITATION (lock-placement pass): this synchronous nested ingest
                          // runs its MCL emission + post-commit retention while the outer write
                          // gate
                          // is still held, so concurrent writers on this URN wait for the full
                          // soft-delete ingest, not just its DB transaction. Liveness-only, not
                          // correctness (reentrant; CAS still guards; the lease bounds any hang),
                          // on
                          // an infrequent path (key-aspect soft-delete under same-URN contention).
                          // Scoping the gate to just the DB write here needs the same
                          // lock-placement
                          // refactor as the outer-retry-backoff case and is deferred with it.
                          this.ingestProposal(opContext, gmce, auditStamp, false);
                        }
                      } else {
                        // Else, only delete the specific aspect.
                        aspectDao.deleteAspect(
                            opContext,
                            latest.getUrn(),
                            latest.getAspectName(),
                            latest.getVersion());
                      }
                    }

                    // 6. Emit the Update
                    try {
                      final RecordTemplate latestValue =
                          latest == null ? null : latest.getRecordTemplate();
                      final RecordTemplate previousValue =
                          survivingAspect == null ? null : latest.getRecordTemplate();

                      final Urn urnObj = Urn.createFromString(urn);
                      // We are not deleting key aspect if hardDelete has not been set so do not
                      // return a
                      // rollback result
                      if (isKeyAspect && !hardDelete) {
                        return TransactionResult.rollback();
                      }
                      return TransactionResult.commit(
                          new RollbackResult(
                              urnObj,
                              urnObj.getEntityType(),
                              latest.getAspectName(),
                              latestValue,
                              previousValue,
                              latestSystemMetadata,
                              previousValue == null ? null : survivingAspect.getSystemMetadata(),
                              survivingAspect == null ? ChangeType.DELETE : ChangeType.UPSERT,
                              isKeyAspect,
                              additionalRowsDeleted));
                    } catch (URISyntaxException e) {
                      throw new RuntimeException(
                          String.format("Failed to emit the update for urn %s", urn));
                    } catch (IllegalStateException e) {
                      log.warn(
                          "Unable to find aspect, rollback result will not be sent. Error: {}",
                          e.getMessage());
                      return TransactionResult.rollback();
                    }
                  },
                  DEFAULT_MAX_TRANSACTION_RETRY)
              .stream()
              .findFirst()
              .orElse(null);
    }

    if (result != null) {
      List<MetadataChangeLog> mclsForSideEffects = new ArrayList<>();
      if (propertyDefinitionBeforeHardDelete.definition != null) {
        mclsForSideEffects.add(
            constructMCL(
                null,
                urnToEntityName(entityUrn),
                entityUrn,
                ChangeType.DELETE,
                STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                auditStamp,
                null,
                null,
                propertyDefinitionBeforeHardDelete.definition,
                propertyDefinitionBeforeHardDelete.metadata));
      }
      mclsForSideEffects.add(result.toMCL(auditStamp));
      processPostCommitMCLSideEffects(opContext, mclsForSideEffects);
      if (result.getChangeType() == ChangeType.DELETE) {
        // Key-aspect / hard delete → entity-wide invalidation (aspectName null in batch).
        // Intentionally not gated on UI source / sync-index header (unlike ingest/MCL paths):
        // destructive deletes should invalidate cache immediately rather than wait for TTL.
        SyncGraphInvalidationBatch invalidationBatch =
            EntityGraphSyncInvalidationSupport.fromSyncEntityDelete(
                opContext,
                entityUrn.toString(),
                entityUrn.getEntityType(),
                result.getAspectName(),
                Boolean.TRUE.equals(result.getKeyAffected()));
        if (!invalidationBatch.isEmpty()) {
          opContext.getEntityGraphCache().invalidateOnSyncBatch(invalidationBatch);
        }
      } else if (result.getChangeType() == ChangeType.UPSERT) {
        SyncGraphInvalidationBatch invalidationBatch =
            EntityGraphSyncInvalidationSupport.fromSyncAspectRollback(
                opContext, entityUrn.toString(), entityUrn.getEntityType(), result.getAspectName());
        if (!invalidationBatch.isEmpty()) {
          opContext.getEntityGraphCache().invalidateOnSyncBatch(invalidationBatch);
        }
      }
    }

    return result;
  }

  protected boolean filterMatch(
      @Nonnull SystemMetadata systemMetadata, Map<String, String> conditions) {
    String runIdCondition = conditions.getOrDefault("runId", null);
    if (runIdCondition != null) {
      if (!runIdCondition.equals(systemMetadata.getRunId())) {
        return false;
      }
    }
    String registryNameCondition = conditions.getOrDefault("registryName", null);
    if (registryNameCondition != null) {
      if (!registryNameCondition.equals(systemMetadata.getRegistryName())) {
        return false;
      }
    }
    String registryVersionCondition = conditions.getOrDefault("registryVersion", null);
    if (registryVersionCondition != null) {
      if (!registryVersionCondition.equals(systemMetadata.getRegistryVersion())) {
        return false;
      }
    }
    return true;
  }

  protected AuditStamp createSystemAuditStamp() {
    return new AuditStamp()
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
        .setTime(System.currentTimeMillis());
  }

  @Nonnull
  private Map<EntityAspectIdentifier, EntityAspect> getLatestAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames,
      boolean forUpdate) {

    log.debug("Invoked getLatestAspects with urns: {}, aspectNames: {}", urns, aspectNames);

    // Create DB keys
    final Set<EntityAspectIdentifier> dbKeys =
        urns.stream()
            .map(
                urn -> {
                  final Set<String> aspectsToFetch =
                      aspectNames.isEmpty() ? opContext.getEntityAspectNames(urn) : aspectNames;
                  return aspectsToFetch.stream()
                      .map(
                          aspectName ->
                              new EntityAspectIdentifier(
                                  urn.toString(), aspectName, ASPECT_LATEST_VERSION))
                      .collect(Collectors.toList());
                })
            .flatMap(List::stream)
            .collect(Collectors.toSet());

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = new HashMap<>();
    Iterators.partition(dbKeys.iterator(), MAX_KEYS_PER_QUERY)
        .forEachRemaining(
            batch ->
                batchGetResults.putAll(
                    aspectDao.batchGet(opContext, ImmutableSet.copyOf(batch), forUpdate)));
    return batchGetResults;
  }

  /*
   * When a user tries to fetch a negative version, we want to index most recent to least recent snapshots.
   * To do this, we want to fetch the maximum version and subtract the negative version from that. Since -1 represents
   * the maximum version, we need to add 1 to the final result.
   */
  private long calculateVersionNumber(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull long version) {
    if (version < 0) {
      return aspectDao.getMaxVersion(opContext, urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  private Map<EntityAspectIdentifier, EnvelopedAspect> getEnvelopedAspects(
      @Nonnull OperationContext opContext, final Set<EntityAspectIdentifier> dbKeys) {
    final Map<EntityAspectIdentifier, EntityAspect> dbEntries =
        aspectDao.batchGet(opContext, dbKeys, false);

    List<SystemAspect> envelopedAspects =
        EntityUtils.toSystemAspects(opContext, opContext.getRetrieverContext(), dbEntries.values());

    return envelopedAspects.stream()
        .collect(
            Collectors.toMap(
                systemAspect ->
                    EntityAspectIdentifier.fromSystemEntityAspect(
                        (EntityAspect.EntitySystemAspect) systemAspect),
                systemAspect ->
                    ((EntityAspect.EntitySystemAspect) systemAspect).toEnvelopedAspects()));
  }

  // Max in-transaction scoped-retry passes over the conflicted subset. Kept separate from
  // ebeanMaxTransactionRetry (the OUTER whole-batch retry bound) so the two don't multiply into an
  // O(retry^2) worst case. No in-loop sleep: the loop is bounded and, on exhaustion, throws so the
  // OUTER retry applies backoff with the DB connection released — never sleeping while a connection
  // is checked out under the very hot-URN contention this targets.
  private static final int SCOPED_RETRY_MAX_ATTEMPTS = 3;

  /**
   * Result of one compute+persist pass ({@link #computeAndPersistWithinTransaction}) over a batch
   * (or a scoped sub-batch) on the optimistic-locking path. Carries the committed {@link
   * UpdateAspectResult}s that downstream retention / MCL consume, plus the per-MCP {@link
   * BatchWriteResult} that drives scoped retry.
   */
  private static final class ScopedComputeResult {
    private final List<ChangeMCP> changeMCPs;
    private final List<UpdateAspectResult> upsertResults;
    private final List<Pair<ChangeMCP, Set<AspectValidationException>>> failedUpsertResults;
    private final Map<String, Map<String, SystemAspect>> updatedLatestAspects;
    private final BatchWriteResult batchWriteResult;
    // Derived (urn, aspect) -> parent base URN(s) produced this pass. Drives branch-scoped retry (a
    // conflict on a derived node re-runs its parent, which re-derives it) and identifies which
    // committed keys are derived (Layer-2, ephemeral — never held in the double-commit guard).
    private final Map<Pair<Urn, String>, Set<Urn>> derivedToParents;

    private ScopedComputeResult(
        List<ChangeMCP> changeMCPs,
        List<UpdateAspectResult> upsertResults,
        List<Pair<ChangeMCP, Set<AspectValidationException>>> failedUpsertResults,
        Map<String, Map<String, SystemAspect>> updatedLatestAspects,
        BatchWriteResult batchWriteResult,
        Map<Pair<Urn, String>, Set<Urn>> derivedToParents) {
      this.changeMCPs = changeMCPs;
      this.upsertResults = upsertResults;
      this.failedUpsertResults = failedUpsertResults;
      this.updatedLatestAspects = updatedLatestAspects;
      this.batchWriteResult = batchWriteResult;
      this.derivedToParents = derivedToParents;
    }

    private static ScopedComputeResult empty(
        Map<String, Map<String, SystemAspect>> updatedLatestAspects,
        Map<Pair<Urn, String>, Set<Urn>> derivedToParents) {
      return new ScopedComputeResult(
          List.of(),
          new ArrayList<>(),
          new ArrayList<>(),
          updatedLatestAspects,
          new BatchWriteResult(List.of()),
          derivedToParents);
    }
  }

  /**
   * COMMITTED (urn, aspect) keys from a single compute pass, for cross-pass double-commit guarding.
   */
  @Nonnull
  static Set<Pair<Urn, String>> committedKeysOf(@Nonnull BatchWriteResult batchWriteResult) {
    return batchWriteResult.committedResults().stream()
        .map(result -> Pair.of(result.getUrn(), result.getAspectName()))
        .collect(Collectors.toSet());
  }

  /**
   * True when {@code writeItem}'s exact (urn, aspectName) already committed in an earlier pass of
   * this transaction and must not be re-persisted on a scoped retry. Keyed on (urn, aspect) — NOT
   * urn alone (which is how {@link #filterItemsForRecompute} scopes the sub-batch) — so a committed
   * sibling aspect is skipped while a conflicted sibling on the same URN is still retried.
   * Package-private for unit testing.
   */
  static boolean isAlreadyCommitted(
      @Nonnull Set<Pair<Urn, String>> committedKeys, @Nonnull ChangeMCP writeItem) {
    return committedKeys.contains(Pair.of(writeItem.getUrn(), writeItem.getAspectName()));
  }

  /**
   * (urn, aspect) keys of the given failed results, for cross-pass dedup of {@code
   * failedUpsertResults} on the scoped-retry path. Package-private for unit testing.
   */
  @Nonnull
  static Set<Pair<Urn, String>> failedKeysOf(
      @Nonnull List<Pair<ChangeMCP, Set<AspectValidationException>>> failedResults) {
    return failedResults.stream()
        .map(failed -> Pair.of(failed.getFirst().getUrn(), failed.getFirst().getAspectName()))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Append only the failed results whose (urn, aspect) has not been recorded yet. A scoped-retry
   * sub-batch is scoped by URN, so an aspect that terminally fails validation is re-validated (and
   * re-fails) on every pass while its conflicting sibling is retried; deduping by (urn, aspect)
   * records that failure exactly once instead of once per pass (which on the consumer path would
   * emit duplicate dead-letter events). Package-private for unit testing.
   *
   * <p>Mutates both {@code accumulator} (appends the new failures) and {@code seenFailedKeys}
   * (records every incoming key).
   */
  static void appendNewFailedResults(
      @Nonnull List<Pair<ChangeMCP, Set<AspectValidationException>>> accumulator, // mutated
      @Nonnull List<Pair<ChangeMCP, Set<AspectValidationException>>> incoming,
      @Nonnull Set<Pair<Urn, String>> seenFailedKeys) { // mutated
    // Suppress only keys recorded in EARLIER passes. Snapshot seenFailedKeys before this pass so
    // that two distinct items sharing a (urn, aspect) WITHIN this pass are both kept — matching the
    // un-deduped first-pass behavior — while a key that already failed in a prior pass is dropped.
    // Dedup is keyed on (urn, aspect), not item identity, because the retry stamps a fresh traceId
    // each pass so a ChangeMCP's content/hashCode is not stable across passes; (urn, aspect) is the
    // only stable cross-pass key for the same logical failure.
    final Set<Pair<Urn, String>> priorPassKeys = new HashSet<>(seenFailedKeys);
    for (Pair<ChangeMCP, Set<AspectValidationException>> failed : incoming) {
      final Pair<Urn, String> key =
          Pair.of(failed.getFirst().getUrn(), failed.getFirst().getAspectName());
      if (!priorPassKeys.contains(key)) {
        accumulator.add(failed);
      }
      // Record unconditionally (no-op if already present) so a later pass sees this key as
      // prior-seen and suppresses a repeat of it.
      seenFailedKeys.add(key);
    }
  }

  /**
   * One compute+persist pass over {@code batch} within the already-open transaction: read latest ->
   * compute next versions -> run mutation / side-effect hooks (via {@code toUpsertBatchItems}) ->
   * CAS-persist each item. Conflicts are collected as data into the returned {@link
   * BatchWriteResult} rather than thrown, so the caller can retry only the conflicted branch.
   *
   * <p>Called for the full batch on the first pass and for a filtered sub-batch on each scoped
   * retry. Under optimistic locking a re-read here (same {@code READ_COMMITTED} txn) is expected to
   * observe the committed row of whichever writer won the CAS — this cross-transaction visibility
   * is the correctness assumption the concurrency IT must confirm on MySQL and PostgreSQL.
   */
  private ScopedComputeResult computeAndPersistWithinTransaction(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull AspectsBatch batch,
      boolean overwrite,
      @Nonnull Set<Pair<Urn, String>> committedKeys) {

    final Map<String, Set<String>> urnAspects = batch.getUrnAspectsMap();

    // Opt-in per-(urn, aspect) write serialization (Postgres advisory lock), taken before the
    // read/CAS. Skipped when the pre-transaction write gate is engaged — the gate already
    // serializes
    // the same (urn, aspect) keys off-connection, so the advisory would be a redundant round trip.
    if (!writeGateEngaged()) {
      aspectDao.lockAspectsForWrite(opContext, urnAspects);
    }

    // read #1 — initial database state.
    final Map<String, Map<String, SystemAspect>> batchAspects =
        aspectDao.getLatestAspects(opContext, urnAspects, true);
    final Map<String, Map<String, SystemAspect>> updatedLatestAspects;

    // read #2 (potentially)
    final Map<String, Map<String, Long>> nextVersions =
        EntityUtils.calculateNextVersions(
            opContext, txContext, aspectDao, batchAspects, urnAspects);

    final BiFunction<ChangeMCP, SystemAspect, SystemAspect> databaseUpsert =
        (changeMCP, systemAspect) ->
            applyUpsert(
                changeMCP,
                systemAspect,
                aspectDao.getSystemAspectValidators(),
                aspectDao.getValidationConfig(),
                opContext);

    // 1. Convert patches to full upserts 2. Run any entity/aspect level hooks 3. Capture derived
    // (urn, aspect) -> parent base URN(s) so a conflict on a derived MCP recomputes exactly its
    // parent's branch (which re-derives it) rather than all base URNs. Depth-1: current side
    // effects
    // (VersionSet / VersionProperties / Aliases) derive one level; a self-rewrite (parent ==
    // child)
    // is not a derivation and is skipped.
    final Map<Pair<Urn, String>, Set<Urn>> derivedToParents = new HashMap<>();
    Pair<Map<String, Set<String>>, List<ChangeMCP>> updatedItems =
        batch.toUpsertBatchItems(
            opContext,
            batchAspects,
            nextVersions,
            databaseUpsert,
            (parent, child) -> {
              if (!(parent.getUrn().equals(child.getUrn())
                  && parent.getAspectName().equals(child.getAspectName()))) {
                derivedToParents
                    .computeIfAbsent(
                        Pair.of(child.getUrn(), child.getAspectName()), k -> new HashSet<>())
                    .add(parent.getUrn());
              }
            });

    final List<ChangeMCP> changeMCPs;
    if (!updatedItems.getFirst().isEmpty()) {
      // These items are new items from side effects
      Map<String, Set<String>> sideEffects = updatedItems.getFirst();

      final Map<String, Map<String, Long>> updatedNextVersions;

      Map<String, Map<String, SystemAspect>> newLatestAspects =
          aspectDao.getLatestAspects(opContext, sideEffects, true);

      // merge
      updatedLatestAspects = AspectsBatch.merge(batchAspects, newLatestAspects);

      Map<String, Map<String, Long>> newNextVersions =
          EntityUtils.calculateNextVersions(
              opContext, txContext, aspectDao, updatedLatestAspects, updatedItems.getFirst());
      // merge
      updatedNextVersions = AspectsBatch.merge(nextVersions, newNextVersions);

      changeMCPs =
          updatedItems.getSecond().stream()
              .peek(
                  changeMCP -> {
                    // Add previous version to each side-effect
                    if (sideEffects
                        .getOrDefault(changeMCP.getUrn().toString(), Collections.emptySet())
                        .contains(changeMCP.getAspectName())) {

                      AspectsBatch.incrementBatchVersion(
                          changeMCP, updatedLatestAspects, updatedNextVersions, databaseUpsert);
                    }
                  })
              .collect(Collectors.toList());
    } else {
      changeMCPs = updatedItems.getSecond();
      updatedLatestAspects = batchAspects;
    }

    // No changes for this (sub-)batch: return an empty result. The caller decides rollback vs.
    // commit
    // from the aggregate across all passes.
    if (changeMCPs.isEmpty()) {
      return ScopedComputeResult.empty(updatedLatestAspects, derivedToParents);
    }

    // do final pre-commit checks with previous aspect value. Pass opContext as the
    // AuthorizationSession (4-arg overload) so in-transaction auth validators (e.g.
    // DomainWriteAuthorizationValidator) run on the scoped path exactly as on the full-batch path
    // —
    // the 3-arg overload passes a null session, which those validators treat as "skip auth".
    ValidationExceptionCollection exceptions =
        AspectsBatch.validatePreCommit(
            opContext, changeMCPs, opContext.getRetrieverContext(), opContext);

    List<Pair<ChangeMCP, Set<AspectValidationException>>> failedUpsertResults = new ArrayList<>();
    if (exceptions.hasFatalExceptions()) {
      // IF this is a client request/API request we fail the `transaction batch`
      if (opContext.getRequestContext() != null) {
        opContext
            .getMetricUtils()
            .ifPresent(
                metricUtils ->
                    metricUtils.increment(
                        EntityServiceImpl.class, "batch_request_validation_exception", 1));
        collectMetrics(opContext.getMetricUtils().orElse(null), exceptions);
        throw new ValidationException(exceptions);
      }

      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      EntityServiceImpl.class, "batch_consumer_validation_exception", 1));
      log.error(
          "mce-consumer batch exceptions: {}",
          collectMetrics(opContext.getMetricUtils().orElse(null), exceptions));
      failedUpsertResults =
          exceptions
              .streamExceptions(changeMCPs.stream())
              .map(
                  writeItem ->
                      Pair.of(
                          writeItem,
                          exceptions.get(Pair.of(writeItem.getUrn(), writeItem.getAspectName()))))
              .collect(Collectors.toList());
    }

    // Database Upsert successfully validated results
    log.debug(
        "Ingesting aspects batch to database: {}",
        AspectsBatch.toAbbreviatedString(changeMCPs, 2048));

    final List<AspectWriteResult> writeResults = new ArrayList<>();
    final List<UpdateAspectResult> upsertResults = new ArrayList<>();

    // OL CAS batching: when active, eligible version-0 CAS UPDATEs are planned (not executed)
    // during
    // the loop and flushed as one JDBC batch afterwards, WITHIN this pass (so per-item conflict
    // results are known before scoped retry runs). Each processed item takes an ordered slot so the
    // deferred flush never reorders writeResults / upsertResults vs the sequential path.
    // Scoped retry is a hard prerequisite, not just OL: this method (and therefore the batch flush)
    // only runs on the scoped-retry compute path, and batching relies on per-item CONFLICT results
    // feeding scoped retry. With scoped retry off, batching stays disabled (writes go sequential).
    final boolean casBatchActive =
        aspectDao.isOptimisticLockingEnabled()
            && aspectDao.isScopedRetryEnabled()
            && aspectDao.isOptimisticWriteBatchEnabled();
    final List<ChangeMCP> orderedItems = new ArrayList<>();
    final List<AspectPersistResult> orderedResults = new ArrayList<>();
    final List<PendingCasWrite> pendingBatch = new ArrayList<>();
    // (urn, aspect) pairs that appear more than once in this pass must NOT be batched: today's
    // sequential invariant is that an earlier write advances the in-memory latest state before a
    // later write to the same pair computes its CAS. Batching defers the flush past the loop, which
    // would invert that ordering — so detect duplicates up front and route ALL their occurrences
    // through the sequential path.
    final Set<Pair<Urn, String>> batchExcludedKeys =
        casBatchActive
            ? exceptions
                .streamSuccessful(changeMCPs.stream())
                .collect(
                    Collectors.groupingBy(
                        it -> Pair.of(it.getUrn(), it.getAspectName()), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet())
            : Set.of();

    for (ChangeMCP writeItem :
        exceptions.streamSuccessful(changeMCPs.stream()).collect(Collectors.toList())) {

      // Already committed in an earlier pass of this transaction (a sibling aspect of a URN that
      // had
      // another aspect conflict). Skip entirely — no re-CAS, no result, no writeResults entry —
      // so we
      // never double-commit / double-emit it. committedKeys is empty on the first pass and on the
      // OL-
      // off path, so this is a no-op there.
      if (isAlreadyCommitted(committedKeys, writeItem)) {
        continue;
      }

      // Latest aspect after possible in-memory mutation
      final SystemAspect latestAspect =
          updatedLatestAspects
              .getOrDefault(writeItem.getUrn().toString(), Map.of())
              .get(writeItem.getAspectName());

      // eliminate unneeded writes within a batch if the latest aspect doesn't match this
      // ChangeMCP
      if (latestAspect != null
          && !Objects.equals(
              latestAspect.getSystemMetadata().getVersion(),
              writeItem.getSystemMetadata().getVersion())) {
        log.debug(
            "Skipping obsolete write: urn: {} aspect: {} version: {}",
            writeItem.getUrn(),
            writeItem.getAspectName(),
            writeItem.getSystemMetadata().getVersion());
        continue;
      }

      /*
        This condition is specifically for an older conditional write ingestAspectIfNotPresent()
        overwrite is always true otherwise
      */
      if (overwrite || latestAspect == null || latestAspect.getDatabaseAspect().isEmpty()) {
        try {
          final Pair<Urn, String> key = Pair.of(writeItem.getUrn(), writeItem.getAspectName());
          if (casBatchActive
              && latestAspect != null
              && latestAspect.getDatabaseAspect().isPresent()
              && !batchExcludedKeys.contains(key)) {
            // Plan the version-0 write WITHOUT executing it, so eligible CAS UPDATEs flush as one
            // batch after the loop. applyUpsert may still throw a size exception here, handled
            // below
            // exactly as the sequential path.
            SystemAspect upsertAspect =
                applyUpsert(
                    writeItem,
                    latestAspect,
                    aspectDao.getSystemAspectValidators(),
                    aspectDao.getValidationConfig(),
                    opContext);
            int maxVersionsToKeep = resolveMaxVersionsToKeep(opContext, writeItem);
            ConditionalWritePlan plan =
                aspectDao.planConditionalWrite(
                    opContext, latestAspect, upsertAspect, maxVersionsToKeep);
            if (plan.getKind() == ConditionalWritePlan.Kind.ELIGIBLE_CAS) {
              pendingBatch.add(
                  new PendingCasWrite(
                      orderedItems.size(),
                      writeItem,
                      latestAspect,
                      upsertAspect,
                      plan,
                      maxVersionsToKeep));
              orderedItems.add(writeItem);
              orderedResults.add(null); // filled after the batch flush
            } else {
              // NOOP / INSERT_NEW / LEGACY: execute inline through the shared execute path (no
              // re-plan), producing a result identical to the sequential path.
              ConditionalSaveResult cond =
                  aspectDao.executePlannedWrite(
                      opContext, txContext, latestAspect, upsertAspect, maxVersionsToKeep, plan);
              orderedItems.add(writeItem);
              orderedResults.add(
                  toScopedPersistResult(opContext, writeItem, latestAspect, upsertAspect, cond));
            }
          } else {
            // Sequential path: OL off, batching off, non-eligible latest (v0 insert), or a
            // duplicate
            // (urn, aspect) whose earlier write must advance in-memory state before this one's CAS.
            orderedItems.add(writeItem);
            orderedResults.add(
                ingestAspectToLocalDBScoped(opContext, txContext, writeItem, latestAspect));
          }
        } catch (com.linkedin.metadata.entity.validation.AspectSizeExceededException e) {
          // Convert to AspectValidationException for uniform batch handling
          AspectValidationException validationException =
              AspectValidationException.forItem(
                  writeItem,
                  String.format(
                      "Aspect size validation failed at %s: %d bytes exceeds threshold of %d bytes",
                      e.getValidationPoint(), e.getActualSize(), e.getThreshold()),
                  e);

          // API requests: fail entire batch immediately
          // Kafka consumers: collect exception and continue
          if (opContext.getRequestContext() != null) {
            ValidationExceptionCollection sizeExceptions =
                ValidationExceptionCollection.newCollection();
            sizeExceptions.addException(validationException);
            throw new ValidationException(sizeExceptions);
          } else {
            exceptions.addException(validationException);
            // Exclude from successful results
          }
        }
      }
    }

    // Flush the collected version-0 CAS batch once, WITHIN this pass, then fill each pending item's
    // result so scoped retry sees per-item conflicts. A thrown PersistenceException (ambiguous /
    // driver error) propagates to runInTransactionWithRetry -> whole-txn rollback + retry.
    if (!pendingBatch.isEmpty()) {
      if (pendingBatch.size() >= aspectDao.getOptimisticWriteBatchMinSize()) {
        // Chunk so a single executeBatch never exceeds the driver packet / heap bound on a large
        // ingest — each item carries full metadata + systemMetadata payloads (KB–MB), so the write
        // batch is packet-limited, unlike the read-side IN-clause knob (queryKeysCountForBatch)
        // which
        // is optimizer-memory-limited on tiny keys. All chunks share this one transaction, so a
        // thrown ambiguous/driver error still rolls back every chunk. Not operator-tunable by
        // design.
        for (List<PendingCasWrite> chunk : Lists.partition(pendingBatch, CAS_BATCH_MAX_CHUNK)) {
          List<ConditionalUpdateResult> outcomes =
              aspectDao.updateAspectsConditionalBatch(
                  opContext,
                  txContext,
                  chunk.stream()
                      .map(
                          p ->
                              new ConditionalAspectUpdate(
                                  p.upsertAspect, p.plan.getExpectedVersion()))
                      .collect(Collectors.toList()));
          for (int i = 0; i < chunk.size(); i++) {
            PendingCasWrite p = chunk.get(i);
            if (outcomes.get(i) == ConditionalUpdateResult.UPDATED) {
              // Winner: apply the deferred version-N history row, then build the committed result
              // from the written content (version-0 row, databaseAspectRowVersion 0) — identical to
              // what the sequential CAS produces.
              Optional<EntityAspect> versionN =
                  aspectDao.applyConditionalHistory(opContext, txContext, p.plan);
              orderedResults.set(
                  p.slot,
                  buildScopedPersistResult(
                      opContext,
                      p.writeItem,
                      p.latestAspect,
                      p.upsertAspect,
                      Optional.of(p.upsertAspect.asLatest()),
                      versionN));
            } else {
              orderedResults.set(p.slot, AspectPersistResult.conflict());
            }
          }
        }
      } else {
        // Below the batching threshold: run the collected eligible items sequentially through the
        // same execute path, avoiding batch overhead for tiny batches.
        for (PendingCasWrite p : pendingBatch) {
          ConditionalSaveResult cond =
              aspectDao.executePlannedWrite(
                  opContext,
                  txContext,
                  p.latestAspect,
                  p.upsertAspect,
                  p.maxVersionsToKeep,
                  p.plan);
          orderedResults.set(
              p.slot,
              toScopedPersistResult(opContext, p.writeItem, p.latestAspect, p.upsertAspect, cond));
        }
      }
    }

    // Materialize writeResults / upsertResults in original item order (batched winners included).
    for (int i = 0; i < orderedItems.size(); i++) {
      ChangeMCP writeItem = orderedItems.get(i);
      AspectPersistResult persistResult = orderedResults.get(i);
      switch (persistResult.getOutcome()) {
        case COMMITTED:
          UpdateAspectResult committed =
              persistResult.getResult().toBuilder().request(writeItem).build();
          upsertResults.add(committed);
          writeResults.add(
              AspectWriteResult.committed(
                  writeItem.getUrn(),
                  writeItem.getAspectName(),
                  committed.getDatabaseAspectRowVersion() == null
                      ? 0L
                      : committed.getDatabaseAspectRowVersion()));
          break;
        case CONFLICT:
          writeResults.add(
              AspectWriteResult.conflict(writeItem.getUrn(), writeItem.getAspectName()));
          break;
        case NOOP:
        default:
          writeResults.add(AspectWriteResult.noop(writeItem.getUrn(), writeItem.getAspectName()));
          break;
      }
    }

    return new ScopedComputeResult(
        changeMCPs,
        upsertResults,
        failedUpsertResults,
        updatedLatestAspects,
        new BatchWriteResult(writeResults),
        derivedToParents);
  }

  /**
   * Max CAS updates per {@code executeBatch} call. Bounds the JDBC packet + driver param buffer on
   * a large ingest (each item carries full metadata payloads). Hardcoded, not operator-tunable: the
   * write batch is packet-limited, a different bound from the read-side {@code
   * queryKeysCountForBatch} (optimizer memory on tiny keys), so reusing that knob would be wrong.
   * 100 balances round-trip savings against packet size; revisit only if a deployment proves it
   * needs tuning.
   */
  private static final int CAS_BATCH_MAX_CHUNK = 100;

  /**
   * A version-0 CAS write planned during the persist loop and deferred to the single batched flush.
   * {@code slot} is the item's index in the ordered result list, so the per-row batch outcome fills
   * the correct position and writeResults / upsertResults order matches the sequential path.
   */
  private static final class PendingCasWrite {
    final int slot;
    final ChangeMCP writeItem;
    final SystemAspect latestAspect;
    final SystemAspect upsertAspect;
    final ConditionalWritePlan plan;
    final int maxVersionsToKeep;

    PendingCasWrite(
        int slot,
        ChangeMCP writeItem,
        SystemAspect latestAspect,
        SystemAspect upsertAspect,
        ConditionalWritePlan plan,
        int maxVersionsToKeep) {
      this.slot = slot;
      this.writeItem = writeItem;
      this.latestAspect = latestAspect;
      this.upsertAspect = upsertAspect;
      this.plan = plan;
      this.maxVersionsToKeep = maxVersionsToKeep;
    }
  }

  /**
   * The base URNs to recompute for a batch of conflicts, scoped to whole branches. Each conflicted
   * {@code (urn, aspect)} contributes: its <b>parent base URN(s)</b> if it is a derived (Layer-2)
   * key (from {@code derivedToParents}) — re-running the parent re-derives the whole branch — or
   * its own URN if it is a base (Layer-1) input. With no in-transaction side effects this reduces
   * to exactly the conflicted base URNs. Package-private for unit testing.
   */
  @Nonnull
  static Set<Urn> branchScopedRecompute(
      @Nonnull BatchWriteResult batchWriteResult,
      @Nonnull Map<Pair<Urn, String>, Set<Urn>> derivedToParents) {
    Set<Urn> recompute = new HashSet<>();
    for (AspectWriteResult result : batchWriteResult.getResults()) {
      if (result.getOutcome() == AspectWriteOutcome.CONFLICT) {
        Set<Urn> parents = derivedToParents.get(Pair.of(result.getUrn(), result.getAspectName()));
        if (parents != null) {
          recompute.addAll(parents); // derived conflict -> recompute its parent base(s)
        } else {
          recompute.add(result.getUrn()); // base conflict -> recompute itself
        }
      }
    }
    return recompute;
  }

  /**
   * Selects the input items belonging to the URNs being recomputed on a scoped retry. Filtering is
   * by URN (not by {@code (urn, aspect)}) so that <b>all</b> of a recomputed URN's aspects go into
   * the sub-batch together — preserving per-URN atomicity — and no aspect of a non-conflicted URN
   * is touched. Package-private for unit testing.
   */
  @Nonnull
  static List<BatchItem> filterItemsForRecompute(
      @Nonnull Collection<? extends BatchItem> items, @Nonnull Set<Urn> recomputeUrns) {
    return items.stream()
        .filter(item -> recomputeUrns.contains(item.getUrn()))
        .collect(Collectors.toList());
  }

  /**
   * @param txContext Transaction context, keeps track of retries, exceptions etc.
   * @param writeItem The aspect being written
   * @param latestAspect The aspect as it exists in the database or was created/updated as part of
   *     the batch.
   * @return result object
   */
  @Nonnull
  private AspectPersistResult ingestAspectToLocalDBScoped(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull final ChangeMCP writeItem,
      @Nullable SystemAspect latestAspect) {

    SystemAspect upsertAspect =
        applyUpsert(
            writeItem,
            latestAspect,
            aspectDao.getSystemAspectValidators(),
            aspectDao.getValidationConfig(),
            opContext);

    int maxVersionsToKeep = resolveMaxVersionsToKeep(opContext, writeItem);

    // save to database
    if (aspectDao.isOptimisticLockingEnabled()) {
      ConditionalSaveResult cond =
          aspectDao.saveLatestAspectConditional(
              opContext, txContext, latestAspect, upsertAspect, maxVersionsToKeep);
      return toScopedPersistResult(opContext, writeItem, latestAspect, upsertAspect, cond);
    }

    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(
            opContext, txContext, latestAspect, upsertAspect, maxVersionsToKeep);
    return buildScopedPersistResult(
        opContext, writeItem, latestAspect, upsertAspect, result.getSecond(), result.getFirst());
  }

  /**
   * Resolve the retention-driven max version count for a write (per aspect): {@code <= 1} means do
   * not write a new history row (version != 0); the version-0 row is still updated. When retention
   * service is not enabled, retain only the current version (1).
   */
  private int resolveMaxVersionsToKeep(
      @Nonnull OperationContext opContext, @Nonnull ChangeMCP writeItem) {
    int maxVersionsToKeep;
    if (retentionService != null) {
      try {
        maxVersionsToKeep =
            retentionService.getMaxVersionsToKeepForWrite(
                opContext, writeItem.getUrn().getEntityType(), writeItem.getAspectName());
      } catch (Exception e) {
        log.warn(
            "Failed to resolve retention for urn={} aspect={}, retaining only current version",
            writeItem.getUrn(),
            writeItem.getAspectName(),
            e);
        maxVersionsToKeep = 1;
      }
    } else {
      maxVersionsToKeep = 1;
    }
    if (maxVersionsToKeep <= 1) {
      log.debug(
          "No version history for urn={} aspect={} (maxVersions={})",
          writeItem.getUrn(),
          writeItem.getAspectName(),
          maxVersionsToKeep);
    }
    return maxVersionsToKeep;
  }

  /**
   * Map an OL {@link ConditionalSaveResult} to an {@link AspectPersistResult}. Shared by the
   * sequential persist path and the batched persist path's non-eligible (NOOP / INSERT / LEGACY)
   * items, so both produce identical results.
   */
  @Nonnull
  private AspectPersistResult toScopedPersistResult(
      @Nonnull OperationContext opContext,
      @Nonnull ChangeMCP writeItem,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect upsertAspect,
      @Nonnull ConditionalSaveResult cond) {
    switch (cond.getOutcome()) {
      case SKIPPED_NOOP:
        // Aspect + system metadata unchanged: a legitimate no-op, never a conflict — do not retry.
        return AspectPersistResult.noop();
      case CONFLICT:
        // Conflict is data, not control flow: the persist loop collects it into a BatchWriteResult
        // and recomputes only the conflicted URN's branch. (The version-0 insert race still
        // surfaces
        // as OptimisticLockConflictException from the DAO, aborting + re-driving the whole txn.)
        return AspectPersistResult.conflict();
      case UPDATED:
      default:
        return buildScopedPersistResult(
            opContext,
            writeItem,
            latestAspect,
            upsertAspect,
            cond.getUpdated(),
            cond.getInserted());
    }
  }

  /**
   * Build the {@link AspectPersistResult} for a successful version-0 write from its {@code
   * version0} (updated row) and optional {@code versionN} (history row). Shared by the sequential
   * path, the OL non-batched path, and the batched winner path — only the inputs differ between
   * them, so the result construction cannot drift.
   */
  @Nonnull
  private AspectPersistResult buildScopedPersistResult(
      @Nonnull OperationContext opContext,
      @Nonnull ChangeMCP writeItem,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect upsertAspect,
      @Nonnull Optional<EntityAspect> version0,
      @Nonnull Optional<EntityAspect> versionN) {
    UpdateAspectResult updateResult =
        version0
            .map(
                updatedAspect -> {
                  // For subsequent updates to the same row, record version persisted
                  if (latestAspect != null) {
                    latestAspect.setDatabaseAspect(upsertAspect);
                  }

                  // metrics
                  aspectDao.incrementWriteMetrics(
                      opContext,
                      writeItem.getAspectName(),
                      1,
                      updatedAspect.getMetadata().getBytes().length
                          + versionN.map(n -> n.getMetadata().getBytes().length).orElse(0));

                  return UpdateAspectResult.builder()
                      .urn(writeItem.getUrn())
                      .oldValue(writeItem.getPreviousRecordTemplate())
                      .newValue(writeItem.getRecordTemplate())
                      .oldSystemMetadata(
                          writeItem.getPreviousSystemAspect() == null
                              ? null
                              : writeItem.getPreviousSystemAspect().getSystemMetadata())
                      .newSystemMetadata(writeItem.getSystemMetadata())
                      .operation(MetadataAuditOperation.UPDATE)
                      .auditStamp(writeItem.getAuditStamp())
                      .maxVersion(versionN.map(EntityAspect::getVersion).orElse(0L))
                      .databaseAspectRowVersion(updatedAspect.getVersion())
                      .build();
                })
            .orElse(null);

    // A legacy no-op (saveLatestAspect skipped the version-0 update) yields no UpdateAspectResult;
    // surface it as NOOP so the persist loop treats it identically to today (excluded from upserts,
    // never retried) rather than as a committed write.
    return updateResult == null
        ? AspectPersistResult.noop()
        : AspectPersistResult.committed(updateResult);
  }

  /**
   * Full-batch (optimistic-locking base) persist of a single aspect. On the OL path a CONFLICT is
   * thrown as {@link OptimisticLockConflictException} so it propagates out of the transaction
   * lambda to {@link AspectDao#runInTransactionWithRetry}, which re-runs the WHOLE batch. A
   * legitimate SKIPPED_NOOP returns {@code null} (excluded from upserts, never retried). With OL
   * off this calls {@code saveLatestAspect} exactly as the legacy path. This is the method the
   * full-batch else-branch of {@link #ingestAspectsToLocalDB} uses; the scoped path uses {@link
   * #ingestAspectToLocalDBScoped} instead (conflict-as-data).
   *
   * @param txContext Transaction context, keeps track of retries, exceptions etc.
   * @param writeItem The aspect being written
   * @param latestAspect The aspect as it exists in the database or was created/updated as part of
   *     the batch.
   * @return result object, or {@code null} for a no-op
   */
  private UpdateAspectResult ingestAspectToLocalDB(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull final ChangeMCP writeItem,
      @Nullable SystemAspect latestAspect) {

    SystemAspect upsertAspect =
        applyUpsert(
            writeItem,
            latestAspect,
            aspectDao.getSystemAspectValidators(),
            aspectDao.getValidationConfig(),
            opContext);

    // Resolve maxVersionsToKeep from retention policy (per aspect): <= 1 means do not write a new
    // history row (version != 0); we still update the existing version 0 row. When retention
    // service is not enabled, we retain only the current version (1).
    int maxVersionsToKeep;
    if (retentionService != null) {
      try {
        maxVersionsToKeep =
            retentionService.getMaxVersionsToKeepForWrite(
                opContext, writeItem.getUrn().getEntityType(), writeItem.getAspectName());
      } catch (Exception e) {
        log.warn(
            "Failed to resolve retention for urn={} aspect={}, retaining only current version",
            writeItem.getUrn(),
            writeItem.getAspectName(),
            e);
        maxVersionsToKeep = 1;
      }
    } else {
      maxVersionsToKeep = 1;
    }
    if (maxVersionsToKeep <= 1) {
      log.debug(
          "No version history for urn={} aspect={} (maxVersions={})",
          writeItem.getUrn(),
          writeItem.getAspectName(),
          maxVersionsToKeep);
    }

    // save to database
    final Optional<EntityAspect> versionN;
    final Optional<EntityAspect> version0;
    if (aspectDao.isOptimisticLockingEnabled()) {
      ConditionalSaveResult result =
          aspectDao.saveLatestAspectConditional(
              opContext, txContext, latestAspect, upsertAspect, maxVersionsToKeep);
      switch (result.getOutcome()) {
        case SKIPPED_NOOP:
          return null;
        case CONFLICT:
          throw new OptimisticLockConflictException(
              String.format(
                  "Optimistic lock conflict on urn=%s aspect=%s: version-0 row changed since read",
                  writeItem.getUrn(), writeItem.getAspectName()));
        case UPDATED:
        default:
          versionN = result.getInserted();
          version0 = result.getUpdated();
      }
    } else {
      Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
          aspectDao.saveLatestAspect(
              opContext, txContext, latestAspect, upsertAspect, maxVersionsToKeep);
      versionN = result.getFirst();
      version0 = result.getSecond();
    }

    return version0
        .map(
            updatedAspect -> {
              // For subsequent updates to the same row, record version persisted
              if (latestAspect != null) {
                latestAspect.setDatabaseAspect(upsertAspect);
              }

              // metrics
              aspectDao.incrementWriteMetrics(
                  opContext,
                  writeItem.getAspectName(),
                  1,
                  updatedAspect.getMetadata().getBytes().length
                      + versionN.map(n -> n.getMetadata().getBytes().length).orElse(0));

              return UpdateAspectResult.builder()
                  .urn(writeItem.getUrn())
                  .oldValue(writeItem.getPreviousRecordTemplate())
                  .newValue(writeItem.getRecordTemplate())
                  .oldSystemMetadata(
                      writeItem.getPreviousSystemAspect() == null
                          ? null
                          : writeItem.getPreviousSystemAspect().getSystemMetadata())
                  .newSystemMetadata(writeItem.getSystemMetadata())
                  .operation(MetadataAuditOperation.UPDATE)
                  .auditStamp(writeItem.getAuditStamp())
                  .maxVersion(versionN.map(EntityAspect::getVersion).orElse(0L))
                  .databaseAspectRowVersion(updatedAspect.getVersion())
                  .build();
            })
        .orElse(null);
  }

  private static boolean shouldAspectEmitChangeLog(@Nonnull final AspectSpec aspectSpec) {
    final List<RelationshipFieldSpec> relationshipFieldSpecs =
        aspectSpec.getRelationshipFieldSpecs();
    return relationshipFieldSpecs.stream().anyMatch(RelationshipFieldSpec::isLineageRelationship);
  }

  private static void conditionalLogLevel(@Nullable TransactionContext txContext, String message) {
    if (txContext != null && txContext.getFailedAttempts() > 1) {
      log.warn(message);
    } else {
      log.debug(message);
    }
  }

  /** Mutable holder for propertyDefinition captured inside a transaction lambda. */
  private static final class PropertyDefinitionBeforeHardDelete {
    private RecordTemplate definition;
    private SystemMetadata metadata;
  }
}
