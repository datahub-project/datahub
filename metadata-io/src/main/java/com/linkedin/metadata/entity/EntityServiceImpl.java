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
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.dao.throttle.APIThrottle;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.validation.AspectDeletionRequest;
import com.linkedin.metadata.entity.validation.AspectValidationContext;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.event.EventProducer;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final Boolean alwaysEmitChangeLog;
  private final Boolean cdcModeChangeLog;
  @Nullable @Getter private SearchIndicesService updateIndicesService;
  private final PreProcessHooks preProcessHooks;
  protected static final int MAX_KEYS_PER_QUERY = 500;
  protected static final int MCP_SIDE_EFFECT_KAFKA_BATCH_SIZE = 500;

  private final Integer ebeanMaxTransactionRetry;
  private final boolean enableBrowseV2;

  @Getter
  private final Map<Set<ThrottleType>, ThrottleEvent> throttleEvents = new ConcurrentHashMap<>();

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      final boolean alwaysEmitChangeLog,
      final PreProcessHooks preProcessHooks,
      final boolean enableBrowsePathV2) {
    this(
        aspectDao,
        producer,
        alwaysEmitChangeLog,
        false,
        preProcessHooks,
        DEFAULT_MAX_TRANSACTION_RETRY,
        enableBrowsePathV2);
  }

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      final boolean alwaysEmitChangeLog,
      final boolean cdcModeChangeLog,
      final PreProcessHooks preProcessHooks,
      final boolean enableBrowsePathV2) {
    this(
        aspectDao,
        producer,
        alwaysEmitChangeLog,
        cdcModeChangeLog,
        preProcessHooks,
        DEFAULT_MAX_TRANSACTION_RETRY,
        enableBrowsePathV2);
  }

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      final boolean alwaysEmitChangeLog,
      final PreProcessHooks preProcessHooks,
      @Nullable final Integer retry,
      final boolean enableBrowsePathV2) {
    this(
        aspectDao,
        producer,
        alwaysEmitChangeLog,
        false,
        preProcessHooks,
        DEFAULT_MAX_TRANSACTION_RETRY,
        enableBrowsePathV2);
  }

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      final boolean alwaysEmitChangeLog,
      final boolean cdcModeChangeLog,
      final PreProcessHooks preProcessHooks,
      @Nullable final Integer retry,
      final boolean enableBrowseV2) {

    this.aspectDao = aspectDao;
    this.producer = producer;
    this.alwaysEmitChangeLog = alwaysEmitChangeLog;
    this.cdcModeChangeLog = cdcModeChangeLog;
    this.preProcessHooks = preProcessHooks;
    ebeanMaxTransactionRetry = retry != null ? retry : DEFAULT_MAX_TRANSACTION_RETRY;
    this.enableBrowseV2 = enableBrowseV2;
    log.info("EntityService cdcModeChangeLog is {}", this.cdcModeChangeLog);
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
  static SystemAspect applyUpsert(ChangeMCP changeMCP, SystemAspect latestAspect) {

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
        EntityUtils.toSystemAspects(opContext.getRetrieverContext(), batchGetResults.values());

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

    return EntityUtils.toSystemAspects(opContext.getRetrieverContext(), batchGetResults.values())
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

    version = calculateVersionNumber(urn, aspectName, version);
    final EntityAspectIdentifier primaryKey =
        new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect = Optional.ofNullable(aspectDao.getAspect(primaryKey));

    return Pair.of(
        EntityUtils.toSystemAspect(opContext.getRetrieverContext(), maybeAspect.orElse(null), false)
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
        aspectDao.listLatestAspectMetadata(entityName, aspectName, start, count);

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
        EntityUtils.toSystemAspects(opContext.getRetrieverContext(), entityAspects).stream()
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

    return updateAspectResults;
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

    Iterable<List<MCPItem>> iterable =
        () ->
            Iterators.partition(
                AspectsBatch.applyPostMCPSideEffects(batch, opContext.getRetrieverContext())
                    .iterator(),
                MCP_SIDE_EFFECT_KAFKA_BATCH_SIZE);
    StreamSupport.stream(iterable.spliterator(), false)
        .forEach(
            sideEffects -> {
              long count =
                  ingestProposalAsync(
                          opContext,
                          AspectsBatchImpl.builder()
                              .items(sideEffects)
                              .retrieverContext(opContext.getRetrieverContext())
                              .build(opContext))
                      .count();
              log.info("Generated {} MCP SideEffects for async processing", count);
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
            // Clear ThreadLocal at start of each request to ensure fresh state
            AspectValidationContext.clearPendingDeletions();

            if (inputBatch.containsDuplicateAspects()) {
              log.warn("Batch contains duplicates: {}", inputBatch.duplicateAspects());
              opContext
                  .getMetricUtils()
                  .ifPresent(
                      metricUtils ->
                          metricUtils.increment(
                              EntityServiceImpl.class, "batch_with_duplicate", 1));
            }

            IngestAspectsResult result =
                aspectDao
                    .runInTransactionWithRetry(
                        (txContext) -> {
                          // Generate default aspects within the transaction (they are re-calculated
                          // on
                          // retry)
                          AspectsBatch batchWithDefaults =
                              DefaultAspectsUtil.withAdditionalChanges(
                                  opContext, inputBatch, this, enableBrowseV2);

                          final Map<String, Set<String>> urnAspects =
                              batchWithDefaults.getUrnAspectsMap();

                          // read #1
                          // READ COMMITED is used in conjunction with SELECT FOR UPDATE (read lock)
                          // in
                          // order
                          // to ensure that the aspect's version is not modified outside the
                          // transaction.
                          // We rely on the retry mechanism if the row is modified and will re-read
                          // (require the
                          // lock)

                          // Initial database state from database
                          final Map<String, Map<String, SystemAspect>> batchAspects =
                              aspectDao.getLatestAspects(opContext, urnAspects, true);
                          final Map<String, Map<String, SystemAspect>> updatedLatestAspects;

                          // read #2 (potentially)
                          final Map<String, Map<String, Long>> nextVersions =
                              EntityUtils.calculateNextVersions(
                                  txContext, aspectDao, batchAspects, urnAspects);

                          // 1. Convert patches to full upserts
                          // 2. Run any entity/aspect level hooks
                          Pair<Map<String, Set<String>>, List<ChangeMCP>> updatedItems =
                              batchWithDefaults.toUpsertBatchItems(
                                  batchAspects, nextVersions, EntityServiceImpl::applyUpsert);

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
                                    txContext,
                                    aspectDao,
                                    updatedLatestAspects,
                                    updatedItems.getFirst());
                            // merge
                            updatedNextVersions = AspectsBatch.merge(nextVersions, newNextVersions);

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
                                                EntityServiceImpl::applyUpsert);
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
                                  changeMCPs, opContext.getRetrieverContext());

                          List<Pair<ChangeMCP, Set<AspectValidationException>>>
                              failedUpsertResults = new ArrayList<>();
                          if (exceptions.hasFatalExceptions()) {
                            // IF this is a client request/API request we fail the `transaction
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
                              collectMetrics(opContext.getMetricUtils().orElse(null), exceptions);
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
                          log.info(
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

                                        // eliminate unneeded writes within a batch if the latest
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
                                        }

                                        return null;
                                      })
                                  .filter(Objects::nonNull)
                                  .collect(Collectors.toList());

                          if (!upsertResults.isEmpty()) {
                            // commit upserts prior to retention or kafka send, if supported by impl
                            if (txContext != null) {
                              try {
                                txContext.commitAndContinue();
                              } catch (EntityNotFoundException e) {
                                if (e.getMessage() != null
                                    && e.getMessage().contains("No rows updated")) {
                                  log.warn(
                                      "Ignoring no rows updated condition for metadata update", e);
                                  opContext
                                      .getMetricUtils()
                                      .ifPresent(
                                          metricUtils ->
                                              metricUtils.increment(
                                                  EntityServiceImpl.class, "no_rows_updated", 1));
                                  return TransactionResult.rollback();
                                }
                                throw e;
                              }
                            }

                            // Retention optimization and tx
                            if (retentionService != null) {
                              opContext.withSpan(
                                  "retentionService",
                                  () -> {
                                    List<RetentionService.RetentionContext> retentionBatch =
                                        upsertResults.stream()
                                            // Only consider retention when there was a previous
                                            // version
                                            .filter(
                                                upsertResult ->
                                                    updatedLatestAspects.containsKey(
                                                            upsertResult.getUrn().toString())
                                                        && updatedLatestAspects
                                                            .get(upsertResult.getUrn().toString())
                                                            .containsKey(
                                                                upsertResult
                                                                    .getRequest()
                                                                    .getAspectName()))
                                            .filter(
                                                upsertResult -> {
                                                  RecordTemplate oldAspect =
                                                      upsertResult.getOldValue();
                                                  RecordTemplate newAspect =
                                                      upsertResult.getNewValue();
                                                  // Apply retention policies if there was an update
                                                  // to
                                                  // existing
                                                  // aspect
                                                  // value
                                                  return oldAspect != newAspect
                                                      && oldAspect != null
                                                      && retentionService != null;
                                                })
                                            .map(
                                                upsertResult ->
                                                    RetentionService.RetentionContext.builder()
                                                        .urn(upsertResult.getUrn())
                                                        .aspectName(
                                                            upsertResult
                                                                .getRequest()
                                                                .getAspectName())
                                                        .maxVersion(
                                                            Optional.of(
                                                                upsertResult.getMaxVersion()))
                                                        .build())
                                            .collect(Collectors.toList());
                                    retentionService.applyRetentionWithPolicyDefaults(
                                        opContext, retentionBatch);
                                  },
                                  BATCH_SIZE_ATTR,
                                  String.valueOf(upsertResults.size()));
                            } else {
                              log.warn("Retention service is missing!");
                            }
                          } else {
                            opContext
                                .getMetricUtils()
                                .ifPresent(
                                    metricUtils ->
                                        metricUtils.increment(
                                            EntityServiceImpl.class, "batch_empty_transaction", 1));
                            // This includes no-op batches. i.e. patch removing non-existent items
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
                        },
                        inputBatch,
                        ebeanMaxTransactionRetry)
                    .stream()
                    .reduce(IngestAspectsResult.EMPTY, IngestAspectsResult::combine);

            // After transaction commits, process any pending deletions collected during validation
            // This follows existing validation mechanics pattern - validation throws exceptions
            // during transaction, they're caught, and handled after tx commits what it can.
            List<AspectDeletionRequest> pendingDeletions =
                AspectValidationContext.getPendingDeletions();
            if (!pendingDeletions.isEmpty()) {
              processPendingDeletions(opContext, pendingDeletions);
            }

            return result;
          } finally {
            // Always cleanup ThreadLocal to prevent memory leaks when thread returns to pool
            AspectValidationContext.clearPendingDeletions();
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

        // Call proper deletion through EntityService
        // This handles all side effects: ES indices, graph edges, consumer hooks, system metadata
        this.deleteAspect(
            opContext, deletion.getUrn().toString(), deletion.getAspectName(), Map.of(), false);

      } catch (Exception e) {
        log.error(
            "Failed to delete oversized aspect: urn={}, aspect={}",
            deletion.getUrn(),
            deletion.getAspectName(),
            e);
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
                    .map(result -> result.getMetadataChangeLog().getEntityUrn())
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
    BulkApplyRetentionResult result = retentionService.batchApplyRetentionEntities(args);
    return result.toString();
  }

  boolean preprocessEvent(
      @Nonnull OperationContext opContext, MetadataChangeLog metadataChangeLog) {
    // Deletes cannot rely on System Metadata being passed through so can't always be determined by
    // system metadata,
    // for all other types of events should use system metadata rather than the boolean param.
    boolean isUISource =
        preProcessHooks.isUiEnabled()
            && metadataChangeLog.getSystemMetadata() != null
            && metadataChangeLog.getSystemMetadata().getProperties() != null
            && UI_SOURCE.equals(
                metadataChangeLog.getSystemMetadata().getProperties().get(APP_SOURCE));
    boolean syncIndexUpdate =
        metadataChangeLog.getHeaders() != null
            && metadataChangeLog
                .getHeaders()
                .getOrDefault(SYNC_INDEX_UPDATE_HEADER_NAME, "false")
                .equalsIgnoreCase(Boolean.toString(true));

    if (updateIndicesService != null && (isUISource || syncIndexUpdate)) {
      updateIndicesService.handleChangeEvent(opContext, metadataChangeLog);
      return true;
    }
    return false;
  }

  @Override
  public Integer getCountAspect(
      @Nonnull OperationContext opContext, @Nonnull String aspectName, @Nullable String urnLike) {
    return aspectDao.countAspect(aspectName, urnLike);
  }

  @Override
  public Integer countAspect(@Nonnull RestoreIndicesArgs args, @Nonnull Consumer<String> logger) {
    logger.accept(String.format("Args are %s", args));
    return aspectDao.countAspect(args);
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

    try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(args)) {
      return stream
          .partition(args.batchSize)
          .map(
              batch -> {
                long timeSqlQueryMs = System.currentTimeMillis() - startTime;

                try {
                  List<SystemAspect> systemAspects =
                      EntityUtils.toSystemAspectFromEbeanAspects(
                          opContext.getRetrieverContext(), batch.collect(Collectors.toList()));

                  RestoreIndicesResult result =
                      restoreIndices(opContext, systemAspects, logger, args.createDefaultAspects());
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
          .collect(Collectors.toList());
    }
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
        aspectDao.listUrns(entityName, keyAspectName, start, count);

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
      log.info(
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
        || Objects.equals(newAspect, oldAspect);
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

    log.info("Ingesting entity urn {} with system metadata {}", urn, systemMetadata.toString());

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
    final Map<EntityAspectIdentifier, EntityAspect> aspects = aspectDao.batchGet(dbKeys, forUpdate);
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
            List.of(deleteItem), opContext.getRetrieverContext(), opContext);
    if (!exceptions.isEmpty()) {
      throw new ValidationException(
          collectMetrics(opContext.getMetricUtils().orElse(null), exceptions).toString());
    }

    final RollbackResult result =
        aspectDao
            .runInTransactionWithRetry(
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
                  Pair<Long, Long> versionRange = aspectDao.getVersionRange(urn, aspectName);
                  long minVersion = Math.max(0, versionRange.getFirst());
                  long maxVersion = Math.max(0, versionRange.getSecond());

                  EntityAspect.EntitySystemAspect survivingAspect = null;

                  boolean filterMatch = true;
                  while (maxVersion > minVersion && filterMatch) {
                    EntityAspect.EntitySystemAspect candidateAspect =
                        (EntityAspect.EntitySystemAspect)
                            EntityUtils.toSystemAspect(
                                    opContext.getRetrieverContext(),
                                    aspectDao.getAspect(urn, aspectName, maxVersion),
                                    true)
                                .orElse(null);
                    SystemMetadata previousSysMetadata =
                        candidateAspect != null ? candidateAspect.getSystemMetadata() : null;
                    filterMatch =
                        previousSysMetadata != null && filterMatch(previousSysMetadata, conditions);
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
                          aspectsToDelete.stream()
                              .map(
                                  toDelete ->
                                      DeleteItemImpl.builder()
                                          .urn(toDelete.getUrn())
                                          .aspectName(toDelete.getAspectName())
                                          .auditStamp(auditStamp)
                                          .build(opContext.getAspectRetriever()))
                              .collect(Collectors.toList()),
                          opContext.getRetrieverContext());
                  if (!preCommitExceptions.isEmpty()) {
                    throw new ValidationException(
                        collectMetrics(opContext.getMetricUtils().orElse(null), preCommitExceptions)
                            .toString());
                  }

                  // 5. Apply deletes and fix up latest row
                  aspectsToDelete.forEach(
                      aspect ->
                          aspectDao.deleteAspect(
                              aspect.getUrn(), aspect.getAspectName(), aspect.getVersion()));

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
                        aspectDao.updateAspect(txContext, latest);

                    // metrics
                    aspectDao.incrementWriteMetrics(
                        opContext,
                        aspectName,
                        1,
                        survivingResult.map(r -> r.getMetadata().getBytes().length).orElse(0));

                    if (survivingAspect.getVersion() > 0) {
                      aspectDao.deleteAspect(
                          survivingAspect.getUrn(),
                          survivingAspect.getAspectName(),
                          survivingAspect.getVersion());
                    }
                  } else {
                    if (isKeyAspect) {
                      if (hardDelete) {
                        // If this is the key aspect, delete the entity entirely.
                        // If Using CDCs, need to ensure key aspect is the deleted last.
                        additionalRowsDeleted = aspectDao.deleteUrn(opContext, txContext, urn);
                      } else if (deleteItem
                          .getEntitySpec()
                          .hasAspect(Constants.STATUS_ASPECT_NAME)) {
                        // soft delete by setting status.removed=true (if applicable)
                        final Status statusAspect = new Status();
                        statusAspect.setRemoved(true);

                        final MetadataChangeProposal gmce = new MetadataChangeProposal();
                        gmce.setEntityUrn(entityUrn);
                        gmce.setChangeType(ChangeType.UPSERT);
                        gmce.setEntityType(entityUrn.getEntityType());
                        gmce.setAspectName(Constants.STATUS_ASPECT_NAME);
                        gmce.setAspect(GenericRecordUtils.serializeAspect(statusAspect));

                        this.ingestProposal(opContext, gmce, auditStamp, false);
                      }
                    } else {
                      // Else, only delete the specific aspect.
                      aspectDao.deleteAspect(
                          latest.getUrn(), latest.getAspectName(), latest.getVersion());
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

    if (result != null) {
      processPostCommitMCLSideEffects(opContext, List.of(result.toMCL(auditStamp)));
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
                batchGetResults.putAll(aspectDao.batchGet(ImmutableSet.copyOf(batch), forUpdate)));
    return batchGetResults;
  }

  /*
   * When a user tries to fetch a negative version, we want to index most recent to least recent snapshots.
   * To do this, we want to fetch the maximum version and subtract the negative version from that. Since -1 represents
   * the maximum version, we need to add 1 to the final result.
   */
  private long calculateVersionNumber(
      @Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {
    if (version < 0) {
      return aspectDao.getMaxVersion(urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  private Map<EntityAspectIdentifier, EnvelopedAspect> getEnvelopedAspects(
      @Nonnull OperationContext opContext, final Set<EntityAspectIdentifier> dbKeys) {
    final Map<EntityAspectIdentifier, EntityAspect> dbEntries = aspectDao.batchGet(dbKeys, false);

    List<SystemAspect> envelopedAspects =
        EntityUtils.toSystemAspects(opContext.getRetrieverContext(), dbEntries.values());

    return envelopedAspects.stream()
        .collect(
            Collectors.toMap(
                systemAspect ->
                    EntityAspectIdentifier.fromSystemEntityAspect(
                        (EntityAspect.EntitySystemAspect) systemAspect),
                systemAspect ->
                    ((EntityAspect.EntitySystemAspect) systemAspect).toEnvelopedAspects()));
  }

  /**
   * @param txContext Transaction context, keeps track of retries, exceptions etc.
   * @param writeItem The aspect being written
   * @param latestAspect The aspect as it exists in the database or was created/updated as part of
   *     the batch.
   * @return result object
   */
  @Nullable
  private UpdateAspectResult ingestAspectToLocalDB(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull final ChangeMCP writeItem,
      @Nullable SystemAspect latestAspect) {

    SystemAspect upsertAspect = applyUpsert(writeItem, latestAspect);

    // save to database
    Pair<Optional<EntityAspect>, Optional<EntityAspect>> result =
        aspectDao.saveLatestAspect(opContext, txContext, latestAspect, upsertAspect);
    Optional<EntityAspect> versionN = result.getFirst();
    Optional<EntityAspect> version0 = result.getSecond();

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
}
