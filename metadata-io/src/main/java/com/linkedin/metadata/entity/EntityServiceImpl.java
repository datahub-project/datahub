package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static com.linkedin.metadata.entity.TransactionContext.DEFAULT_MAX_TRANSACTION_RETRY;
import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;
import static com.linkedin.metadata.utils.PegasusUtils.getDataTemplateClassFromSchema;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;
import static com.linkedin.metadata.utils.metrics.ExceptionUtils.collectMetrics;

import com.codahale.metrics.Timer;
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
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.utils.DefaultAspectsUtil;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.dao.throttle.APIThrottle;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import com.linkedin.metadata.dao.throttle.ThrottleEvent;
import com.linkedin.metadata.dao.throttle.ThrottleType;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.DeleteItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.extension.annotations.WithSpan;
import jakarta.persistence.EntityNotFoundException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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
 * Change Log on ingestion using {@link #conditionallyProduceMCLAsync(OperationContext,
 * RecordTemplate, SystemMetadata, RecordTemplate, SystemMetadata, MetadataChangeProposal, Urn,
 * AuditStamp, AspectSpec)}.
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
  @Nullable @Getter private SearchIndicesService updateIndicesService;
  private final PreProcessHooks preProcessHooks;
  protected static final int MAX_KEYS_PER_QUERY = 500;
  protected static final int MCP_SIDE_EFFECT_KAFKA_BATCH_SIZE = 500;

  private final Integer ebeanMaxTransactionRetry;
  private final boolean enableBrowseV2;

  private static final long DB_TIMER_LOG_THRESHOLD_MS = 50;

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
      final boolean enableBrowseV2) {

    this.aspectDao = aspectDao;
    this.producer = producer;
    this.alwaysEmitChangeLog = alwaysEmitChangeLog;
    this.preProcessHooks = preProcessHooks;
    ebeanMaxTransactionRetry = retry != null ? retry : DEFAULT_MAX_TRANSACTION_RETRY;
    this.enableBrowseV2 = enableBrowseV2;
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
        EntityUtils.toSystemAspect(opContext.getRetrieverContext(), maybeAspect.orElse(null))
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
      final int count) {

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
            .build(),
        true,
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

    List<UpdateAspectResult> ingestResults =
        ingestAspectsToLocalDB(opContext, aspectsBatch, overwrite);

    List<UpdateAspectResult> mclResults = emitMCL(opContext, ingestResults, emitMCL);

    processPostCommitMCLSideEffects(
        opContext, mclResults.stream().map(UpdateAspectResult::toMCL).collect(Collectors.toList()));

    return mclResults;
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
                          AspectsBatchImpl.builder()
                              .items(sideEffects)
                              .retrieverContext(opContext.getRetrieverContext())
                              .build())
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
  private List<UpdateAspectResult> ingestAspectsToLocalDB(
      @Nonnull OperationContext opContext,
      @Nonnull final AspectsBatch inputBatch,
      boolean overwrite) {

    if (inputBatch.containsDuplicateAspects()) {
      log.warn("Batch contains duplicates: {}", inputBatch.duplicateAspects());
      MetricUtils.counter(EntityServiceImpl.class, "batch_with_duplicate").inc();
    }

    return aspectDao
        .runInTransactionWithRetry(
            (txContext) -> {
              // Generate default aspects within the transaction (they are re-calculated on retry)
              AspectsBatch batchWithDefaults =
                  DefaultAspectsUtil.withAdditionalChanges(
                      opContext, inputBatch, this, enableBrowseV2);

              // Read before write is unfortunate, however batch it
              final Map<String, Set<String>> urnAspects = batchWithDefaults.getUrnAspectsMap();

              // read #1
              // READ COMMITED is used in conjunction with SELECT FOR UPDATE (read lock) in order
              // to ensure that the aspect's version is not modified outside the transaction.
              // We rely on the retry mechanism if the row is modified and will re-read (require the
              // lock)
              Map<String, Map<String, EntityAspect>> databaseAspects =
                  aspectDao.getLatestAspects(urnAspects, true);

              final Map<String, Map<String, SystemAspect>> batchAspects =
                  EntityUtils.toSystemAspects(opContext.getRetrieverContext(), databaseAspects);

              // read #2 (potentially)
              final Map<String, Map<String, Long>> nextVersions =
                  EntityUtils.calculateNextVersions(txContext, aspectDao, batchAspects, urnAspects);

              // 1. Convert patches to full upserts
              // 2. Run any entity/aspect level hooks
              Pair<Map<String, Set<String>>, List<ChangeMCP>> updatedItems =
                  batchWithDefaults.toUpsertBatchItems(batchAspects, nextVersions);

              // Fetch additional information if needed
              final List<ChangeMCP> changeMCPs;

              if (!updatedItems.getFirst().isEmpty()) {
                // These items are new items from side effects
                Map<String, Set<String>> sideEffects = updatedItems.getFirst();

                final Map<String, Map<String, SystemAspect>> updatedLatestAspects;
                final Map<String, Map<String, Long>> updatedNextVersions;

                Map<String, Map<String, SystemAspect>> newLatestAspects =
                    EntityUtils.toSystemAspects(
                        opContext.getRetrieverContext(),
                        aspectDao.getLatestAspects(updatedItems.getFirst(), true));
                // merge
                updatedLatestAspects = AspectsBatch.merge(batchAspects, newLatestAspects);

                Map<String, Map<String, Long>> newNextVersions =
                    EntityUtils.calculateNextVersions(
                        txContext, aspectDao, updatedLatestAspects, updatedItems.getFirst());
                // merge
                updatedNextVersions = AspectsBatch.merge(nextVersions, newNextVersions);

                changeMCPs =
                    updatedItems.getSecond().stream()
                        .peek(
                            changeMCP -> {
                              // Add previous version to each side-effect
                              if (sideEffects
                                  .getOrDefault(
                                      changeMCP.getUrn().toString(), Collections.emptySet())
                                  .contains(changeMCP.getAspectName())) {

                                AspectsBatch.incrementBatchVersion(
                                    changeMCP, updatedLatestAspects, updatedNextVersions);
                              }
                            })
                        .collect(Collectors.toList());
              } else {
                changeMCPs = updatedItems.getSecond();
              }

              // No changes, return
              if (changeMCPs.isEmpty()) {
                MetricUtils.counter(EntityServiceImpl.class, "batch_empty").inc();
                return Collections.<UpdateAspectResult>emptyList();
              }

              // do final pre-commit checks with previous aspect value
              ValidationExceptionCollection exceptions =
                  AspectsBatch.validatePreCommit(changeMCPs, opContext.getRetrieverContext());

              if (exceptions.hasFatalExceptions()) {
                // IF this is a client request/API request we fail the `transaction batch`
                if (opContext.getRequestContext() != null) {
                  MetricUtils.counter(EntityServiceImpl.class, "batch_request_validation_exception")
                      .inc();
                  throw new ValidationException(collectMetrics(exceptions).toString());
                }

                MetricUtils.counter(EntityServiceImpl.class, "batch_consumer_validation_exception")
                    .inc();
                log.error("mce-consumer batch exceptions: {}", collectMetrics(exceptions));
              }

              // Database Upsert successfully validated results
              log.info(
                  "Ingesting aspects batch to database: {}",
                  AspectsBatch.toAbbreviatedString(changeMCPs, 2048));
              Timer.Context ingestToLocalDBTimer =
                  MetricUtils.timer(this.getClass(), "ingestAspectsToLocalDB").time();
              List<UpdateAspectResult> upsertResults =
                  exceptions
                      .streamSuccessful(changeMCPs.stream())
                      .map(
                          writeItem -> {

                            /*
                              database*Aspect - should be used for comparisons of before batch operation information
                            */
                            final EntityAspect databaseAspect =
                                databaseAspects
                                    .getOrDefault(writeItem.getUrn().toString(), Map.of())
                                    .get(writeItem.getAspectName());
                            final EntityAspect.EntitySystemAspect databaseSystemAspect =
                                databaseAspect == null
                                    ? null
                                    : EntityAspect.EntitySystemAspect.builder()
                                        .build(
                                            writeItem.getEntitySpec(),
                                            writeItem.getAspectSpec(),
                                            databaseAspect);

                            /*
                              This condition is specifically for an older conditional write ingestAspectIfNotPresent()
                              overwrite is always true otherwise
                            */
                            if (overwrite || databaseAspect == null) {
                              return Optional.ofNullable(
                                      ingestAspectToLocalDB(
                                          txContext, writeItem, databaseSystemAspect))
                                  .map(
                                      optResult -> optResult.toBuilder().request(writeItem).build())
                                  .orElse(null);
                            }

                            return null;
                          })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());

              if (!upsertResults.isEmpty()) {
                // commit upserts prior to retention or kafka send, if supported by impl
                if (txContext != null) {
                  txContext.commitAndContinue();
                }
                long took = TimeUnit.NANOSECONDS.toMillis(ingestToLocalDBTimer.stop());
                if (took > DB_TIMER_LOG_THRESHOLD_MS) {
                  log.info("Ingestion of aspects batch to database took {} ms", took);
                }

                // Retention optimization and tx
                if (retentionService != null) {
                  List<RetentionService.RetentionContext> retentionBatch =
                      upsertResults.stream()
                          // Only consider retention when there was a previous version
                          .filter(
                              result ->
                                  batchAspects.containsKey(result.getUrn().toString())
                                      && batchAspects
                                          .get(result.getUrn().toString())
                                          .containsKey(result.getRequest().getAspectName()))
                          .filter(
                              result -> {
                                RecordTemplate oldAspect = result.getOldValue();
                                RecordTemplate newAspect = result.getNewValue();
                                // Apply retention policies if there was an update to existing
                                // aspect
                                // value
                                return oldAspect != newAspect
                                    && oldAspect != null
                                    && retentionService != null;
                              })
                          .map(
                              result ->
                                  RetentionService.RetentionContext.builder()
                                      .urn(result.getUrn())
                                      .aspectName(result.getRequest().getAspectName())
                                      .maxVersion(Optional.of(result.getMaxVersion()))
                                      .build())
                          .collect(Collectors.toList());
                  retentionService.applyRetentionWithPolicyDefaults(opContext, retentionBatch);
                } else {
                  log.warn("Retention service is missing!");
                }
              } else {
                MetricUtils.counter(EntityServiceImpl.class, "batch_empty_transaction").inc();
                // This includes no-op batches. i.e. patch removing non-existent items
                log.debug("Empty transaction detected");
              }

              return upsertResults;
            },
            inputBatch,
            DEFAULT_MAX_TRANSACTION_RETRY)
        .stream()
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  @Nonnull
  private List<UpdateAspectResult> emitMCL(
      @Nonnull OperationContext opContext, List<UpdateAspectResult> sqlResults, boolean emitMCL) {
    List<UpdateAspectResult> withEmitMCL =
        sqlResults.stream()
            .map(result -> emitMCL ? conditionallyProduceMCLAsync(opContext, result) : result)
            .collect(Collectors.toList());

    // join futures messages, capture error state
    List<Pair<Boolean, UpdateAspectResult>> statusPairs =
        withEmitMCL.stream()
            .filter(result -> result.getMclFuture() != null)
            .map(
                result -> {
                  try {
                    result.getMclFuture().get();
                    return Pair.of(true, result);
                  } catch (InterruptedException | ExecutionException e) {
                    return Pair.of(false, result);
                  }
                })
            .collect(Collectors.toList());

    if (statusPairs.stream().anyMatch(p -> !p.getFirst())) {
      log.error(
          "Failed to produce MCLs: {}",
          statusPairs.stream()
              .filter(p -> !p.getFirst())
              .map(Pair::getValue)
              .map(v -> v.getRequest().toString())
              .collect(Collectors.toList()));
      // TODO restoreIndices?
      throw new RuntimeException("Failed to produce MCLs");
    }

    return withEmitMCL;
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
            .build();
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
                .build(),
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
        async ? ingestProposalAsync(aspectsBatch) : ingestProposalSync(opContext, aspectsBatch);

    return Stream.concat(nonTimeseriesIngestResults, timeseriesIngestResults)
        .collect(Collectors.toList());
  }

  /**
   * Timeseries is pass through to MCL, no MCP
   *
   * @param aspectsBatch timeseries upserts batch
   * @return returns ingest proposal result, however was never in the MCP topic
   */
  private Stream<IngestResult> ingestTimeseriesProposal(
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

    if (!async) {
      // Handle throttling
      APIThrottle.evaluate(opContext, new HashSet<>(throttleEvents.values()), true);

      // Create default non-timeseries aspects for timeseries aspects
      List<MCPItem> timeseriesKeyAspects =
          aspectsBatch.getMCPItems().stream()
              .filter(item -> item.getAspectSpec() != null && item.getAspectSpec().isTimeseries())
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

      ingestProposalSync(
          opContext,
          AspectsBatchImpl.builder()
              .retrieverContext(aspectsBatch.getRetrieverContext())
              .items(timeseriesKeyAspects)
              .build());
    }

    // Emit timeseries MCLs
    List<Pair<MCPItem, Optional<Pair<Future<?>, Boolean>>>> timeseriesResults =
        aspectsBatch.getItems().stream()
            .filter(item -> item.getAspectSpec() != null && item.getAspectSpec().isTimeseries())
            .map(item -> (MCPItem) item)
            .map(
                item ->
                    Pair.of(
                        item,
                        conditionallyProduceMCLAsync(
                            opContext,
                            null,
                            null,
                            item.getRecordTemplate(),
                            item.getSystemMetadata(),
                            item.getMetadataChangeProposal(),
                            item.getUrn(),
                            item.getAuditStamp(),
                            item.getAspectSpec())))
            .collect(Collectors.toList());

    return timeseriesResults.stream()
        .map(
            result -> {
              MCPItem item = result.getFirst();
              Optional<Pair<Future<?>, Boolean>> emissionStatus = result.getSecond();

              emissionStatus.ifPresent(
                  status -> {
                    try {
                      status.getFirst().get();
                    } catch (InterruptedException | ExecutionException e) {
                      throw new RuntimeException(e);
                    }
                  });

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
                  .publishedMCL(
                      emissionStatus.map(status -> status.getFirst() != null).orElse(false))
                  .processedMCL(emissionStatus.map(Pair::getSecond).orElse(false))
                  .build();
            });
  }

  /**
   * For async ingestion of non-timeseries, any change type
   *
   * @param aspectsBatch non-timeseries ingest aspects
   * @return produced items to the MCP topic
   */
  private Stream<IngestResult> ingestProposalAsync(AspectsBatch aspectsBatch) {
    List<? extends MCPItem> nonTimeseries =
        aspectsBatch.getMCPItems().stream()
            .filter(item -> item.getAspectSpec() == null || !item.getAspectSpec().isTimeseries())
            .collect(Collectors.toList());

    List<Future<?>> futures =
        nonTimeseries.stream()
            .map(
                item ->
                    // When async is turned on, we write to proposal log and return without waiting
                    producer.produceMetadataChangeProposal(
                        item.getUrn(), item.getMetadataChangeProposal()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    try {
      return nonTimeseries.stream()
          .map(
              item ->
                  IngestResult.<MCPItem>builder()
                      .urn(item.getUrn())
                      .request(item)
                      .publishedMCP(true)
                      .build());
    } finally {
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

  private Stream<IngestResult> ingestProposalSync(
      @Nonnull OperationContext opContext, AspectsBatch aspectsBatch) {

    AspectsBatchImpl nonTimeseries =
        AspectsBatchImpl.builder()
            .retrieverContext(aspectsBatch.getRetrieverContext())
            .items(
                aspectsBatch.getItems().stream()
                    .filter(item -> !item.getAspectSpec().isTimeseries())
                    .collect(Collectors.toList()))
            .build();

    List<? extends MCPItem> unsupported =
        nonTimeseries.getMCPItems().stream()
            .filter(item -> !MCPItem.isValidChangeType(item.getChangeType(), item.getAspectSpec()))
            .collect(Collectors.toList());
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException(
          "ChangeType not supported: "
              + unsupported.stream().map(item -> item.getChangeType()).collect(Collectors.toSet()));
    }

    List<UpdateAspectResult> upsertResults = ingestAspects(opContext, nonTimeseries, true, true);

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

  private boolean preprocessEvent(
      @Nonnull OperationContext opContext, MetadataChangeLog metadataChangeLog) {
    if (preProcessHooks.isUiEnabled()) {
      if (metadataChangeLog.getSystemMetadata() != null) {
        if (metadataChangeLog.getSystemMetadata().getProperties() != null) {
          if (UI_SOURCE.equals(
              metadataChangeLog.getSystemMetadata().getProperties().get(APP_SOURCE))) {
            // Pre-process the update indices hook for UI updates to avoid perceived lag from Kafka
            if (updateIndicesService != null) {
              updateIndicesService.handleChangeEvent(opContext, metadataChangeLog);
            }
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public Integer getCountAspect(
      @Nonnull OperationContext opContext, @Nonnull String aspectName, @Nullable String urnLike) {
    return aspectDao.countAspect(aspectName, urnLike);
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

                List<SystemAspect> systemAspects =
                    EntityUtils.toSystemAspectFromEbeanAspects(
                        opContext.getRetrieverContext(), batch.collect(Collectors.toList()));

                RestoreIndicesResult result = restoreIndices(opContext, systemAspects, logger);
                result.timeSqlQueryMs = timeSqlQueryMs;

                logger.accept("Batch completed.");
                try {
                  TimeUnit.MILLISECONDS.sleep(args.batchDelayMs);
                } catch (InterruptedException e) {
                  throw new RuntimeException(
                      "Thread interrupted while sleeping after successful batch migration.");
                }
                return result;
              })
          .collect(Collectors.toList());
    }
  }

  @Nonnull
  @Override
  public List<RestoreIndicesResult> restoreIndices(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nullable Set<String> inputAspectNames,
      @Nullable Integer inputBatchSize)
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

        RestoreIndicesResult result = restoreIndices(opContext, systemAspects, s -> {});
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
      @Nonnull Consumer<String> logger) {
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
                  .recordTemplate(EntityApiUtils.buildKeyAspect(opContext.getEntityRegistry(), urn))
                  .build(opContext.getAspectRetriever()));
      Stream<IngestResult> defaultAspectsResult =
          ingestProposalSync(
              opContext,
              AspectsBatchImpl.builder()
                  .retrieverContext(opContext.getRetrieverContext())
                  .items(keyAspect)
                  .build());
      defaultAspectsCreated += defaultAspectsResult.count();

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
      final int count) {
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
    Future<?> future = producer.produceMetadataChangeLog(urn, aspectSpec, metadataChangeLog);
    return Pair.of(future, preprocessEvent(opContext, metadataChangeLog));
  }

  @Override
  public Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue,
      @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata,
      @Nonnull AuditStamp auditStamp,
      @Nonnull final ChangeType changeType) {
    final MetadataChangeLog metadataChangeLog =
        constructMCL(
            null,
            entityName,
            urn,
            changeType,
            aspectName,
            auditStamp,
            newAspectValue,
            newSystemMetadata,
            oldAspectValue,
            oldSystemMetadata);
    return alwaysProduceMCLAsync(opContext, urn, aspectSpec, metadataChangeLog);
  }

  public Optional<Pair<Future<?>, Boolean>> conditionallyProduceMCLAsync(
      @Nonnull OperationContext opContext,
      @Nullable RecordTemplate oldAspect,
      @Nullable SystemMetadata oldSystemMetadata,
      RecordTemplate newAspect,
      SystemMetadata newSystemMetadata,
      @Nullable MetadataChangeProposal mcp,
      Urn entityUrn,
      AuditStamp auditStamp,
      AspectSpec aspectSpec) {
    boolean isNoOp = oldAspect == newAspect;
    if (!isNoOp || alwaysEmitChangeLog || shouldAspectEmitChangeLog(aspectSpec)) {
      log.info("Producing MCL for ingested aspect {}, urn {}", aspectSpec.getName(), entityUrn);

      final MetadataChangeLog metadataChangeLog =
          constructMCL(
              mcp,
              urnToEntityName(entityUrn),
              entityUrn,
              isNoOp ? ChangeType.RESTATE : ChangeType.UPSERT,
              aspectSpec.getName(),
              auditStamp,
              newAspect,
              newSystemMetadata,
              oldAspect,
              oldSystemMetadata);

      log.debug("Serialized MCL event: {}", metadataChangeLog);
      Pair<Future<?>, Boolean> emissionStatus =
          alwaysProduceMCLAsync(opContext, entityUrn, aspectSpec, metadataChangeLog);
      return emissionStatus.getFirst() != null ? Optional.of(emissionStatus) : Optional.empty();
    } else {
      log.info(
          "Skipped producing MCL for ingested aspect {}, urn {}. Aspect has not changed.",
          aspectSpec.getName(),
          entityUrn);
      return Optional.empty();
    }
  }

  private UpdateAspectResult conditionallyProduceMCLAsync(
      @Nonnull OperationContext opContext, UpdateAspectResult result) {
    ChangeMCP request = result.getRequest();
    Optional<Pair<Future<?>, Boolean>> emissionStatus =
        conditionallyProduceMCLAsync(
            opContext,
            result.getOldValue(),
            result.getOldSystemMetadata(),
            result.getNewValue(),
            result.getNewSystemMetadata(),
            request.getMetadataChangeProposal(),
            result.getUrn(),
            result.getAuditStamp(),
            request.getAspectSpec());

    return emissionStatus
        .map(
            status ->
                result.toBuilder()
                    .mclFuture(status.getFirst())
                    .processedMCL(status.getSecond())
                    .build())
        .orElse(result);
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
            .build();

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
        opContext, aspectRows, Collections.singletonMap("runId", runId), hardDelete);
  }

  @Override
  public RollbackRunResult rollbackWithConditions(
      @Nonnull OperationContext opContext,
      List<AspectRowSummary> aspectRows,
      Map<String, String> conditions,
      boolean hardDelete) {
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

    EntityAspect latestKey = null;
    try {
      latestKey = aspectDao.getLatestAspect(urn.toString(), keyAspectName, false);
    } catch (EntityNotFoundException e) {
      log.warn("Entity to delete does not exist. {}", urn.toString());
    }
    if (latestKey == null || latestKey.getSystemMetadata() == null) {
      return new RollbackRunResult(
          removedAspects, rowsDeletedFromEntityDeletion, removedAspectResults);
    }

    SystemMetadata latestKeySystemMetadata =
        EntityUtils.toSystemAspect(opContext.getRetrieverContext(), latestKey)
            .map(SystemAspect::getSystemMetadata)
            .get();
    RollbackResult result =
        deleteAspectWithoutMCL(
            opContext,
            urn.toString(),
            keyAspectName,
            Collections.singletonMap("runId", latestKeySystemMetadata.getRunId()),
            true);

    if (result != null) {
      AspectRowSummary summary = new AspectRowSummary();
      summary.setUrn(urn.toString());
      summary.setKeyAspect(true);
      summary.setAspectName(keyAspectName);
      summary.setVersion(0);
      summary.setTimestamp(latestKey.getCreatedOn().getTime());

      rowsDeletedFromEntityDeletion = result.additionalRowsAffected;
      removedAspects.add(summary);
      removedAspectResults.add(result);
      Future<?> future =
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
                  // TODO: Use a proper inferred audit stamp
                  createSystemAuditStamp(),
                  result.getChangeType())
              .getFirst();

      if (future != null) {
        try {
          future.get();
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
        AspectsBatch.validateProposed(List.of(deleteItem), opContext.getRetrieverContext());
    if (!exceptions.isEmpty()) {
      throw new ValidationException(collectMetrics(exceptions).toString());
    }

    final RollbackResult result =
        aspectDao.runInTransactionWithRetry(
            (txContext) -> {
              Integer additionalRowsDeleted = 0;

              // 1. Fetch the latest existing version of the aspect.
              final EntityAspect.EntitySystemAspect latest =
                  (EntityAspect.EntitySystemAspect)
                      EntityUtils.toSystemAspect(
                              opContext.getRetrieverContext(),
                              aspectDao.getLatestAspect(urn, aspectName, false))
                          .orElse(null);

              // 1.1 If no latest exists, skip this aspect
              if (latest == null) {
                return null;
              }

              // 2. Compare the match conditions, if they don't match, ignore.
              SystemMetadata latestSystemMetadata = latest.getSystemMetadata();
              if (!filterMatch(latestSystemMetadata, conditions)) {
                return null;
              }

              // 3. Check if this is a key aspect
              Boolean isKeyAspect = opContext.getKeyAspectName(entityUrn).equals(aspectName);

              // 4. Fetch all preceding aspects, that match
              List<EntityAspect> aspectsToDelete = new ArrayList<>();
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
                                aspectDao.getAspect(urn, aspectName, maxVersion))
                            .orElse(null);
                SystemMetadata previousSysMetadata =
                    candidateAspect != null ? candidateAspect.getSystemMetadata() : null;
                filterMatch =
                    previousSysMetadata != null && filterMatch(previousSysMetadata, conditions);
                if (filterMatch) {
                  aspectsToDelete.add(candidateAspect.getEntityAspect());
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
                                      .urn(UrnUtils.getUrn(toDelete.getUrn()))
                                      .aspectName(toDelete.getAspect())
                                      .auditStamp(auditStamp)
                                      .build(opContext.getAspectRetriever()))
                          .collect(Collectors.toList()),
                      opContext.getRetrieverContext());
              if (!preCommitExceptions.isEmpty()) {
                throw new ValidationException(collectMetrics(preCommitExceptions).toString());
              }

              // 5. Apply deletes and fix up latest row
              aspectsToDelete.forEach(aspect -> aspectDao.deleteAspect(txContext, aspect));

              if (survivingAspect != null) {
                // if there was a surviving aspect, copy its information into the latest row
                // eBean does not like us updating a pkey column (version) for the surviving aspect
                // as a result we copy information from survivingAspect to latest and delete
                // survivingAspect
                latest
                    .getEntityAspect()
                    .setMetadata(survivingAspect.getEntityAspect().getMetadata());
                latest
                    .getEntityAspect()
                    .setSystemMetadata(survivingAspect.getEntityAspect().getSystemMetadata());
                latest.getEntityAspect().setCreatedOn(survivingAspect.getCreatedOn());
                latest.getEntityAspect().setCreatedBy(survivingAspect.getCreatedBy());
                latest
                    .getEntityAspect()
                    .setCreatedFor(survivingAspect.getEntityAspect().getCreatedFor());
                aspectDao.saveAspect(txContext, latest.getEntityAspect(), false);
                // metrics
                aspectDao.incrementWriteMetrics(
                    aspectName, 1, latest.getMetadataRaw().getBytes(StandardCharsets.UTF_8).length);
                aspectDao.deleteAspect(txContext, survivingAspect.getEntityAspect());
              } else {
                if (isKeyAspect) {
                  if (hardDelete) {
                    // If this is the key aspect, delete the entity entirely.
                    additionalRowsDeleted = aspectDao.deleteUrn(txContext, urn);
                  } else if (deleteItem.getEntitySpec().hasAspect(Constants.STATUS_ASPECT_NAME)) {
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
                  aspectDao.deleteAspect(txContext, latest.getEntityAspect());
                }
              }

              // 6. Emit the Update
              try {
                final RecordTemplate latestValue =
                    latest == null ? null : latest.getRecordTemplate();
                final RecordTemplate previousValue =
                    survivingAspect == null ? null : latest.getRecordTemplate();

                final Urn urnObj = Urn.createFromString(urn);
                // We are not deleting key aspect if hardDelete has not been set so do not return a
                // rollback result
                if (isKeyAspect && !hardDelete) {
                  return null;
                }
                return new RollbackResult(
                    urnObj,
                    urnObj.getEntityType(),
                    latest.getAspectName(),
                    latestValue,
                    previousValue,
                    latestSystemMetadata,
                    previousValue == null ? null : survivingAspect.getSystemMetadata(),
                    survivingAspect == null ? ChangeType.DELETE : ChangeType.UPSERT,
                    isKeyAspect,
                    additionalRowsDeleted);
              } catch (URISyntaxException e) {
                throw new RuntimeException(
                    String.format("Failed to emit the update for urn %s", urn));
              } catch (IllegalStateException e) {
                log.warn(
                    "Unable to find aspect, rollback result will not be sent. Error: {}",
                    e.getMessage());
                return null;
              }
            },
            DEFAULT_MAX_TRANSACTION_RETRY);

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
   * @param databaseAspect The aspect as it exists in the database.
   * @return result object
   */
  @Nullable
  private UpdateAspectResult ingestAspectToLocalDB(
      @Nullable TransactionContext txContext,
      @Nonnull final ChangeMCP writeItem,
      @Nullable final EntityAspect.EntitySystemAspect databaseAspect) {

    if (writeItem.getRecordTemplate() == null) {
      log.error(
          "Unexpected write of null aspect with name {}, urn {}",
          writeItem.getAspectName(),
          writeItem.getUrn());
      return null;
    }

    // Set the "last run id" to be the run id provided with the new system metadata. This will be
    // stored in index
    // for all aspects that have a run id, regardless of whether they change.
    writeItem
        .getSystemMetadata()
        .setLastRunId(writeItem.getSystemMetadata().getRunId(GetMode.NULL), SetMode.IGNORE_NULL);

    // 2. Compare the latest existing and new.
    final EntityAspect.EntitySystemAspect previousBatchAspect =
        (EntityAspect.EntitySystemAspect) writeItem.getPreviousSystemAspect();
    final RecordTemplate previousValue =
        previousBatchAspect == null ? null : previousBatchAspect.getRecordTemplate();

    // 3. If there is no difference between existing and new, we just update
    // the lastObserved in system metadata. RunId should stay as the original runId
    if (previousValue != null
        && DataTemplateUtil.areEqual(previousValue, writeItem.getRecordTemplate())) {

      Optional<SystemMetadata> latestSystemMetadataDiff =
          systemMetadataDiff(
              txContext,
              writeItem.getUrn(),
              previousBatchAspect.getSystemMetadata(),
              writeItem.getSystemMetadata(),
              databaseAspect == null ? null : databaseAspect.getSystemMetadata());

      if (latestSystemMetadataDiff.isPresent()) {
        // Inserts & update order is not guaranteed, flush the insert for potential updates within
        // same tx
        if (databaseAspect == null && txContext != null) {
          conditionalLogLevel(
              txContext,
              String.format(
                  "Flushing for systemMetadata update aspect with name %s, urn %s",
                  writeItem.getAspectName(), writeItem.getUrn()));
          txContext.flush();
        }

        conditionalLogLevel(
            txContext,
            String.format(
                "Update aspect with name %s, urn %s, txContext: %s, databaseAspect: %s, newMetadata: %s newSystemMetadata: %s",
                previousBatchAspect.getAspectName(),
                previousBatchAspect.getUrn(),
                txContext != null,
                databaseAspect == null ? null : databaseAspect.getEntityAspect(),
                previousBatchAspect.getEntityAspect().getMetadata(),
                latestSystemMetadataDiff.get()));

        aspectDao.saveAspect(
            txContext,
            previousBatchAspect.getUrnRaw(),
            previousBatchAspect.getAspectName(),
            previousBatchAspect.getMetadataRaw(),
            previousBatchAspect.getCreatedBy(),
            null,
            previousBatchAspect.getCreatedOn(),
            RecordUtils.toJsonString(latestSystemMetadataDiff.get()),
            previousBatchAspect.getVersion(),
            false);

        // metrics
        aspectDao.incrementWriteMetrics(
            previousBatchAspect.getAspectName(),
            1,
            previousBatchAspect.getMetadataRaw().getBytes(StandardCharsets.UTF_8).length);

        return UpdateAspectResult.builder()
            .urn(writeItem.getUrn())
            .oldValue(previousValue)
            .newValue(previousValue)
            .oldSystemMetadata(previousBatchAspect.getSystemMetadata())
            .newSystemMetadata(latestSystemMetadataDiff.get())
            .operation(MetadataAuditOperation.UPDATE)
            .auditStamp(writeItem.getAuditStamp())
            .maxVersion(0)
            .build();
      } else {
        MetricUtils.counter(EntityServiceImpl.class, "batch_with_noop_sysmetadata").inc();
        return null;
      }
    }

    // 4. Save the newValue as the latest version
    if (writeItem.getRecordTemplate() != null
        && !DataTemplateUtil.areEqual(previousValue, writeItem.getRecordTemplate())) {
      conditionalLogLevel(
          txContext,
          String.format(
              "Insert aspect with name %s, urn %s", writeItem.getAspectName(), writeItem.getUrn()));

      // Inserts & update order is not guaranteed, flush the insert for potential updates within
      // same tx
      if (databaseAspect == null
          && !ASPECT_LATEST_VERSION.equals(writeItem.getNextAspectVersion())
          && txContext != null) {
        conditionalLogLevel(
            txContext,
            String.format(
                "Flushing for update aspect with name %s, urn %s",
                writeItem.getAspectName(), writeItem.getUrn()));
        txContext.flush();
      }

      String newValueStr = EntityApiUtils.toJsonAspect(writeItem.getRecordTemplate());
      long versionOfOld =
          aspectDao.saveLatestAspect(
              txContext,
              writeItem.getUrn().toString(),
              writeItem.getAspectName(),
              previousBatchAspect == null ? null : EntityApiUtils.toJsonAspect(previousValue),
              previousBatchAspect == null ? null : previousBatchAspect.getCreatedBy(),
              previousBatchAspect == null
                  ? null
                  : previousBatchAspect.getEntityAspect().getCreatedFor(),
              previousBatchAspect == null ? null : previousBatchAspect.getCreatedOn(),
              previousBatchAspect == null ? null : previousBatchAspect.getSystemMetadataRaw(),
              newValueStr,
              writeItem.getAuditStamp().getActor().toString(),
              writeItem.getAuditStamp().hasImpersonator()
                  ? writeItem.getAuditStamp().getImpersonator().toString()
                  : null,
              new Timestamp(writeItem.getAuditStamp().getTime()),
              EntityApiUtils.toJsonAspect(writeItem.getSystemMetadata()),
              writeItem.getNextAspectVersion());

      // metrics
      aspectDao.incrementWriteMetrics(
          writeItem.getAspectName(), 1, newValueStr.getBytes(StandardCharsets.UTF_8).length);

      return UpdateAspectResult.builder()
          .urn(writeItem.getUrn())
          .oldValue(previousValue)
          .newValue(writeItem.getRecordTemplate())
          .oldSystemMetadata(
              previousBatchAspect == null ? null : previousBatchAspect.getSystemMetadata())
          .newSystemMetadata(writeItem.getSystemMetadata())
          .operation(MetadataAuditOperation.UPDATE)
          .auditStamp(writeItem.getAuditStamp())
          .maxVersion(versionOfOld)
          .build();
    }

    return null;
  }

  private static boolean shouldAspectEmitChangeLog(@Nonnull final AspectSpec aspectSpec) {
    final List<RelationshipFieldSpec> relationshipFieldSpecs =
        aspectSpec.getRelationshipFieldSpecs();
    return relationshipFieldSpecs.stream().anyMatch(RelationshipFieldSpec::isLineageRelationship);
  }

  private static Optional<SystemMetadata> systemMetadataDiff(
      @Nullable TransactionContext txContext,
      @Nonnull Urn urn,
      @Nullable SystemMetadata previous,
      @Nonnull SystemMetadata current,
      @Nullable SystemMetadata database) {

    SystemMetadata latestSystemMetadata = GenericRecordUtils.copy(previous, SystemMetadata.class);

    latestSystemMetadata.setLastRunId(latestSystemMetadata.getRunId(), SetMode.REMOVE_IF_NULL);
    latestSystemMetadata.setLastObserved(current.getLastObserved(), SetMode.IGNORE_NULL);
    latestSystemMetadata.setRunId(current.getRunId(), SetMode.REMOVE_IF_NULL);

    if (!DataTemplateUtil.areEqual(latestSystemMetadata, previous)
        && !DataTemplateUtil.areEqual(latestSystemMetadata, database)) {

      conditionalLogLevel(
          txContext,
          String.format(
              "systemMetdataDiff urn %s, %s != %s AND %s",
              urn,
              RecordUtils.toJsonString(latestSystemMetadata),
              previous == null ? null : RecordUtils.toJsonString(previous),
              database == null ? null : RecordUtils.toJsonString(database)));

      return Optional.of(latestSystemMetadata);
    }

    return Optional.empty();
  }

  private static void conditionalLogLevel(@Nullable TransactionContext txContext, String message) {
    if (txContext != null && txContext.getFailedAttempts() > 1) {
      log.warn(message);
    } else {
      log.debug(message);
    }
  }
}
