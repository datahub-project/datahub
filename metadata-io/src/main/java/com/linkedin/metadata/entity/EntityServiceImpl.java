package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.BrowsePathUtils.*;
import static com.linkedin.metadata.utils.PegasusUtils.*;

import com.codahale.metrics.Timer;
import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.BrowsePathsV2;
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
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.transactions.PatchBatchItem;
import com.linkedin.metadata.entity.ebean.transactions.UpsertBatchItem;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.transactions.AbstractBatchItem;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.search.utils.BrowsePathV2Utils;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.ebean.PagedList;
import io.ebean.Transaction;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityNotFoundException;
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
 * Change Log on ingestion using {@link #conditionallyProduceMCLAsync(RecordTemplate,
 * SystemMetadata, RecordTemplate, SystemMetadata, MetadataChangeProposal, Urn, AuditStamp,
 * AspectSpec)}.
 *
 * <p>TODO: Consider whether we can abstract away virtual versioning semantics to subclasses of this
 * class.
 */
@Slf4j
public class EntityServiceImpl implements EntityService {

  /**
   * As described above, the latest version of an aspect should <b>always</b> take the value 0, with
   * monotonically increasing version incrementing as usual once the latest version is replaced.
   */
  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  protected final AspectDao _aspectDao;
  private final EventProducer _producer;
  private final EntityRegistry _entityRegistry;
  private final Map<String, Set<String>> _entityToValidAspects;
  private RetentionService _retentionService;
  private final Boolean _alwaysEmitChangeLog;
  @Getter private final UpdateIndicesService _updateIndicesService;
  private final PreProcessHooks _preProcessHooks;
  protected static final int MAX_KEYS_PER_QUERY = 500;

  private final Integer ebeanMaxTransactionRetry;

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      @Nonnull final EntityRegistry entityRegistry,
      final boolean alwaysEmitChangeLog,
      final UpdateIndicesService updateIndicesService,
      final PreProcessHooks preProcessHooks) {
    this(
        aspectDao,
        producer,
        entityRegistry,
        alwaysEmitChangeLog,
        updateIndicesService,
        preProcessHooks,
        DEFAULT_MAX_TRANSACTION_RETRY);
  }

  public EntityServiceImpl(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      @Nonnull final EntityRegistry entityRegistry,
      final boolean alwaysEmitChangeLog,
      final UpdateIndicesService updateIndicesService,
      final PreProcessHooks preProcessHooks,
      final Integer retry) {

    _aspectDao = aspectDao;
    _producer = producer;
    _entityRegistry = entityRegistry;
    _entityToValidAspects = buildEntityToValidAspects(entityRegistry);
    _alwaysEmitChangeLog = alwaysEmitChangeLog;
    _updateIndicesService = updateIndicesService;
    _preProcessHooks = preProcessHooks;
    ebeanMaxTransactionRetry = retry != null ? retry : DEFAULT_MAX_TRANSACTION_RETRY;
  }

  @Override
  public void setSystemEntityClient(SystemEntityClient systemEntityClient) {
    this._updateIndicesService.setSystemEntityClient(systemEntityClient);
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
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = getLatestAspect(urns, aspectNames);

    // Fetch from db and populate urn -> aspect map.
    final Map<Urn, List<RecordTemplate>> urnToAspects = new HashMap<>();

    // Each urn should have some result, regardless of whether aspects are found in the DB.
    for (Urn urn : urns) {
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
    }

    // Add "key" aspects for each urn. TODO: Replace this with a materialized key aspect.
    urnToAspects
        .keySet()
        .forEach(
            key -> {
              final RecordTemplate keyAspect = EntityUtils.buildKeyAspect(_entityRegistry, key);
              urnToAspects.get(key).add(keyAspect);
            });

    batchGetResults.forEach(
        (key, aspectEntry) -> {
          final Urn urn = toUrn(key.getUrn());
          final String aspectName = key.getAspect();
          // for now, don't add the key aspect here- we have already added it above
          if (aspectName.equals(getKeyAspectName(urn))) {
            return;
          }

          final RecordTemplate aspectRecord =
              EntityUtils.toAspectRecord(
                  urn, aspectName, aspectEntry.getMetadata(), getEntityRegistry());
          urnToAspects.putIfAbsent(urn, new ArrayList<>());
          urnToAspects.get(urn).add(aspectRecord);
        });

    return urnToAspects;
  }

  @Nonnull
  @Override
  public Map<String, RecordTemplate> getLatestAspectsForUrn(
      @Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    Map<EntityAspectIdentifier, EntityAspect> batchGetResults =
        getLatestAspect(new HashSet<>(Arrays.asList(urn)), aspectNames);

    final Map<String, RecordTemplate> result = new HashMap<>();
    batchGetResults.forEach(
        (key, aspectEntry) -> {
          final String aspectName = key.getAspect();
          final RecordTemplate aspectRecord =
              EntityUtils.toAspectRecord(
                  urn, aspectName, aspectEntry.getMetadata(), getEntityRegistry());
          result.put(aspectName, aspectRecord);
        });
    return result;
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
      @Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {

    log.debug(
        "Invoked getAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    version = calculateVersionNumber(urn, aspectName, version);
    final EntityAspectIdentifier primaryKey =
        new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect =
        Optional.ofNullable(_aspectDao.getAspect(primaryKey));
    return maybeAspect
        .map(
            aspect ->
                EntityUtils.toAspectRecord(
                    urn, aspectName, aspect.getMetadata(), getEntityRegistry()))
        .orElse(null);
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
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException {
    return getEntitiesV2(entityName, Collections.singleton(urn), aspectNames).get(urn);
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
  @Override
  public Map<Urn, EntityResponse> getEntitiesV2(
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames)
      throws URISyntaxException {
    return getLatestEnvelopedAspects(entityName, urns, aspectNames).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> toEntityResponse(entry.getKey(), entry.getValue())));
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
      @Nonnull final Set<VersionedUrn> versionedUrns, @Nonnull final Set<String> aspectNames)
      throws URISyntaxException {
    return getVersionedEnvelopedAspects(versionedUrns, aspectNames).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> toEntityResponse(entry.getKey(), entry.getValue())));
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  @Override
  public Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      // TODO: entityName is unused, can we remove this as a param?
      @Nonnull String entityName, @Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames)
      throws URISyntaxException {

    final Set<EntityAspectIdentifier> dbKeys =
        urns.stream()
            .map(
                urn ->
                    aspectNames.stream()
                        .map(
                            aspectName ->
                                new EntityAspectIdentifier(
                                    urn.toString(), aspectName, ASPECT_LATEST_VERSION))
                        .collect(Collectors.toList()))
            .flatMap(List::stream)
            .collect(Collectors.toSet());

    return getCorrespondingAspects(dbKeys, urns);
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
      @Nonnull Set<VersionedUrn> versionedUrns, @Nonnull Set<String> aspectNames)
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

    return getCorrespondingAspects(
        dbKeys,
        versionedUrns.stream()
            .map(versionedUrn -> versionedUrn.getUrn().toString())
            .map(UrnUtils::getUrn)
            .collect(Collectors.toSet()));
  }

  private Map<Urn, List<EnvelopedAspect>> getCorrespondingAspects(
      Set<EntityAspectIdentifier> dbKeys, Set<Urn> urns) {

    final Map<EntityAspectIdentifier, EnvelopedAspect> envelopedAspectMap =
        getEnvelopedAspects(dbKeys);

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
      EnvelopedAspect keyAspect = getKeyEnvelopedAspect(urn);
      // Add key aspect if it does not exist in the returned aspects
      if (aspects.isEmpty()
          || aspects.stream().noneMatch(aspect -> keyAspect.getName().equals(aspect.getName()))) {
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
      @Nonnull final String entityName, @Nonnull final Urn urn, @Nonnull final String aspectName)
      throws Exception {
    return getLatestEnvelopedAspects(entityName, ImmutableSet.of(urn), ImmutableSet.of(aspectName))
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
      @Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug(
        "Invoked getVersionedAspect with urn: {}, aspectName: {}, version: {}",
        urn,
        aspectName,
        version);

    VersionedAspect result = new VersionedAspect();

    version = calculateVersionNumber(urn, aspectName, version);

    final EntityAspectIdentifier primaryKey =
        new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect =
        Optional.ofNullable(_aspectDao.getAspect(primaryKey));
    RecordTemplate aspectRecord =
        maybeAspect
            .map(
                aspect ->
                    EntityUtils.toAspectRecord(
                        urn, aspectName, aspect.getMetadata(), getEntityRegistry()))
            .orElse(null);

    if (aspectRecord == null) {
      return null;
    }

    Aspect resultAspect = new Aspect();

    RecordUtils.setSelectedRecordTemplateInUnion(resultAspect, aspectRecord);
    result.setAspect(resultAspect);
    result.setVersion(version);

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
        _aspectDao.listLatestAspectMetadata(entityName, aspectName, start, count);

    final List<RecordTemplate> aspects = new ArrayList<>();
    for (int i = 0; i < aspectMetadataList.getValues().size(); i++) {
      aspects.add(
          EntityUtils.toAspectRecord(
              aspectMetadataList.getMetadata().getExtraInfos().get(i).getUrn(),
              aspectName,
              aspectMetadataList.getValues().get(i),
              getEntityRegistry()));
    }

    return new ListResult<>(
        aspects,
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
      @Nonnull Urn entityUrn,
      List<Pair<String, RecordTemplate>> pairList,
      @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    List<? extends AbstractBatchItem> items =
        pairList.stream()
            .map(
                pair ->
                    UpsertBatchItem.builder()
                        .urn(entityUrn)
                        .aspectName(pair.getKey())
                        .aspect(pair.getValue())
                        .systemMetadata(systemMetadata)
                        .build(_entityRegistry))
            .collect(Collectors.toList());
    return ingestAspects(AspectsBatchImpl.builder().items(items).build(), auditStamp, true, true);
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link
   * com.linkedin.mxe.MetadataChangeLog}.
   *
   * @param aspectsBatch aspects to write
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param emitMCL whether a {@link com.linkedin.mxe.MetadataChangeLog} should be emitted in
   *     correspondence upon successful update
   * @return the {@link RecordTemplate} representation of the written aspect object
   */
  @Override
  public List<UpdateAspectResult> ingestAspects(
      @Nonnull final AspectsBatch aspectsBatch,
      @Nonnull final AuditStamp auditStamp,
      boolean emitMCL,
      boolean overwrite) {

    Timer.Context ingestToLocalDBTimer =
        MetricUtils.timer(this.getClass(), "ingestAspectsToLocalDB").time();
    List<UpdateAspectResult> ingestResults =
        ingestAspectsToLocalDB(aspectsBatch, auditStamp, overwrite);
    List<UpdateAspectResult> mclResults = emitMCL(ingestResults, emitMCL);
    ingestToLocalDBTimer.stop();

    return mclResults;
  }

  /**
   * Checks whether there is an actual update to the aspect by applying the updateLambda If there is
   * an update, push the new version into the local DB. Otherwise, do not push the new version, but
   * just update the system metadata.
   *
   * @param aspectsBatch Collection of the following: an urn associated with the new aspect, name of
   *     the aspect being inserted, and a function to apply to the latest version of the aspect to
   *     get the updated version
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @return Details about the new and old version of the aspect
   */
  @Nonnull
  private List<UpdateAspectResult> ingestAspectsToLocalDB(
      @Nonnull final AspectsBatch aspectsBatch,
      @Nonnull final AuditStamp auditStamp,
      boolean overwrite) {

    if (aspectsBatch.containsDuplicateAspects()) {
      log.warn(String.format("Batch contains duplicates: %s", aspectsBatch));
    }

    return _aspectDao.runInTransactionWithRetry(
        (tx) -> {
          // Read before write is unfortunate, however batch it
          Map<String, Set<String>> urnAspects = aspectsBatch.getUrnAspectsMap();
          // read #1
          Map<String, Map<String, EntityAspect>> latestAspects =
              _aspectDao.getLatestAspects(urnAspects);
          // read #2
          Map<String, Map<String, Long>> nextVersions = _aspectDao.getNextVersions(urnAspects);

          List<UpsertBatchItem> items =
              aspectsBatch.getItems().stream()
                  .map(
                      item -> {
                        if (item instanceof UpsertBatchItem) {
                          return (UpsertBatchItem) item;
                        } else {
                          // patch to upsert
                          PatchBatchItem patchBatchItem = (PatchBatchItem) item;
                          final String urnStr = patchBatchItem.getUrn().toString();
                          final EntityAspect latest =
                              latestAspects
                                  .getOrDefault(urnStr, Map.of())
                                  .get(patchBatchItem.getAspectName());
                          final RecordTemplate currentValue =
                              latest != null
                                  ? EntityUtils.toAspectRecord(
                                      patchBatchItem.getUrn(),
                                      patchBatchItem.getAspectName(),
                                      latest.getMetadata(),
                                      _entityRegistry)
                                  : null;
                          return patchBatchItem.applyPatch(_entityRegistry, currentValue);
                        }
                      })
                  .collect(Collectors.toList());

          // Database Upsert results
          List<UpdateAspectResult> upsertResults =
              items.stream()
                  .map(
                      item -> {
                        final String urnStr = item.getUrn().toString();
                        final EntityAspect latest =
                            latestAspects.getOrDefault(urnStr, Map.of()).get(item.getAspectName());
                        final long nextVersion =
                            nextVersions
                                .getOrDefault(urnStr, Map.of())
                                .getOrDefault(item.getAspectName(), 0L);

                        final UpdateAspectResult result;
                        if (overwrite || latest == null) {
                          result =
                              ingestAspectToLocalDB(
                                      tx,
                                      item.getUrn(),
                                      item.getAspectName(),
                                      item.getAspect(),
                                      auditStamp,
                                      item.getSystemMetadata(),
                                      latest,
                                      nextVersion)
                                  .toBuilder()
                                  .request(item)
                                  .build();

                          // support inner-batch upserts
                          latestAspects
                              .computeIfAbsent(urnStr, key -> new HashMap<>())
                              .put(item.getAspectName(), item.toLatestEntityAspect(auditStamp));
                          nextVersions
                              .computeIfAbsent(urnStr, key -> new HashMap<>())
                              .put(item.getAspectName(), nextVersion + 1);
                        } else {
                          RecordTemplate oldValue =
                              EntityUtils.toAspectRecord(
                                  item.getUrn().getEntityType(),
                                  item.getAspectName(),
                                  latest.getMetadata(),
                                  getEntityRegistry());
                          SystemMetadata oldMetadata =
                              EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
                          result =
                              UpdateAspectResult.builder()
                                  .urn(item.getUrn())
                                  .request(item)
                                  .oldValue(oldValue)
                                  .newValue(oldValue)
                                  .oldSystemMetadata(oldMetadata)
                                  .newSystemMetadata(oldMetadata)
                                  .operation(MetadataAuditOperation.UPDATE)
                                  .auditStamp(auditStamp)
                                  .maxVersion(latest.getVersion())
                                  .build();
                        }

                        return result;
                      })
                  .collect(Collectors.toList());

          // commit upserts prior to retention or kafka send, if supported by impl
          if (tx != null) {
            tx.commitAndContinue();
          }

          // Retention optimization and tx
          if (_retentionService != null) {
            List<RetentionService.RetentionContext> retentionBatch =
                upsertResults.stream()
                    // Only consider retention when there was a previous version
                    .filter(
                        result ->
                            latestAspects.containsKey(result.getUrn().toString())
                                && latestAspects
                                    .get(result.getUrn().toString())
                                    .containsKey(result.getRequest().getAspectName()))
                    .filter(
                        result -> {
                          RecordTemplate oldAspect = result.getOldValue();
                          RecordTemplate newAspect = result.getNewValue();
                          // Apply retention policies if there was an update to existing aspect
                          // value
                          return oldAspect != newAspect
                              && oldAspect != null
                              && _retentionService != null;
                        })
                    .map(
                        result ->
                            RetentionService.RetentionContext.builder()
                                .urn(result.getUrn())
                                .aspectName(result.getRequest().getAspectName())
                                .maxVersion(Optional.of(result.getMaxVersion()))
                                .build())
                    .collect(Collectors.toList());
            _retentionService.applyRetentionWithPolicyDefaults(retentionBatch);
          } else {
            log.warn("Retention service is missing!");
          }

          return upsertResults;
        },
        aspectsBatch,
        DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  private List<UpdateAspectResult> emitMCL(List<UpdateAspectResult> sqlResults, boolean emitMCL) {
    List<UpdateAspectResult> withEmitMCL =
        sqlResults.stream()
            .map(result -> emitMCL ? conditionallyProduceMCLAsync(result) : result)
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
   */
  @Nullable
  @Override
  public RecordTemplate ingestAspectIfNotPresent(
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
                UpsertBatchItem.builder()
                    .urn(urn)
                    .aspectName(aspectName)
                    .aspect(newValue)
                    .systemMetadata(systemMetadata)
                    .build(_entityRegistry))
            .build();
    List<UpdateAspectResult> ingested = ingestAspects(aspectsBatch, auditStamp, true, false);

    return ingested.stream().findFirst().get().getNewValue();
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
      MetadataChangeProposal proposal, AuditStamp auditStamp, final boolean async) {
    return ingestProposal(
            AspectsBatchImpl.builder().mcps(List.of(proposal), getEntityRegistry()).build(),
            auditStamp,
            async)
        .stream()
        .findFirst()
        .get();
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
   * @param auditStamp an audit stamp representing the time and actor proposing the change
   * @param async a flag to control whether we commit to primary store or just write to proposal log
   *     before returning
   * @return an {@link IngestResult} containing the results
   */
  @Override
  public Set<IngestResult> ingestProposal(
      AspectsBatch aspectsBatch, AuditStamp auditStamp, final boolean async) {

    Stream<IngestResult> timeseriesIngestResults =
        ingestTimeseriesProposal(aspectsBatch, auditStamp);
    Stream<IngestResult> nonTimeseriesIngestResults =
        async ? ingestProposalAsync(aspectsBatch) : ingestProposalSync(aspectsBatch, auditStamp);

    return Stream.concat(timeseriesIngestResults, nonTimeseriesIngestResults)
        .collect(Collectors.toSet());
  }

  /**
   * Timeseries is pass through to MCL, no MCP
   *
   * @param aspectsBatch timeseries upserts batch
   * @param auditStamp provided audit information
   * @return returns ingest proposal result, however was never in the MCP topic
   */
  private Stream<IngestResult> ingestTimeseriesProposal(
      AspectsBatch aspectsBatch, AuditStamp auditStamp) {
    List<? extends AbstractBatchItem> unsupported =
        aspectsBatch.getItems().stream()
            .filter(
                item ->
                    item.getAspectSpec().isTimeseries()
                        && item.getChangeType() != ChangeType.UPSERT)
            .collect(Collectors.toList());
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException(
          "ChangeType not supported: "
              + unsupported.stream()
                  .map(AbstractBatchItem::getChangeType)
                  .collect(Collectors.toSet()));
    }

    List<Pair<UpsertBatchItem, Optional<Pair<Future<?>, Boolean>>>> timeseriesResults =
        aspectsBatch.getItems().stream()
            .filter(item -> item.getAspectSpec().isTimeseries())
            .map(item -> (UpsertBatchItem) item)
            .map(
                item ->
                    Pair.of(
                        item,
                        conditionallyProduceMCLAsync(
                            null,
                            null,
                            item.getAspect(),
                            item.getSystemMetadata(),
                            item.getMetadataChangeProposal(),
                            item.getUrn(),
                            auditStamp,
                            item.getAspectSpec())))
            .collect(Collectors.toList());

    return timeseriesResults.stream()
        .map(
            result -> {
              Optional<Pair<Future<?>, Boolean>> emissionStatus = result.getSecond();

              emissionStatus.ifPresent(
                  status -> {
                    try {
                      status.getFirst().get();
                    } catch (InterruptedException | ExecutionException e) {
                      throw new RuntimeException(e);
                    }
                  });

              UpsertBatchItem request = result.getFirst();
              return IngestResult.builder()
                  .urn(request.getUrn())
                  .request(request)
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
    List<? extends AbstractBatchItem> nonTimeseries =
        aspectsBatch.getItems().stream()
            .filter(item -> !item.getAspectSpec().isTimeseries())
            .collect(Collectors.toList());

    List<Future<?>> futures =
        nonTimeseries.stream()
            .map(
                item ->
                    // When async is turned on, we write to proposal log and return without waiting
                    _producer.produceMetadataChangeProposal(
                        item.getUrn(), item.getMetadataChangeProposal()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    try {
      return nonTimeseries.stream()
          .map(
              item ->
                  IngestResult.builder()
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
      AspectsBatch aspectsBatch, AuditStamp auditStamp) {
    AspectsBatchImpl nonTimeseries =
        AspectsBatchImpl.builder()
            .items(
                aspectsBatch.getItems().stream()
                    .filter(item -> !item.getAspectSpec().isTimeseries())
                    .collect(Collectors.toList()))
            .build();

    List<? extends AbstractBatchItem> unsupported =
        nonTimeseries.getItems().stream()
            .filter(
                item ->
                    item.getMetadataChangeProposal().getChangeType() != ChangeType.PATCH
                        && item.getMetadataChangeProposal().getChangeType() != ChangeType.UPSERT)
            .collect(Collectors.toList());
    if (!unsupported.isEmpty()) {
      throw new UnsupportedOperationException(
          "ChangeType not supported: "
              + unsupported.stream()
                  .map(item -> item.getMetadataChangeProposal().getChangeType())
                  .collect(Collectors.toSet()));
    }

    List<UpdateAspectResult> upsertResults = ingestAspects(nonTimeseries, auditStamp, true, true);

    return upsertResults.stream()
        .map(
            result -> {
              AbstractBatchItem item = result.getRequest();

              return IngestResult.builder()
                  .urn(item.getUrn())
                  .request(item)
                  .publishedMCL(result.getMclFuture() != null)
                  .sqlCommitted(true)
                  .isUpdate(result.getOldValue() != null)
                  .build();
            });
  }

  @Override
  public String batchApplyRetention(
      Integer start, Integer count, Integer attemptWithVersion, String aspectName, String urn) {
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
    BulkApplyRetentionResult result = _retentionService.batchApplyRetentionEntities(args);
    return result.toString();
  }

  private boolean preprocessEvent(MetadataChangeLog metadataChangeLog) {
    if (_preProcessHooks.isUiEnabled()) {
      if (metadataChangeLog.getSystemMetadata() != null) {
        if (metadataChangeLog.getSystemMetadata().getProperties() != null) {
          if (UI_SOURCE.equals(
              metadataChangeLog.getSystemMetadata().getProperties().get(APP_SOURCE))) {
            // Pre-process the update indices hook for UI updates to avoid perceived lag from Kafka
            _updateIndicesService.handleChangeEvent(metadataChangeLog);
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public Integer getCountAspect(@Nonnull String aspectName, @Nullable String urnLike) {
    return _aspectDao.countAspect(aspectName, urnLike);
  }

  @Nonnull
  @Override
  public RestoreIndicesResult restoreIndices(
      @Nonnull RestoreIndicesArgs args, @Nonnull Consumer<String> logger) {
    RestoreIndicesResult result = new RestoreIndicesResult();
    int ignored = 0;
    int rowsMigrated = 0;
    logger.accept(String.format("Args are %s", args));
    logger.accept(
        String.format(
            "Reading rows %s through %s from the aspects table started.",
            args.start, args.start + args.batchSize));
    long startTime = System.currentTimeMillis();
    PagedList<EbeanAspectV2> rows = _aspectDao.getPagedAspects(args);
    result.timeSqlQueryMs = System.currentTimeMillis() - startTime;
    startTime = System.currentTimeMillis();
    logger.accept(
        String.format(
            "Reading rows %s through %s from the aspects table completed.",
            args.start, args.start + args.batchSize));

    LinkedList<Future<?>> futures = new LinkedList<>();

    for (EbeanAspectV2 aspect : rows != null ? rows.getList() : List.<EbeanAspectV2>of()) {
      // 1. Extract an Entity type from the entity Urn
      result.timeGetRowMs = System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      Urn urn;
      try {
        urn = Urn.createFromString(aspect.getKey().getUrn());
      } catch (Exception e) {
        logger.accept(
            String.format(
                "Failed to bind Urn with value %s into Urn object: %s. Ignoring row.",
                aspect.getKey().getUrn(), e));
        ignored = ignored + 1;
        continue;
      }
      result.timeUrnMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // 2. Verify that the entity associated with the aspect is found in the registry.
      final String entityName = urn.getEntityType();
      final EntitySpec entitySpec;
      try {
        entitySpec = _entityRegistry.getEntitySpec(entityName);
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
      final String aspectName = aspect.getKey().getAspect();

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
        aspectRecord =
            EntityUtils.toAspectRecord(
                entityName, aspectName, aspect.getMetadata(), _entityRegistry);
      } catch (Exception e) {
        logger.accept(
            String.format(
                "Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
                aspect.getMetadata(), entityName, aspectName, e));
        ignored = ignored + 1;
        continue;
      }
      result.createRecordMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // Force indexing to skip diff mode and fix error states
      SystemMetadata latestSystemMetadata =
          EntityUtils.parseSystemMetadata(aspect.getSystemMetadata());
      StringMap properties =
          latestSystemMetadata.getProperties() != null
              ? latestSystemMetadata.getProperties()
              : new StringMap();
      properties.put(FORCE_INDEXING_KEY, Boolean.TRUE.toString());
      latestSystemMetadata.setProperties(properties);

      // 5. Produce MAE events for the aspect record
      futures.add(
          alwaysProduceMCLAsync(
                  urn,
                  entityName,
                  aspectName,
                  aspectSpec,
                  null,
                  aspectRecord,
                  null,
                  latestSystemMetadata,
                  new AuditStamp()
                      .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
                      .setTime(System.currentTimeMillis()),
                  ChangeType.RESTATE)
              .getFirst());
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
    try {
      TimeUnit.MILLISECONDS.sleep(args.batchDelayMs);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Thread interrupted while sleeping after successful batch migration.");
    }
    result.ignored = ignored;
    result.rowsMigrated = rowsMigrated;
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
      @Nonnull final String entityName, final int start, final int count) {
    log.debug(
        "Invoked listUrns with entityName: {}, start: {}, count: {}", entityName, start, count);

    // If a keyAspect exists, the entity exists.
    final String keyAspectName =
        getEntityRegistry().getEntitySpec(entityName).getKeyAspectSpec().getName();
    final ListResult<String> keyAspectList =
        _aspectDao.listUrns(entityName, keyAspectName, start, count);

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
  public Entity getEntity(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    return getEntities(Collections.singleton(urn), aspectNames).values().stream()
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
      @Nonnull final Set<Urn> urns, @Nonnull Set<String> aspectNames) {
    log.debug("Invoked getEntities with urns {}, aspects {}", urns, aspectNames);
    if (urns.isEmpty()) {
      return Collections.emptyMap();
    }
    return getSnapshotUnions(urns, aspectNames).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toEntity(entry.getValue())));
  }

  @Override
  public Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    Future<?> future = _producer.produceMetadataChangeLog(urn, aspectSpec, metadataChangeLog);
    return Pair.of(future, preprocessEvent(metadataChangeLog));
  }

  @Override
  public Pair<Future<?>, Boolean> alwaysProduceMCLAsync(
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
    return alwaysProduceMCLAsync(urn, aspectSpec, metadataChangeLog);
  }

  public Optional<Pair<Future<?>, Boolean>> conditionallyProduceMCLAsync(
      @Nullable RecordTemplate oldAspect,
      @Nullable SystemMetadata oldSystemMetadata,
      RecordTemplate newAspect,
      SystemMetadata newSystemMetadata,
      @Nullable MetadataChangeProposal mcp,
      Urn entityUrn,
      AuditStamp auditStamp,
      AspectSpec aspectSpec) {
    boolean isNoOp = oldAspect == newAspect;
    if (!isNoOp || _alwaysEmitChangeLog || shouldAspectEmitChangeLog(aspectSpec)) {
      log.debug(
          "Producing MetadataChangeLog for ingested aspect {}, urn {}",
          aspectSpec.getName(),
          entityUrn);

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
          alwaysProduceMCLAsync(entityUrn, aspectSpec, metadataChangeLog);
      return emissionStatus.getFirst() != null ? Optional.of(emissionStatus) : Optional.empty();
    } else {
      log.debug(
          "Skipped producing MetadataChangeLog for ingested aspect {}, urn {}. Aspect has not changed.",
          aspectSpec.getName(),
          entityUrn);
      return Optional.empty();
    }
  }

  private UpdateAspectResult conditionallyProduceMCLAsync(UpdateAspectResult result) {
    AbstractBatchItem request = result.getRequest();
    Optional<Pair<Future<?>, Boolean>> emissionStatus =
        conditionallyProduceMCLAsync(
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
  public RecordTemplate getLatestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName) {
    log.debug("Invoked getLatestAspect with urn {}, aspect {}", urn, aspectName);
    return getAspect(urn, aspectName, ASPECT_LATEST_VERSION);
  }

  @Override
  public void ingestEntities(
      @Nonnull final List<Entity> entities,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata) {
    log.debug("Invoked ingestEntities with entities {}, audit stamp {}", entities, auditStamp);
    Streams.zip(
            entities.stream(),
            systemMetadata.stream(),
            (a, b) -> new Pair<Entity, SystemMetadata>(a, b))
        .forEach(pair -> ingestEntity(pair.getFirst(), auditStamp, pair.getSecond()));
  }

  @Override
  public SystemMetadata ingestEntity(Entity entity, AuditStamp auditStamp) {
    SystemMetadata generatedSystemMetadata = new SystemMetadata();
    generatedSystemMetadata.setRunId(DEFAULT_RUN_ID);
    generatedSystemMetadata.setLastObserved(System.currentTimeMillis());

    ingestEntity(entity, auditStamp, generatedSystemMetadata);
    return generatedSystemMetadata;
  }

  @Override
  public void ingestEntity(
      @Nonnull Entity entity,
      @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata) {
    log.debug(
        "Invoked ingestEntity with entity {}, audit stamp {} systemMetadata {}",
        entity,
        auditStamp,
        systemMetadata.toString());
    ingestSnapshotUnion(entity.getValue(), auditStamp, systemMetadata);
  }

  @Nonnull
  protected Map<Urn, Snapshot> getSnapshotUnions(
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return getSnapshotRecords(urns, aspectNames).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toSnapshotUnion(entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, RecordTemplate> getSnapshotRecords(
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return getLatestAspectUnions(urns, aspectNames).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, entry -> toSnapshotRecord(entry.getKey(), entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, List<UnionTemplate>> getLatestAspectUnions(
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return getLatestAspects(urns, aspectNames).entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(aspectRecord -> toAspectUnion(entry.getKey(), aspectRecord))
                        .collect(Collectors.toList())));
  }

  /**
   * Returns true if entityType should have some aspect as per its definition but aspects given does
   * not have that aspect
   */
  private boolean isAspectMissing(String entityType, String aspectName, Set<String> aspects) {
    return _entityRegistry.getEntitySpec(entityType).getAspectSpecMap().containsKey(aspectName)
        && !aspects.contains(aspectName);
  }

  @Override
  public Pair<Boolean, List<Pair<String, RecordTemplate>>> generateDefaultAspectsOnFirstWrite(
      @Nonnull final Urn urn, Map<String, RecordTemplate> includedAspects) {
    List<Pair<String, RecordTemplate>> returnAspects = new ArrayList<>();

    final String keyAspectName = getKeyAspectName(urn);
    final Map<String, RecordTemplate> latestAspects =
        new HashMap<>(getLatestAspectsForUrn(urn, Set.of(keyAspectName)));

    // key aspect: does not exist in database && is being written
    boolean generateDefaults =
        !latestAspects.containsKey(keyAspectName) && includedAspects.containsKey(keyAspectName);

    // conditionally generate defaults
    if (generateDefaults) {
      String entityType = urnToEntityName(urn);
      Set<String> aspectsToGet = new HashSet<>();

      boolean shouldCheckBrowsePath =
          isAspectMissing(entityType, BROWSE_PATHS_ASPECT_NAME, includedAspects.keySet());
      if (shouldCheckBrowsePath) {
        aspectsToGet.add(BROWSE_PATHS_ASPECT_NAME);
      }

      boolean shouldCheckBrowsePathV2 =
          isAspectMissing(entityType, BROWSE_PATHS_V2_ASPECT_NAME, includedAspects.keySet());
      if (shouldCheckBrowsePathV2) {
        aspectsToGet.add(BROWSE_PATHS_V2_ASPECT_NAME);
      }

      boolean shouldCheckDataPlatform =
          isAspectMissing(entityType, DATA_PLATFORM_INSTANCE_ASPECT_NAME, includedAspects.keySet());
      if (shouldCheckDataPlatform) {
        aspectsToGet.add(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
      }

      // fetch additional aspects
      latestAspects.putAll(getLatestAspectsForUrn(urn, aspectsToGet));

      if (shouldCheckBrowsePath
          && latestAspects.get(BROWSE_PATHS_ASPECT_NAME) == null
          && !includedAspects.containsKey(BROWSE_PATHS_ASPECT_NAME)) {
        try {
          BrowsePaths generatedBrowsePath = buildDefaultBrowsePath(urn);
          returnAspects.add(Pair.of(BROWSE_PATHS_ASPECT_NAME, generatedBrowsePath));
        } catch (URISyntaxException e) {
          log.error("Failed to parse urn: {}", urn);
        }
      }

      if (shouldCheckBrowsePathV2
          && latestAspects.get(BROWSE_PATHS_V2_ASPECT_NAME) == null
          && !includedAspects.containsKey(BROWSE_PATHS_V2_ASPECT_NAME)) {
        try {
          BrowsePathsV2 generatedBrowsePathV2 = buildDefaultBrowsePathV2(urn, false);
          returnAspects.add(Pair.of(BROWSE_PATHS_V2_ASPECT_NAME, generatedBrowsePathV2));
        } catch (URISyntaxException e) {
          log.error("Failed to parse urn: {}", urn);
        }
      }

      if (shouldCheckDataPlatform
          && latestAspects.get(DATA_PLATFORM_INSTANCE_ASPECT_NAME) == null
          && !includedAspects.containsKey(DATA_PLATFORM_INSTANCE_ASPECT_NAME)) {
        RecordTemplate keyAspect = includedAspects.get(keyAspectName);
        DataPlatformInstanceUtils.buildDataPlatformInstance(entityType, keyAspect)
            .ifPresent(
                aspect -> returnAspects.add(Pair.of(DATA_PLATFORM_INSTANCE_ASPECT_NAME, aspect)));
      }
    }

    return Pair.of(latestAspects.containsKey(keyAspectName), returnAspects);
  }

  @Override
  public List<Pair<String, RecordTemplate>> generateDefaultAspectsIfMissing(
      @Nonnull final Urn urn, Map<String, RecordTemplate> includedAspects) {

    final String keyAspectName = getKeyAspectName(urn);

    if (includedAspects.containsKey(keyAspectName)) {
      return generateDefaultAspectsOnFirstWrite(urn, includedAspects).getValue();
    } else {
      // No key aspect being written, generate it and potentially suggest writing it later
      HashMap<String, RecordTemplate> includedWithKeyAspect = new HashMap<>(includedAspects);
      Pair<String, RecordTemplate> keyAspect =
          Pair.of(keyAspectName, EntityUtils.buildKeyAspect(_entityRegistry, urn));
      includedWithKeyAspect.put(keyAspect.getKey(), keyAspect.getValue());

      Pair<Boolean, List<Pair<String, RecordTemplate>>> returnAspects =
          generateDefaultAspectsOnFirstWrite(urn, includedWithKeyAspect);

      // missing key aspect in database, add it
      if (!returnAspects.getFirst()) {
        returnAspects.getValue().add(keyAspect);
      }

      return returnAspects.getValue();
    }
  }

  private void ingestSnapshotUnion(
      @Nonnull final Snapshot snapshotUnion,
      @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    final RecordTemplate snapshotRecord =
        RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = com.datahub.util.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final List<Pair<String, RecordTemplate>> aspectRecordsToIngest =
        NewModelUtils.getAspectsFromSnapshot(snapshotRecord);

    log.info("INGEST urn {} with system metadata {}", urn.toString(), systemMetadata.toString());
    aspectRecordsToIngest.addAll(
        generateDefaultAspectsIfMissing(
            urn,
            aspectRecordsToIngest.stream()
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue))));

    AspectsBatchImpl aspectsBatch =
        AspectsBatchImpl.builder()
            .items(
                aspectRecordsToIngest.stream()
                    .map(
                        pair ->
                            UpsertBatchItem.builder()
                                .urn(urn)
                                .aspectName(pair.getKey())
                                .aspect(pair.getValue())
                                .systemMetadata(systemMetadata)
                                .build(_entityRegistry))
                    .collect(Collectors.toList()))
            .build();

    ingestAspects(aspectsBatch, auditStamp, true, true);
  }

  @Override
  public AspectSpec getKeyAspectSpec(@Nonnull final Urn urn) {
    return getKeyAspectSpec(urnToEntityName(urn));
  }

  @Override
  public AspectSpec getKeyAspectSpec(@Nonnull final String entityName) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(entityName);
    return spec.getKeyAspectSpec();
  }

  @Override
  public Optional<AspectSpec> getAspectSpec(
      @Nonnull final String entityName, @Nonnull final String aspectName) {
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    return Optional.ofNullable(entitySpec.getAspectSpec(aspectName));
  }

  @Override
  public String getKeyAspectName(@Nonnull final Urn urn) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return keySpec.getName();
  }

  protected Entity toEntity(@Nonnull final Snapshot snapshot) {
    return new Entity().setValue(snapshot);
  }

  protected Snapshot toSnapshotUnion(@Nonnull final RecordTemplate snapshotRecord) {
    final Snapshot snapshot = new Snapshot();
    RecordUtils.setSelectedRecordTemplateInUnion(snapshot, snapshotRecord);
    return snapshot;
  }

  protected RecordTemplate toSnapshotRecord(
      @Nonnull final Urn urn, @Nonnull final List<UnionTemplate> aspectUnionTemplates) {
    final String entityName = urnToEntityName(urn);
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    return com.datahub.util.ModelUtils.newSnapshot(
        getDataTemplateClassFromSchema(entitySpec.getSnapshotSchema(), RecordTemplate.class),
        urn,
        aspectUnionTemplates);
  }

  protected UnionTemplate toAspectUnion(
      @Nonnull final Urn urn, @Nonnull final RecordTemplate aspectRecord) {
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
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

  protected Urn toUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      log.error("Failed to convert urn string {} into Urn object", urnStr);
      throw new ModelConversionException(
          String.format("Failed to convert urn string %s into Urn object ", urnStr), e);
    }
  }

  private EntityResponse toEntityResponse(
      final Urn urn, final List<EnvelopedAspect> envelopedAspects) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urnToEntityName(urn));
    response.setAspects(
        new EnvelopedAspectMap(
            envelopedAspects.stream()
                .collect(Collectors.toMap(EnvelopedAspect::getName, aspect -> aspect))));
    return response;
  }

  private Map<String, Set<String>> buildEntityToValidAspects(final EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs().values().stream()
        .collect(
            Collectors.toMap(
                EntitySpec::getName,
                entry ->
                    entry.getAspectSpecs().stream()
                        .map(AspectSpec::getName)
                        .collect(Collectors.toSet())));
  }

  @Override
  public EntityRegistry getEntityRegistry() {
    return _entityRegistry;
  }

  @Override
  public void setRetentionService(RetentionService retentionService) {
    _retentionService = retentionService;
  }

  protected Set<String> getEntityAspectNames(final Urn entityUrn) {
    return getEntityAspectNames(urnToEntityName(entityUrn));
  }

  @Override
  public Set<String> getEntityAspectNames(final String entityName) {
    return _entityToValidAspects.get(entityName);
  }

  @Override
  public void setWritable(boolean canWrite) {
    log.debug("Setting writable to {}", canWrite);
    _aspectDao.setWritable(canWrite);
  }

  @Override
  public RollbackRunResult rollbackRun(
      List<AspectRowSummary> aspectRows, String runId, boolean hardDelete) {
    return rollbackWithConditions(aspectRows, Collections.singletonMap("runId", runId), hardDelete);
  }

  @Override
  public RollbackRunResult rollbackWithConditions(
      List<AspectRowSummary> aspectRows, Map<String, String> conditions, boolean hardDelete) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    AtomicInteger rowsDeletedFromEntityDeletion = new AtomicInteger(0);

    List<Future<?>> futures =
        aspectRows.stream()
            .map(
                aspectToRemove -> {
                  RollbackResult result =
                      deleteAspect(
                          aspectToRemove.getUrn(),
                          aspectToRemove.getAspectName(),
                          conditions,
                          hardDelete);
                  if (result != null) {
                    Optional<AspectSpec> aspectSpec =
                        getAspectSpec(result.entityName, result.aspectName);
                    if (!aspectSpec.isPresent()) {
                      log.error(
                          "Issue while rolling back: unknown aspect {} for entity {}",
                          result.entityName,
                          result.aspectName);
                      return null;
                    }

                    rowsDeletedFromEntityDeletion.addAndGet(result.additionalRowsAffected);
                    removedAspects.add(aspectToRemove);
                    return alwaysProduceMCLAsync(
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

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion.get());
  }

  @Override
  public RollbackRunResult deleteUrn(Urn urn) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    Integer rowsDeletedFromEntityDeletion = 0;

    final EntitySpec spec = getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    String keyAspectName = getKeyAspectName(urn);

    EntityAspect latestKey = null;
    try {
      latestKey = _aspectDao.getLatestAspect(urn.toString(), keyAspectName);
    } catch (EntityNotFoundException e) {
      log.warn("Entity to delete does not exist. {}", urn.toString());
    }
    if (latestKey == null || latestKey.getSystemMetadata() == null) {
      return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
    }

    SystemMetadata latestKeySystemMetadata =
        EntityUtils.parseSystemMetadata(latestKey.getSystemMetadata());
    RollbackResult result =
        deleteAspect(
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
      Future<?> future =
          alwaysProduceMCLAsync(
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

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
  }

  /**
   * Returns true if the entity exists (has materialized aspects)
   *
   * @param urn the urn of the entity to check
   * @return true if the entity exists, false otherwise
   */
  @Override
  public Boolean exists(Urn urn) {
    final Set<String> aspectsToFetch = getEntityAspectNames(urn);
    final List<EntityAspectIdentifier> dbKeys =
        aspectsToFetch.stream()
            .map(
                aspectName ->
                    new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
            .collect(Collectors.toList());

    Map<EntityAspectIdentifier, EntityAspect> aspects = _aspectDao.batchGet(new HashSet(dbKeys));
    return aspects.values().stream().anyMatch(aspect -> aspect != null);
  }

  /**
   * Returns true if an entity is soft-deleted.
   *
   * @param urn the urn to check
   * @return true is the entity is soft deleted, false otherwise.
   */
  @Override
  public Boolean isSoftDeleted(@Nonnull final Urn urn) {
    Objects.requireNonNull(urn, "urn is required");
    final RecordTemplate statusAspect = getLatestAspect(urn, STATUS_ASPECT_NAME);
    return statusAspect != null && ((Status) statusAspect).isRemoved();
  }

  @Override
  public Boolean exists(Urn urn, String aspectName) {
    EntityAspectIdentifier dbKey =
        new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION);
    Map<EntityAspectIdentifier, EntityAspect> aspects = _aspectDao.batchGet(Set.of(dbKey));
    return aspects.values().stream().anyMatch(Objects::nonNull);
  }

  @Nullable
  @Override
  public RollbackResult deleteAspect(
      String urn, String aspectName, @Nonnull Map<String, String> conditions, boolean hardDelete) {
    // Validate pre-conditions before running queries
    Urn entityUrn;
    EntitySpec entitySpec;
    try {
      entityUrn = Urn.createFromString(urn);
      String entityName = PegasusUtils.urnToEntityName(entityUrn);
      entitySpec = getEntityRegistry().getEntitySpec(entityName);
    } catch (URISyntaxException uriSyntaxException) {
      // don't expect this to happen, so raising RuntimeException here
      throw new RuntimeException(String.format("Failed to extract urn from %s", urn));
    }

    final RollbackResult result =
        _aspectDao.runInTransactionWithRetry(
            (tx) -> {
              Integer additionalRowsDeleted = 0;

              // 1. Fetch the latest existing version of the aspect.
              final EntityAspect latest = _aspectDao.getLatestAspect(urn, aspectName);

              // 1.1 If no latest exists, skip this aspect
              if (latest == null) {
                return null;
              }

              // 2. Compare the match conditions, if they don't match, ignore.
              SystemMetadata latestSystemMetadata =
                  EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
              if (!filterMatch(latestSystemMetadata, conditions)) {
                return null;
              }
              String latestMetadata = latest.getMetadata();

              // 3. Check if this is a key aspect
              Boolean isKeyAspect = false;
              try {
                isKeyAspect = getKeyAspectName(Urn.createFromString(urn)).equals(aspectName);
              } catch (URISyntaxException e) {
                log.error("Error occurred while parsing urn: {}", urn, e);
              }

              // 4. Fetch all preceding aspects, that match
              List<EntityAspect> aspectsToDelete = new ArrayList<>();
              long maxVersion = _aspectDao.getMaxVersion(urn, aspectName);
              EntityAspect survivingAspect = null;
              String previousMetadata = null;
              boolean filterMatch = true;
              while (maxVersion > 0 && filterMatch) {
                EntityAspect candidateAspect = _aspectDao.getAspect(urn, aspectName, maxVersion);
                SystemMetadata previousSysMetadata =
                    EntityUtils.parseSystemMetadata(candidateAspect.getSystemMetadata());
                filterMatch = filterMatch(previousSysMetadata, conditions);
                if (filterMatch) {
                  aspectsToDelete.add(candidateAspect);
                  maxVersion = maxVersion - 1;
                } else {
                  survivingAspect = candidateAspect;
                  previousMetadata = survivingAspect.getMetadata();
                }
              }

              // 5. Apply deletes and fix up latest row

              aspectsToDelete.forEach(aspect -> _aspectDao.deleteAspect(tx, aspect));

              if (survivingAspect != null) {
                // if there was a surviving aspect, copy its information into the latest row
                // eBean does not like us updating a pkey column (version) for the surviving aspect
                // as a result we copy information from survivingAspect to latest and delete
                // survivingAspect
                latest.setMetadata(survivingAspect.getMetadata());
                latest.setSystemMetadata(survivingAspect.getSystemMetadata());
                latest.setCreatedOn(survivingAspect.getCreatedOn());
                latest.setCreatedBy(survivingAspect.getCreatedBy());
                latest.setCreatedFor(survivingAspect.getCreatedFor());
                _aspectDao.saveAspect(tx, latest, false);
                // metrics
                _aspectDao.incrementWriteMetrics(
                    aspectName, 1, latest.getAspect().getBytes(StandardCharsets.UTF_8).length);
                _aspectDao.deleteAspect(tx, survivingAspect);
              } else {
                if (isKeyAspect) {
                  if (hardDelete) {
                    // If this is the key aspect, delete the entity entirely.
                    additionalRowsDeleted = _aspectDao.deleteUrn(tx, urn);
                  } else if (entitySpec.hasAspect(Constants.STATUS_ASPECT_NAME)) {
                    // soft delete by setting status.removed=true (if applicable)
                    final Status statusAspect = new Status();
                    statusAspect.setRemoved(true);

                    final MetadataChangeProposal gmce = new MetadataChangeProposal();
                    gmce.setEntityUrn(entityUrn);
                    gmce.setChangeType(ChangeType.UPSERT);
                    gmce.setEntityType(entityUrn.getEntityType());
                    gmce.setAspectName(Constants.STATUS_ASPECT_NAME);
                    gmce.setAspect(GenericRecordUtils.serializeAspect(statusAspect));
                    final AuditStamp auditStamp =
                        new AuditStamp()
                            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
                            .setTime(System.currentTimeMillis());

                    this.ingestProposal(gmce, auditStamp, false);
                  }
                } else {
                  // Else, only delete the specific aspect.
                  _aspectDao.deleteAspect(tx, latest);
                }
              }

              // 6. Emit the Update
              try {
                final RecordTemplate latestValue =
                    latest == null
                        ? null
                        : EntityUtils.toAspectRecord(
                            Urn.createFromString(latest.getUrn()),
                            latest.getAspect(),
                            latestMetadata,
                            getEntityRegistry());

                final RecordTemplate previousValue =
                    survivingAspect == null
                        ? null
                        : EntityUtils.toAspectRecord(
                            Urn.createFromString(survivingAspect.getUrn()),
                            survivingAspect.getAspect(),
                            previousMetadata,
                            getEntityRegistry());

                final Urn urnObj = Urn.createFromString(urn);
                // We are not deleting key aspect if hardDelete has not been set so do not return a
                // rollback result
                if (isKeyAspect && !hardDelete) {
                  return null;
                }
                return new RollbackResult(
                    urnObj,
                    urnObj.getEntityType(),
                    latest.getAspect(),
                    latestValue,
                    previousValue,
                    latestSystemMetadata,
                    previousValue == null
                        ? null
                        : EntityUtils.parseSystemMetadata(survivingAspect.getSystemMetadata()),
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

    return result;
  }

  protected boolean filterMatch(SystemMetadata systemMetadata, Map<String, String> conditions) {
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
      @Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {

    log.debug("Invoked getLatestAspects with urns: {}, aspectNames: {}", urns, aspectNames);

    // Create DB keys
    final Set<EntityAspectIdentifier> dbKeys =
        urns.stream()
            .map(
                urn -> {
                  final Set<String> aspectsToFetch =
                      aspectNames.isEmpty() ? getEntityAspectNames(urn) : aspectNames;
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
            batch -> batchGetResults.putAll(_aspectDao.batchGet(ImmutableSet.copyOf(batch))));
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
      return _aspectDao.getMaxVersion(urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  private Map<EntityAspectIdentifier, EnvelopedAspect> getEnvelopedAspects(
      final Set<EntityAspectIdentifier> dbKeys) {
    final Map<EntityAspectIdentifier, EnvelopedAspect> result = new HashMap<>();
    final Map<EntityAspectIdentifier, EntityAspect> dbEntries = _aspectDao.batchGet(dbKeys);

    for (EntityAspectIdentifier currKey : dbKeys) {

      final EntityAspect currAspectEntry = dbEntries.get(currKey);

      if (currAspectEntry == null) {
        // No aspect found.
        continue;
      }

      // Aspect found. Now turn it into an EnvelopedAspect
      final com.linkedin.entity.Aspect aspect =
          RecordUtils.toRecordTemplate(
              com.linkedin.entity.Aspect.class, currAspectEntry.getMetadata());
      final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
      envelopedAspect.setName(currAspectEntry.getAspect());
      envelopedAspect.setVersion(currAspectEntry.getVersion());
      // TODO: I think we can assume this here, adding as it's a required field so object mapping
      // barfs when trying to access it,
      //    since nowhere else is using it should be safe for now at least
      envelopedAspect.setType(AspectType.VERSIONED);
      envelopedAspect.setValue(aspect);

      try {
        if (currAspectEntry.getSystemMetadata() != null) {
          final SystemMetadata systemMetadata =
              RecordUtils.toRecordTemplate(
                  SystemMetadata.class, currAspectEntry.getSystemMetadata());
          envelopedAspect.setSystemMetadata(systemMetadata);
        }
      } catch (Exception e) {
        log.warn(
            "Exception encountered when setting system metadata on enveloped aspect {}. Error: {}",
            envelopedAspect.getName(),
            e);
      }

      envelopedAspect.setCreated(
          new AuditStamp()
              .setActor(UrnUtils.getUrn(currAspectEntry.getCreatedBy()))
              .setTime(currAspectEntry.getCreatedOn().getTime()));
      result.put(currKey, envelopedAspect);
    }
    return result;
  }

  private EnvelopedAspect getKeyEnvelopedAspect(final Urn urn) {
    final EntitySpec spec = getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    final com.linkedin.entity.Aspect aspect =
        new com.linkedin.entity.Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data());

    final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(keySpec.getName());
    envelopedAspect.setVersion(ASPECT_LATEST_VERSION);
    envelopedAspect.setValue(aspect);
    // TODO: I think we can assume this here, adding as it's a required field so object mapping
    // barfs when trying to access it,
    //    since nowhere else is using it should be safe for now at least
    envelopedAspect.setType(AspectType.VERSIONED);
    envelopedAspect.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()));

    return envelopedAspect;
  }

  @Nonnull
  private UpdateAspectResult ingestAspectToLocalDB(
      @Nullable Transaction tx,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata providedSystemMetadata,
      @Nullable final EntityAspect latest,
      @Nonnull final Long nextVersion) {

    // Set the "last run id" to be the run id provided with the new system metadata. This will be
    // stored in index
    // for all aspects that have a run id, regardless of whether they change.
    providedSystemMetadata.setLastRunId(
        providedSystemMetadata.getRunId(GetMode.NULL), SetMode.IGNORE_NULL);

    // 2. Compare the latest existing and new.
    final RecordTemplate oldValue =
        latest == null
            ? null
            : EntityUtils.toAspectRecord(
                urn, aspectName, latest.getMetadata(), getEntityRegistry());

    // 3. If there is no difference between existing and new, we just update
    // the lastObserved in system metadata. RunId should stay as the original runId
    if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
      SystemMetadata latestSystemMetadata =
          EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
      latestSystemMetadata.setLastObserved(providedSystemMetadata.getLastObserved());
      latestSystemMetadata.setLastRunId(
          providedSystemMetadata.getLastRunId(GetMode.NULL), SetMode.IGNORE_NULL);

      latest.setSystemMetadata(RecordUtils.toJsonString(latestSystemMetadata));

      log.info("Ingesting aspect with name {}, urn {}", aspectName, urn);
      _aspectDao.saveAspect(tx, latest, false);

      // metrics
      _aspectDao.incrementWriteMetrics(
          aspectName, 1, latest.getAspect().getBytes(StandardCharsets.UTF_8).length);

      return UpdateAspectResult.builder()
          .urn(urn)
          .oldValue(oldValue)
          .newValue(oldValue)
          .oldSystemMetadata(EntityUtils.parseSystemMetadata(latest.getSystemMetadata()))
          .newSystemMetadata(latestSystemMetadata)
          .operation(MetadataAuditOperation.UPDATE)
          .auditStamp(auditStamp)
          .maxVersion(0)
          .build();
    }

    // 4. Save the newValue as the latest version
    log.debug("Ingesting aspect with name {}, urn {}", aspectName, urn);
    String newValueStr = EntityUtils.toJsonAspect(newValue);
    long versionOfOld =
        _aspectDao.saveLatestAspect(
            tx,
            urn.toString(),
            aspectName,
            latest == null ? null : EntityUtils.toJsonAspect(oldValue),
            latest == null ? null : latest.getCreatedBy(),
            latest == null ? null : latest.getCreatedFor(),
            latest == null ? null : latest.getCreatedOn(),
            latest == null ? null : latest.getSystemMetadata(),
            newValueStr,
            auditStamp.getActor().toString(),
            auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
            new Timestamp(auditStamp.getTime()),
            EntityUtils.toJsonAspect(providedSystemMetadata),
            nextVersion);

    // metrics
    _aspectDao.incrementWriteMetrics(
        aspectName, 1, newValueStr.getBytes(StandardCharsets.UTF_8).length);

    return UpdateAspectResult.builder()
        .urn(urn)
        .oldValue(oldValue)
        .newValue(newValue)
        .oldSystemMetadata(
            latest == null ? null : EntityUtils.parseSystemMetadata(latest.getSystemMetadata()))
        .newSystemMetadata(providedSystemMetadata)
        .operation(MetadataAuditOperation.UPDATE)
        .auditStamp(auditStamp)
        .maxVersion(versionOfOld)
        .build();
  }

  /**
   * Builds the default browse path aspects for a subset of well-supported entities.
   *
   * <p>This method currently supports datasets, charts, dashboards, data flows, data jobs, and
   * glossary terms.
   */
  @Nonnull
  @Override
  public BrowsePaths buildDefaultBrowsePath(final @Nonnull Urn urn) throws URISyntaxException {
    Character dataPlatformDelimiter = getDataPlatformDelimiter(urn);
    String defaultBrowsePath =
        getDefaultBrowsePath(urn, this.getEntityRegistry(), dataPlatformDelimiter);
    StringArray browsePaths = new StringArray();
    browsePaths.add(defaultBrowsePath);
    BrowsePaths browsePathAspect = new BrowsePaths();
    browsePathAspect.setPaths(browsePaths);
    return browsePathAspect;
  }

  /**
   * Builds the default browse path V2 aspects for all entities.
   *
   * <p>This method currently supports datasets, charts, dashboards, and data jobs best. Everything
   * else will have a basic "Default" folder added to their browsePathV2.
   */
  @Nonnull
  @Override
  public BrowsePathsV2 buildDefaultBrowsePathV2(final @Nonnull Urn urn, boolean useContainerPaths)
      throws URISyntaxException {
    Character dataPlatformDelimiter = getDataPlatformDelimiter(urn);
    return BrowsePathV2Utils.getDefaultBrowsePathV2(
        urn, this.getEntityRegistry(), dataPlatformDelimiter, this, useContainerPaths);
  }

  /** Returns a delimiter on which the name of an asset may be split. */
  private Character getDataPlatformDelimiter(Urn urn) {
    // Attempt to construct the appropriate Data Platform URN
    Urn dataPlatformUrn = buildDataPlatformUrn(urn, this.getEntityRegistry());
    if (dataPlatformUrn != null) {
      // Attempt to resolve the delimiter from Data Platform Info
      DataPlatformInfo dataPlatformInfo = getDataPlatformInfo(dataPlatformUrn);
      if (dataPlatformInfo != null && dataPlatformInfo.hasDatasetNameDelimiter()) {
        return dataPlatformInfo.getDatasetNameDelimiter().charAt(0);
      }
    }
    // Else, fallback to a default delimiter (period) if one cannot be resolved.
    return '.';
  }

  @Nullable
  private DataPlatformInfo getDataPlatformInfo(Urn urn) {
    try {
      final EntityResponse entityResponse =
          getEntityV2(
              Constants.DATA_PLATFORM_ENTITY_NAME,
              urn,
              ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME));
      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)) {
        return new DataPlatformInfo(
            entityResponse
                .getAspects()
                .get(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)
                .getValue()
                .data());
      }
    } catch (Exception e) {
      log.warn(String.format("Failed to find Data Platform Info for urn %s", urn));
    }
    return null;
  }

  private static boolean shouldAspectEmitChangeLog(@Nonnull final AspectSpec aspectSpec) {
    final List<RelationshipFieldSpec> relationshipFieldSpecs =
        aspectSpec.getRelationshipFieldSpecs();
    return relationshipFieldSpecs.stream().anyMatch(RelationshipFieldSpec::isLineageRelationship);
  }
}
