package com.linkedin.metadata.entity;

import com.codahale.metrics.Timer;
import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.utils.PegasusUtils.getDataTemplateClassFromSchema;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;


/**
 * An abstract base class specifying create, update, and read operations against metadata entities and aspects
 * by primary key (urn).
 *
 * This interface is meant to abstract away the storage concerns of these pieces of metadata, permitting
 * any underlying storage system to be used in materializing GMS domain objects, which are implemented using Pegasus
 * {@link RecordTemplate}s.
 *
 * A key requirement of any implementation is being able to bind what is persisted in storage to an aspect
 * {@link RecordTemplate}, using help from the {@link EntityRegistry}.
 *
 * Another requirement is that any implementation honors the internal versioning semantics. The latest version of any aspect
 * is set to 0 for efficient retrieval; in most cases the latest state of an aspect will be the only fetched.
 *
 * As such, 0 is treated as a special number. Once an aspect is no longer the latest, versions will increment
 * monotonically, starting from 1. Thus, the second-to-last version of an aspect will be equal to total # versions
 * of the aspect - 1.
 *
 * For example, if there are 5 instances of a single aspect, the latest will have version 0, and the second-to-last
 * will have version 4. The "true" latest version of an aspect is always equal to the highest stored version
 * of a given aspect + 1.
 *
 * Note that currently, implementations of this interface are responsible for producing Metadata Audit Events on
 * ingestion using {@link #produceMetadataChangeLog(Urn, String, String, AspectSpec, RecordTemplate, RecordTemplate,
 * SystemMetadata, SystemMetadata, AuditStamp, ChangeType)}.
 *
 * TODO: Consider whether we can abstract away virtual versioning semantics to subclasses of this class.
 * TODO: Extract out a nested 'AspectService'.
 */
@Slf4j
public abstract class EntityService {

  /**
   * As described above, the latest version of an aspect should <b>always</b> take the value 0, with
   * monotonically increasing version incrementing as usual once the latest version is replaced.
   */
  private final EventProducer _producer;
  private final EntityRegistry _entityRegistry;
  private final Map<String, Set<String>> _entityToValidAspects;
  @Getter
  @Setter
  private RetentionService retentionService;
  private Boolean _alwaysEmitAuditEvent = false;
  public static final String DEFAULT_RUN_ID = "no-run-id-provided";
  public static final String BROWSE_PATHS = "browsePaths";
  public static final String DATA_PLATFORM_INSTANCE = "dataPlatformInstance";
  protected static final int MAX_KEYS_PER_QUERY = 500;
  public static final String STATUS = "status";

  protected EntityService(@Nonnull final EventProducer producer, @Nonnull final EntityRegistry entityRegistry) {
    _producer = producer;
    _entityRegistry = entityRegistry;
    _entityToValidAspects = buildEntityToValidAspects(entityRegistry);
  }

  /**
   * Retrieves the latest aspects corresponding to a batch of {@link Urn}s based on a provided
   * set of aspect names.
   *
   * @param urns set of urns to fetch aspects for
   * @param aspectNames aspects to fetch for each urn in urns set
   * @return a map of provided {@link Urn} to a List containing the requested aspects.
   */
  public abstract Map<Urn, List<RecordTemplate>> getLatestAspects(
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames);


  public abstract Map<String, RecordTemplate> getLatestAspectsForUrn(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames);

  /**
   * Retrieves an aspect having a specific {@link Urn}, name, & version.
   *
   * Note that once we drop support for legacy aspect-specific resources,
   * we should make this a protected method. Only visible for backwards compatibility.
   *
   * @param urn an urn associated with the requested aspect
   * @param aspectName name of the aspect requested
   * @param version specific version of the aspect being requests
   * @return the {@link RecordTemplate} representation of the requested aspect object, or null if one cannot be found
   */
  @Nullable
  public abstract RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, long version);

  /**
   * Retrieves the latest aspects for the given urn as dynamic aspect objects
   * (Without having to define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urn urn of entity to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Nullable
  public EntityResponse getEntityV2(
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException {
    return getEntitiesV2(entityName, Collections.singleton(urn), aspectNames).get(urn);
  }

  /**
   * Retrieves the latest aspects for the given set of urns as dynamic aspect objects
   * (Without having to define union objects)
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  public Map<Urn, EntityResponse> getEntitiesV2(
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException {
    return getLatestEnvelopedAspects(entityName, urns, aspectNames)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toEntityResponse(entry.getKey(), entry.getValue())));
  }

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  public abstract Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      @Nonnull final String entityName,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException;

  /**
   * Retrieves the latest aspect for the given urn as a list of enveloped aspects
   *
   * @param entityName name of the entity to fetch
   * @param urn urn to fetch
   * @param aspectName name of the aspect to fetch
   * @return {@link EnvelopedAspect} object, or null if one cannot be found
   */
  public EnvelopedAspect getLatestEnvelopedAspect(
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName) throws Exception {
    return getLatestEnvelopedAspects(entityName, ImmutableSet.of(urn), ImmutableSet.of(aspectName)).getOrDefault(urn, Collections.emptyList())
        .stream()
        .filter(envelopedAspect -> envelopedAspect.getName().equals(aspectName))
        .findFirst()
        .orElse(null);
  }

  /**
   * Retrieves the specific version of the aspect for the given urn
   *
   * @param entityName name of the entity to fetch
   * @param urn urn to fetch
   * @param aspectName name of the aspect to fetch
   * @param version version to fetch
   * @return {@link EnvelopedAspect} object, or null if one cannot be found
   */
  public abstract EnvelopedAspect getEnvelopedAspect(
      @Nonnull final String entityName,
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      long version) throws Exception;

  /**
   * Retrieves an {@link VersionedAspect}, or null if one cannot be found.
   */
  @Nullable
  public abstract VersionedAspect getVersionedAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      long version);

  /**
   * Retrieves a list of all aspects belonging to an entity of a particular type, sorted by urn.
   *
   * Note that once we drop support for legacy 'getAllDataPlatforms' endpoint,
   * we can drop support for this unless otherwise required. Only visible for backwards compatibility.
   *
   * @param entityName name of the entity type the aspect belongs to, e.g. 'dataset'
   * @param aspectName name of the aspect requested, e.g. 'ownership'
   * @param start the starting index of the returned aspects, used in pagination
   * @param count the count of the aspects to be returned, used in pagination
   * @return a {@link ListResult} of {@link RecordTemplate}s representing the requested aspect.
   */
  @Nonnull
  public abstract ListResult<RecordTemplate> listLatestAspects(@Nonnull final String entityName,
      @Nonnull final String aspectName, final int start, int count);


  @Nonnull
  private UpdateAspectResult wrappedIngestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata systemMetadata) {
    validateUrn(urn);
    validateAspect(urn, updateLambda.apply(null));
    return ingestAspectToLocalDB(urn, aspectName, updateLambda, auditStamp, systemMetadata);
  }

  @Nonnull
  private List<Pair<String, UpdateAspectResult>> wrappedIngestAspectsToLocalDB(@Nonnull final Urn urn,
      @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest,
      @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata providedSystemMetadata) {
    validateUrn(urn);
    aspectRecordsToIngest.forEach(pair -> validateAspect(urn, pair.getSecond()));
    return ingestAspectsToLocalDB(urn, aspectRecordsToIngest, auditStamp, providedSystemMetadata);
  }

  private void validateAspect(Urn urn, RecordTemplate aspect) {
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(_entityRegistry);
    validator.setCurrentEntitySpec(_entityRegistry.getEntitySpec(urn.getEntityType()));
    RecordTemplateValidator.validate(aspect, validationResult -> {
        throw new IllegalArgumentException("Invalid urn format for aspect: " + aspect + " for entity: " + urn + "\n Cause: "
        + validationResult.getMessages());
      }, validator);
  }
  /**
   * Checks whether there is an actual update to the aspect by applying the updateLambda
   * If there is an update, push the new version into the local DB.
   * Otherwise, do not push the new version, but just update the system metadata.
   * DO NOT CALL DIRECTLY, USE WRAPPED METHODS TO VALIDATE URN
   *
   * @param urn an urn associated with the new aspect
   * @param aspectName name of the aspect being inserted
   * @param updateLambda Function to apply to the latest version of the aspect to get the updated version
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time   * @param providedSystemMetadata
   * @return Details about the new and old version of the aspect
   */
  @Nonnull
  @Deprecated
  protected abstract UpdateAspectResult ingestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata systemMetadata);

  /**
   * Same as ingestAspectToLocalDB but for multiple aspects
   * DO NOT CALL DIRECTLY, USE WRAPPED METHODS TO VALIDATE URN
   */
  @Nonnull
  @Deprecated
  protected abstract List<Pair<String, UpdateAspectResult>> ingestAspectsToLocalDB(@Nonnull final Urn urn,
    @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest,
    @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata providedSystemMetadata);

  @Nonnull
  private SystemMetadata generateSystemMetadataIfEmpty(SystemMetadata systemMetadata) {
    if (systemMetadata == null) {
      systemMetadata = new SystemMetadata();
      systemMetadata.setRunId(DEFAULT_RUN_ID);
      systemMetadata.setLastObserved(System.currentTimeMillis());
    }
    return systemMetadata;
  }

  private void validateUrn(@Nonnull final Urn urn) {
    if (!urn.toString().trim().equals(urn.toString())) {
      throw new IllegalArgumentException("Error: cannot provide an URN with leading or trailing whitespace");
    }
  }

  public void ingestAspects(@Nonnull final Urn urn, @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest,
    @Nonnull final AuditStamp auditStamp, SystemMetadata systemMetadata) {

    systemMetadata = generateSystemMetadataIfEmpty(systemMetadata);

    Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestAspectsToLocalDB").time();
    List<Pair<String, UpdateAspectResult>> ingestResults = wrappedIngestAspectsToLocalDB(urn, aspectRecordsToIngest, auditStamp, systemMetadata);
    ingestToLocalDBTimer.stop();

    for (Pair<String, UpdateAspectResult> result: ingestResults) {
      sendEventForUpdateAspectResult(urn, result.getFirst(), result.getSecond());
    }
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link com.linkedin.mxe.MetadataAuditEvent}.
   *
   * Note that in general, this should not be used externally. It is currently serving upgrade scripts and
   * is as such public.
   *
   * @param urn an urn associated with the new aspect
   * @param aspectName name of the aspect being inserted
   * @param newValue value of the aspect being inserted
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param systemMetadata
   * @return the {@link RecordTemplate} representation of the written aspect object
   */
  public RecordTemplate ingestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, SystemMetadata systemMetadata) {

    log.debug("Invoked ingestAspect with urn: {}, aspectName: {}, newValue: {}", urn, aspectName, newValue);

    systemMetadata = generateSystemMetadataIfEmpty(systemMetadata);

    Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestAspectToLocalDB").time();
    UpdateAspectResult result = wrappedIngestAspectToLocalDB(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata);
    ingestToLocalDBTimer.stop();

    return sendEventForUpdateAspectResult(urn, aspectName, result);
  }

  private RecordTemplate sendEventForUpdateAspectResult(@Nonnull final Urn urn, @Nonnull final String aspectName,
    @Nonnull UpdateAspectResult result) {

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate updatedValue = result.getNewValue();
    final SystemMetadata oldSystemMetadata = result.getOldSystemMetadata();
    final SystemMetadata updatedSystemMetadata = result.getNewSystemMetadata();

    // Apply retention policies asynchronously if there was an update to existing aspect value
    if (oldValue != updatedValue && oldValue != null && retentionService != null) {
      retentionService.applyRetention(urn, aspectName,
              Optional.of(new RetentionService.RetentionContext(Optional.of(result.maxVersion))));
    }

    // Produce MCL after a successful update
    if (oldValue != updatedValue || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataChangeLog for ingested aspect %s, urn %s", aspectName, urn));
      String entityName = urnToEntityName(urn);
      EntitySpec entitySpec = getEntityRegistry().getEntitySpec(entityName);
      AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
      if (aspectSpec == null) {
        throw new RuntimeException(String.format("Unknown aspect %s for entity %s", aspectName, entityName));
      }

      Timer.Context produceMCLTimer = MetricUtils.timer(this.getClass(), "produceMCL").time();
      produceMetadataChangeLog(urn, entityName, aspectName, aspectSpec, oldValue, updatedValue, oldSystemMetadata,
          updatedSystemMetadata, result.getAuditStamp(), ChangeType.UPSERT);
      produceMCLTimer.stop();

      // For legacy reasons, keep producing to the MAE event stream without blocking ingest
      Timer.Context produceMAETimer = MetricUtils.timer(this.getClass(), "produceMAE").time();
      produceMetadataAuditEvent(urn, aspectName, oldValue, updatedValue, result.getOldSystemMetadata(),
          result.getNewSystemMetadata(), MetadataAuditOperation.UPDATE);
      produceMAETimer.stop();
    } else {
      log.debug("Skipped producing MetadataAuditEvent for ingested aspect {}, urn {}. Aspect has not changed.",
        aspectName, urn);
    }
    return updatedValue;
  }

  public IngestProposalResult ingestProposal(@Nonnull MetadataChangeProposal metadataChangeProposal,
      AuditStamp auditStamp) {

    log.debug("entity type = {}", metadataChangeProposal.getEntityType());
    EntitySpec entitySpec = getEntityRegistry().getEntitySpec(metadataChangeProposal.getEntityType());
    log.debug("entity spec = {}", entitySpec);

    Urn entityUrn = EntityKeyUtils.getUrnFromProposal(metadataChangeProposal, entitySpec.getKeyAspectSpec());

    if (metadataChangeProposal.getChangeType() != ChangeType.UPSERT) {
      throw new UnsupportedOperationException("Only upsert operation is supported");
    }

    if (!metadataChangeProposal.hasAspectName() || !metadataChangeProposal.hasAspect()) {
      throw new UnsupportedOperationException("Aspect and aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(metadataChangeProposal.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format("Unknown aspect %s for entity %s", metadataChangeProposal.getAspectName(),
              metadataChangeProposal.getEntityType()));
    }

    log.debug("aspect spec = {}", aspectSpec);

    RecordTemplate aspect;
    try {
      aspect = GenericRecordUtils.deserializeAspect(metadataChangeProposal.getAspect().getValue(),
          metadataChangeProposal.getAspect().getContentType(), aspectSpec);
      ValidationUtils.validateOrThrow(aspect);
    } catch (ModelConversionException e) {
      throw new RuntimeException(
          String.format("Could not deserialize %s for aspect %s", metadataChangeProposal.getAspect().getValue(),
              metadataChangeProposal.getAspectName()));
    }
    log.debug("aspect = {}", aspect);

    SystemMetadata systemMetadata = generateSystemMetadataIfEmpty(metadataChangeProposal.getSystemMetadata());
    systemMetadata.setRegistryName(aspectSpec.getRegistryName());
    systemMetadata.setRegistryVersion(aspectSpec.getRegistryVersion().toString());

    RecordTemplate oldAspect = null;
    SystemMetadata oldSystemMetadata = null;
    RecordTemplate newAspect = aspect;
    SystemMetadata newSystemMetadata = systemMetadata;

    if (!aspectSpec.isTimeseries()) {
      Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestProposalToLocalDB").time();
      UpdateAspectResult result =
          wrappedIngestAspectToLocalDB(entityUrn, metadataChangeProposal.getAspectName(), ignored -> aspect, auditStamp,
              systemMetadata);
      ingestToLocalDBTimer.stop();
      oldAspect = result.getOldValue();
      oldSystemMetadata = result.getOldSystemMetadata();
      newAspect = result.getNewValue();
      newSystemMetadata = result.getNewSystemMetadata();
      // Apply retention policies asynchronously if there was an update to existing aspect value
      if (oldAspect != newAspect && oldAspect != null && retentionService != null) {
        retentionService.applyRetention(entityUrn, aspectSpec.getName(),
            Optional.of(new RetentionService.RetentionContext(Optional.of(result.maxVersion))));
      }
    }

    if (oldAspect != newAspect || getAlwaysEmitAuditEvent()) {
      log.debug("Producing MetadataChangeLog for ingested aspect {}, urn {}", metadataChangeProposal.getAspectName(), entityUrn);

      final MetadataChangeLog metadataChangeLog = new MetadataChangeLog(metadataChangeProposal.data());
      metadataChangeLog.setEntityUrn(entityUrn);
      metadataChangeLog.setCreated(auditStamp);

      if (oldAspect != null) {
        metadataChangeLog.setPreviousAspectValue(GenericRecordUtils.serializeAspect(oldAspect));
      }
      if (oldSystemMetadata != null) {
        metadataChangeLog.setPreviousSystemMetadata(oldSystemMetadata);
      }
      if (newAspect != null) {
        metadataChangeLog.setAspect(GenericRecordUtils.serializeAspect(newAspect));
      }
      if (newSystemMetadata != null) {
        metadataChangeLog.setSystemMetadata(newSystemMetadata);
      }

      log.debug("Serialized MCL event: {}", metadataChangeLog);
      // Since only timeseries aspects are ingested as of now, simply produce mae event for it
      produceMetadataChangeLog(entityUrn, aspectSpec, metadataChangeLog);
    } else {
      log.debug(
          "Skipped producing MetadataChangeLog for ingested aspect {}, urn {}. Aspect has not changed.",
              metadataChangeProposal.getAspectName(), entityUrn);
    }

    return new IngestProposalResult(entityUrn, oldAspect != newAspect);
  }

  /**
   * Updates a particular version of an aspect & optionally emits a {@link com.linkedin.mxe.MetadataAuditEvent}.
   *
   * Note that in general, this should not be used externally. It is currently serving upgrade scripts and
   * is as such public.
   *
   * @param urn an urn associated with the aspect to update
   * @param entityName name of the entity being updated
   * @param aspectName name of the aspect being updated
   * @param aspectSpec spec of the aspect being updated
   * @param newValue new value of the aspect being updated
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param version specific version of the aspect being requests
   * @param emitMae whether a {@link com.linkedin.mxe.MetadataAuditEvent} should be emitted in correspondence upon
   *                successful update
   * @return the {@link RecordTemplate} representation of the requested aspect object
   */
  public abstract RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String entityName,
      @Nonnull final String aspectName, @Nonnull final AspectSpec aspectSpec, @Nonnull final RecordTemplate newValue,
      @Nonnull final AuditStamp auditStamp, @Nonnull final long version, @Nonnull final boolean emitMae);

  /**
   * Lists the entity URNs found in storage.
   *
   * @param entityName the name associated with the entity
   * @param start the start offset
   * @param count the count
   */
  public abstract ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count);

  /**
   * Default implementations. Subclasses should feel free to override if it's more efficient to do so.
   */
  public Entity getEntity(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    return getEntities(Collections.singleton(urn), aspectNames).values().stream().findFirst().orElse(null);
  }

  /**
   * Deprecated! Use getEntitiesV2 instead.
   *
   * Retrieves multiple entities.
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  @Deprecated
  public Map<Urn, Entity> getEntities(@Nonnull final Set<Urn> urns, @Nonnull Set<String> aspectNames) {
    log.debug("Invoked getEntities with urns {}, aspects {}", urns, aspectNames);
    if (urns.isEmpty()) {
      return Collections.emptyMap();
    }
    return getSnapshotUnions(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toEntity(entry.getValue())));
  }

  public void produceMetadataAuditEvent(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nullable final RecordTemplate oldAspectValue, @Nullable final RecordTemplate newAspectValue,
      @Nullable final SystemMetadata oldSystemMetadata, @Nullable final SystemMetadata newSystemMetadata,
      @Nullable final MetadataAuditOperation operation) {
    log.debug(String.format("Producing MetadataAuditEvent for ingested aspect %s, urn %s", aspectName, urn));
    if (aspectName.equals(getKeyAspectName(urn))) {
      produceMetadataAuditEventForKey(urn, newSystemMetadata);
    } else {
      final Snapshot newSnapshot = buildSnapshot(urn, newAspectValue);
      Snapshot oldSnapshot = null;
      if (oldAspectValue != null) {
        oldSnapshot = buildSnapshot(urn, oldAspectValue);
      }
      _producer.produceMetadataAuditEvent(urn, oldSnapshot, newSnapshot, oldSystemMetadata, newSystemMetadata,
          operation);
    }
  }

  protected Snapshot buildKeySnapshot(@Nonnull final Urn urn) {
    final RecordTemplate keyAspectValue = buildKeyAspect(urn);
    return toSnapshotUnion(toSnapshotRecord(urn, ImmutableList.of(toAspectUnion(urn, keyAspectValue))));
  }

  public void produceMetadataAuditEventForKey(@Nonnull final Urn urn,
      @Nullable final SystemMetadata newSystemMetadata) {

    final Snapshot newSnapshot = buildKeySnapshot(urn);

    _producer.produceMetadataAuditEvent(urn, null, newSnapshot, null, newSystemMetadata, MetadataAuditOperation.UPDATE);
  }

  /**
   * Produces a {@link com.linkedin.mxe.MetadataChangeLog} from a
   * new & previous aspect.
   *
   * @param urn the urn associated with the entity changed
   * @param aspectSpec AspectSpec of the aspect being updated
   * @param metadataChangeLog metadata change log to push into MCL kafka topic
   */
  public void produceMetadataChangeLog(@Nonnull final Urn urn, AspectSpec aspectSpec,
      @Nonnull final MetadataChangeLog metadataChangeLog) {
    _producer.produceMetadataChangeLog(urn, aspectSpec, metadataChangeLog);
  }

  public void produceMetadataChangeLog(@Nonnull final Urn urn, @Nonnull String entityName, @Nonnull String aspectName,
      @Nonnull final AspectSpec aspectSpec, @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue, @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata, @Nonnull AuditStamp auditStamp, @Nonnull final ChangeType changeType) {
    final MetadataChangeLog metadataChangeLog = new MetadataChangeLog();
    metadataChangeLog.setEntityType(entityName);
    metadataChangeLog.setEntityUrn(urn);
    metadataChangeLog.setChangeType(changeType);
    metadataChangeLog.setAspectName(aspectName);
    metadataChangeLog.setCreated(auditStamp);
    if (newAspectValue != null) {
      metadataChangeLog.setAspect(GenericRecordUtils.serializeAspect(newAspectValue));
    }
    if (newSystemMetadata != null) {
      metadataChangeLog.setSystemMetadata(newSystemMetadata);
    }
    if (oldAspectValue != null) {
      metadataChangeLog.setPreviousAspectValue(GenericRecordUtils.serializeAspect(oldAspectValue));
    }
    if (oldSystemMetadata != null) {
      metadataChangeLog.setPreviousSystemMetadata(oldSystemMetadata);
    }
    produceMetadataChangeLog(urn, aspectSpec, metadataChangeLog);
  }

  public RecordTemplate getLatestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName) {
    log.debug("Invoked getLatestAspect with urn {}, aspect {}", urn, aspectName);
    return getAspect(urn, aspectName, ASPECT_LATEST_VERSION);
  }

  public void ingestEntities(@Nonnull final List<Entity> entities, @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata) {
    log.debug("Invoked ingestEntities with entities {}, audit stamp {}", entities, auditStamp);
    Streams.zip(entities.stream(), systemMetadata.stream(), (a, b) -> new Pair<Entity, SystemMetadata>(a, b))
        .forEach(pair -> ingestEntity(pair.getFirst(), auditStamp, pair.getSecond()));
  }

  public void ingestEntity(Entity entity, AuditStamp auditStamp) {
    SystemMetadata generatedSystemMetadata = new SystemMetadata();
    generatedSystemMetadata.setRunId(DEFAULT_RUN_ID);
    generatedSystemMetadata.setLastObserved(System.currentTimeMillis());

    ingestEntity(entity, auditStamp, generatedSystemMetadata);
  }

  public void ingestEntity(@Nonnull Entity entity, @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata) {
    log.debug("Invoked ingestEntity with entity {}, audit stamp {} systemMetadata {}", entity, auditStamp, systemMetadata.toString());
    ingestSnapshotUnion(entity.getValue(), auditStamp, systemMetadata);
  }

  @Nonnull
  protected Map<Urn, Snapshot> getSnapshotUnions(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return getSnapshotRecords(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toSnapshotUnion(entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, RecordTemplate> getSnapshotRecords(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    return getLatestAspectUnions(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toSnapshotRecord(entry.getKey(), entry.getValue())));
  }

  @Nonnull
  protected Map<Urn, List<UnionTemplate>> getLatestAspectUnions(
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    return getLatestAspects(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
            .stream()
            .map(aspectRecord -> toAspectUnion(entry.getKey(), aspectRecord))
            .collect(Collectors.toList())));
  }

  /**
  Returns true if entityType should have some aspect as per its definition
    but aspects given does not have that aspect
   */
  private boolean isAspectProvided(String entityType, String aspectName, Set<String> aspects) {
    return _entityRegistry.getEntitySpec(entityType).getAspectSpecMap().containsKey(aspectName)
        && !aspects.contains(aspectName);
  }

  public List<Pair<String, RecordTemplate>> generateDefaultAspectsIfMissing(@Nonnull final Urn urn,
      Set<String> includedAspects) {

    Set<String> aspectsToGet = new HashSet<>();
    String entityType = urnToEntityName(urn);

    boolean shouldCheckBrowsePath = isAspectProvided(entityType, BROWSE_PATHS, includedAspects);
    if (shouldCheckBrowsePath) {
      aspectsToGet.add(BROWSE_PATHS);
    }

    boolean shouldCheckDataPlatform = isAspectProvided(entityType, DATA_PLATFORM_INSTANCE, includedAspects);
    if (shouldCheckDataPlatform) {
      aspectsToGet.add(DATA_PLATFORM_INSTANCE);
    }

    boolean shouldHaveStatusSet = isAspectProvided(entityType, STATUS, includedAspects);
    if (shouldHaveStatusSet) {
      aspectsToGet.add(STATUS);
    }

    List<Pair<String, RecordTemplate>> aspects = new ArrayList<>();
    final String keyAspectName = getKeyAspectName(urn);
    aspectsToGet.add(keyAspectName);

    Map<String, RecordTemplate> latestAspects = getLatestAspectsForUrn(urn, aspectsToGet);

    RecordTemplate keyAspect = latestAspects.get(keyAspectName);
    if (keyAspect == null) {
      keyAspect = buildKeyAspect(urn);
      aspects.add(Pair.of(keyAspectName, keyAspect));
    }

    if (shouldCheckBrowsePath && latestAspects.get(BROWSE_PATHS) == null) {
      try {
        BrowsePaths generatedBrowsePath = BrowsePathUtils.buildBrowsePath(urn, getEntityRegistry());
        if (generatedBrowsePath != null) {
          aspects.add(Pair.of(BROWSE_PATHS, generatedBrowsePath));
        }
      } catch (URISyntaxException e) {
        log.error("Failed to parse urn: {}", urn);
      }
    }

    if (shouldCheckDataPlatform && latestAspects.get(DATA_PLATFORM_INSTANCE) == null) {
      DataPlatformInstanceUtils.buildDataPlatformInstance(entityType, keyAspect)
          .ifPresent(aspect -> aspects.add(Pair.of(DATA_PLATFORM_INSTANCE, aspect)));
    }

    if (shouldHaveStatusSet && latestAspects.get(STATUS) != null) {
      Status status = new Status();
      status.setRemoved(false);
      aspects.add(Pair.of(STATUS, status));
    }

    return aspects;
  }

  private void ingestSnapshotUnion(@Nonnull final Snapshot snapshotUnion, @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = com.datahub.util.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final List<Pair<String, RecordTemplate>> aspectRecordsToIngest =
        NewModelUtils.getAspectsFromSnapshot(snapshotRecord);

    log.info("INGEST urn {} with system metadata {}", urn.toString(), systemMetadata.toString());
    aspectRecordsToIngest.addAll(generateDefaultAspectsIfMissing(urn,
        aspectRecordsToIngest.stream().map(pair -> pair.getFirst()).collect(Collectors.toSet())));

    ingestAspects(urn, aspectRecordsToIngest, auditStamp, systemMetadata);
  }

  public Snapshot buildSnapshot(@Nonnull final Urn urn, @Nonnull final RecordTemplate aspectValue) {
    // if the aspect value is the key, we do not need to include the key a second time
    if (PegasusUtils.getAspectNameFromSchema(aspectValue.schema()).equals(getKeyAspectName(urn))) {
      return toSnapshotUnion(toSnapshotRecord(urn, ImmutableList.of(toAspectUnion(urn, aspectValue))));
    }

    final RecordTemplate keyAspectValue = buildKeyAspect(urn);
    return toSnapshotUnion(
        toSnapshotRecord(urn, ImmutableList.of(toAspectUnion(urn, keyAspectValue), toAspectUnion(urn, aspectValue))));
  }

  protected RecordTemplate buildKeyAspect(@Nonnull final Urn urn) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    final RecordDataSchema keySchema = keySpec.getPegasusSchema();
    return EntityKeyUtils.convertUrnToEntityKey(urn, keySchema);
  }

  public AspectSpec getKeyAspectSpec(@Nonnull final Urn urn) {
    return getKeyAspectSpec(urnToEntityName(urn));
  }

  public AspectSpec getKeyAspectSpec(@Nonnull final String entityName) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(entityName);
    return spec.getKeyAspectSpec();
  }

  public Optional<AspectSpec> getAspectSpec(@Nonnull final String entityName, @Nonnull final String aspectName) {
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    return Optional.ofNullable(entitySpec.getAspectSpec(aspectName));
  }

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

  protected RecordTemplate toSnapshotRecord(@Nonnull final Urn urn,
      @Nonnull final List<UnionTemplate> aspectUnionTemplates) {
    final String entityName = urnToEntityName(urn);
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    return com.datahub.util.ModelUtils.newSnapshot(
        getDataTemplateClassFromSchema(entitySpec.getSnapshotSchema(), RecordTemplate.class), urn,
        aspectUnionTemplates);
  }

  protected UnionTemplate toAspectUnion(@Nonnull final Urn urn, @Nonnull final RecordTemplate aspectRecord) {
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
    final TyperefDataSchema aspectSchema = entitySpec.getAspectTyperefSchema();
    if (aspectSchema == null) {
      throw new RuntimeException(
          String.format("Aspect schema for %s is null: v4 operation is not supported on this entity registry",
              entitySpec.getName()));
    }
    return com.datahub.util.ModelUtils.newAspectUnion(
        getDataTemplateClassFromSchema(entitySpec.getAspectTyperefSchema(), UnionTemplate.class), aspectRecord);
  }

  protected Urn toUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      log.error("Failed to convert urn string {} into Urn object", urnStr);
      throw new ModelConversionException(String.format("Failed to convert urn string %s into Urn object ", urnStr), e);
    }
  }

  private EntityResponse toEntityResponse(final Urn urn, final List<EnvelopedAspect> envelopedAspects) {
    final EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urnToEntityName(urn));
    response.setAspects(new EnvelopedAspectMap(
        envelopedAspects.stream().collect(Collectors.toMap(EnvelopedAspect::getName, aspect -> aspect))
    ));
    return response;
  }

  private Map<String, Set<String>> buildEntityToValidAspects(final EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs()
        .values()
        .stream()
        .collect(Collectors.toMap(EntitySpec::getName,
            entry -> entry.getAspectSpecs().stream().map(AspectSpec::getName).collect(Collectors.toSet())));
  }

  public Boolean getAlwaysEmitAuditEvent() {
    return _alwaysEmitAuditEvent;
  }

  public void setAlwaysEmitAuditEvent(Boolean alwaysEmitAuditEvent) {
    _alwaysEmitAuditEvent = alwaysEmitAuditEvent;
  }

  public EntityRegistry getEntityRegistry() {
    return _entityRegistry;
  }

  protected Set<String> getEntityAspectNames(final Urn entityUrn) {
    return getEntityAspectNames(urnToEntityName(entityUrn));
  }

  public Set<String> getEntityAspectNames(final String entityName) {
    return _entityToValidAspects.get(entityName);
  }

  public abstract void setWritable(boolean canWrite);

  public RollbackRunResult rollbackRun(List<AspectRowSummary> aspectRows, String runId, boolean hardDelete) {
    return rollbackWithConditions(aspectRows, Collections.singletonMap("runId", runId), hardDelete);
  }

  public abstract RollbackRunResult rollbackWithConditions(List<AspectRowSummary> aspectRows,
                                                           Map<String, String> conditions, boolean hardDelete);

  public abstract RollbackRunResult deleteUrn(Urn urn);

  public abstract Boolean exists(Urn urn);

  @Value
  public static class UpdateAspectResult {
    Urn urn;
    RecordTemplate oldValue;
    RecordTemplate newValue;
    SystemMetadata oldSystemMetadata;
    SystemMetadata newSystemMetadata;
    MetadataAuditOperation operation;
    AuditStamp auditStamp;
    long maxVersion;
  }

  @Value
  public static class IngestProposalResult {
    Urn urn;
    boolean didUpdate;
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
}
