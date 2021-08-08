package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntityKeyUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.PegasusUtils.getDataTemplateClassFromSchema;
import static com.linkedin.metadata.PegasusUtils.urnToEntityName;


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
 * ingestion using {@link #produceMetadataAuditEvent(
 *Urn, RecordTemplate, RecordTemplate, SystemMetadata, SystemMetadata, MetadataAuditOperation)}.
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
  public static final long LATEST_ASPECT_VERSION = 0;

  private final EntityEventProducer _producer;
  private final EntityRegistry _entityRegistry;
  private final Map<String, Set<String>> _entityToValidAspects;
  private Boolean _emitAspectSpecificAuditEvent = false;
  public static final String DEFAULT_RUN_ID = "no-run-id-provided";

  protected EntityService(@Nonnull final EntityEventProducer producer, @Nonnull final EntityRegistry entityRegistry) {
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
  protected abstract Map<Urn, List<RecordTemplate>> getLatestAspects(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames);

  /**
   * Retrieves an aspect having a specific {@link Urn}, name, & version.
   *
   * Note that once we drop support for legacy aspect-specific resources,
   * we should make this a protected method. Only visible for backwards compatibility.
   *
   * @param urn an urn associated with the requested aspect
   * @param aspectName name of the aspect requested
   * @param version specific version of the aspect being requests
   * @return the {@link RecordTemplate} representation of the requested aspect object
   */
  public abstract RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, long version);

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
  public abstract ListResult<RecordTemplate> listLatestAspects(@Nonnull final String entityName,
      @Nonnull final String aspectName, final int start, int count);

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
  public abstract RecordTemplate ingestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, SystemMetadata systemMetadata);

  public RecordTemplate ingestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp) {

    SystemMetadata generatedSystemMetadata = new SystemMetadata();
    generatedSystemMetadata.setLastObserved(System.currentTimeMillis());

    return ingestAspect(urn, aspectName, newValue, auditStamp, generatedSystemMetadata);
  }

  /**
   * Updates a particular version of an aspect & optionally emits a {@link com.linkedin.mxe.MetadataAuditEvent}.
   *
   * Note that in general, this should not be used externally. It is currently serving upgrade scripts and
   * is as such public.
   *
   * @param urn an urn associated with the aspect to update
   * @param aspectName name of the aspect being updated
   * @param newValue new value of the aspect being updated
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time
   * @param version specific version of the aspect being requests
   * @param emitMae whether a {@link com.linkedin.mxe.MetadataAuditEvent} should be emitted in correspondence upon
   *                successful update
   * @return the {@link RecordTemplate} representation of the requested aspect object
   */
  public abstract RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, final long version,
      final boolean emitMae);

  /**
   * Default implementations. Subclasses should feel free to override if it's more efficient to do so.
   */
  public Entity getEntity(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    return getEntities(Collections.singleton(urn), aspectNames).values().stream().findFirst().orElse(null);
  }

  /**
   * Retrieves multiple entities.
   *
   * @param urns set of urns to fetch
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  public Map<Urn, Entity> getEntities(@Nonnull final Set<Urn> urns, @Nonnull Set<String> aspectNames) {
    log.debug(String.format("Invoked getEntities with urns %s, aspects %s", urns, aspectNames));
    if (urns.isEmpty()) {
      return Collections.emptyMap();
    }
    return getSnapshotUnions(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toEntity(entry.getValue())));
  }

  /**
   * Produce metadata audit event and push.
   *
   * @param urn Urn to push
   * @param oldAspectValue Value of aspect before the update.
   * @param newAspectValue Value of aspect after the update
   */
  public void produceMetadataAuditEvent(@Nonnull final Urn urn, @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue, @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata, @Nullable final MetadataAuditOperation operation) {

    final Snapshot newSnapshot = buildSnapshot(urn, newAspectValue);
    Snapshot oldSnapshot = null;
    if (oldAspectValue != null) {
      oldSnapshot = buildSnapshot(urn, oldAspectValue);
    }

    _producer.produceMetadataAuditEvent(urn, oldSnapshot, newSnapshot, oldSystemMetadata, newSystemMetadata, operation);

    // 4.1 Produce aspect specific MAE after a successful update
    if (_emitAspectSpecificAuditEvent) {
      _producer.produceAspectSpecificMetadataAuditEvent(urn, oldAspectValue, newAspectValue, oldSystemMetadata,
          newSystemMetadata, operation);
    }
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

  public void produceMetadataAuditEventForKey(@Nonnull final Urn urn,
      @Nullable final SystemMetadata newSystemMetadata) {

    final Snapshot newSnapshot = buildKeySnapshot(urn);

    _producer.produceMetadataAuditEvent(urn, null, newSnapshot, null, newSystemMetadata, MetadataAuditOperation.UPDATE);
  }

  public RecordTemplate getLatestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName) {
    log.debug(String.format("Invoked getLatestAspect with urn %s, aspect %s", urn, aspectName));
    return getAspect(urn, aspectName, LATEST_ASPECT_VERSION);
  }

  public void ingestEntities(@Nonnull final List<Entity> entities, @Nonnull final AuditStamp auditStamp,
      @Nonnull final List<SystemMetadata> systemMetadata) {
    log.debug(String.format("Invoked ingestEntities with entities %s, audit stamp %s", entities, auditStamp));
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
    log.debug(String.format("Invoked ingestEntity with entity %s, audit stamp %s systemMetadata %s", entity, auditStamp,
        systemMetadata.toString()));
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
  protected Map<Urn, List<UnionTemplate>> getLatestAspectUnions(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {
    return getLatestAspects(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()
            .stream()
            .map(aspectRecord -> toAspectUnion(entry.getKey(), aspectRecord))
            .collect(Collectors.toList())));
  }

  private void ingestSnapshotUnion(@Nonnull final Snapshot snapshotUnion, @Nonnull final AuditStamp auditStamp,
      SystemMetadata systemMetadata) {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = com.linkedin.metadata.dao.utils.ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final List<RecordTemplate> aspectRecordsToIngest =
        com.linkedin.metadata.dao.utils.ModelUtils.getAspectsFromSnapshot(snapshotRecord);

    final String keyAspectName = getKeyAspectName(urn);
    if (getLatestAspect(urn, keyAspectName) == null) {
      aspectRecordsToIngest.add(buildKeyAspect(urn));
    }

    aspectRecordsToIngest.forEach(aspect -> {
      final String aspectName = PegasusUtils.getAspectNameFromSchema(aspect.schema());
      ingestAspect(urn, aspectName, aspect, auditStamp, systemMetadata);
    });
  }

  private Snapshot buildSnapshot(@Nonnull final Urn urn, @Nonnull final RecordTemplate aspectValue) {
    // if the aspect value is the key, we do not need to include the key a second time
    if (PegasusUtils.getAspectNameFromSchema(aspectValue.schema()).equals(getKeyAspectName(urn))) {
      return toSnapshotUnion(toSnapshotRecord(urn, ImmutableList.of(toAspectUnion(urn, aspectValue))));
    }

    final RecordTemplate keyAspectValue = buildKeyAspect(urn);
    return toSnapshotUnion(
        toSnapshotRecord(urn, ImmutableList.of(toAspectUnion(urn, keyAspectValue), toAspectUnion(urn, aspectValue))));
  }

  protected Snapshot buildKeySnapshot(@Nonnull final Urn urn) {
    final RecordTemplate keyAspectValue = buildKeyAspect(urn);
    return toSnapshotUnion(toSnapshotRecord(urn, ImmutableList.of(toAspectUnion(urn, keyAspectValue))));
  }

  protected RecordTemplate buildKeyAspect(@Nonnull final Urn urn) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    final RecordDataSchema keySchema = keySpec.getPegasusSchema();
    return EntityKeyUtils.convertUrnToEntityKey(urn, keySchema);
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
    return com.linkedin.metadata.dao.utils.ModelUtils.newSnapshot(
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
    return com.linkedin.metadata.dao.utils.ModelUtils.newAspectUnion(
        getDataTemplateClassFromSchema(entitySpec.getAspectTyperefSchema(), UnionTemplate.class), aspectRecord);
  }

  protected Urn toUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      log.error(String.format("Failed to convert urn string %s into Urn object", urnStr));
      throw new ModelConversionException(String.format("Failed to convert urn string %s into Urn object ", urnStr), e);
    }
  }

  private Map<String, Set<String>> buildEntityToValidAspects(final EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs()
        .values()
        .stream()
        .collect(Collectors.toMap(EntitySpec::getName,
            entry -> entry.getAspectSpecs().stream().map(AspectSpec::getName).collect(Collectors.toSet())));
  }

  public Boolean getEmitAspectSpecificAuditEvent() {
    return _emitAspectSpecificAuditEvent;
  }

  public void setEmitAspectSpecificAuditEvent(Boolean emitAspectSpecificAuditEvent) {
    _emitAspectSpecificAuditEvent = emitAspectSpecificAuditEvent;
  }

  protected EntityRegistry getEntityRegistry() {
    return _entityRegistry;
  }

  protected Set<String> getEntityAspectNames(final Urn entityUrn) {
    return getEntityAspectNames(urnToEntityName(entityUrn));
  }

  protected Set<String> getEntityAspectNames(final String entityName) {
    return _entityToValidAspects.get(entityName);
  }

  public abstract void setWritable(boolean canWrite);

  public abstract void ingestProposal(MetadataChangeProposal metadataChangeProposal, AuditStamp auditStamp);

  public abstract RollbackRunResult rollbackRun(List<AspectRowSummary> aspectRows, String runId);

  public abstract RollbackRunResult deleteUrn(Urn urn);
}
