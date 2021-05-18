package com.linkedin.metadata.dao;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.experimental.Entity;
import com.linkedin.metadata.EntitySpecUtils;
import com.linkedin.metadata.dao.ebean.EbeanAspect;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.producer.EntityKafkaMetadataEventProducer;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntityKeyUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

import static com.linkedin.metadata.EntitySpecUtils.*;
import static com.linkedin.metadata.dao.EbeanAspectDao.*;


public class EntityService {

  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  private final EbeanAspectDao _entityDao;
  private final EntityKafkaMetadataEventProducer _kafkaProducer;
  private final EntityRegistry _entityRegistry;

  private final Map<String, Set<String>> _entityToValidAspects;
  private Boolean _emitAspectSpecificAuditEvent = false;

  /**
   * Constructs an Entity Service object.
   *
   * @param entityDao
   * @param kafkaProducer
   */
  public EntityService(
      @Nonnull final EbeanAspectDao entityDao,
      @Nonnull final EntityKafkaMetadataEventProducer kafkaProducer,
      @Nonnull final EntityRegistry entityRegistry) {
    _entityDao = entityDao;
    _kafkaProducer = kafkaProducer;
    _entityRegistry = entityRegistry;
    _entityToValidAspects = buildEntityToValidAspects(entityRegistry);
  }

  @Nullable
  public Entity getEntity(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    return batchGetEntities(Collections.singleton(urn), aspectNames).entrySet().stream()
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  @Nonnull
  public Map<Urn, Entity> batchGetEntities(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return batchGetSnapshotUnion(urns, aspectNames).entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> toEntity(entry.getValue())));
  }

  @Nonnull
  public Map<Urn, Snapshot> batchGetSnapshotUnion(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return batchGetSnapshotRecord(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toSnapshotUnion(entry.getValue())));
  }

  @Nonnull
  public Map<Urn, RecordTemplate> batchGetSnapshotRecord(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return batchGetAspectUnionLists(urns, aspectNames).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> toSnapshotRecord(entry.getKey(), entry.getValue())));
  }

  @Nonnull
  public Map<Urn, List<UnionTemplate>> batchGetAspectUnionLists(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    return batchGetAspectRecordLists(urns, aspectNames).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          final EntitySpec entitySpec = _entityRegistry.getEntitySpec(urnToEntityName(entry.getKey()));
          return entry.getValue().stream().map(aspectRecord -> toAspectUnion(entitySpec.getAspectTyperefSchema(), aspectRecord))
              .collect(Collectors.toList());
        }));
  }

  @Nonnull
  public Map<Urn, List<RecordTemplate>> batchGetAspectRecordLists(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {
    // Create DB keys
    final Set<EbeanAspect.PrimaryKey> dbKeys = urns.stream()
        .map(urn -> {
          final Set<String> aspectsToFetch = aspectNames.isEmpty()
              ? _entityToValidAspects.get(urnToEntityName(urn))
              : aspectNames;
          return aspectsToFetch.stream()
              .map(aspectName -> new EbeanAspect.PrimaryKey(urn.toString(), aspectName, LATEST_VERSION))
              .collect(Collectors.toList());
        })
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    // Fetch from db and populate urn -> aspect map.
    final Map<Urn, List<RecordTemplate>> urnToAspects = new HashMap<>();
    _entityDao.batchGet(dbKeys).forEach((key, aspectEntry) -> {
      final Urn urn = toUrn(key.getUrn());
      final String aspectName = key.getAspect();
      final RecordTemplate aspectRecord = toAspectRecord(urnToEntityName(urn), aspectName, aspectEntry.getMetadata());
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
      urnToAspects.get(urn).add(aspectRecord);
    });

    // Add "key" aspects to any non null keys.
    urnToAspects.keySet().forEach(key -> {
      final RecordTemplate keyAspect = buildKeyAspect(key);
      urnToAspects.get(key).add(keyAspect);
    });

    return urnToAspects;
  }

  @Nullable
  public RecordTemplate getLatestAspectRecord(@Nonnull final Urn urn, @Nonnull final String aspectName) {
    return getAspectRecord(urn, aspectName, LATEST_VERSION);
  }

  @Nullable
  public RecordTemplate getAspectRecord(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {
    final EbeanAspect.PrimaryKey primaryKey = new EbeanAspect.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<EbeanAspect> maybeAspect = Optional.ofNullable(_entityDao.getAspect(primaryKey));
    return maybeAspect
        .map(ebeanAspect -> toAspectRecord(urnToEntityName(urn), aspectName, ebeanAspect.getMetadata()))
        .orElse(null);
  }

  @Nonnull
  public ListResult<RecordTemplate> listAspects(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final int start,
      @Nonnull int count) {

    final ListResult<String> aspectMetadataList = _entityDao.listLatestAspectMetadata(aspectName, start, count);
    final List<RecordTemplate> aspects = aspectMetadataList.getValues()
        .stream()
        .map(aspectMetadata -> toAspectRecord(entityName, aspectName, aspectMetadata))
        .collect(Collectors.toList());
    return new ListResult<>(
        aspects,
        aspectMetadataList.getMetadata(),
        aspectMetadataList.getNextStart(),
        aspectMetadataList.isHavingMore(),
        aspectMetadataList.getTotalCount(),
        aspectMetadataList.getTotalPageCount(),
        aspectMetadataList.getPageSize()
    );
  }

  public void ingestEntities(@Nonnull final List<Entity> entities, @Nonnull final AuditStamp auditStamp) {
    // TODO: Make this more efficient.
    for (final Entity entity : entities) {
      ingestSnapshot(entity.getValue(), auditStamp);
    }
  }

  public void ingestEntity(@Nonnull final Entity entity, @Nonnull final AuditStamp auditStamp) {
    ingestSnapshot(entity.getValue(), auditStamp);
  }

  public void ingestSnapshot(@Nonnull final Snapshot snapshotUnion, @Nonnull final AuditStamp auditStamp) {
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(snapshotUnion);
    final Urn urn = ModelUtils.getUrnFromSnapshot(snapshotRecord);
    final List<RecordTemplate> aspectRecordsToIngest = ModelUtils.getAspectsFromSnapshot(snapshotRecord);

    // TODO the following should run in a transaction.
    aspectRecordsToIngest.stream().map(aspect -> {
      final String aspectName = EntitySpecUtils.getAspectNameFromSchema(aspect.schema());
      return ingestAspect(urn, aspectName, aspect, auditStamp); // TODO: Can we memoize this lookup?
    })
    .collect(Collectors.toList());
  }


  @Nonnull
  public RecordTemplate ingestAspect(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue,
      @Nonnull final AuditStamp auditStamp) {
    return ingestAspect(urn, aspectName, ignored -> newValue, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  public RecordTemplate ingestAspect(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp,
      final int maxTransactionRetry) {

    final AddAspectResult result = _entityDao.runInTransactionWithRetry(() -> {

      // 1. Fetch the latest existing version of the aspect.
      final EbeanAspect latest = _entityDao.getLatestAspect(urn.toString(), aspectName);

      // 2. Compare the latest existing and new.
      final RecordTemplate oldValue = latest == null ? null : toAspectRecord(urnToEntityName(urn), aspectName, latest.getMetadata());
      final RecordTemplate newValue = updateLambda.apply(Optional.ofNullable(oldValue));

      // 2. Skip updating if there is no difference between existing and new.
      if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
        return new AddAspectResult(urn, oldValue, oldValue);
      }

      // 3. Save the newValue as the latest version
      _entityDao.saveLatestAspect(
          urn.toString(),
          aspectName,
          latest == null ? null : toJsonAspect(oldValue),
          latest == null ? null : latest.getCreatedBy(),
          latest == null ? null : latest.getCreatedFor(),
          latest == null ? null : latest.getCreatedOn(),
          toJsonAspect(newValue),
          auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime())
      );

      return new AddAspectResult(urn, oldValue, newValue);

    }, maxTransactionRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    // 4. Produce MAE after a successful update
    if (oldValue != newValue) {
      produceMetadataAuditEvent(urn, oldValue, newValue);
    }

    return newValue;
  }

  @Value
  private static class AddAspectResult {
    Urn urn;
    RecordTemplate oldValue;
    RecordTemplate newValue;
  }

  private void produceMetadataAuditEvent(@Nonnull final Urn urn, @Nullable final RecordTemplate oldValue, @Nonnull final RecordTemplate newValue) {
    // First, try to create a new and an old snapshot.
    final Snapshot newSnapshot = buildSnapshot(urn, newValue);
    Snapshot oldSnapshot = null;
    if (oldValue != null) {
      oldSnapshot = buildSnapshot(urn, oldValue);
    }

    _kafkaProducer.produceMetadataAuditEvent(urn, oldSnapshot, newSnapshot);

    // 4.1 Produce aspect specific MAE after a successful update
    if (_emitAspectSpecificAuditEvent) {
      _kafkaProducer.produceAspectSpecificMetadataAuditEvent(urn, oldValue, newValue);
    }
  }

  private Snapshot buildSnapshot(@Nonnull final Urn urn, @Nonnull final RecordTemplate aspectValue) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
    final RecordTemplate keyAspectValue = buildKeyAspect(urn);

    return toSnapshotUnion(
        toSnapshotRecord(
            urn,
            ImmutableList.of(toAspectUnion(spec.getAspectTyperefSchema(), keyAspectValue), toAspectUnion(spec.getAspectTyperefSchema(), aspectValue))
        )
    );
  }

  private RecordTemplate buildKeyAspect(@Nonnull final Urn urn) {
    final EntitySpec spec = _entityRegistry.getEntitySpec(urnToEntityName(urn));
    final AspectSpec keySpec = spec.getAspectSpecs().stream().filter(AspectSpec::isKey).findFirst().get();
    final RecordDataSchema keySchema = keySpec.getPegasusSchema();
    return EntityKeyUtils.convertUrnToEntityKey(urn, keySchema);
  }

  private Urn toUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new ModelConversionException(String.format("Failed to convert urn string %s into Urn object ", urnStr), e);
    }
  }

  private Entity toEntity(@Nonnull final Snapshot snapshot) {
    return new Entity().setValue(snapshot);
  }

  private Snapshot toSnapshotUnion(@Nonnull final RecordTemplate snapshotRecord) {
    final Snapshot snapshot = new Snapshot();
    RecordUtils.setSelectedRecordTemplateInUnion(
        snapshot,
        snapshotRecord
    );
    return snapshot;
  }

  private RecordTemplate toSnapshotRecord(
      @Nonnull final Urn urn,
      @Nonnull final List<UnionTemplate> aspectUnionTemplates) {
    final String entityName = urnToEntityName(urn);
    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    return ModelUtils.newSnapshot(
        getDataTemplateClassFromSchema(entitySpec.getSnapshotSchema(), RecordTemplate.class),
        urn,
        aspectUnionTemplates);
  }

  private UnionTemplate toAspectUnion(
      @Nonnull final TyperefDataSchema aspectUnionSchema,
      @Nonnull final RecordTemplate aspectRecord) {
    // TODO:
    return ModelUtils.newAspectUnion(
        getDataTemplateClassFromSchema(aspectUnionSchema, UnionTemplate.class),
        aspectRecord
    );
  }

  private RecordTemplate toAspectRecord(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final String jsonAspect) {

    final EntitySpec entitySpec = _entityRegistry.getEntitySpec(entityName);
    final AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
    final RecordDataSchema aspectSchema = aspectSpec.getPegasusSchema();
    return RecordUtils.toRecordTemplate(getDataTemplateClassFromSchema(aspectSchema, RecordTemplate.class), jsonAspect);
  }

  @Nonnull
  private static String toJsonAspect(@Nonnull final RecordTemplate aspectRecord) {
    return RecordUtils.toJsonString(aspectRecord);
  }

  private Map<String, Set<String>> buildEntityToValidAspects(final EntityRegistry entityRegistry) {
    return entityRegistry.getEntitySpecs()
        .stream()
        .collect(Collectors.toMap(EntitySpec::getName,
            entry -> entry.getAspectSpecs().stream()
                .map(AspectSpec::getName)
                .collect(Collectors.toSet())
        ));
  }
}
