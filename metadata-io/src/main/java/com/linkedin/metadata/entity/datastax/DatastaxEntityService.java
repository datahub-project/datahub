package com.linkedin.metadata.entity.datastax;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;

import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import com.linkedin.metadata.entity.EntityUtils;

@Slf4j
public class DatastaxEntityService extends EntityService {

  private final DatastaxAspectDao _entityDao;
  private Boolean _alwaysEmitAuditEvent = false;
  private static final int DEFAULT_MAX_CONDITIONAL_RETRY = 3;

  public DatastaxEntityService(@Nonnull final DatastaxAspectDao entityDao, @Nonnull final EntityEventProducer eventProducer,
      @Nonnull final EntityRegistry entityRegistry) {
    super(eventProducer, entityRegistry);
    _entityDao = entityDao;
  }

  @Override
  @Nonnull
  public Map<Urn, List<RecordTemplate>> getLatestAspects(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {

    log.debug(String.format("Invoked getLatestAspects with urns: %s, aspectNames: %s", urns, aspectNames));

    // Create DB keys
    final Set<DatastaxAspect.PrimaryKey> dbKeys = urns.stream().map(urn -> {
      final Set<String> aspectsToFetch = aspectNames.isEmpty() ? getEntityAspectNames(urn) : aspectNames;
      return aspectsToFetch.stream()
          .map(aspectName -> new DatastaxAspect.PrimaryKey(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
          .collect(Collectors.toList());
    }).flatMap(List::stream).collect(Collectors.toSet());

    // Fetch from db and populate urn -> aspect map.
    final Map<Urn, List<RecordTemplate>> urnToAspects = new HashMap<>();

    // Each urn should have some result, regardless of whether aspects are found in the DB.
    for (Urn urn : urns) {
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
    }

    // Add "key" aspects for each urn. TODO: Replace this with a materialized key aspect.
    urnToAspects.keySet().forEach(key -> {
      final RecordTemplate keyAspect = buildKeyAspect(key);
      urnToAspects.get(key).add(keyAspect);
    });

    _entityDao.batchGet(dbKeys).forEach((key, aspectEntry) -> {
      final Urn urn = toUrn(key.getUrn());
      final String aspectName = key.getAspect();
      // for now, don't add the key aspect here- we have already added it above
      if (aspectName.equals(getKeyAspectName(urn))) {
        return;
      }

      final RecordTemplate aspectRecord =
          EntityUtils.toAspectRecord(urn, aspectName, aspectEntry.getMetadata(), getEntityRegistry());
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
      urnToAspects.get(urn).add(aspectRecord);
    });

    return urnToAspects;
  }

  /*
   * When a user tries to fetch a negative version, we want to index most recent to the least recent snapshots.
   * To do this, we want to fetch the maximum version and subtract the negative version from that. Since -1 represents
   * the maximum version, we need to add 1 to the final result.
   */
  private long calculateVersionNumber(@Nonnull final Urn urn, @Nonnull final String aspectName, long version) {
    if (version < 0) {
      return _entityDao.getMaxVersion(urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  @Override
  @Nullable
  public RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, long version) {
    log.debug(String.format("Invoked getAspect with urn: %s, aspectName: %s, version: %s", urn, aspectName, version));

    version = calculateVersionNumber(urn, aspectName, version);
    final DatastaxAspect.PrimaryKey primaryKey = new DatastaxAspect.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<DatastaxAspect> maybeAspect = Optional.ofNullable(_entityDao.getAspect(primaryKey));

    return maybeAspect.map(
        aspect -> EntityUtils.toAspectRecord(urn, aspectName, aspect.getMetadata(), getEntityRegistry())).orElse(null);
  }

  @Override
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug(String.format("Invoked getVersionedAspect with urn: %s, aspectName: %s, version: %s", urn, aspectName,
        version));

    VersionedAspect result = new VersionedAspect();

    version = calculateVersionNumber(urn, aspectName, version);

    final Optional<DatastaxAspect> maybeAspect = Optional.ofNullable(_entityDao.getAspect(urn.toString(), aspectName, version));

    RecordTemplate aspect =
        maybeAspect.map(a -> EntityUtils.toAspectRecord(urn, aspectName, a.getMetadata(), getEntityRegistry()))
            .orElse(null);

    if (aspect == null) {
      return null;
    }

    Aspect resultAspect = new Aspect();

    RecordUtils.setSelectedRecordTemplateInUnion(resultAspect, aspect);
    result.setAspect(resultAspect);
    result.setVersion(version);

    return result;
  }

  @Override
  @Nonnull
  public ListResult<RecordTemplate> listLatestAspects(@Nonnull final String entityName,
      @Nonnull final String aspectName, final int start, int count) {

    log.debug(
        String.format("Invoked listLatestAspects with entityName: %s, aspectName: %s, start: %s, count: %s", entityName,
            aspectName, start, count));

    final ListResult<String> aspectMetadataList =
        _entityDao.listLatestAspectMetadata(entityName, aspectName, start, count);

    final List<RecordTemplate> aspects = new ArrayList<>();
    for (int i = 0; i < aspectMetadataList.getValues().size(); i++) {
      aspects.add(EntityUtils.toAspectRecord(aspectMetadataList.getMetadata().getExtraInfos().get(i).getUrn(), aspectName,
          aspectMetadataList.getValues().get(i), getEntityRegistry()));
    }

    return new ListResult<>(aspects, aspectMetadataList.getMetadata(), aspectMetadataList.getNextStart(),
        aspectMetadataList.isHasNext(), aspectMetadataList.getTotalCount(), aspectMetadataList.getTotalPageCount(),
        aspectMetadataList.getPageSize());
  }

  @Override
  @Nonnull
  public RecordTemplate ingestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata systemMetadata) {
    log.debug("Invoked ingestAspect with urn: {}, aspectName: {}, newValue: {}", urn, aspectName, newValue);

    UpdateAspectResult result = ingestAspectToLocalDB(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata, DEFAULT_MAX_CONDITIONAL_RETRY);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate updatedValue = result.getNewValue();

    // 5. Produce MAE after a successful update
    if (oldValue != updatedValue || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataAuditEvent for ingested aspect %s, urn %s", aspectName, urn));
      if (aspectName.equals(getKeyAspectName(urn))) {
        produceMetadataAuditEventForKey(urn, result.getNewSystemMetadata());
      } else {
        produceMetadataAuditEvent(urn, oldValue, updatedValue, result.getOldSystemMetadata(),
            result.getNewSystemMetadata(), MetadataAuditOperation.UPDATE);
      }
    } else {
      log.debug(
          String.format("Skipped producing MetadataAuditEvent for ingested aspect %s, urn %s. Aspect has not changed.",
              aspectName, urn));
    }

    return updatedValue;
  }

  private int getNextVersion(List<DatastaxAspect> aspectVersions) {
    int maxVersion = -1;
    for (DatastaxAspect da : aspectVersions ) {
      int version  = (int) da.getVersion();
      if (version > maxVersion) {
        maxVersion = version;
      }
    }
    return maxVersion + 1;
  }

  @Nonnull
  private UpdateAspectResult ingestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
    @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
    @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata providedSystemMetadata,
                                                   final int maxConditionalRetry) {

    return _entityDao.runInConditionalWithRetry(() -> {

      // 1. Fetch all versions of the aspect.
      final List<DatastaxAspect> aspectVersions = _entityDao.getAllAspects(urn.toString(), aspectName);
      final int nextVersion = getNextVersion(aspectVersions);

      // 2. Compare the latest existing and new.
      final DatastaxAspect latest = aspectVersions.size() > 0 ? aspectVersions.get(0) : null;
      final RecordTemplate oldValue = latest == null ? null
              : EntityUtils.toAspectRecord(urn, aspectName, latest.getMetadata(), getEntityRegistry());
      final RecordTemplate newValue = updateLambda.apply(Optional.ofNullable(oldValue));

      if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
        SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
        latestSystemMetadata.setLastObserved(providedSystemMetadata.getLastObserved());

        final DatastaxAspect latestUpdated = new DatastaxAspect(latest.getUrn(), latest.getAspect(),
                latest.getVersion(), latest.getMetadata(), RecordUtils.toJsonString(latestSystemMetadata), latest.getCreatedOn(),
                latest.getCreatedBy(), latest.getCreatedFor());

        _entityDao.condUpsertAspect(latestUpdated, latest);

        return new UpdateAspectResult(urn, oldValue, oldValue,
                EntityUtils.parseSystemMetadata(latestUpdated.getSystemMetadata()), latestSystemMetadata,
                MetadataAuditOperation.UPDATE);
      }

      log.debug(String.format("Ingesting aspect with name %s, urn %s", aspectName, urn));

      _entityDao.batchSaveLatestAspect(urn.toString(), aspectName, latest == null ? null : EntityUtils.toJsonAspect(oldValue),
              latest == null ? null : latest.getCreatedBy(), latest == null ? null : latest.getCreatedFor(),
              latest == null ? null : latest.getCreatedOn(), latest == null ? null : latest.getSystemMetadata(),
              EntityUtils.toJsonAspect(newValue), auditStamp.getActor().toString(),
              auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
              new Timestamp(auditStamp.getTime()), EntityUtils.toJsonAspect(providedSystemMetadata), nextVersion);

      return new UpdateAspectResult(urn, oldValue, newValue,
              latest == null ? null : EntityUtils.parseSystemMetadata(latest.getSystemMetadata()), providedSystemMetadata,
              MetadataAuditOperation.UPDATE);

    }, maxConditionalRetry);

  }

  @Nonnull
  public RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, final long version,
      final boolean emitMae) {

    log.debug(
        String.format("Invoked updateAspect with urn: %s, aspectName: %s, newValue: %s, version: %s, emitMae: %s", urn,
            aspectName, newValue, version, emitMae));
    return updateAspect(urn, aspectName, newValue, auditStamp, version, emitMae, DEFAULT_MAX_CONDITIONAL_RETRY);
  }

  @Nonnull
  private RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
    @Nonnull final RecordTemplate value, @Nonnull final AuditStamp auditStamp, final long version,
    final boolean emitMae, final int maxConditionalUpdateRetry) {

    final UpdateAspectResult result = _entityDao.runInConditionalWithRetry(() -> {

      final DatastaxAspect oldAspect = _entityDao.getAspect(urn.toString(), aspectName, version);

      final RecordTemplate oldValue = oldAspect == null ? null
              : EntityUtils.toAspectRecord(urn, aspectName, oldAspect.getMetadata(), getEntityRegistry());

      SystemMetadata oldSystemMetadata = oldAspect == null ? new SystemMetadata()
              : EntityUtils.parseSystemMetadata(oldAspect.getSystemMetadata());
      // create a duplicate of the old system metadata to update and write back
      SystemMetadata newSystemMetadata = oldAspect == null ? new SystemMetadata()
              : EntityUtils.parseSystemMetadata(oldAspect.getSystemMetadata());
      newSystemMetadata.setLastObserved(System.currentTimeMillis());

      log.debug(String.format("Updating aspect with name %s, urn %s", aspectName, urn));

      DatastaxAspect aspectToSave = new DatastaxAspect(urn.toString(), aspectName, version,
              RecordUtils.toJsonString(value), RecordUtils.toJsonString(newSystemMetadata),
              new Timestamp(auditStamp.getTime()), auditStamp.getActor().toString(),
              auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null);

      ResultSet resultSet = _entityDao.condUpsertAspect(aspectToSave, oldAspect);
      if (!resultSet.wasApplied()) {
        throw new ConditionalWriteFailedException("aspect update failed");
      }

      return new UpdateAspectResult(urn, oldValue, value, oldSystemMetadata,
              newSystemMetadata, MetadataAuditOperation.UPDATE);
    }, maxConditionalUpdateRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    if (emitMae) {
      log.debug(String.format("Producing MetadataAuditEvent for updated aspect %s, urn %s", aspectName, urn));
      produceMetadataAuditEvent(urn, oldValue, newValue, result.getOldSystemMetadata(),
          result.getNewSystemMetadata(), MetadataAuditOperation.UPDATE);
    } else {
      log.debug(String.format("Skipped producing MetadataAuditEvent for updated aspect %s, urn %s. emitMAE is false.",
          aspectName, urn));
    }

    return newValue;
  }

  public Boolean getAlwaysEmitAuditEvent() {
    return _alwaysEmitAuditEvent;
  }

  public void setAlwaysEmitAuditEvent(Boolean alwaysEmitAuditEvent) {
    _alwaysEmitAuditEvent = alwaysEmitAuditEvent;
  }

  public void setWritable(boolean canWrite) {
  }

  @Override
  public Urn ingestProposal(@Nonnull MetadataChangeProposal metadataChangeProposal, AuditStamp auditStamp) {
    return null;
  }

  @Value
  private static class UpdateAspectResult {
    Urn urn;
    RecordTemplate oldValue;
    RecordTemplate newValue;
    SystemMetadata oldSystemMetadata;
    SystemMetadata newSystemMetadata;
    MetadataAuditOperation operation;
  }

  public RollbackResult deleteAspect(String urn, String aspectName, String runId) {
    int additionalRowsDeleted = 0;

    // 1. Fetch the latest existing version of the aspect.
    final DatastaxAspect latest = _entityDao.getLatestAspect(urn, aspectName);

    // 1.1 If no latest exists, skip this aspect
    if (latest == null) {
      return null;
    }

    // 2. Compare the latest run id. If the run id does not match this run, ignore.
    SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
    String latestMetadata = latest.getMetadata();
    if (!latestSystemMetadata.getRunId().equals(runId)) {
      return null;
    }

    // 3. Fetch what precedes it, if there is another aspect
    final long maxVersion = _entityDao.getMaxVersion(urn, aspectName);
    DatastaxAspect previousAspect = null;
    String previousMetadata = null;
    if (maxVersion > 0) {
      previousAspect = _entityDao.getAspect(urn, aspectName, maxVersion);
      previousMetadata = previousAspect.getMetadata();
    }

    // 4. Update the mysql table
    boolean isKeyAspect = false;
    try {
      isKeyAspect = getKeyAspectName(Urn.createFromString(urn)).equals(aspectName);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    if (previousAspect != null) {
      // if there was a previous aspect, delete it and them write it to version 0
      latest.setMetadata(previousAspect.getMetadata());
      latest.setSystemMetadata(previousAspect.getSystemMetadata());
      _entityDao.saveAspect(latest, false);
      _entityDao.deleteAspect(previousAspect);
    } else {
      // if this is the key aspect, we also want to delete the entity entirely
      if (isKeyAspect) {
        if (_entityDao.getEarliestAspect(urn).get().getCreatedOn().equals(latest.getCreatedOn())) {
          additionalRowsDeleted = _entityDao.deleteUrn(urn);
          _entityDao.deleteAspect(latest);
        } else {
          return null;
        }
      } else {
        // if there was not a previous aspect, just delete the latest one
        _entityDao.deleteAspect(latest);
      }
    }

    // 5. Emit the Update
    try {
      final RecordTemplate latestValue = EntityUtils.toAspectRecord(
              Urn.createFromString(latest.getUrn()), latest.getAspect(), latestMetadata, getEntityRegistry());

      final RecordTemplate previousValue = previousAspect == null ? null
          : EntityUtils.toAspectRecord(Urn.createFromString(previousAspect.getUrn()), previousAspect.getAspect(),
              previousMetadata, getEntityRegistry());

      return new RollbackResult(Urn.createFromString(urn), latestValue,
          previousValue == null ? latestValue : previousValue, latestSystemMetadata,
          previousValue == null ? null : EntityUtils.parseSystemMetadata(previousAspect.getSystemMetadata()),
          previousAspect == null ? MetadataAuditOperation.DELETE : MetadataAuditOperation.UPDATE, isKeyAspect,
          additionalRowsDeleted);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public RollbackRunResult rollbackRun(List<AspectRowSummary> aspectRows, String runId) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    AtomicInteger rowsDeletedFromEntityDeletion = new AtomicInteger(0);

    aspectRows.forEach(aspectToRemove -> {

      RollbackResult result = deleteAspect(aspectToRemove.getUrn(), aspectToRemove.getAspectName(), runId);

      if (result != null) {
        rowsDeletedFromEntityDeletion.addAndGet(result.additionalRowsAffected);
        removedAspects.add(aspectToRemove);
        produceMetadataAuditEvent(result.getUrn(), result.getOldValue(), result.getNewValue(),
            result.getOldSystemMetadata(), result.getNewSystemMetadata(), result.getOperation());
      }
    });

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion.get());
  }

  @Override
  public RollbackRunResult deleteUrn(Urn urn) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    Integer rowsDeletedFromEntityDeletion = 0;

    String keyAspectName = getKeyAspectName(urn);
    DatastaxAspect latestKey = _entityDao.getLatestAspect(urn.toString(), keyAspectName);
    if (latestKey == null || latestKey.getSystemMetadata() == null) {
      return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
    }

    SystemMetadata latestKeySystemMetadata = EntityUtils.parseSystemMetadata(latestKey.getSystemMetadata());

    RollbackResult result = deleteAspect(urn.toString(), keyAspectName, latestKeySystemMetadata.getRunId());

    if (result != null) {
      AspectRowSummary summary = new AspectRowSummary();
      summary.setUrn(urn.toString());
      summary.setKeyAspect(true);
      summary.setAspectName(keyAspectName);
      summary.setVersion(0);
      summary.setTimestamp(latestKey.getCreatedOn().getTime());

      rowsDeletedFromEntityDeletion = result.additionalRowsAffected;
      removedAspects.add(summary);
      produceMetadataAuditEvent(result.getUrn(), result.getOldValue(), result.getNewValue(),
          result.getOldSystemMetadata(), result.getNewSystemMetadata(), result.getOperation());
    }

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
  }

  @Override
  public Boolean exists(Urn urn) {
    final Set<String> aspectsToFetch = getEntityAspectNames(urn);
    final List<DatastaxAspect.PrimaryKey> dbKeys = aspectsToFetch.stream()
            .map(aspectName -> new DatastaxAspect.PrimaryKey(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
            .collect(Collectors.toList());

    Map<DatastaxAspect.PrimaryKey, DatastaxAspect> aspects = _entityDao.batchGet(new HashSet(dbKeys));
    return aspects.values().stream().anyMatch(aspect -> aspect != null);
  }

  @Override
  @Nonnull
  public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count) {
    log.debug(String.format("Invoked listUrns with entityName: %s, start: %s, count: %s", entityName, start, count));

    // If a keyAspect exists, the entity exists.
    final String keyAspectName = getEntityRegistry().getEntitySpec(entityName).getKeyAspectSpec().getName();
    final ListResult<String> keyAspectList = _entityDao.listUrns(keyAspectName, start, count);

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
        throw new IllegalArgumentException(String.format("Failed to convert urn %s found in db to Urn object.", urn), e);
      }
    }
    result.setEntities(entityUrns);
    return result;
  }
}