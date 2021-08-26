package com.linkedin.metadata.entity.ebean;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.util.GenericAspectUtils;
import com.linkedin.metadata.utils.mxe.EntityKeyUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.entity.ebean.EbeanUtils.parseSystemMetadata;
import static com.linkedin.metadata.entity.ebean.EbeanUtils.toAspectRecord;
import static com.linkedin.metadata.entity.ebean.EbeanUtils.toJsonAspect;


/**
 * Ebean-based implementation of {@link EntityService}, serving entity and aspect {@link RecordTemplate}s
 * based on data stored in a relational table supported by Ebean ORM.
 */
@Slf4j
public class EbeanEntityService extends EntityService {

  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  private final EbeanAspectDao _entityDao;
  private final JacksonDataTemplateCodec _dataTemplateCodec = new JacksonDataTemplateCodec();
  private Boolean _alwaysEmitAuditEvent = false;

  public EbeanEntityService(@Nonnull final EbeanAspectDao entityDao, @Nonnull final EntityEventProducer eventProducer,
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
    final Set<EbeanAspectV2.PrimaryKey> dbKeys = urns.stream().map(urn -> {
      final Set<String> aspectsToFetch = aspectNames.isEmpty() ? getEntityAspectNames(urn) : aspectNames;
      return aspectsToFetch.stream()
          .map(aspectName -> new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, LATEST_ASPECT_VERSION))
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
          toAspectRecord(urn, aspectName, aspectEntry.getMetadata(), getEntityRegistry());
      urnToAspects.putIfAbsent(urn, new ArrayList<>());
      urnToAspects.get(urn).add(aspectRecord);
    });

    return urnToAspects;
  }

  /*
   * When a user tries to fetch a negative version, we want to index most recent to least recent snapshots.
   * To do this, we want to fetch the maximum version and subtract the negative version from that. Since -1 represents
   * the maximum version, we need to add 1 to the final result.
   */
  private long calculateVersionNumber(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {
    if (version < 0) {
      return _entityDao.getMaxVersion(urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  @Override
  @Nullable
  public RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {

    log.debug(String.format("Invoked getAspect with urn: %s, aspectName: %s, version: %s", urn, aspectName, version));

    version = calculateVersionNumber(urn, aspectName, version);
    final EbeanAspectV2.PrimaryKey primaryKey = new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<EbeanAspectV2> maybeAspect = Optional.ofNullable(_entityDao.getAspect(primaryKey));
    return maybeAspect.map(
        ebeanAspect -> toAspectRecord(urn, aspectName, ebeanAspect.getMetadata(), getEntityRegistry())).orElse(null);
  }

  @Override
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug(String.format("Invoked getVersionedAspect with urn: %s, aspectName: %s, version: %s", urn, aspectName,
        version));

    VersionedAspect result = new VersionedAspect();

    version = calculateVersionNumber(urn, aspectName, version);

    final EbeanAspectV2.PrimaryKey primaryKey = new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<EbeanAspectV2> maybeAspect = Optional.ofNullable(_entityDao.getAspect(primaryKey));
    RecordTemplate aspect =
        maybeAspect.map(ebeanAspect -> toAspectRecord(urn, aspectName, ebeanAspect.getMetadata(), getEntityRegistry()))
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
      aspects.add(toAspectRecord(aspectMetadataList.getMetadata().getExtraInfos().get(i).getUrn(), aspectName,
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
    UpdateAspectResult result = ingestAspectToLocalDB(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata,
        DEFAULT_MAX_TRANSACTION_RETRY);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate updatedValue = result.getNewValue();

    // 5. Produce MAE after a successful update
    if (oldValue != updatedValue || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataAuditEvent for ingested aspect %s, urn %s", aspectName, urn));
      if (aspectName == getKeyAspectName(urn)) {
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

  @Nonnull
  private UpdateAspectResult ingestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata providedSystemMetadata,
      final int maxTransactionRetry) {

    return _entityDao.runInTransactionWithRetry(() -> {

      // 1. Fetch the latest existing version of the aspect.
      final EbeanAspectV2 latest = _entityDao.getLatestAspect(urn.toString(), aspectName);
      final EbeanAspectV2 keyAspect = _entityDao.getLatestAspect(urn.toString(), getKeyAspectName(urn));

      // 2. Compare the latest existing and new.
      final RecordTemplate oldValue =
          latest == null ? null : toAspectRecord(urn, aspectName, latest.getMetadata(), getEntityRegistry());
      final RecordTemplate newValue = updateLambda.apply(Optional.ofNullable(oldValue));

      // 3. If there is no difference between existing and new, we just update
      // the lastObserved in system metadata. RunId should stay as the original runId
      if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
        SystemMetadata latestSystemMetadata = EbeanUtils.parseSystemMetadata(latest.getSystemMetadata());
        latestSystemMetadata.setLastObserved(providedSystemMetadata.getLastObserved());

        latest.setSystemMetadata(RecordUtils.toJsonString(latestSystemMetadata));

        _entityDao.saveAspect(latest, false);

        return new UpdateAspectResult(urn, oldValue, oldValue,
            EbeanUtils.parseSystemMetadata(latest.getSystemMetadata()), latestSystemMetadata,
            MetadataAuditOperation.UPDATE);
      }

      // 4. Save the newValue as the latest version
      log.debug(String.format("Ingesting aspect with name %s, urn %s", aspectName, urn));
      _entityDao.saveLatestAspect(urn.toString(), aspectName, latest == null ? null : toJsonAspect(oldValue),
          latest == null ? null : latest.getCreatedBy(), latest == null ? null : latest.getCreatedFor(),
          latest == null ? null : latest.getCreatedOn(), latest == null ? null : latest.getSystemMetadata(),
          toJsonAspect(newValue), auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()), toJsonAspect(providedSystemMetadata));

      return new UpdateAspectResult(urn, oldValue, newValue,
          latest == null ? null : EbeanUtils.parseSystemMetadata(latest.getSystemMetadata()), providedSystemMetadata,
          MetadataAuditOperation.UPDATE);
    }, maxTransactionRetry);
  }

  @Override
  @Nonnull
  public RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, @Nonnull final long version,
      @Nonnull final boolean emitMae) {
    log.debug(
        String.format("Invoked updateAspect with urn: %s, aspectName: %s, newValue: %s, version: %s, emitMae: %s", urn,
            aspectName, newValue, version, emitMae));
    return updateAspect(urn, aspectName, newValue, auditStamp, version, emitMae, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  private RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate value, @Nonnull final AuditStamp auditStamp, @Nonnull final long version,
      @Nonnull final boolean emitMae, final int maxTransactionRetry) {

    final UpdateAspectResult result = _entityDao.runInTransactionWithRetry(() -> {

      final EbeanAspectV2 oldAspect = _entityDao.getAspect(urn.toString(), aspectName, version);
      final RecordTemplate oldValue =
          oldAspect == null ? null : toAspectRecord(urn, aspectName, oldAspect.getMetadata(), getEntityRegistry());

      SystemMetadata oldSystemMetadata =
          oldAspect == null ? new SystemMetadata() : EbeanUtils.parseSystemMetadata(oldAspect.getSystemMetadata());
      // create a duplicate of the old system metadata to update and write back
      SystemMetadata newSystemMetadata =
          oldAspect == null ? new SystemMetadata() : EbeanUtils.parseSystemMetadata(oldAspect.getSystemMetadata());
      newSystemMetadata.setLastObserved(System.currentTimeMillis());

      log.debug(String.format("Updating aspect with name %s, urn %s", aspectName, urn));
      _entityDao.saveAspect(urn.toString(), aspectName, toJsonAspect(value), auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()), toJsonAspect(newSystemMetadata), version, oldAspect == null);

      return new UpdateAspectResult(urn, oldValue, value, oldSystemMetadata, newSystemMetadata,
          MetadataAuditOperation.UPDATE);
    }, maxTransactionRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    if (emitMae) {
      log.debug(String.format("Producing MetadataAuditEvent for updated aspect %s, urn %s", aspectName, urn));
      produceMetadataAuditEvent(urn, oldValue, newValue, result.getOldSystemMetadata(), result.getNewSystemMetadata(),
          MetadataAuditOperation.UPDATE);
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
    log.debug("Enabling writes");
    _entityDao.setWritable(canWrite);
  }

  @Override
  public void ingestProposal(@Nonnull MetadataChangeProposal metadataChangeProposal, AuditStamp auditStamp) {
    log.debug("entity type = {}", metadataChangeProposal.getEntityType());
    EntitySpec entitySpec = getEntityRegistry().getEntitySpec(metadataChangeProposal.getEntityType());
    log.debug("entity spec = {}", entitySpec);

    Urn entityUrn = EntityKeyUtils.getUrnFromProposal(metadataChangeProposal);

    // Validate Key
    try {
      RecordTemplate entityKey = com.linkedin.metadata.models.EntityKeyUtils.convertUrnToEntityKey(entityUrn,
          entitySpec.getKeyAspectSpec().getPegasusSchema());
    } catch (RuntimeException re) {
      log.warn("Failed to validate key {}", entityUrn);
      throw new RuntimeException("Failed to validate key", re);
    }

    if (metadataChangeProposal.getChangeType() != ChangeType.UPSERT) {
      log.error("Only upsert operation is supported");
      return;
    }

    if (!metadataChangeProposal.hasAspectName() || !metadataChangeProposal.hasAspect()) {
      log.error("Aspect and aspect name is required for create and update operations");
      return;
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(metadataChangeProposal.getAspectName());

    if (aspectSpec == null) {
      log.error("Unknown aspect {} for entity {}", metadataChangeProposal.getAspectName(),
          metadataChangeProposal.getEntityType());
      return;
    }
    log.debug("aspect spec = {}", aspectSpec);

    RecordTemplate aspect;
    try {
      aspect = GenericAspectUtils.deserializeAspect(metadataChangeProposal.getAspect().getValue(),
          metadataChangeProposal.getAspect().getContentType(), aspectSpec);
    } catch (ModelConversionException e) {
      log.error("Could not deserialize {} for aspect {}", metadataChangeProposal.getAspect().getValue(),
          metadataChangeProposal.getAspectName());
      return;
    }
    log.debug("aspect = {}", aspect);

    SystemMetadata systemMetadata = metadataChangeProposal.getSystemMetadata();
    if (systemMetadata == null) {
      systemMetadata = new SystemMetadata();
      systemMetadata.setRunId(DEFAULT_RUN_ID);
      systemMetadata.setLastObserved(System.currentTimeMillis());
    }

    RecordTemplate oldAspect = null;
    SystemMetadata oldSystemMetadata = null;
    RecordTemplate newAspect = aspect;
    SystemMetadata newSystemMetadata = systemMetadata;

    if (!aspectSpec.isTimeseries()) {
      UpdateAspectResult result =
          ingestAspectToLocalDB(entityUrn, metadataChangeProposal.getAspectName(), ignored -> aspect, auditStamp,
              systemMetadata, DEFAULT_MAX_TRANSACTION_RETRY);
      oldAspect = result.oldValue;
      oldSystemMetadata = result.oldSystemMetadata;
      newAspect = result.newValue;
      newSystemMetadata = result.newSystemMetadata;
    }

    if (oldAspect != newAspect || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataChangeLog for ingested aspect %s, urn %s",
          metadataChangeProposal.getAspectName(), entityUrn));

      final MetadataChangeLog metadataChangeLog = new MetadataChangeLog(metadataChangeProposal.data());
      if (oldAspect != null) {
        metadataChangeLog.setPreviousAspectValue(GenericAspectUtils.serializeAspect(oldAspect));
      }
      if (oldSystemMetadata != null) {
        metadataChangeLog.setPreviousSystemMetadata(oldSystemMetadata);
      }
      if (newAspect != null) {
        metadataChangeLog.setAspect(GenericAspectUtils.serializeAspect(newAspect));
      }
      if (newSystemMetadata != null) {
        metadataChangeLog.setSystemMetadata(newSystemMetadata);
      }

      log.debug(String.format("Serialized MCL event: %s", metadataChangeLog.toString()));
      // Since only temporal aspect are ingested as of now, simply produce mae event for it
      produceMetadataChangeLog(entityUrn, aspectSpec, metadataChangeLog);
    } else {
      log.debug(
          String.format("Skipped producing MetadataAuditEvent for ingested aspect %s, urn %s. Aspect has not changed.",
              metadataChangeProposal.getAspectName(), entityUrn));
    }
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
    final RollbackResult result = _entityDao.runInTransactionWithRetry(() -> {
      Integer additionalRowsDeleted = 0;

      // 1. Fetch the latest existing version of the aspect.
      final EbeanAspectV2 latest = _entityDao.getLatestAspect(urn, aspectName);

      // 1.1 If no latest exists, skip this aspect
      if (latest == null) {
        return null;
      }

      // 2. Compare the latest run id. If the run id does not match this run, ignore.
      SystemMetadata latestSystemMetadata = EbeanUtils.parseSystemMetadata(latest.getSystemMetadata());
      String latestMetadata = latest.getMetadata();
      if (!latestSystemMetadata.getRunId().equals(runId)) {
        return null;
      }

      // 3. Fetch what precedes it, if there is another aspect
      final long maxVersion = _entityDao.getMaxVersion(urn, aspectName);
      EbeanAspectV2 previousAspect = null;
      String previousMetadata = null;
      if (maxVersion > 0) {
        previousAspect = _entityDao.getAspect(urn, aspectName, maxVersion);
        previousMetadata = previousAspect.getMetadata();
      }

      // 4. Update the mysql table
      Boolean isKeyAspect = false;
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
        final RecordTemplate latestValue = latest == null ? null
            : toAspectRecord(Urn.createFromString(latest.getKey().getUrn()), latest.getKey().getAspect(),
                latestMetadata, getEntityRegistry());

        final RecordTemplate previousValue = previousAspect == null ? null
            : toAspectRecord(Urn.createFromString(previousAspect.getKey().getUrn()),
                previousAspect.getKey().getAspect(), previousMetadata, getEntityRegistry());

        return new RollbackResult(Urn.createFromString(urn), latestValue,
            previousValue == null ? latestValue : previousValue, latestSystemMetadata,
            previousValue == null ? null : parseSystemMetadata(previousAspect.getSystemMetadata()),
            previousAspect == null ? MetadataAuditOperation.DELETE : MetadataAuditOperation.UPDATE, isKeyAspect,
            additionalRowsDeleted);
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }

      return null;
    }, DEFAULT_MAX_TRANSACTION_RETRY);

    return result;
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
    EbeanAspectV2 latestKey = _entityDao.getLatestAspect(urn.toString(), keyAspectName);
    if (latestKey == null || latestKey.getSystemMetadata() == null) {
      return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
    }

    SystemMetadata latestKeySystemMetadata = parseSystemMetadata(latestKey.getSystemMetadata());

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
}
