package com.linkedin.metadata.entity.ebean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

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

  private Boolean _alwaysEmitAuditEvent = false;

  private final ObjectMapper objectMapper = new ObjectMapper();


  public EbeanEntityService(
      @Nonnull final EbeanAspectDao entityDao,
      @Nonnull final EntityEventProducer eventProducer,
      @Nonnull final EntityRegistry entityRegistry) {
    super(eventProducer, entityRegistry);
    _entityDao = entityDao;
  }

  @Override
  @Nonnull
  public Map<Urn, List<RecordTemplate>> getLatestAspects(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {

    log.debug(String.format("Invoked getLatestAspects with urns: %s, aspectNames: %s", urns, aspectNames));

    // Create DB keys
    final Set<EbeanAspectV2.PrimaryKey> dbKeys = urns.stream()
        .map(urn -> {
          final Set<String> aspectsToFetch = aspectNames.isEmpty()
            ? getEntityAspectNames(urn)
            : aspectNames;
          return aspectsToFetch.stream()
            .map(aspectName -> new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, LATEST_ASPECT_VERSION))
            .collect(Collectors.toList());
        })
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    // Fetch from db and populate urn -> aspect map.
    final Map<Urn, List<RecordTemplate>> urnToAspects = new HashMap<>();

    // Each urn should have some result, regardless of whether aspects are found in the DB.
    for (Urn urn: urns) {
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
    return maybeAspect
        .map(ebeanAspect -> toAspectRecord(urn, aspectName, ebeanAspect.getMetadata(), getEntityRegistry()))
        .orElse(null);
  }

  @Override
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug(String.format("Invoked getVersionedAspect with urn: %s, aspectName: %s, version: %s", urn, aspectName, version));

    VersionedAspect result = new VersionedAspect();

    version = calculateVersionNumber(urn, aspectName, version);

    final EbeanAspectV2.PrimaryKey primaryKey = new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<EbeanAspectV2> maybeAspect = Optional.ofNullable(_entityDao.getAspect(primaryKey));
    RecordTemplate aspect = maybeAspect
        .map(ebeanAspect -> toAspectRecord(urn, aspectName, ebeanAspect.getMetadata(), getEntityRegistry()))
        .orElse(null);

    if (aspect == null) {
      return null;
    }

    Aspect resultAspect = new Aspect();

    RecordUtils.setSelectedRecordTemplateInUnion(
        resultAspect,
        aspect
    );
;
    result.setAspect(resultAspect);
    result.setVersion(version);

    return result;
  }

  @Override
  @Nonnull
  public ListResult<RecordTemplate> listLatestAspects(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      int count) {

    log.debug(String.format("Invoked listLatestAspects with entityName: %s, aspectName: %s, start: %s, count: %s",
        entityName, aspectName, start, count));

    final ListResult<String> aspectMetadataList = _entityDao.listLatestAspectMetadata(entityName, aspectName, start, count);

    final List<RecordTemplate> aspects = new ArrayList<>();
    for (int i = 0; i < aspectMetadataList.getValues().size(); i++) {
      aspects.add(toAspectRecord(aspectMetadataList.getMetadata().getExtraInfos().get(i).getUrn(), aspectName,
          aspectMetadataList.getValues().get(i), getEntityRegistry()));
    }

    return new ListResult<>(
        aspects,
        aspectMetadataList.getMetadata(),
        aspectMetadataList.getNextStart(),
        aspectMetadataList.isHasNext(),
        aspectMetadataList.getTotalCount(),
        aspectMetadataList.getTotalPageCount(),
        aspectMetadataList.getPageSize()
    );
  }

  @Override
  @Nonnull
  public RecordTemplate ingestAspect(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata systemMetadata) {
    log.debug(String.format(
        "Invoked ingestAspect with urn: %s, aspectName: %s, newValue: %s",
        urn, aspectName, newValue));
    return ingestAspect(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  private RecordTemplate ingestAspect(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata providedSystemMetadata,
      final int maxTransactionRetry) {

    final UpdateAspectResult result = _entityDao.runInTransactionWithRetry(() -> {

      // 1. Fetch the latest existing version of the aspect.
      final EbeanAspectV2 latest = _entityDao.getLatestAspect(urn.toString(), aspectName);

      // 2. Compare the latest existing and new.
      final RecordTemplate oldValue =
          latest == null ? null : toAspectRecord(urn, aspectName, latest.getMetadata(), getEntityRegistry());
      final RecordTemplate newValue = updateLambda.apply(Optional.ofNullable(oldValue));

      // 3. If there is no difference between existing and new, we just update
      // the lastObserved in system metadata. RunId should stay as the original runId
      if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
        SystemMetadata latestSystemMetadata = RecordUtils.toRecordTemplate(
            SystemMetadata.class,
            latest.getSystemMetadata()
        );
        latestSystemMetadata.setLastObserved(providedSystemMetadata.getLastObserved());

        latest.setSystemMetadata(RecordUtils.toJsonString(latestSystemMetadata));

        _entityDao.saveAspect(latest, false);

        return new UpdateAspectResult(urn, oldValue, oldValue);
      }

      // 4. Save the newValue as the latest version
      log.debug(String.format("Ingesting aspect with name %s, urn %s", aspectName, urn));
      _entityDao.saveLatestAspect(
          urn.toString(),
          aspectName,
          latest == null ? null : toJsonAspect(oldValue),
          latest == null ? null : latest.getCreatedBy(),
          latest == null ? null : latest.getCreatedFor(),
          latest == null ? null : latest.getCreatedOn(),
          latest == null ? null : latest.getSystemMetadata(),
          toJsonAspect(newValue),
          auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()),
          toJsonAspect(providedSystemMetadata)
      );

      return new UpdateAspectResult(urn, oldValue, newValue);

    }, maxTransactionRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    // 5. Produce MAE after a successful update
    if (oldValue != newValue || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataAuditEvent for ingested aspect %s, urn %s", aspectName, urn));
      produceMetadataAuditEvent(urn, oldValue, newValue, providedSystemMetadata);
    } else {
      log.debug(String.format("Skipped producing MetadataAuditEvent for ingested aspect %s, urn %s. Aspect has not changed.", aspectName, urn));
    }

    return newValue;
  }

  @Override
  @Nonnull
  public RecordTemplate updateAspect(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final RecordTemplate newValue,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final long version,
      @Nonnull final boolean emitMae) {
    log.debug(String.format("Invoked updateAspect with urn: %s, aspectName: %s, newValue: %s, version: %s, emitMae: %s",
        urn, aspectName, newValue, version, emitMae));
    return updateAspect(
        urn,
        aspectName,
        newValue,
        auditStamp,
        version,
        emitMae,
        DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  private RecordTemplate updateAspect(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final RecordTemplate value,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final long version,
      @Nonnull final boolean emitMae,
      final int maxTransactionRetry) {

    final UpdateAspectResult result = _entityDao.runInTransactionWithRetry(() -> {

      final EbeanAspectV2 oldAspect = _entityDao.getAspect(urn.toString(), aspectName, version);
      final RecordTemplate oldValue = oldAspect == null ? null
          : toAspectRecord(urn, aspectName, oldAspect.getMetadata(), getEntityRegistry());

      log.debug(String.format("Updating aspect with name %s, urn %s", aspectName, urn));
      _entityDao.saveAspect(
          urn.toString(),
          aspectName,
          toJsonAspect(value),
          auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()),
          oldAspect != null ? oldAspect.getSystemMetadata() : "{}",
          version,
          oldAspect == null
      );

      return new UpdateAspectResult(urn, oldValue, value);

    }, maxTransactionRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    if (emitMae) {
      log.debug(String.format("Producing MetadataAuditEvent for updated aspect %s, urn %s", aspectName, urn));
      produceMetadataAuditEvent(urn, oldValue, newValue);
    } else {
      log.debug(String.format("Skipped producing MetadataAuditEvent for updated aspect %s, urn %s. emitMAE is false.", aspectName, urn));
    }

    return newValue;
  }

  public void setAlwaysEmitAuditEvent(Boolean alwaysEmitAuditEvent) {
    _alwaysEmitAuditEvent = alwaysEmitAuditEvent;
  }

  public Boolean getAlwaysEmitAuditEvent() {
    return _alwaysEmitAuditEvent;
  }

  @Value
  private static class UpdateAspectResult {
    Urn urn;
    RecordTemplate oldValue;
    RecordTemplate newValue;
  }

  public void setWritable(boolean canWrite) {
    log.debug("Enabling writes");
    _entityDao.setWritable(canWrite);
  }

  public List<IngestionRunSummary> listRuns(
      final Integer minRunSize,
      final Integer maxRunSize,
      final Integer pageOffset,
      final Integer pageSize) {
    return _entityDao.listRuns(minRunSize, maxRunSize, pageOffset, pageSize);
  }

  @Override
  public List<AspectRowSummary> rollbackRun(String runId, Boolean dryRun) {
    List<EbeanAspectV2> relatedAspects = _entityDao.listAspectsRelatedToRun(runId);

    List<EbeanAspectV2> removedAspects = new ArrayList<>();

    relatedAspects.forEach(aspectToRemove -> {

      final UpdateAspectResult result = _entityDao.runInTransactionWithRetry(() -> {

        // 1. Fetch the latest existing version of the aspect.
        final EbeanAspectV2 latest =
            _entityDao.getLatestAspect(aspectToRemove.getKey().getUrn(), aspectToRemove.getKey().getAspect());

        // 2. Compare the latest existing and new. If the aspect is not the latest, ignore
        if (latest.getKey().getVersion() != aspectToRemove.getKey().getVersion()) {
          return null;
        }

        // 3. Fetch what precedes it
        final EbeanAspectV2 previousAspect =
            _entityDao.getAspect(aspectToRemove.getKey().getUrn(), aspectToRemove.getKey().getAspect(), -1);

        // 4. Update the mysql table if not dry run
        if (!dryRun) {
          if (previousAspect != null) {
            _entityDao.deleteAspect(previousAspect);

            EbeanAspectV2.@NonNull PrimaryKey updatedPreviousKey = new EbeanAspectV2.PrimaryKey(
                previousAspect.getKey().getUrn(),
                previousAspect.getKey().getAspect(),
                0
            );

            previousAspect.setKey(updatedPreviousKey);

            _entityDao.saveAspect(previousAspect, false);
          } else {
            _entityDao.deleteAspect(aspectToRemove);
          }
        }

        // 5. Emit the Update
        try {
          final RecordTemplate latestValue = latest == null ? null
              : toAspectRecord(Urn.createFromString(latest.getKey().getUrn()), latest.getKey().getAspect(), latest.getMetadata(),
                  getEntityRegistry());

          final RecordTemplate previousValue = previousAspect == null ? null
              : toAspectRecord(Urn.createFromString(previousAspect.getKey().getUrn()), previousAspect.getKey().getAspect(), previousAspect.getMetadata(),
                  getEntityRegistry());

          return new UpdateAspectResult(Urn.createFromString(aspectToRemove.getKey().getUrn()), latestValue,
              previousValue);
        } catch (URISyntaxException e) {
          e.printStackTrace();
        }

        return null;
      }, DEFAULT_MAX_TRANSACTION_RETRY);

      if (result != null) {
        removedAspects.add(aspectToRemove);
        if (!dryRun) {
          produceMetadataAuditEvent(result.getUrn(), result.getOldValue(), result.getNewValue());
        }
      }
    });

    Stream<AspectRowSummary> results = removedAspects.stream().map(aspect -> {
      AspectRowSummary result = new AspectRowSummary();
      result.setAspectName(aspect.getKey().getAspect());
      result.setRunId(aspect.getSystemMetadata());
      result.setUrn(aspect.getKey().getUrn());
      result.setTimestamp(aspect.getCreatedOn().getTime());
      result.setMetadata(aspect.getMetadata());
      result.setVersion(aspect.getKey().getVersion());
      return result;
    });

    List<AspectRowSummary> response = results.collect(Collectors.toList());
    return response;
  }

  @Override
  public List<String> ghostRun(String runId, Boolean dryRun) {

    List<String> urnsToDelete = _entityDao.fetchUrnsRelatedToRun(runId);

    if (dryRun) {
      return urnsToDelete;
    }

    Status deleteStatus = new Status();
    deleteStatus.setRemoved(true);

    AuditStamp deleteAuditStamp = new AuditStamp();

    Urn deleteActor = null;
    try {
      deleteActor = Urn.createFromString("urn:li:principal:system");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    deleteAuditStamp.setActor(deleteActor);
    deleteAuditStamp.setTime(System.currentTimeMillis());

    SystemMetadata deleteSystemMetadata = new SystemMetadata();
    deleteSystemMetadata.setRunId("revert-" + runId);

    urnsToDelete.forEach(urn -> {
      try {
        ingestAspect(Urn.createFromString(urn), "status", deleteStatus, deleteAuditStamp, deleteSystemMetadata);
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }
    });

    return urnsToDelete;
  }
}
