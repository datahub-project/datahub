package com.linkedin.metadata.entity.ebean;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.event.EntityEventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntityKeyUtils;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.util.AspectDeserializationUtil;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.sql.Timestamp;
import java.util.ArrayList;
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
  private final TimeseriesAspectService _timeseriesAspectService;
  private final JacksonDataTemplateCodec _dataTemplateCodec = new JacksonDataTemplateCodec();
  private Boolean _alwaysEmitAuditEvent = false;

  public EbeanEntityService(@Nonnull final EbeanAspectDao entityDao, @Nonnull final EntityEventProducer eventProducer,
      @Nonnull final EntityRegistry entityRegistry, @Nonnull final TimeseriesAspectService timeseriesAspectService) {
    super(eventProducer, entityRegistry);
    _entityDao = entityDao;
    _timeseriesAspectService = timeseriesAspectService;
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
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp) {
    log.debug("Invoked ingestAspect with urn: {}, aspectName: {}, newValue: {}", urn, aspectName, newValue);
    AddAspectResult result =
        ingestAspectToLocalDB(urn, aspectName, ignored -> newValue, auditStamp, DEFAULT_MAX_TRANSACTION_RETRY);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate updatedValue = result.getNewValue();

    if (oldValue != updatedValue || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataAuditEvent for ingested aspect %s, urn %s", aspectName, urn));
      produceMetadataAuditEvent(urn, oldValue, updatedValue);
    } else {
      log.debug(
          String.format("Skipped producing MetadataAuditEvent for ingested aspect %s, urn %s. Aspect has not changed.",
              aspectName, urn));
    }

    return updatedValue;
  }

  @Nonnull
  private AddAspectResult ingestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp, final int maxTransactionRetry) {

    return _entityDao.runInTransactionWithRetry(() -> {

      // 1. Fetch the latest existing version of the aspect.
      final EbeanAspectV2 latest = _entityDao.getLatestAspect(urn.toString(), aspectName);

      // 2. Compare the latest existing and new.
      final RecordTemplate oldValue =
          latest == null ? null : toAspectRecord(urn, aspectName, latest.getMetadata(), getEntityRegistry());
      final RecordTemplate newValue = updateLambda.apply(Optional.ofNullable(oldValue));

      // 3. Skip updating if there is no difference between existing and new.
      if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
        return new AddAspectResult(urn, oldValue, oldValue);
      }

      // 4. Save the newValue as the latest version
      log.debug(String.format("Ingesting aspect with name %s, urn %s", aspectName, urn));
      _entityDao.saveLatestAspect(urn.toString(), aspectName, latest == null ? null : toJsonAspect(oldValue),
          latest == null ? null : latest.getCreatedBy(), latest == null ? null : latest.getCreatedFor(),
          latest == null ? null : latest.getCreatedOn(), toJsonAspect(newValue), auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()));

      return new AddAspectResult(urn, oldValue, newValue);
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

    final AddAspectResult result = _entityDao.runInTransactionWithRetry(() -> {

      final EbeanAspectV2 oldAspect = _entityDao.getAspect(urn.toString(), aspectName, version);
      final RecordTemplate oldValue =
          oldAspect == null ? null : toAspectRecord(urn, aspectName, oldAspect.getMetadata(), getEntityRegistry());

      log.debug(String.format("Updating aspect with name %s, urn %s", aspectName, urn));
      _entityDao.saveAspect(urn.toString(), aspectName, toJsonAspect(value), auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()), version, oldAspect == null);

      return new AddAspectResult(urn, oldValue, value);
    }, maxTransactionRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    if (emitMae) {
      log.debug(String.format("Producing MetadataAuditEvent for updated aspect %s, urn %s", aspectName, urn));
      produceMetadataAuditEvent(urn, oldValue, newValue);
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

    if (metadataChangeProposal.getEntityKey().isGenericAspect()) {
      log.error("Key as struct is not yet supported");
      return;
    }

    // Validate Key
    try {
      if (metadataChangeProposal.getEntityKey().isUrn()) {
        RecordTemplate entityKey = EntityKeyUtils.convertUrnToEntityKey(metadataChangeProposal.getEntityKey().getUrn(),
            entitySpec.getKeyAspectSpec().getPegasusSchema());
      }
    } catch (RuntimeException re) {
      log.warn("Failed to validate key {}", metadataChangeProposal.getEntityKey().getUrn());
      throw new RuntimeException("Failed to validate key", re);
    }

    if (metadataChangeProposal.getChangeType() == ChangeType.DELETE) {
      log.error("Delete operation is not yet supported");
      return;
    }

    if (!metadataChangeProposal.hasAspectName() || !metadataChangeProposal.hasAspect()) {
      log.error("Aspect and aspect name is required for create and update operations");
      return;
    }

    Urn entityUrn = metadataChangeProposal.getEntityKey().getUrn();
    AspectSpec aspectSpec = entitySpec.getAspectSpec(metadataChangeProposal.getAspectName());

    if (aspectSpec == null) {
      log.error("Unknown aspect {} for entity {}", metadataChangeProposal.getAspectName(),
          metadataChangeProposal.getEntityType());
      return;
    }
    log.debug("aspect spec = {}", aspectSpec);

    RecordTemplate aspect;
    try {
      aspect = AspectDeserializationUtil.deserializeAspect(metadataChangeProposal.getAspect().getValue(),
          metadataChangeProposal.getAspect().getContentType(), aspectSpec);
    } catch (ModelConversionException e) {
      log.error("Could not deserialize {} for aspect {}", metadataChangeProposal.getAspect().getValue(),
          metadataChangeProposal.getAspectName());
      return;
    }
    log.debug("aspect = {}", aspect);

    RecordTemplate oldAspect = null;
    RecordTemplate newAspect = aspect;

    if (!aspectSpec.isTimeseries()) {
      AddAspectResult result =
          ingestAspectToLocalDB(entityUrn, metadataChangeProposal.getAspectName(), ignored -> aspect, auditStamp,
              DEFAULT_MAX_TRANSACTION_RETRY);
      oldAspect = result.oldValue;
      newAspect = result.newValue;
    }

    if (oldAspect != newAspect || _alwaysEmitAuditEvent) {
      log.debug(String.format("Producing MetadataChangeLog for ingested aspect %s, urn %s",
          metadataChangeProposal.getAspectName(), entityUrn));

      final MetadataChangeLog metadataChangeLog = new MetadataChangeLog(metadataChangeProposal.data());
      if (oldAspect != null) {
        GenericAspect oldGenericAspect = new GenericAspect();
        oldGenericAspect.setValue(ByteString.copy(RecordUtils.toJsonString(oldAspect).getBytes()));
        oldGenericAspect.setContentType(AspectDeserializationUtil.JSON);
        metadataChangeLog.setPreviousAspectValue(oldGenericAspect);
      }
      if (newAspect != null) {
        GenericAspect newGenericAspect = new GenericAspect();
        newGenericAspect.setValue(ByteString.copy(RecordUtils.toJsonString(newAspect).getBytes()));
        newGenericAspect.setContentType(AspectDeserializationUtil.JSON);
        metadataChangeLog.setAspect(newGenericAspect);
      }

      // Since only temporal aspect are ingested as of now, simply produce mae event for it
      produceMetadataChangeLog(entityUrn, metadataChangeLog);
    } else {
      log.debug(
          String.format("Skipped producing MetadataAuditEvent for ingested aspect %s, urn %s. Aspect has not changed.",
              metadataChangeProposal.getAspectName(), entityUrn));
    }
  }

  @Override
  public List<EnvelopedAspect> getAspect(@Nonnull final Urn urn, @Nonnull String entityName, @Nonnull String aspectName,
      @Nullable Filter filter, int limit) {
    return _timeseriesAspectService.getAspect(urn, entityName, aspectName, filter, limit);
  }

  @Value
  private static class AddAspectResult {
    Urn urn;
    RecordTemplate oldValue;
    RecordTemplate newValue;
  }
}
