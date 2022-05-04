package com.linkedin.metadata.entity.ebean;

import com.datahub.util.RecordUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.common.urn.VersionedUrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataAuditOperation;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;


/**
 * Ebean-based implementation of {@link EntityService}, serving entity and aspect {@link RecordTemplate}s
 * based on data stored in a relational table supported by Ebean ORM.
 */
@Slf4j
public class EbeanEntityService extends EntityService {

  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  private final EbeanAspectDao _aspectDao;
  private final JacksonDataTemplateCodec _dataTemplateCodec = new JacksonDataTemplateCodec();

  public EbeanEntityService(@Nonnull final EbeanAspectDao aspectDao, @Nonnull final EventProducer eventProducer,
      @Nonnull final EntityRegistry entityRegistry) {
    super(eventProducer, entityRegistry);
    _aspectDao = aspectDao;
  }

  @Nonnull
  private Map<String, EbeanAspectV2> getLatestAspectForUrn(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
      Set<Urn> urns = new HashSet<>();
      urns.add(urn);

      Map<String, EbeanAspectV2> result = new HashMap<>();
      getLatestAspect(urns, aspectNames).forEach((key, aspectEntry) -> {
        final String aspectName = key.getAspect();
        result.put(aspectName, aspectEntry);
      });
      return result;
  }

  @Nonnull
  private Map<EbeanAspectV2.PrimaryKey, EbeanAspectV2> getLatestAspect(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {

    log.debug("Invoked getLatestAspects with urns: {}, aspectNames: {}", urns, aspectNames);

    // Create DB keys
    final Set<EbeanAspectV2.PrimaryKey> dbKeys = urns.stream().map(urn -> {
      final Set<String> aspectsToFetch = aspectNames.isEmpty() ? getEntityAspectNames(urn) : aspectNames;
      return aspectsToFetch.stream()
          .map(aspectName -> new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
          .collect(Collectors.toList());
    }).flatMap(List::stream).collect(Collectors.toSet());

    Map<EbeanAspectV2.PrimaryKey, EbeanAspectV2> batchGetResults = new HashMap<>();
    Iterators.partition(dbKeys.iterator(), MAX_KEYS_PER_QUERY)
        .forEachRemaining(batch -> batchGetResults.putAll(_aspectDao.batchGet(ImmutableSet.copyOf(batch))));
    return batchGetResults;
  }

  @Override
  @Nonnull
  public Map<String, RecordTemplate> getLatestAspectsForUrn(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    Map<EbeanAspectV2.PrimaryKey, EbeanAspectV2> batchGetResults = getLatestAspect(new HashSet<>(Arrays.asList(urn)), aspectNames);

    final Map<String, RecordTemplate> result = new HashMap<>();
    batchGetResults.forEach((key, aspectEntry) -> {
      final String aspectName = key.getAspect();
      final RecordTemplate aspectRecord = EntityUtils.toAspectRecord(urn, aspectName, aspectEntry.getMetadata(), getEntityRegistry());
      result.put(aspectName, aspectRecord);
    });
    return result;
  }

  @Override
  @Nonnull
  public Map<Urn, List<RecordTemplate>> getLatestAspects(@Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {

    Map<EbeanAspectV2.PrimaryKey, EbeanAspectV2> batchGetResults = getLatestAspect(urns, aspectNames);

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

    batchGetResults.forEach((key, aspectEntry) -> {
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
   * When a user tries to fetch a negative version, we want to index most recent to least recent snapshots.
   * To do this, we want to fetch the maximum version and subtract the negative version from that. Since -1 represents
   * the maximum version, we need to add 1 to the final result.
   */
  private long calculateVersionNumber(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {
    if (version < 0) {
      return _aspectDao.getMaxVersion(urn.toString(), aspectName) + version + 1;
    }
    return version;
  }

  @Override
  @Nullable
  public RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {

    log.debug("Invoked getAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    version = calculateVersionNumber(urn, aspectName, version);
    final EbeanAspectV2.PrimaryKey primaryKey = new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<EbeanAspectV2> maybeAspect = Optional.ofNullable(_aspectDao.getAspect(primaryKey));
    return maybeAspect.map(
        aspect -> EntityUtils.toAspectRecord(urn, aspectName, aspect.getMetadata(), getEntityRegistry())).orElse(null);
  }

  @Override
  public Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
      // TODO: entityName is unused, can we remove this as a param?
      @Nonnull String entityName,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames) throws URISyntaxException {

    final Set<EbeanAspectV2.PrimaryKey> dbKeys = urns.stream()
        .map(urn -> aspectNames.stream()
            .map(aspectName -> new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    return getCorrespondingAspects(dbKeys, urns);
  }

  @Override
  public Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(
      @Nonnull Set<VersionedUrn> versionedUrns,
      @Nonnull Set<String> aspectNames) throws URISyntaxException {

    Map<String, Map<String, Long>> urnAspectVersionMap = versionedUrns.stream()
        .collect(Collectors.toMap(versionedUrn -> versionedUrn.getUrn().toString(),
            versionedUrn -> VersionedUrnUtils.convertVersionStamp(versionedUrn.getVersionStamp())));

    // Cover full/partial versionStamp
    final Set<EbeanAspectV2.PrimaryKey> dbKeys = urnAspectVersionMap.entrySet().stream()
        .filter(entry -> !entry.getValue().isEmpty())
        .map(entry -> aspectNames.stream()
            .filter(aspectName -> entry.getValue().containsKey(aspectName))
            .map(aspectName -> new EbeanAspectV2.PrimaryKey(entry.getKey(), aspectName,
                entry.getValue().get(aspectName)))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    // Cover empty versionStamp
    dbKeys.addAll(urnAspectVersionMap.entrySet().stream()
        .filter(entry -> entry.getValue().isEmpty())
        .map(entry -> aspectNames.stream()
            .map(aspectName -> new EbeanAspectV2.PrimaryKey(entry.getKey(), aspectName, 0L))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet()));

    return getCorrespondingAspects(dbKeys, versionedUrns.stream()
        .map(versionedUrn -> versionedUrn.getUrn().toString())
        .map(UrnUtils::getUrn).collect(Collectors.toSet()));
  }

  private Map<Urn, List<EnvelopedAspect>> getCorrespondingAspects(Set<EbeanAspectV2.PrimaryKey> dbKeys, Set<Urn> urns)
      throws URISyntaxException {

    final Map<EbeanAspectV2.PrimaryKey, EnvelopedAspect> envelopedAspectMap = getEnvelopedAspects(dbKeys);

    // Group result by Urn
    final Map<String, List<EnvelopedAspect>> urnToAspects = envelopedAspectMap.entrySet()
        .stream()
        .collect(Collectors.groupingBy(entry -> entry.getKey().getUrn(),
            Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

    final Map<Urn, List<EnvelopedAspect>> result = new HashMap<>();
    for (Urn urn : urns) {
      List<EnvelopedAspect> aspects = urnToAspects.getOrDefault(urn.toString(), Collections.emptyList());
      EnvelopedAspect keyAspect = getKeyEnvelopedAspect(urn);
      // Add key aspect if it does not exist in the returned aspects
      if (aspects.isEmpty() || aspects.stream().noneMatch(aspect -> keyAspect.getName().equals(aspect.getName()))) {
        result.put(urn, ImmutableList.<EnvelopedAspect>builder().addAll(aspects).add(keyAspect).build());
      } else {
        result.put(urn, aspects);
      }
    }
    return result;
  }

  @Override
  public EnvelopedAspect getEnvelopedAspect(
      // TODO: entityName is only used for a debug statement, can we remove this as a param?
      @Nonnull String entityName,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long version) throws Exception {
    log.debug(String.format("Invoked getEnvelopedAspect with entityName: %s, urn: %s, aspectName: %s, version: %s",
        entityName,
        urn,
        aspectName,
        version));

    version = calculateVersionNumber(urn, aspectName, version);

    final EbeanAspectV2.PrimaryKey primaryKey = new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, version);
    return getEnvelopedAspects(ImmutableSet.of(primaryKey)).get(primaryKey);
  }

  @Override
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug("Invoked getVersionedAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    VersionedAspect result = new VersionedAspect();

    version = calculateVersionNumber(urn, aspectName, version);

    final EbeanAspectV2.PrimaryKey primaryKey = new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, version);
    final Optional<EbeanAspectV2> maybeAspect = Optional.ofNullable(_aspectDao.getAspect(primaryKey));
    RecordTemplate aspectRecord =
        maybeAspect.map(aspect -> EntityUtils.toAspectRecord(urn, aspectName, aspect.getMetadata(), getEntityRegistry()))
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

  @Override
  @Nonnull
  public ListResult<RecordTemplate> listLatestAspects(@Nonnull final String entityName,
      @Nonnull final String aspectName, final int start, final int count) {

    log.debug("Invoked listLatestAspects with entityName: {}, aspectName: {}, start: {}, count: {}", entityName,
            aspectName, start, count);

    final ListResult<String> aspectMetadataList =
        _aspectDao.listLatestAspectMetadata(entityName, aspectName, start, count);

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
  protected UpdateAspectResult ingestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata providedSystemMetadata) {

    return _aspectDao.runInTransactionWithRetry(() -> {
      final String urnStr = urn.toString();
      final EbeanAspectV2 latest = _aspectDao.getLatestAspect(urnStr, aspectName);
      long nextVersion = _aspectDao.getNextVersion(urnStr, aspectName);

      return ingestAspectToLocalDBNoTransaction(urn, aspectName, updateLambda, auditStamp, providedSystemMetadata, latest, nextVersion);
    }, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Override
  @Nonnull
  protected List<Pair<String, UpdateAspectResult>> ingestAspectsToLocalDB(
      @Nonnull final Urn urn,
      @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata systemMetadata) {

    return _aspectDao.runInTransactionWithRetry(() -> {

      final Set<String> aspectNames = aspectRecordsToIngest
        .stream()
        .map(Pair::getFirst)
        .collect(Collectors.toSet());

      Map<String, EbeanAspectV2> latestAspects = getLatestAspectForUrn(urn, aspectNames);
      Map<String, Long> nextVersions = _aspectDao.getNextVersions(urn.toString(), aspectNames);

      List<Pair<String, UpdateAspectResult>> result = new ArrayList<>();
      for (Pair<String, RecordTemplate> aspectRecord: aspectRecordsToIngest) {
        String aspectName = aspectRecord.getFirst();
        RecordTemplate newValue = aspectRecord.getSecond();
        EbeanAspectV2 latest = latestAspects.get(aspectName);
        long nextVersion = nextVersions.get(aspectName);
        UpdateAspectResult updateResult = ingestAspectToLocalDBNoTransaction(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata,
          latest, nextVersion);
        result.add(new Pair<>(aspectName, updateResult));
      }
      return result;
    }, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  private UpdateAspectResult ingestAspectToLocalDBNoTransaction(@Nonnull final Urn urn,
     @Nonnull final String aspectName, @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
     @Nonnull final AuditStamp auditStamp, @Nonnull final SystemMetadata providedSystemMetadata, @Nullable final EbeanAspectV2 latest,
     @Nonnull final Long nextVersion) {

    // 2. Compare the latest existing and new.
    final RecordTemplate oldValue =
            latest == null ? null : EntityUtils.toAspectRecord(urn, aspectName, latest.getMetadata(), getEntityRegistry());
    final RecordTemplate newValue = updateLambda.apply(Optional.ofNullable(oldValue));

    // 3. If there is no difference between existing and new, we just update
    // the lastObserved in system metadata. RunId should stay as the original runId
    if (oldValue != null && DataTemplateUtil.areEqual(oldValue, newValue)) {
      SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
      latestSystemMetadata.setLastObserved(providedSystemMetadata.getLastObserved());

      latest.setSystemMetadata(RecordUtils.toJsonString(latestSystemMetadata));

      _aspectDao.saveAspect(latest, false);

      return new UpdateAspectResult(urn, oldValue, oldValue,
              EntityUtils.parseSystemMetadata(latest.getSystemMetadata()), latestSystemMetadata,
              MetadataAuditOperation.UPDATE, auditStamp, 0);
    }

    // 4. Save the newValue as the latest version
    log.debug("Ingesting aspect with name {}, urn {}", aspectName, urn);
    long versionOfOld = _aspectDao.saveLatestAspect(urn.toString(), aspectName, latest == null ? null : EntityUtils.toJsonAspect(oldValue),
            latest == null ? null : latest.getCreatedBy(), latest == null ? null : latest.getCreatedFor(),
            latest == null ? null : latest.getCreatedOn(), latest == null ? null : latest.getSystemMetadata(),
            EntityUtils.toJsonAspect(newValue), auditStamp.getActor().toString(),
            auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
            new Timestamp(auditStamp.getTime()), EntityUtils.toJsonAspect(providedSystemMetadata), nextVersion);

    return new UpdateAspectResult(urn, oldValue, newValue,
            latest == null ? null : EntityUtils.parseSystemMetadata(latest.getSystemMetadata()), providedSystemMetadata,
            MetadataAuditOperation.UPDATE, auditStamp, versionOfOld);
  }

  @Override
  @Nonnull
  public RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String entityName,
      @Nonnull final String aspectName, @Nonnull final AspectSpec aspectSpec, @Nonnull final RecordTemplate newValue,
      @Nonnull final AuditStamp auditStamp, @Nonnull final long version, @Nonnull final boolean emitMae) {
    log.debug(
        "Invoked updateAspect with urn: {}, aspectName: {}, newValue: {}, version: {}, emitMae: {}", urn,
            aspectName, newValue, version, emitMae);
    return updateAspect(urn, entityName, aspectName, aspectSpec, newValue, auditStamp, version, emitMae,
        DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  private RecordTemplate updateAspect(@Nonnull final Urn urn, @Nonnull final String entityName,
      @Nonnull final String aspectName, @Nonnull final AspectSpec aspectSpec, @Nonnull final RecordTemplate value,
      @Nonnull final AuditStamp auditStamp, @Nonnull final long version, @Nonnull final boolean emitMae,
      final int maxTransactionRetry) {

    final UpdateAspectResult result = _aspectDao.runInTransactionWithRetry(() -> {

      final EbeanAspectV2 oldAspect = _aspectDao.getAspect(urn.toString(), aspectName, version);
      final RecordTemplate oldValue =
          oldAspect == null ? null : EntityUtils.toAspectRecord(urn, aspectName, oldAspect.getMetadata(), getEntityRegistry());

      SystemMetadata oldSystemMetadata =
          oldAspect == null ? new SystemMetadata() : EntityUtils.parseSystemMetadata(oldAspect.getSystemMetadata());
      // create a duplicate of the old system metadata to update and write back
      SystemMetadata newSystemMetadata =
          oldAspect == null ? new SystemMetadata() : EntityUtils.parseSystemMetadata(oldAspect.getSystemMetadata());
      newSystemMetadata.setLastObserved(System.currentTimeMillis());

      log.debug("Updating aspect with name {}, urn {}", aspectName, urn);
      _aspectDao.saveAspect(urn.toString(), aspectName, EntityUtils.toJsonAspect(value), auditStamp.getActor().toString(),
          auditStamp.hasImpersonator() ? auditStamp.getImpersonator().toString() : null,
          new Timestamp(auditStamp.getTime()), EntityUtils.toJsonAspect(newSystemMetadata), version, oldAspect == null);

      return new UpdateAspectResult(urn, oldValue, value, oldSystemMetadata, newSystemMetadata,
          MetadataAuditOperation.UPDATE, auditStamp, version);
    }, maxTransactionRetry);

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate newValue = result.getNewValue();

    if (emitMae) {
      log.debug("Producing MetadataAuditEvent for updated aspect {}, urn {}", aspectName, urn);
      produceMetadataChangeLog(urn, entityName, aspectName, aspectSpec, oldValue, newValue,
          result.getOldSystemMetadata(), result.getNewSystemMetadata(), auditStamp, ChangeType.UPSERT);
    } else {
      log.debug("Skipped producing MetadataAuditEvent for updated aspect {}, urn {}. emitMAE is false.",
          aspectName, urn);
    }

    return newValue;
  }

  public void setWritable(boolean canWrite) {
    log.debug("Setting writable to {}", canWrite);
    _aspectDao.setWritable(canWrite);
  }

  @Nullable
  public RollbackResult deleteAspect(String urn, String aspectName, Map<String, String> conditions, boolean hardDelete) {
    // Validate pre-conditions before running queries
    Urn entityUrn;
    EntitySpec entitySpec;
    try {
      entityUrn = Urn.createFromString(urn);
      String entityName = PegasusUtils.urnToEntityName(entityUrn);
      entitySpec = getEntityRegistry().getEntitySpec(entityName);
      Preconditions.checkState(entitySpec != null, String.format("Could not find entity definition for %s", entityName));
      Preconditions.checkState(entitySpec.hasAspect(aspectName), String.format("Could not find aspect %s in definition for %s", aspectName, entityName));
    } catch (URISyntaxException uriSyntaxException) {
      // don't expect this to happen, so raising RuntimeException here
      throw new RuntimeException(String.format("Failed to extract urn from %s", urn));
    }

    final RollbackResult result = _aspectDao.runInTransactionWithRetry(() -> {
      Integer additionalRowsDeleted = 0;

      // 1. Fetch the latest existing version of the aspect.
      final EbeanAspectV2 latest = _aspectDao.getLatestAspect(urn, aspectName);

      // 1.1 If no latest exists, skip this aspect
      if (latest == null) {
        return null;
      }

      // 2. Compare the match conditions, if they don't match, ignore.
      SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
      if (!filterMatch(latestSystemMetadata, conditions)) {
        return null;
      }
      String latestMetadata = latest.getMetadata();

      // 3. Check if this is a key aspect
      Boolean isKeyAspect = false;
      try {
        isKeyAspect = getKeyAspectName(Urn.createFromString(urn)).equals(aspectName);
      } catch (URISyntaxException e) {
        e.printStackTrace();
      }

      // 4. Fetch all preceding aspects, that match
      List<EbeanAspectV2> aspectsToDelete = new ArrayList<>();
      long maxVersion = _aspectDao.getMaxVersion(urn, aspectName);
      EbeanAspectV2 survivingAspect = null;
      String previousMetadata = null;
      boolean filterMatch = true;
      while (maxVersion > 0 && filterMatch)  {
        EbeanAspectV2 candidateAspect = _aspectDao.getAspect(urn, aspectName, maxVersion);
        SystemMetadata previousSysMetadata = EntityUtils.parseSystemMetadata(candidateAspect.getSystemMetadata());
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

      aspectsToDelete.forEach(aspect -> _aspectDao.deleteAspect(aspect));

      if (survivingAspect != null) {
        // if there was a surviving aspect, copy its information into the latest row
        // eBean does not like us updating a pkey column (version) for the surviving aspect
        // as a result we copy information from survivingAspect to latest and delete survivingAspect
        latest.setMetadata(survivingAspect.getMetadata());
        latest.setSystemMetadata(survivingAspect.getSystemMetadata());
        latest.setCreatedOn(survivingAspect.getCreatedOn());
        latest.setCreatedBy(survivingAspect.getCreatedBy());
        latest.setCreatedFor(survivingAspect.getCreatedFor());
        _aspectDao.saveAspect(latest, false);
        _aspectDao.deleteAspect(survivingAspect);
      } else {
        if (isKeyAspect) {
          if (hardDelete) {
            // If this is the key aspect, delete the entity entirely.
            additionalRowsDeleted = _aspectDao.deleteUrn(urn);
          } else if (entitySpec.hasAspect(Constants.STATUS_ASPECT_NAME)) {
            // soft delete by setting status.removed=true (if applicable)
            final Status statusAspect = new Status();
            statusAspect.setRemoved(true);
            final SystemMetadata systemMetadata = SystemMetadataUtils.createDefaultSystemMetadata();
            final AuditStamp auditStamp = AuditStampUtils.createDefaultAuditStamp();

            this.ingestAspect(entityUrn, Constants.STATUS_ASPECT_NAME, statusAspect, auditStamp, systemMetadata);
          }
        } else {
          // Else, only delete the specific aspect.
          _aspectDao.deleteAspect(latest);
        }
      }

      // 6. Emit the Update
      try {
        final RecordTemplate latestValue = latest == null ? null
            : EntityUtils.toAspectRecord(Urn.createFromString(latest.getKey().getUrn()), latest.getKey().getAspect(),
                latestMetadata, getEntityRegistry());

        final RecordTemplate previousValue = survivingAspect == null ? null
            : EntityUtils.toAspectRecord(Urn.createFromString(survivingAspect.getKey().getUrn()),
                survivingAspect.getKey().getAspect(), previousMetadata, getEntityRegistry());

        final Urn urnObj = Urn.createFromString(urn);
        // We are not deleting key aspect if hardDelete has not been set so do not return a rollback result
        if (isKeyAspect && !hardDelete) {
          return null;
        }
        return new RollbackResult(urnObj, urnObj.getEntityType(), latest.getAspect(), latestValue,
            previousValue, latestSystemMetadata,
            previousValue == null ? null : EntityUtils.parseSystemMetadata(survivingAspect.getSystemMetadata()),
            survivingAspect == null ? ChangeType.DELETE : ChangeType.UPSERT, isKeyAspect, additionalRowsDeleted);
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to emit the update for urn %s", urn));
      }
    }, DEFAULT_MAX_TRANSACTION_RETRY);

    return result;
  }

  @Override
  public RollbackRunResult rollbackWithConditions(List<AspectRowSummary> aspectRows, Map<String, String> conditions, boolean hardDelete) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    AtomicInteger rowsDeletedFromEntityDeletion = new AtomicInteger(0);

    aspectRows.forEach(aspectToRemove -> {

      RollbackResult result = deleteAspect(aspectToRemove.getUrn(), aspectToRemove.getAspectName(),
          conditions, hardDelete);
      if (result != null) {
        Optional<AspectSpec> aspectSpec = getAspectSpec(result.entityName, result.aspectName);
        if (!aspectSpec.isPresent()) {
          log.error("Issue while rolling back: unknown aspect {} for entity {}", result.entityName, result.aspectName);
          return;
        }

        rowsDeletedFromEntityDeletion.addAndGet(result.additionalRowsAffected);
        removedAspects.add(aspectToRemove);
        produceMetadataChangeLog(result.getUrn(), result.getEntityName(), result.getAspectName(), aspectSpec.get(),
            result.getOldValue(), result.getNewValue(), result.getOldSystemMetadata(), result.getNewSystemMetadata(),
            // TODO: use properly attributed audit stamp.
            createSystemAuditStamp(),
            result.getChangeType());
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

    EbeanAspectV2 latestKey = _aspectDao.getLatestAspect(urn.toString(), keyAspectName);
    if (latestKey == null || latestKey.getSystemMetadata() == null) {
      return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
    }

    SystemMetadata latestKeySystemMetadata = EntityUtils.parseSystemMetadata(latestKey.getSystemMetadata());
    RollbackResult result = deleteAspect(urn.toString(), keyAspectName, Collections.singletonMap("runId", latestKeySystemMetadata.getRunId()), true);

    if (result != null) {
      AspectRowSummary summary = new AspectRowSummary();
      summary.setUrn(urn.toString());
      summary.setKeyAspect(true);
      summary.setAspectName(keyAspectName);
      summary.setVersion(0);
      summary.setTimestamp(latestKey.getCreatedOn().getTime());

      rowsDeletedFromEntityDeletion = result.additionalRowsAffected;
      removedAspects.add(summary);
      produceMetadataChangeLog(result.getUrn(), result.getEntityName(), result.getAspectName(), keySpec,
          result.getOldValue(), result.getNewValue(), result.getOldSystemMetadata(), result.getNewSystemMetadata(),
          // TODO: Use a proper inferred audit stamp
          createSystemAuditStamp(),
          result.getChangeType());
    }

    return new RollbackRunResult(removedAspects, rowsDeletedFromEntityDeletion);
  }

  @Override
  public Boolean exists(Urn urn) {
    final Set<String> aspectsToFetch = getEntityAspectNames(urn);
    final List<EbeanAspectV2.PrimaryKey> dbKeys = aspectsToFetch.stream()
        .map(aspectName -> new EbeanAspectV2.PrimaryKey(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
        .collect(Collectors.toList());

    Map<EbeanAspectV2.PrimaryKey, EbeanAspectV2> aspects = _aspectDao.batchGet(new HashSet(dbKeys));
    return aspects.values().stream().anyMatch(aspect -> aspect != null);
  }

  @Override
  @Nonnull
  public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count) {
    log.debug("Invoked listUrns with entityName: {}, start: {}, count: {}", entityName, start, count);

    // If a keyAspect exists, the entity exists.
    final String keyAspectName = getEntityRegistry().getEntitySpec(entityName).getKeyAspectSpec().getName();
    final ListResult<String> keyAspectList = _aspectDao.listUrns(entityName, keyAspectName, start, count);

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
        throw new IllegalArgumentException(String.format("Failed to convert urn %s found in db to Urn object.", urn),
            e);
      }
    }
    result.setEntities(entityUrns);
    return result;
  }

  private Map<EbeanAspectV2.PrimaryKey, EnvelopedAspect> getEnvelopedAspects(final Set<EbeanAspectV2.PrimaryKey> dbKeys) throws URISyntaxException {
    final Map<EbeanAspectV2.PrimaryKey, EnvelopedAspect> result = new HashMap<>();
    final Map<EbeanAspectV2.PrimaryKey, EbeanAspectV2> dbEntries = _aspectDao.batchGet(dbKeys);

    for (EbeanAspectV2.PrimaryKey currKey : dbKeys) {

      final EbeanAspectV2 currAspectEntry = dbEntries.get(currKey);

      if (currAspectEntry == null) {
        // No aspect found.
        continue;
      }

      // Aspect found. Now turn it into an EnvelopedAspect
      final com.linkedin.entity.Aspect aspect = RecordUtils.toRecordTemplate(com.linkedin.entity.Aspect.class, currAspectEntry
          .getMetadata());
      final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
      envelopedAspect.setName(currAspectEntry.getKey().getAspect());
      envelopedAspect.setVersion(currAspectEntry.getKey().getVersion());
      // TODO: I think we can assume this here, adding as it's a required field so object mapping barfs when trying to access it,
      //    since nowhere else is using it should be safe for now at least
      envelopedAspect.setType(AspectType.VERSIONED);
      envelopedAspect.setValue(aspect);
      envelopedAspect.setCreated(new AuditStamp()
          .setActor(Urn.createFromString(currAspectEntry.getCreatedBy()))
          .setTime(currAspectEntry.getCreatedOn().getTime())
      );
      result.put(currKey, envelopedAspect);
    }
    return result;
  }

  private EnvelopedAspect getKeyEnvelopedAspect(final Urn urn) throws URISyntaxException {
    final EntitySpec spec = getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    final RecordDataSchema keySchema = keySpec.getPegasusSchema();
    final com.linkedin.entity.Aspect aspect =
        new com.linkedin.entity.Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySchema).data());

    final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(keySpec.getName());
    envelopedAspect.setVersion(ASPECT_LATEST_VERSION);
    envelopedAspect.setValue(aspect);
    // TODO: I think we can assume this here, adding as it's a required field so object mapping barfs when trying to access it,
    //    since nowhere else is using it should be safe for now at least
    envelopedAspect.setType(AspectType.VERSIONED);
    envelopedAspect.setCreated(
        new AuditStamp().setActor(Urn.createFromString(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    return envelopedAspect;
  }
}
