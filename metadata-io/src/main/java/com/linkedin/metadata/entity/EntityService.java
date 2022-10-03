package com.linkedin.metadata.entity;

import com.codahale.metrics.Timer;
import com.datahub.util.RecordUtils;
import com.datahub.util.exception.ModelConversionException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.Patch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.common.urn.VersionedUrnUtils;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.validator.Validator;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.Aspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.validation.EntityRegistryUrnValidator;
import com.linkedin.metadata.entity.validation.RecordTemplateValidator;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
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
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.ebean.PagedList;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.BrowsePathUtils.*;
import static com.linkedin.metadata.utils.PegasusUtils.*;


/**
 * A class specifying create, update, and read operations against metadata entities and aspects
 * by primary key (urn).
 *
 * This interface is meant to abstract away the storage concerns of these pieces of metadata, permitting
 * any underlying storage system to be used in materializing GMS domain objects, which are implemented using Pegasus
 * {@link RecordTemplate}s.
 *
 * Internal versioning semantics
 * =============================
 *
 * The latest version of any aspect is set to 0 for efficient retrieval; in most cases the latest state of an aspect
 * will be the only fetched.
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
 */
@Slf4j
public class EntityService {

  /**
   * As described above, the latest version of an aspect should <b>always</b> take the value 0, with
   * monotonically increasing version incrementing as usual once the latest version is replaced.
   */

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

  private static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;

  protected final AspectDao _aspectDao;
  private final EventProducer _producer;
  private final EntityRegistry _entityRegistry;
  private final Map<String, Set<String>> _entityToValidAspects;
  private RetentionService _retentionService;
  private final Boolean _alwaysEmitAuditEvent = false;
  public static final String DEFAULT_RUN_ID = "no-run-id-provided";
  public static final String BROWSE_PATHS = "browsePaths";
  public static final String DATA_PLATFORM_INSTANCE = "dataPlatformInstance";
  protected static final int MAX_KEYS_PER_QUERY = 500;

  public EntityService(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final EventProducer producer,
      @Nonnull final EntityRegistry entityRegistry) {

    _aspectDao = aspectDao;
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
  public Map<Urn, List<RecordTemplate>> getLatestAspects(
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<String> aspectNames) {

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = getLatestAspect(urns, aspectNames);

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

  @Nonnull
  public Map<String, RecordTemplate> getLatestAspectsForUrn(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = getLatestAspect(new HashSet<>(Arrays.asList(urn)), aspectNames);

    final Map<String, RecordTemplate> result = new HashMap<>();
    batchGetResults.forEach((key, aspectEntry) -> {
      final String aspectName = key.getAspect();
      final RecordTemplate aspectRecord = EntityUtils.toAspectRecord(urn, aspectName, aspectEntry.getMetadata(), getEntityRegistry());
      result.put(aspectName, aspectRecord);
    });
    return result;
  }

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
  public RecordTemplate getAspect(@Nonnull final Urn urn, @Nonnull final String aspectName, @Nonnull long version) {

    log.debug("Invoked getAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    version = calculateVersionNumber(urn, aspectName, version);
    final EntityAspectIdentifier primaryKey = new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect = Optional.ofNullable(_aspectDao.getAspect(primaryKey));
    return maybeAspect.map(
        aspect -> EntityUtils.toAspectRecord(urn, aspectName, aspect.getMetadata(), getEntityRegistry())).orElse(null);
  }

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
   * Retrieves the aspects for the given set of urns and versions as dynamic aspect objects
   * (Without having to define union objects)
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link Entity} object
   */
  public Map<Urn, EntityResponse> getEntitiesVersionedV2(
      @Nonnull final Set<VersionedUrn> versionedUrns,
      @Nonnull final Set<String> aspectNames) throws URISyntaxException {
    return getVersionedEnvelopedAspects(versionedUrns, aspectNames)
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
  public Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(
    // TODO: entityName is unused, can we remove this as a param?
    @Nonnull String entityName,
    @Nonnull Set<Urn> urns,
    @Nonnull Set<String> aspectNames) throws URISyntaxException {

  final Set<EntityAspectIdentifier> dbKeys = urns.stream()
      .map(urn -> aspectNames.stream()
          .map(aspectName -> new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
          .collect(Collectors.toList()))
      .flatMap(List::stream)
      .collect(Collectors.toSet());

  return getCorrespondingAspects(dbKeys, urns);
}

  /**
   * Retrieves the latest aspects for the given set of urns as a list of enveloped aspects
   *
   * @param versionedUrns set of urns to fetch with versions of aspects specified in a specialized string
   * @param aspectNames set of aspects to fetch
   * @return a map of {@link Urn} to {@link EnvelopedAspect} object
   */
  public Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(
    @Nonnull Set<VersionedUrn> versionedUrns,
    @Nonnull Set<String> aspectNames) throws URISyntaxException {

  Map<String, Map<String, Long>> urnAspectVersionMap = versionedUrns.stream()
      .collect(Collectors.toMap(versionedUrn -> versionedUrn.getUrn().toString(),
          versionedUrn -> VersionedUrnUtils.convertVersionStamp(versionedUrn.getVersionStamp())));

  // Cover full/partial versionStamp
  final Set<EntityAspectIdentifier> dbKeys = urnAspectVersionMap.entrySet().stream()
      .filter(entry -> !entry.getValue().isEmpty())
      .map(entry -> aspectNames.stream()
          .filter(aspectName -> entry.getValue().containsKey(aspectName))
          .map(aspectName -> new EntityAspectIdentifier(entry.getKey(), aspectName,
              entry.getValue().get(aspectName)))
          .collect(Collectors.toList()))
      .flatMap(List::stream)
      .collect(Collectors.toSet());

  // Cover empty versionStamp
  dbKeys.addAll(urnAspectVersionMap.entrySet().stream()
      .filter(entry -> entry.getValue().isEmpty())
      .map(entry -> aspectNames.stream()
          .map(aspectName -> new EntityAspectIdentifier(entry.getKey(), aspectName, 0L))
          .collect(Collectors.toList()))
      .flatMap(List::stream)
      .collect(Collectors.toSet()));

  return getCorrespondingAspects(dbKeys, versionedUrns.stream()
      .map(versionedUrn -> versionedUrn.getUrn().toString())
      .map(UrnUtils::getUrn).collect(Collectors.toSet()));
}

private Map<Urn, List<EnvelopedAspect>> getCorrespondingAspects(Set<EntityAspectIdentifier> dbKeys, Set<Urn> urns)
    throws URISyntaxException {

  final Map<EntityAspectIdentifier, EnvelopedAspect> envelopedAspectMap = getEnvelopedAspects(dbKeys);

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
  public EnvelopedAspect getEnvelopedAspect(
      // TODO: entityName is only used for a debug statement, can we remove this as a param?
      String entityName,
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      long version) throws Exception {
    log.debug(String.format("Invoked getEnvelopedAspect with entityName: %s, urn: %s, aspectName: %s, version: %s",
        urn.getEntityType(),
        urn,
        aspectName,
        version));

    version = calculateVersionNumber(urn, aspectName, version);

    final EntityAspectIdentifier primaryKey = new EntityAspectIdentifier(urn.toString(), aspectName, version);
    return getEnvelopedAspects(ImmutableSet.of(primaryKey)).get(primaryKey);
  }

  /**
   * Retrieves an {@link VersionedAspect}, or null if one cannot be found.
   */
  @Nullable
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {

    log.debug("Invoked getVersionedAspect with urn: {}, aspectName: {}, version: {}", urn, aspectName, version);

    VersionedAspect result = new VersionedAspect();

    version = calculateVersionNumber(urn, aspectName, version);

    final EntityAspectIdentifier primaryKey = new EntityAspectIdentifier(urn.toString(), aspectName, version);
    final Optional<EntityAspect> maybeAspect = Optional.ofNullable(_aspectDao.getAspect(primaryKey));
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
  public ListResult<RecordTemplate> listLatestAspects(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int count) {

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


  @Nonnull
  protected UpdateAspectResult wrappedIngestAspectToLocalDB(@Nonnull final Urn urn, @Nonnull final String aspectName,
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

  // Validates urn subfields using EntityRegistryUrnValidator and does basic field validation for type alignment
  // due to validator logic which inherently does coercion
  private void validateAspect(Urn urn, RecordTemplate aspect) {
    EntityRegistryUrnValidator validator = new EntityRegistryUrnValidator(_entityRegistry);
    validator.setCurrentEntitySpec(_entityRegistry.getEntitySpec(urn.getEntityType()));
    validateAspect(urn, aspect, validator);
  }

  private void validateAspect(Urn urn, RecordTemplate aspect, Validator validator) {
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
  protected UpdateAspectResult ingestAspectToLocalDB(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata providedSystemMetadata) {

    return _aspectDao.runInTransactionWithRetry(() -> {
      final String urnStr = urn.toString();
      final EntityAspect latest = _aspectDao.getLatestAspect(urnStr, aspectName);
      long nextVersion = _aspectDao.getNextVersion(urnStr, aspectName);

      return ingestAspectToLocalDBNoTransaction(urn, aspectName, updateLambda, auditStamp, providedSystemMetadata, latest, nextVersion);
    }, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  /**
   * Apply patch update to aspect within a single transaction
   *
   * @param urn an urn associated with the new aspect
   * @param aspectSpec AspectSpec of the aspect to update
   * @param jsonPatch JsonPatch to apply to the aspect
   * @param auditStamp an {@link AuditStamp} containing metadata about the writer & current time   * @param providedSystemMetadata
   * @return Details about the new and old version of the aspect
   */
  @Nonnull
  @Deprecated
  protected UpdateAspectResult patchAspectToLocalDB(
      @Nonnull final Urn urn,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final Patch jsonPatch,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata providedSystemMetadata) {

    return _aspectDao.runInTransactionWithRetry(() -> {
      final String urnStr = urn.toString();
      final String aspectName = aspectSpec.getName();
      EntityAspect latest = _aspectDao.getLatestAspect(urnStr, aspectName);
      if (latest == null) {
        //TODO: best effort mint
        RecordTemplate defaultTemplate = _entityRegistry.getAspectTemplateEngine().getDefaultTemplate(aspectSpec.getName());

        if (defaultTemplate != null) {
          latest = new EntityAspect();
          latest.setAspect(aspectName);
          latest.setMetadata(EntityUtils.toJsonAspect(defaultTemplate));
          latest.setUrn(urnStr);
          latest.setVersion(ASPECT_LATEST_VERSION);
          latest.setCreatedOn(new Timestamp(auditStamp.getTime()));
          latest.setCreatedBy(auditStamp.getActor().toString());
        } else {
          throw new UnsupportedOperationException("Patch not supported for empty aspect for aspect name: " + aspectName);
        }
      }

      long nextVersion = _aspectDao.getNextVersion(urnStr, aspectName);
      try {
        RecordTemplate currentValue = EntityUtils.toAspectRecord(urn, aspectName, latest.getMetadata(), _entityRegistry);
        RecordTemplate updatedValue =  _entityRegistry.getAspectTemplateEngine().applyPatch(currentValue, jsonPatch, aspectSpec);

        validateAspect(urn, updatedValue);
        return ingestAspectToLocalDBNoTransaction(urn, aspectName, ignored -> updatedValue, auditStamp, providedSystemMetadata,
            latest, nextVersion);
      } catch (JsonProcessingException | JsonPatchException e) {
        throw new IllegalStateException(e);
      }
    }, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  /**
   * Same as ingestAspectToLocalDB but for multiple aspects
   * DO NOT CALL DIRECTLY, USE WRAPPED METHODS TO VALIDATE URN
   */
  @Nonnull
  @Deprecated
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

      Map<String, EntityAspect> latestAspects = getLatestAspectForUrn(urn, aspectNames);
      Map<String, Long> nextVersions = _aspectDao.getNextVersions(urn.toString(), aspectNames);

      List<Pair<String, UpdateAspectResult>> result = new ArrayList<>();
      for (Pair<String, RecordTemplate> aspectRecord: aspectRecordsToIngest) {
        String aspectName = aspectRecord.getFirst();
        RecordTemplate newValue = aspectRecord.getSecond();
        EntityAspect latest = latestAspects.get(aspectName);
        long nextVersion = nextVersions.get(aspectName);
        UpdateAspectResult updateResult = ingestAspectToLocalDBNoTransaction(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata,
            latest, nextVersion);
        result.add(new Pair<>(aspectName, updateResult));
      }
      return result;
    }, DEFAULT_MAX_TRANSACTION_RETRY);
  }

  @Nonnull
  protected SystemMetadata generateSystemMetadataIfEmpty(SystemMetadata systemMetadata) {
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
      @Nonnull final RecordTemplate newValue, @Nonnull final AuditStamp auditStamp, @Nonnull SystemMetadata systemMetadata) {

    log.debug("Invoked ingestAspect with urn: {}, aspectName: {}, newValue: {}", urn, aspectName, newValue);

    systemMetadata = generateSystemMetadataIfEmpty(systemMetadata);

    Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestAspectToLocalDB").time();
    UpdateAspectResult result = wrappedIngestAspectToLocalDB(urn, aspectName, ignored -> newValue, auditStamp, systemMetadata);
    ingestToLocalDBTimer.stop();

    return sendEventForUpdateAspectResult(urn, aspectName, result);
  }

  /**
   * Ingests (inserts) a new version of an entity aspect & emits a {@link com.linkedin.mxe.MetadataAuditEvent}.
   *
   * This method runs a read -> write atomically in a single transaction, this is to prevent multiple IDs from being created.
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
  @Nullable
  public RecordTemplate ingestAspectIfNotPresent(@Nonnull Urn urn, @Nonnull String aspectName,
      @Nonnull RecordTemplate newValue, @Nonnull AuditStamp auditStamp, @Nonnull SystemMetadata systemMetadata) {
    log.debug("Invoked ingestAspectIfNotPresent with urn: {}, aspectName: {}, newValue: {}", urn, aspectName, newValue);

    final SystemMetadata internalSystemMetadata = generateSystemMetadataIfEmpty(systemMetadata);

    Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestAspectToLocalDB").time();
    UpdateAspectResult result = _aspectDao.runInTransactionWithRetry(() -> {
      final String urnStr = urn.toString();
      final EntityAspect latest = _aspectDao.getLatestAspect(urnStr, aspectName);
      if (latest == null) {
        long nextVersion = _aspectDao.getNextVersion(urnStr, aspectName);

        return ingestAspectToLocalDBNoTransaction(urn, aspectName, ignored -> newValue, auditStamp,
            internalSystemMetadata, latest, nextVersion);
      }
      RecordTemplate oldValue = EntityUtils.toAspectRecord(urn, aspectName, latest.getMetadata(), getEntityRegistry());
      SystemMetadata oldMetadata = EntityUtils.parseSystemMetadata(latest.getSystemMetadata());
      return new UpdateAspectResult(urn, oldValue, oldValue, oldMetadata, oldMetadata, MetadataAuditOperation.UPDATE, auditStamp,
          latest.getVersion());
    }, DEFAULT_MAX_TRANSACTION_RETRY);
    ingestToLocalDBTimer.stop();

    return sendEventForUpdateAspectResult(urn, aspectName, result);
  }

  protected RecordTemplate sendEventForUpdateAspectResult(@Nonnull final Urn urn, @Nonnull final String aspectName,
    @Nonnull UpdateAspectResult result) {

    final RecordTemplate oldValue = result.getOldValue();
    final RecordTemplate updatedValue = result.getNewValue();
    final SystemMetadata oldSystemMetadata = result.getOldSystemMetadata();
    final SystemMetadata updatedSystemMetadata = result.getNewSystemMetadata();

    // Apply retention policies asynchronously if there was an update to existing aspect value
    if (oldValue != updatedValue && oldValue != null && _retentionService != null) {
      _retentionService.applyRetention(urn, aspectName,
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
      try {
        Timer.Context produceMAETimer = MetricUtils.timer(this.getClass(), "produceMAE").time();
        produceMetadataAuditEvent(urn, aspectName, oldValue, updatedValue, result.getOldSystemMetadata(),
            result.getNewSystemMetadata(), MetadataAuditOperation.UPDATE);
        produceMAETimer.stop();
      } catch (Exception e) {
        log.warn("Unable to produce legacy MAE, entity may not have legacy Snapshot schema.", e);
      }
    } else {
      log.debug("Skipped producing MetadataAuditEvent for ingested aspect {}, urn {}. Aspect has not changed.",
        aspectName, urn);
    }
    return updatedValue;
  }

  /**
   * Validates that a change type is valid for the given aspect
   * @param changeType
   * @param aspectSpec
   * @return
   */
  private boolean isValidChangeType(ChangeType changeType, AspectSpec aspectSpec) {
    if (aspectSpec.isTimeseries()) {
      // Timeseries aspects only support UPSERT
      return ChangeType.UPSERT.equals(changeType);
    } else {
      return (ChangeType.UPSERT.equals(changeType) || ChangeType.PATCH.equals(changeType));
    }
  }


    /**
     * Ingest a new {@link MetadataChangeProposal}. Note that this method does NOT include any additional aspects or do any
     * enrichment, instead it changes only those which are provided inside the metadata change proposal.
     *
     * Do not use this method directly for creating new entities, as it DOES NOT create an Entity Key aspect in the DB. Instead,
     * use an Entity Client.
     *
     * @param mcp the proposal to ingest
     * @param auditStamp an audit stamp representing the time and actor proposing the change
     * @return an {@link IngestProposalResult} containing the results
     */
  public IngestProposalResult ingestProposal(@Nonnull MetadataChangeProposal mcp,
      AuditStamp auditStamp) {

    log.debug("entity type = {}", mcp.getEntityType());
    EntitySpec entitySpec = getEntityRegistry().getEntitySpec(mcp.getEntityType());
    log.debug("entity spec = {}", entitySpec);

    Urn entityUrn = EntityKeyUtils.getUrnFromProposal(mcp, entitySpec.getKeyAspectSpec());


    AspectSpec aspectSpec = validateAspect(mcp, entitySpec);

    log.debug("aspect spec = {}", aspectSpec);

    if (!isValidChangeType(mcp.getChangeType(), aspectSpec)) {
      throw new UnsupportedOperationException("ChangeType not supported: " + mcp.getChangeType() + " for aspect " + mcp.getAspectName());
    }


    SystemMetadata systemMetadata = generateSystemMetadataIfEmpty(mcp.getSystemMetadata());
    systemMetadata.setRegistryName(aspectSpec.getRegistryName());
    systemMetadata.setRegistryVersion(aspectSpec.getRegistryVersion().toString());

    RecordTemplate oldAspect = null;
    SystemMetadata oldSystemMetadata = null;
    RecordTemplate newAspect = null;
    SystemMetadata newSystemMetadata = null;

    if (!aspectSpec.isTimeseries()) {
      UpdateAspectResult result = null;
      switch (mcp.getChangeType()) {
        case UPSERT:
          result = performUpsert(mcp, aspectSpec, systemMetadata, entityUrn, auditStamp);
          break;
        case PATCH:
          result = performPatch(mcp, aspectSpec, systemMetadata, entityUrn, auditStamp);
          break;
        default:
          // Should never reach since we throw error above
          throw new UnsupportedOperationException("ChangeType not supported: " + mcp.getChangeType());
      }
      oldAspect = result != null ? result.getOldValue() : null;
      oldSystemMetadata = result != null ? result.getOldSystemMetadata() : null;
      newAspect = result != null ? result.getNewValue() : null;
      newSystemMetadata = result != null ? result.getNewSystemMetadata() : null;
    } else {
      // For timeseries aspects
      newAspect = convertToRecordTemplate(mcp, aspectSpec);
      newSystemMetadata = mcp.getSystemMetadata();
    }

    boolean didUpdate = emitChangeLog(oldAspect, oldSystemMetadata, newAspect, newSystemMetadata, mcp, entityUrn, auditStamp, aspectSpec);

    return new IngestProposalResult(entityUrn, didUpdate);
  }

  private AspectSpec validateAspect(MetadataChangeProposal mcp, EntitySpec entitySpec) {
    if (!mcp.hasAspectName() || !mcp.hasAspect()) {
      throw new UnsupportedOperationException("Aspect and aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(mcp.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format("Unknown aspect %s for entity %s", mcp.getAspectName(),
              mcp.getEntityType()));
    }

    return aspectSpec;
  }

  private UpdateAspectResult performUpsert(MetadataChangeProposal mcp, AspectSpec aspectSpec, SystemMetadata
      systemMetadata, Urn entityUrn, AuditStamp auditStamp) {
    RecordTemplate aspect = convertToRecordTemplate(mcp, aspectSpec);
    log.debug("aspect = {}", aspect);

    return upsertAspect(aspect, systemMetadata, mcp, entityUrn, auditStamp, aspectSpec);
  }

  private UpdateAspectResult performPatch(MetadataChangeProposal mcp, AspectSpec aspectSpec, SystemMetadata
      systemMetadata, Urn entityUrn, AuditStamp auditStamp) {
    if (!supportsPatch(aspectSpec)) {
      // Prevent unexpected behavior for aspects that do not currently have 1st class patch support,
      // specifically having array based fields that require merging without specifying merge behavior can get into bad states
      throw new UnsupportedOperationException("Aspect: " + aspectSpec.getName() + " does not currently support patch "
          + "operations.");
    }
    Patch jsonPatch = convertToJsonPatch(mcp);
    log.debug("patch = {}", jsonPatch);

    return patchAspect(jsonPatch, systemMetadata, mcp, entityUrn, auditStamp, aspectSpec);
  }

  private boolean supportsPatch(AspectSpec aspectSpec) {
    // Limit initial support to defined templates
    return AspectTemplateEngine.SUPPORTED_TEMPLATES.contains(aspectSpec.getName());
  }

  private RecordTemplate convertToRecordTemplate(MetadataChangeProposal mcp, AspectSpec aspectSpec) {
    RecordTemplate aspect;
    try {
      aspect = GenericRecordUtils.deserializeAspect(mcp.getAspect().getValue(),
          mcp.getAspect().getContentType(), aspectSpec);
      ValidationUtils.validateOrThrow(aspect);
    } catch (ModelConversionException e) {
      throw new RuntimeException(
          String.format("Could not deserialize %s for aspect %s", mcp.getAspect().getValue(),
              mcp.getAspectName()));
    }
    log.debug("aspect = {}", aspect);
    return aspect;
  }

  private Patch convertToJsonPatch(MetadataChangeProposal mcp) {
    JsonNode json;
    try {
      json = OBJECT_MAPPER.readTree(mcp.getAspect().getValue().asString(StandardCharsets.UTF_8));
      return JsonPatch.fromJson(json);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid JSON Patch: " + mcp.getAspect().getValue(), e);
    }
  }

  private UpdateAspectResult upsertAspect(final RecordTemplate aspect, final SystemMetadata systemMetadata,
      MetadataChangeProposal mcp, Urn entityUrn, AuditStamp auditStamp, AspectSpec aspectSpec) {
    Timer.Context ingestToLocalDBTimer = MetricUtils.timer(this.getClass(), "ingestProposalToLocalDB").time();
    UpdateAspectResult result =
        wrappedIngestAspectToLocalDB(entityUrn, mcp.getAspectName(), ignored -> aspect, auditStamp,
            systemMetadata);
    ingestToLocalDBTimer.stop();
    RecordTemplate oldAspect = result.getOldValue();
    RecordTemplate newAspect = result.getNewValue();
    // Apply retention policies asynchronously if there was an update to existing aspect value
    if (oldAspect != newAspect && oldAspect != null && _retentionService != null) {
      _retentionService.applyRetention(entityUrn, aspectSpec.getName(),
          Optional.of(new RetentionService.RetentionContext(Optional.of(result.maxVersion))));
    }
    return result;
  }

  private UpdateAspectResult patchAspect(final Patch patch, final SystemMetadata systemMetadata,
      MetadataChangeProposal mcp, Urn entityUrn, AuditStamp auditStamp, AspectSpec aspectSpec) {
    Timer.Context patchAspectToLocalDBTimer = MetricUtils.timer(this.getClass(), "patchAspect").time();
    UpdateAspectResult result = patchAspectToLocalDB(entityUrn, aspectSpec, patch, auditStamp, systemMetadata);
    patchAspectToLocalDBTimer.stop();
    RecordTemplate oldAspect = result.getOldValue();
    RecordTemplate newAspect = result.getNewValue();
    // Apply retention policies asynchronously if there was an update to existing aspect value
    if (oldAspect != newAspect && oldAspect != null && _retentionService != null) {
      _retentionService.applyRetention(entityUrn, aspectSpec.getName(),
          Optional.of(new RetentionService.RetentionContext(Optional.of(result.maxVersion))));
    }
    return result;
  }

  public String batchApplyRetention(Integer start, Integer count, Integer attemptWithVersion, String aspectName,
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
    BulkApplyRetentionResult result = _retentionService.batchApplyRetentionEntities(args);
    return result.toString();
  }

  private boolean emitChangeLog(@Nullable RecordTemplate oldAspect, @Nullable SystemMetadata oldSystemMetadata,
      RecordTemplate newAspect, SystemMetadata newSystemMetadata,
      MetadataChangeProposal mcp, Urn entityUrn,
      AuditStamp auditStamp, AspectSpec aspectSpec) {

    if (oldAspect != newAspect || _alwaysEmitAuditEvent) {
      log.debug("Producing MetadataChangeLog for ingested aspect {}, urn {}", mcp.getAspectName(), entityUrn);

      final MetadataChangeLog metadataChangeLog = new MetadataChangeLog(mcp.data());
      metadataChangeLog.setEntityUrn(entityUrn);
      metadataChangeLog.setCreated(auditStamp);
      // The change log produced by this method is always an upsert as it contains the entire RecordTemplate update
      metadataChangeLog.setChangeType(ChangeType.UPSERT);

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

      produceMetadataChangeLog(entityUrn, aspectSpec, metadataChangeLog);

      return true;
    } else {
      log.debug(
          "Skipped producing MetadataChangeLog for ingested aspect {}, urn {}. Aspect has not changed.",
          mcp.getAspectName(), entityUrn);
      return false;
    }
  }

  public Integer getCountAspect(@Nonnull String aspectName, @Nullable String urnLike) {
    return _aspectDao.countAspect(aspectName, urnLike);
  }

  @Nonnull
  public RestoreIndicesResult restoreIndices(@Nonnull RestoreIndicesArgs args, @Nonnull Consumer<String> logger) {
    RestoreIndicesResult result = new RestoreIndicesResult();
    int ignored = 0;
    int rowsMigrated = 0;
    logger.accept(String.format("Args are %s", args));
    logger.accept(String.format(
            "Reading rows %s through %s from the aspects table started.", args.start, args.start + args.batchSize));
    long startTime = System.currentTimeMillis();
    PagedList<EbeanAspectV2> rows = _aspectDao.getPagedAspects(args);
    result.timeSqlQueryMs = System.currentTimeMillis() - startTime;
    startTime = System.currentTimeMillis();
    logger.accept(String.format(
            "Reading rows %s through %s from the aspects table completed.", args.start, args.start + args.batchSize));

    for (EbeanAspectV2 aspect : rows.getList()) {
      // 1. Extract an Entity type from the entity Urn
      result.timeGetRowMs = System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      Urn urn;
      try {
        urn = Urn.createFromString(aspect.getKey().getUrn());
      } catch (Exception e) {
        logger.accept(String.format("Failed to bind Urn with value %s into Urn object: %s. Ignoring row.",
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
        logger.accept(String.format("Failed to find entity with name %s in Entity Registry: %s. Ignoring row.",
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
        logger.accept(String.format("Failed to find aspect with name %s associated with entity named %s", aspectName,
                entityName));
        ignored = ignored + 1;
        continue;
      }
      result.aspectCheckMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      // 4. Create record from json aspect
      final RecordTemplate aspectRecord;
      try {
        aspectRecord = EntityUtils.toAspectRecord(entityName, aspectName, aspect.getMetadata(), _entityRegistry);
      } catch (Exception e) {
        logger.accept(String.format("Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
                aspect.getMetadata(), entityName, aspectName, e));
        ignored = ignored + 1;
        continue;
      }
      result.createRecordMs += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();

      SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(aspect.getSystemMetadata());

      // 5. Produce MAE events for the aspect record
      produceMetadataChangeLog(urn, entityName, aspectName, aspectSpec, null, aspectRecord, null,
              latestSystemMetadata,
              new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()),
              ChangeType.RESTATE);
      result.sendMessageMs += System.currentTimeMillis() - startTime;

      rowsMigrated++;
    }
    result.ignored = ignored;
    result.rowsMigrated = rowsMigrated;
    return result;
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
  public RecordTemplate updateAspect(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate newValue,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final long version,
      @Nonnull final boolean emitMae) {
    log.debug(
        "Invoked updateAspect with urn: {}, aspectName: {}, newValue: {}, version: {}, emitMae: {}", urn,
        aspectName, newValue, version, emitMae);
    return updateAspect(urn, entityName, aspectName, aspectSpec, newValue, auditStamp, version, emitMae,
        DEFAULT_MAX_TRANSACTION_RETRY);
  }

  /**
   * Lists the entity URNs found in storage.
   *
   * @param entityName the name associated with the entity
   * @param start the start offset
   * @param count the count
   */
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
  private boolean isAspectMissing(String entityType, String aspectName, Set<String> aspects) {
    return _entityRegistry.getEntitySpec(entityType).getAspectSpecMap().containsKey(aspectName)
        && !aspects.contains(aspectName);
  }

  public List<Pair<String, RecordTemplate>> generateDefaultAspectsIfMissing(@Nonnull final Urn urn,
      Set<String> includedAspects) {

    Set<String> aspectsToGet = new HashSet<>();
    String entityType = urnToEntityName(urn);

    boolean shouldCheckBrowsePath = isAspectMissing(entityType, BROWSE_PATHS, includedAspects);
    if (shouldCheckBrowsePath) {
      aspectsToGet.add(BROWSE_PATHS);
    }

    boolean shouldCheckDataPlatform = isAspectMissing(entityType, DATA_PLATFORM_INSTANCE, includedAspects);
    if (shouldCheckDataPlatform) {
      aspectsToGet.add(DATA_PLATFORM_INSTANCE);
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
        BrowsePaths generatedBrowsePath = buildDefaultBrowsePath(urn);
        aspects.add(Pair.of(BROWSE_PATHS, generatedBrowsePath));
      } catch (URISyntaxException e) {
        log.error("Failed to parse urn: {}", urn);
      }
    }

    if (shouldCheckDataPlatform && latestAspects.get(DATA_PLATFORM_INSTANCE) == null) {
      DataPlatformInstanceUtils.buildDataPlatformInstance(entityType, keyAspect)
          .ifPresent(aspect -> aspects.add(Pair.of(DATA_PLATFORM_INSTANCE, aspect)));
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
    return EntityKeyUtils.convertUrnToEntityKey(urn, keySpec);
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

  public EntityRegistry getEntityRegistry() {
    return _entityRegistry;
  }

  public void setRetentionService(RetentionService retentionService) {
    _retentionService = retentionService;
  }

  protected Set<String> getEntityAspectNames(final Urn entityUrn) {
    return getEntityAspectNames(urnToEntityName(entityUrn));
  }

  public Set<String> getEntityAspectNames(final String entityName) {
    return _entityToValidAspects.get(entityName);
  }

  public void setWritable(boolean canWrite) {
    log.debug("Setting writable to {}", canWrite);
    _aspectDao.setWritable(canWrite);
  }

  public RollbackRunResult rollbackRun(List<AspectRowSummary> aspectRows, String runId, boolean hardDelete) {
    return rollbackWithConditions(aspectRows, Collections.singletonMap("runId", runId), hardDelete);
  }

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

  public RollbackRunResult deleteUrn(Urn urn) {
    List<AspectRowSummary> removedAspects = new ArrayList<>();
    Integer rowsDeletedFromEntityDeletion = 0;

    final EntitySpec spec = getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    String keyAspectName = getKeyAspectName(urn);

    EntityAspect latestKey = _aspectDao.getLatestAspect(urn.toString(), keyAspectName);
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

  public Boolean exists(Urn urn) {
    final Set<String> aspectsToFetch = getEntityAspectNames(urn);
    final List<EntityAspectIdentifier> dbKeys = aspectsToFetch.stream()
        .map(aspectName -> new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
        .collect(Collectors.toList());

    Map<EntityAspectIdentifier, EntityAspect> aspects = _aspectDao.batchGet(new HashSet(dbKeys));
    return aspects.values().stream().anyMatch(aspect -> aspect != null);
  }

  @Nullable
  public RollbackResult deleteAspect(String urn, String aspectName, @Nonnull Map<String, String> conditions, boolean hardDelete) {
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

    final RollbackResult result = _aspectDao.runInTransactionWithRetry(() -> {
      Integer additionalRowsDeleted = 0;

      // 1. Fetch the latest existing version of the aspect.
      final EntityAspect latest = _aspectDao.getLatestAspect(urn, aspectName);

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
        log.error("Error occurred while parsing urn: {}", urn, e);
      }

      // 4. Fetch all preceding aspects, that match
      List<EntityAspect> aspectsToDelete = new ArrayList<>();
      long maxVersion = _aspectDao.getMaxVersion(urn, aspectName);
      EntityAspect survivingAspect = null;
      String previousMetadata = null;
      boolean filterMatch = true;
      while (maxVersion > 0 && filterMatch)  {
        EntityAspect candidateAspect = _aspectDao.getAspect(urn, aspectName, maxVersion);
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

            final MetadataChangeProposal gmce = new MetadataChangeProposal();
            gmce.setEntityUrn(entityUrn);
            gmce.setChangeType(ChangeType.UPSERT);
            gmce.setEntityType(entityUrn.getEntityType());
            gmce.setAspectName(Constants.STATUS_ASPECT_NAME);
            gmce.setAspect(GenericRecordUtils.serializeAspect(statusAspect));
            final AuditStamp auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

            this.ingestProposal(gmce, auditStamp);
          }
        } else {
          // Else, only delete the specific aspect.
          _aspectDao.deleteAspect(latest);
        }
      }

      // 6. Emit the Update
      try {
        final RecordTemplate latestValue = latest == null ? null
            : EntityUtils.toAspectRecord(Urn.createFromString(latest.getUrn()), latest.getAspect(),
            latestMetadata, getEntityRegistry());

        final RecordTemplate previousValue = survivingAspect == null ? null
            : EntityUtils.toAspectRecord(Urn.createFromString(survivingAspect.getUrn()),
            survivingAspect.getAspect(), previousMetadata, getEntityRegistry());

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
      } catch (IllegalStateException e) {
        log.warn("Unable to find aspect, rollback result will not be sent. Error: {}", e.getMessage());
        return null;
      }
    }, DEFAULT_MAX_TRANSACTION_RETRY);

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
  private Map<EntityAspectIdentifier, EntityAspect> getLatestAspect(@Nonnull final Set<Urn> urns, @Nonnull final Set<String> aspectNames) {

    log.debug("Invoked getLatestAspects with urns: {}, aspectNames: {}", urns, aspectNames);

    // Create DB keys
    final Set<EntityAspectIdentifier> dbKeys = urns.stream().map(urn -> {
      final Set<String> aspectsToFetch = aspectNames.isEmpty() ? getEntityAspectNames(urn) : aspectNames;
      return aspectsToFetch.stream()
          .map(aspectName -> new EntityAspectIdentifier(urn.toString(), aspectName, ASPECT_LATEST_VERSION))
          .collect(Collectors.toList());
    }).flatMap(List::stream).collect(Collectors.toSet());

    Map<EntityAspectIdentifier, EntityAspect> batchGetResults = new HashMap<>();
    Iterators.partition(dbKeys.iterator(), MAX_KEYS_PER_QUERY)
        .forEachRemaining(batch -> batchGetResults.putAll(_aspectDao.batchGet(ImmutableSet.copyOf(batch))));
    return batchGetResults;
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

  private Map<EntityAspectIdentifier, EnvelopedAspect> getEnvelopedAspects(final Set<EntityAspectIdentifier> dbKeys) {
    final Map<EntityAspectIdentifier, EnvelopedAspect> result = new HashMap<>();
    final Map<EntityAspectIdentifier, EntityAspect> dbEntries = _aspectDao.batchGet(dbKeys);

    for (EntityAspectIdentifier currKey : dbKeys) {

      final EntityAspect currAspectEntry = dbEntries.get(currKey);

      if (currAspectEntry == null) {
        // No aspect found.
        continue;
      }

      // Aspect found. Now turn it into an EnvelopedAspect
      final com.linkedin.entity.Aspect aspect = RecordUtils.toRecordTemplate(com.linkedin.entity.Aspect.class, currAspectEntry
          .getMetadata());
      final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
      envelopedAspect.setName(currAspectEntry.getAspect());
      envelopedAspect.setVersion(currAspectEntry.getVersion());
      // TODO: I think we can assume this here, adding as it's a required field so object mapping barfs when trying to access it,
      //    since nowhere else is using it should be safe for now at least
      envelopedAspect.setType(AspectType.VERSIONED);
      envelopedAspect.setValue(aspect);

      try {
        if (currAspectEntry.getSystemMetadata() != null) {
          final SystemMetadata systemMetadata = RecordUtils.toRecordTemplate(SystemMetadata.class, currAspectEntry.getSystemMetadata());
          envelopedAspect.setSystemMetadata(systemMetadata);
        }
      } catch (Exception e) {
        log.warn("Exception encountered when setting system metadata on enveloped aspect {}. Error: {}", envelopedAspect.getName(), e);
      }

      envelopedAspect.setCreated(new AuditStamp()
          .setActor(UrnUtils.getUrn(currAspectEntry.getCreatedBy()))
          .setTime(currAspectEntry.getCreatedOn().getTime())
      );
      result.put(currKey, envelopedAspect);
    }
    return result;
  }

  private EnvelopedAspect getKeyEnvelopedAspect(final Urn urn) {
    final EntitySpec spec = getEntityRegistry().getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    final RecordDataSchema keySchema = keySpec.getPegasusSchema();
    final com.linkedin.entity.Aspect aspect =
        new com.linkedin.entity.Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data());

    final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(keySpec.getName());
    envelopedAspect.setVersion(ASPECT_LATEST_VERSION);
    envelopedAspect.setValue(aspect);
    // TODO: I think we can assume this here, adding as it's a required field so object mapping barfs when trying to access it,
    //    since nowhere else is using it should be safe for now at least
    envelopedAspect.setType(AspectType.VERSIONED);
    envelopedAspect.setCreated(
      new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()));

    return envelopedAspect;
  }

  @Nonnull
  private UpdateAspectResult ingestAspectToLocalDBNoTransaction(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final Function<Optional<RecordTemplate>, RecordTemplate> updateLambda,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final SystemMetadata providedSystemMetadata,
      @Nullable final EntityAspect latest,
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

  @Nonnull
  private Map<String, EntityAspect> getLatestAspectForUrn(@Nonnull final Urn urn, @Nonnull final Set<String> aspectNames) {
    Set<Urn> urns = new HashSet<>();
    urns.add(urn);

    Map<String, EntityAspect> result = new HashMap<>();
    getLatestAspect(urns, aspectNames).forEach((key, aspectEntry) -> {
      final String aspectName = key.getAspect();
      result.put(aspectName, aspectEntry);
    });
    return result;
  }

  @Nonnull
  private RecordTemplate updateAspect(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final AspectSpec aspectSpec,
      @Nonnull final RecordTemplate value,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final long version,
      @Nonnull final boolean emitMae,
      final int maxTransactionRetry) {

    final UpdateAspectResult result = _aspectDao.runInTransactionWithRetry(() -> {

      final EntityAspect oldAspect = _aspectDao.getAspect(urn.toString(), aspectName, version);
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

  /**
   * Builds the default browse path aspects for a subset of well-supported entities.
   *
   * This method currently supports datasets, charts, dashboards, data flows, data jobs, and glossary terms.
   */
  @Nonnull
  public BrowsePaths buildDefaultBrowsePath(final @Nonnull Urn urn) throws URISyntaxException {
    Character dataPlatformDelimiter = getDataPlatformDelimiter(urn);
    String defaultBrowsePath = getDefaultBrowsePath(urn, this.getEntityRegistry(), dataPlatformDelimiter);
    StringArray browsePaths = new StringArray();
    browsePaths.add(defaultBrowsePath);
    BrowsePaths browsePathAspect = new BrowsePaths();
    browsePathAspect.setPaths(browsePaths);
    return browsePathAspect;
  }

  /**
   * Returns a delimiter on which the name of an asset may be split.
   */
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
      final EntityResponse entityResponse = getEntityV2(
          Constants.DATA_PLATFORM_ENTITY_NAME,
          urn,
          ImmutableSet.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)
      );
      if (entityResponse != null && entityResponse.hasAspects() && entityResponse.getAspects().containsKey(Constants.DATA_PLATFORM_INFO_ASPECT_NAME)) {
        return new DataPlatformInfo(entityResponse.getAspects().get(Constants.DATA_PLATFORM_INFO_ASPECT_NAME).getValue().data());
      }
    } catch (Exception e) {
      log.warn(String.format("Failed to find Data Platform Info for urn %s", urn));
    }
    return null;
  }
}
