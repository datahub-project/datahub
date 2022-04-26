package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.ValidationException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.RelatedAspect;
import com.linkedin.metadata.graph.RelatedAspectArray;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.graph.RelationshipResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.resources.utils.AspectProcessor;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.internal.server.methods.AnyRecord;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import com.linkedin.util.Pair;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.artifact.versioning.ComparableVersion;

import static com.linkedin.metadata.entity.ValidationUtils.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static com.linkedin.metadata.utils.PegasusUtils.*;


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiCollection(name = "entities", namespace = "com.linkedin.entity")
public class EntityResource extends CollectionResourceTaskTemplate<String, Entity> {

  private static final String ACTION_SEARCH = "search";
  private static final String ACTION_LIST = "list";
  private static final String ACTION_SEARCH_ACROSS_ENTITIES = "searchAcrossEntities";
  private static final String ACTION_SEARCH_ACROSS_LINEAGE = "searchAcrossLineage";
  private static final String ACTION_BATCH_INGEST = "batchIngest";
  private static final String ACTION_LIST_URNS = "listUrns";
  private static final String ACTION_FILTER = "filter";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ENTITIES = "entities";
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_VALUE = "value";
  private static final String SYSTEM_METADATA = "systemMetadata";
  private static final Integer ELASTIC_BATCH_DELETE_SLEEP_SEC = 5;

  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  @Inject
  @Named("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Inject
  @Named("systemMetadataService")
  private SystemMetadataService _systemMetadataService;

  @Inject
  @Named("relationshipSearchService")
  private LineageSearchService _lineageSearchService;

  @Inject
  @Named("kafkaEventProducer")
  private EventProducer _eventProducer;

  @Inject
  @Named("graphService")
  private GraphService _graphService;

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<AnyRecord> get(@Nonnull String urnStr,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    log.info("GET {}", urnStr);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
      final Entity entity = _entityService.getEntity(urn, projectedAspects);
      if (entity == null) {
        throw RestliUtil.resourceNotFoundException();
      }
      return new AnyRecord(entity.data());
    }, MetricRegistry.name(this.getClass(), "get"));
  }

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<String, AnyRecord>> batchGet(@Nonnull Set<String> urnStrs,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    log.info("BATCH GET {}", urnStrs);
    final Set<Urn> urns = new HashSet<>();
    for (final String urnStr : urnStrs) {
      urns.add(Urn.createFromString(urnStr));
    }
    return RestliUtil.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
      return _entityService.getEntities(urns, projectedAspects)
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(entry -> entry.getKey().toString(), entry -> new AnyRecord(entry.getValue().data())));
    }, MetricRegistry.name(this.getClass(), "batchGet"));
  }

  private SystemMetadata populateDefaultFieldsIfEmpty(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata result = systemMetadata;
    if (result == null) {
      result = new SystemMetadata();
    }

    if (result.getLastObserved() == 0) {
      result.setLastObserved(System.currentTimeMillis());
    }

    return result;
  }

  @Action(name = ACTION_INGEST)
  @Nonnull
  @WithSpan
  public Task<Void> ingest(@ActionParam(PARAM_ENTITY) @Nonnull Entity entity,
      @ActionParam(SYSTEM_METADATA) @Optional @Nullable SystemMetadata providedSystemMetadata)
      throws URISyntaxException {
    try {
      validateOrThrow(entity);
    } catch (ValidationException e) {
      throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, e);
    }

    SystemMetadata systemMetadata = populateDefaultFieldsIfEmpty(providedSystemMetadata);

    // TODO Correctly audit ingestions.
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(Constants.UNKNOWN_ACTOR));

    // variables referenced in lambdas are required to be final
    final SystemMetadata finalSystemMetadata = systemMetadata;
    return RestliUtil.toTask(() -> {
      _entityService.ingestEntity(entity, auditStamp, finalSystemMetadata);
      return null;
    }, MetricRegistry.name(this.getClass(), "ingest"));
  }

  @Action(name = ACTION_BATCH_INGEST)
  @Nonnull
  @WithSpan
  public Task<Void> batchIngest(@ActionParam(PARAM_ENTITIES) @Nonnull Entity[] entities,
      @ActionParam(SYSTEM_METADATA) @Optional @Nullable SystemMetadata[] systemMetadataList) throws URISyntaxException {

    for (Entity entity : entities) {
      try {
        validateOrThrow(entity);
      } catch (ValidationException e) {
        throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, e);
      }
    }

    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(Constants.UNKNOWN_ACTOR));

    if (systemMetadataList == null) {
      systemMetadataList = new SystemMetadata[entities.length];
    }

    if (entities.length != systemMetadataList.length) {
      throw RestliUtil.invalidArgumentsException("entities and systemMetadata length must match");
    }

    final List<SystemMetadata> finalSystemMetadataList = Arrays.stream(systemMetadataList)
        .map(systemMetadata -> populateDefaultFieldsIfEmpty(systemMetadata))
        .collect(Collectors.toList());

    return RestliUtil.toTask(() -> {
      _entityService.ingestEntities(Arrays.asList(entities), auditStamp, finalSystemMetadataList);
      return null;
    }, MetricRegistry.name(this.getClass(), "batchIngest"));
  }

  @Action(name = ACTION_SEARCH)
  @Nonnull
  @WithSpan
  public Task<SearchResult> search(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_INPUT) @Nonnull String input, @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    log.info("GET SEARCH RESULTS for {} with query {}", entityName, input);
    // TODO - change it to use _searchService once we are confident on it's latency
    return RestliUtil.toTask(() -> _entitySearchService.search(entityName, input, filter, sortCriterion, start, count),
        MetricRegistry.name(this.getClass(), "search"));
  }

  @Action(name = ACTION_SEARCH_ACROSS_ENTITIES)
  @Nonnull
  @WithSpan
  public Task<SearchResult> searchAcrossEntities(@ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Nonnull String input, @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {
    List<String> entityList = entities == null ? Collections.emptyList() : Arrays.asList(entities);
    log.info("GET SEARCH RESULTS ACROSS ENTITIES for {} with query {}", entityList, input);
    return RestliUtil.toTask(
        () -> _searchService.searchAcrossEntities(entityList, input, filter, sortCriterion, start, count, null),
        "searchAcrossEntities");
  }

  @Action(name = ACTION_SEARCH_ACROSS_LINEAGE)
  @Nonnull
  @WithSpan
  public Task<LineageSearchResult> searchAcrossLineage(@ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_DIRECTION) String direction,
      @ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Optional @Nullable String input, @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) throws URISyntaxException {
    Urn urn = Urn.createFromString(urnStr);
    List<String> entityList = entities == null ? Collections.emptyList() : Arrays.asList(entities);
    log.info("GET SEARCH RESULTS ACROSS RELATIONSHIPS for source urn {}, direction {}, entities {} with query {}",
        urnStr, direction, entityList, input);
    return RestliUtil.toTask(
        () -> _lineageSearchService.searchAcrossLineage(urn, LineageDirection.valueOf(direction), entityList,
            input, filter, sortCriterion, start, count), "searchAcrossRelationships");
  }

  @Action(name = ACTION_LIST)
  @Nonnull
  @WithSpan
  public Task<ListResult> list(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    log.info("GET LIST RESULTS for {} with filter {}", entityName, filter);
    return RestliUtil.toTask(
        () -> toListResult(_entitySearchService.filter(entityName, filter, sortCriterion, start, count)),
        MetricRegistry.name(this.getClass(), "filter"));
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Nonnull
  @WithSpan
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_QUERY) @Nonnull String query, @ActionParam(PARAM_FIELD) @Optional @Nullable String field,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_LIMIT) int limit) {

    return RestliUtil.toTask(() -> _entitySearchService.autoComplete(entityName, query, field, filter, limit),
        MetricRegistry.name(this.getClass(), "autocomplete"));
  }

  @Action(name = ACTION_BROWSE)
  @Nonnull
  @WithSpan
  public Task<BrowseResult> browse(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_PATH) @Nonnull String path, @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_START) int start, @ActionParam(PARAM_LIMIT) int limit) {

    log.info("GET BROWSE RESULTS for {} at path {}", entityName, path);
    return RestliUtil.toTask(() -> _entitySearchService.browse(entityName, path, filter, start, limit),
        MetricRegistry.name(this.getClass(), "browse"));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Nonnull
  @WithSpan
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = PARAM_URN, typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    log.info("GET BROWSE PATHS for {}", urn);
    return RestliUtil.toTask(() -> new StringArray(_entitySearchService.getBrowsePaths(urnToEntityName(urn), urn)),
        MetricRegistry.name(this.getClass(), "getBrowsePaths"));
  }

  private static final Integer ELASTIC_MAX_PAGE_SIZE = 10000;

  private String stringifyRowCount(int size) {
    if (size < ELASTIC_MAX_PAGE_SIZE) {
      return String.valueOf(size);
    } else {
      return "at least " + size;
    }
  }

  /*
  Used to delete all data related to a filter criteria based on registryId, runId etc.
   */
  @Action(name = "deleteAll")
  @Nonnull
  @WithSpan
  public Task<RollbackResponse> deleteEntities(@ActionParam("registryId") @Optional String registryId,
      @ActionParam("dryRun") @Optional Boolean dryRun) {
    String registryName = null;
    ComparableVersion registryVersion = new ComparableVersion("0.0.0-dev");

    if (registryId != null) {
      try {
        registryName = registryId.split(":")[0];
        registryVersion = new ComparableVersion(registryId.split(":")[1]);
      } catch (Exception e) {
        throw new RestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Failed to parse registry id: " + registryId, e);
      }
    }
    String finalRegistryName = registryName;
    ComparableVersion finalRegistryVersion = registryVersion;
    String finalRegistryName1 = registryName;
    ComparableVersion finalRegistryVersion1 = registryVersion;
    return RestliUtil.toTask(() -> {
      RollbackResponse response = new RollbackResponse();
      List<AspectRowSummary> aspectRowsToDelete =
          _systemMetadataService.findByRegistry(finalRegistryName, finalRegistryVersion.toString(), false);
      log.info("found {} rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
      response.setAspectsAffected(aspectRowsToDelete.size());
      response.setEntitiesAffected(
          aspectRowsToDelete.stream().collect(Collectors.groupingBy(AspectRowSummary::getUrn)).keySet().size());
      response.setEntitiesDeleted(aspectRowsToDelete.stream().filter(row -> row.isKeyAspect()).count());
      response.setAspectRowSummaries(
          new AspectRowSummaryArray(aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size()))));
      if ((dryRun == null) || (!dryRun)) {
        Map<String, String> conditions = new HashMap();
        conditions.put("registryName", finalRegistryName1);
        conditions.put("registryVersion", finalRegistryVersion1.toString());
        _entityService.rollbackWithConditions(aspectRowsToDelete, conditions, false);
      }
      return response;
    }, MetricRegistry.name(this.getClass(), "deleteAll"));
  }

  /*
  Used to delete all data related to an individual urn
   */
  @Action(name = "delete")
  @Nonnull
  @WithSpan
  public Task<DeleteEntityResponse> deleteEntity(@ActionParam(PARAM_URN) @Nonnull String urnStr)
      throws URISyntaxException {
    Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      DeleteEntityResponse response = new DeleteEntityResponse();
      RollbackRunResult result = _entityService.deleteUrn(urn);

      response.setUrn(urnStr);
      response.setRows(result.getRowsDeletedFromEntityDeletion());

      return response;
    }, MetricRegistry.name(this.getClass(), "delete"));
  }

  @Action(name = "deleteReferences")
  @Nonnull
  @WithSpan
  public Task<RelationshipResult> deleteReferencesTo(@ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam("dryRun") @Optional Boolean dryRun)
      throws URISyntaxException {
    Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {

      final RelationshipResult result = new RelationshipResult();
      RelatedEntitiesResult relatedEntities =
          _graphService.findRelatedEntities(null, newFilter("urn", urn.toString()), null,
              EMPTY_FILTER,
              ImmutableList.of(),
              newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 10000);

      final List<RelatedAspect> relatedAspects = relatedEntities.getEntities().stream().map(relatedEntity -> {
        final Urn relatedUrn = UrnUtils.getUrn(relatedEntity.getUrn());
        final String relationType = relatedEntity.getRelationshipType();
        final String relatedEntityName = relatedUrn.getEntityType();
        final EntitySpec relatedEntitySpec = _entityService.getEntityRegistry().getEntitySpec(relatedEntityName);

        return findAspectDetails(urn, relatedUrn, relationType, relatedEntitySpec).map(pair -> {
          final RelatedAspect relatedAspect = new RelatedAspect();
          relatedAspect.setEntity(urn);
          relatedAspect.setRelationship(relationType);
          relatedAspect.setAspect(pair.getSecond().getName());
          return relatedAspect;
        });
      }).filter(java.util.Optional::isPresent)
          .map(java.util.Optional::get)
          .limit(10).collect(Collectors.toList());

      result.setRelatedAspects(new RelatedAspectArray(relatedAspects));
      result.setTotal(relatedEntities.getTotal());

      if (dryRun) {
        return result;
      }

      // Delete first 10k
      relatedEntities.getEntities().forEach(entity -> deleteRelatedEntities(urn, entity));

      // Delete until less than 10k are left
      while (relatedEntities.getCount() > 10000) {
        sleep(5);
        relatedEntities = _graphService.findRelatedEntities(null, newFilter("urn", urn.toString()),
            null, EMPTY_FILTER, ImmutableList.of(),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 10000);
        relatedEntities.getEntities().forEach(entity -> deleteRelatedEntities(urn, entity));
      }

      // Delete the <10k that remain from the loop
      relatedEntities.getEntities().forEach(entity -> deleteRelatedEntities(urn, entity));

      return result;
    }, MetricRegistry.name(this.getClass(), "getReferencesTo"));
  }

  /**
   * Utility method that finds all aspects of in a given {@link RelatedEntity} instance that reference a given {@link Urn}
   * removes said urn from the aspects and submits an MCP with the updated aspects.
   * @param urn           The urn to be found.
   * @param relatedEntity The entity to be modified.
   */
  private void deleteRelatedEntities(Urn urn, RelatedEntity relatedEntity) {
      try {
        final Urn relatedUrn = UrnUtils.getUrn(relatedEntity.getUrn());
        final String relationType = relatedEntity.getRelationshipType();
        final String relatedEntityName = relatedUrn.getEntityType();

        log.info(String.format("Processing related entity %s with relationship %s", relatedEntityName,
            relationType));

        final EntitySpec relatedEntitySpec = _entityService.getEntityRegistry().getEntitySpec(relatedEntityName);

        final java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> optionalAspectInfo =
            findAspectDetails(urn, relatedUrn, relationType, relatedEntitySpec);

        if (!optionalAspectInfo.isPresent()) {
          log.error(String.format("Unable to find aspect information that relates %s %s via relationship %s",
              urn, relatedUrn, relationType));
          return;
        }

        final String aspectName = optionalAspectInfo.get().getKey().getName();
        final Aspect aspect = optionalAspectInfo.get().getKey().getValue();
        final AspectSpec aspectSpec = optionalAspectInfo.get().getValue();
        final java.util.Optional<RelationshipFieldSpec> optionalRelationshipFieldSpec =
            aspectSpec.findRelationshipFor(relationType, urn.getEntityType());

        if (!optionalRelationshipFieldSpec.isPresent()) {
          log.error(String.format("Unable to find relationship spec information in %s that connects to %s via %s",
              aspectName, urn.getEntityType(), relationType));
          return;
        }

        final PathSpec path = optionalRelationshipFieldSpec.get().getPath();
        final Aspect updatedAspect = AspectProcessor.removeAspect(urn.toString(), aspect,
            aspectSpec.getPegasusSchema(), path);

        final MetadataChangeProposal gmce = new MetadataChangeProposal();
        gmce.setEntityUrn(relatedUrn);
        gmce.setChangeType(ChangeType.UPSERT);
        gmce.setEntityType(relatedEntityName);
        gmce.setAspectName(aspectName);
        gmce.setAspect(GenericRecordUtils.serializeAspect(updatedAspect));

        final AuditStamp auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
        final EntityService.IngestProposalResult ingestProposalResult = _entityService.ingestProposal(gmce, auditStamp);

        if (!ingestProposalResult.isDidUpdate()) {
          log.warn(String.format("Aspect update did not update metadata graph. Before %s, after: %s",
              aspect, updatedAspect));
        }
      } catch (CloneNotSupportedException e) {
        log.error(String.format("Failed to clone aspect from entity %s", relatedEntity), e);
      }
  }

  private void sleep(Integer seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Utility method that attempts to find Aspect information as well as the associated path spec for a given urn that
   * has a relationship of type `relationType` to another urn
   * @param urn               The urn of the entity for which we want to find the aspect that relates the urn to
   *                          `relatedUrn`.
   * @param relatedUrn        The urn of the related entity in which we want to find the aspect that has a relationship
   *                          to `urn`.
   * @param relationType      The relationship type that details how the `urn` entity is related to `relatedUrn` entity.
   * @param relatedEntitySpec The entity spec of the related entity.
   * @return An {@link java.util.Optional} object containing the aspect content & respective spec that contains the
   * relationship between `urn` & `relatedUrn`.
   * @throws URISyntaxException
   */
  private java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> findAspectDetails(Urn urn, Urn relatedUrn,
      String relationType, EntitySpec relatedEntitySpec) {

    // Find which aspects are the candidates for the relationship we are looking for
    final Map<String, AspectSpec> aspectSpecs = Objects.requireNonNull(relatedEntitySpec).getAspectSpecMap()
        .entrySet()
        .stream()
        .filter(aspectSpec -> aspectSpec.getValue().getRelationshipFieldSpecs()
            .stream()
            .anyMatch(spec -> spec.getRelationshipName().equals(relationType)
                && spec.getValidDestinationTypes().contains(urn.getEntityType())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (aspectSpecs.size() > 1) {
      log.warn(String.format("More than 1 relationship of type %s expected for destination entity: %s", relationType,
          urn.getEntityType()));
    }

    // FIXME: Can we not depend on entity service?
    final EntityResponse entityResponse;
    try {
      entityResponse = _entityService.getEntityV2(relatedUrn.getEntityType(),
          relatedUrn,
          aspectSpecs.keySet());
    } catch (URISyntaxException e) {
      log.error("Unable to retrieve entity data for relatedUrn " + relatedUrn, e);
      return java.util.Optional.empty();
    }

    // Find aspect which contains the relationship with the value we are looking for
    return entityResponse
        .getAspects().values()
        .stream()
        .filter(aspect -> aspect.getValue().toString().contains(urn.getEntityType()))
        .map(aspect -> Pair.of(aspect, aspectSpecs.get(aspect.getName())))
        .findFirst();
  }


  /*
  Used to enable writes in GMS after data migration is complete
   */
  @Action(name = "setWritable")
  @Nonnull
  @WithSpan
  public Task<Void> setWriteable(@ActionParam(PARAM_VALUE) @Optional("true") @Nonnull Boolean value) {
    log.info("setting entity resource to be writable");
    return RestliUtil.toTask(() -> {
      _entityService.setWritable(value);
      return null;
    });
  }

  @Action(name = "getTotalEntityCount")
  @Nonnull
  @WithSpan
  public Task<Long> getTotalEntityCount(@ActionParam(PARAM_ENTITY) @Nonnull String entityName) {
    return RestliUtil.toTask(() -> _entitySearchService.docCount(entityName));
  }

  @Action(name = "batchGetTotalEntityCount")
  @Nonnull
  @WithSpan
  public Task<LongMap> batchGetTotalEntityCount(@ActionParam(PARAM_ENTITIES) @Nonnull String[] entityNames) {
    return RestliUtil.toTask(() -> new LongMap(_searchService.docCountPerEntity(Arrays.asList(entityNames))));
  }

  @Action(name = ACTION_LIST_URNS)
  @Nonnull
  @WithSpan
  public Task<ListUrnsResult> listUrns(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_START) int start, @ActionParam(PARAM_COUNT) int count) throws URISyntaxException {
    log.info("LIST URNS for {} with start {} and count {}", entityName, start, count);
    return RestliUtil.toTask(() -> _entityService.listUrns(entityName, start, count), "listUrns");
  }

  public static ListResult toListResult(final SearchResult searchResult) {
    if (searchResult == null) {
      return null;
    }
    final ListResult listResult = new ListResult();
    listResult.setStart(searchResult.getFrom());
    listResult.setCount(searchResult.getPageSize());
    listResult.setTotal(searchResult.getNumEntities());
    listResult.setEntities(
        new UrnArray(searchResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList())));
    return listResult;
  }

  @Action(name = ACTION_FILTER)
  @Nonnull
  @WithSpan
  public Task<SearchResult> filter(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_FILTER) Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    log.info("FILTER RESULTS for {} with filter {}", entityName, filter);
    return RestliUtil.toTask(() -> _entitySearchService.filter(entityName, filter, sortCriterion, start, count),
        MetricRegistry.name(this.getClass(), "search"));
  }
}
