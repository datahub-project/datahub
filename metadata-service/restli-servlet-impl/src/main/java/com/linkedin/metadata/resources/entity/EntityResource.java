package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.models.EntitySpecUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
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
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.artifact.versioning.ComparableVersion;

import static com.linkedin.metadata.resources.entity.ResourceUtils.*;
import static com.linkedin.metadata.entity.validation.ValidationUtils.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.search.utils.SearchUtils.*;
import static com.linkedin.metadata.shared.ValidationUtils.*;
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
  private static final String ACTION_DELETE = "delete";
  private static final String ACTION_EXISTS = "exists";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ENTITIES = "entities";
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_VALUE = "value";
  private static final String PARAM_ASPECT_NAME = "aspectName";
  private static final String PARAM_START_TIME_MILLIS = "startTimeMillis";
  private static final String PARAM_END_TIME_MILLIS = "endTimeMillis";
  private static final String PARAM_URN = "urn";
  private static final String SYSTEM_METADATA = "systemMetadata";
  private static final String ES_FILED_TIMESTAMP = "timestampMillis";
  private static final Integer ELASTIC_MAX_PAGE_SIZE = 10000;
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

  @Inject
  @Named("deleteEntityService")
  private DeleteEntityService _deleteEntityService;

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService _timeseriesAspectService;

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

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    final AuditStamp auditStamp = new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    // variables referenced in lambdas are required to be final
    final SystemMetadata finalSystemMetadata = systemMetadata;
    return RestliUtil.toTask(() -> {
      _entityService.ingestEntity(entity, auditStamp, finalSystemMetadata);
      tryIndexRunId(com.datahub.util.ModelUtils.getUrnFromSnapshotUnion(entity.getValue()), systemMetadata,
          _entitySearchService);
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

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    final AuditStamp auditStamp = new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    if (systemMetadataList == null) {
      systemMetadataList = new SystemMetadata[entities.length];
    }

    if (entities.length != systemMetadataList.length) {
      throw RestliUtil.invalidArgumentsException("entities and systemMetadata length must match");
    }

    final List<SystemMetadata> finalSystemMetadataList = Arrays.stream(systemMetadataList)
        .map(systemMetadata -> populateDefaultFieldsIfEmpty(systemMetadata))
        .collect(Collectors.toList());

    SystemMetadata[] finalSystemMetadataList1 = systemMetadataList;
    return RestliUtil.toTask(() -> {
      _entityService.ingestEntities(Arrays.asList(entities), auditStamp, finalSystemMetadataList);
      for (int i = 0; i < entities.length; i++) {
        SystemMetadata systemMetadata = finalSystemMetadataList1[i];
        Entity entity = entities[i];
        tryIndexRunId(com.datahub.util.ModelUtils.getUrnFromSnapshotUnion(entity.getValue()), systemMetadata,
            _entitySearchService);
      }
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
    return RestliUtil.toTask(
        () -> validateSearchResult(_entitySearchService.search(entityName, input, filter, sortCriterion, start, count),
            _entityService), MetricRegistry.name(this.getClass(), "search"));
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
    return RestliUtil.toTask(() -> validateSearchResult(
        _searchService.searchAcrossEntities(entityList, input, filter, sortCriterion, start, count, null),
        _entityService), "searchAcrossEntities");
  }

  @Action(name = ACTION_SEARCH_ACROSS_LINEAGE)
  @Nonnull
  @WithSpan
  public Task<LineageSearchResult> searchAcrossLineage(@ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_DIRECTION) String direction,
      @ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Optional @Nullable String input,
      @ActionParam(PARAM_MAX_HOPS) @Optional @Nullable Integer maxHops,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) throws URISyntaxException {
    Urn urn = Urn.createFromString(urnStr);
    List<String> entityList = entities == null ? Collections.emptyList() : Arrays.asList(entities);
    log.info("GET SEARCH RESULTS ACROSS RELATIONSHIPS for source urn {}, direction {}, entities {} with query {}",
        urnStr, direction, entityList, input);
    return RestliUtil.toTask(() -> validateLineageSearchResult(
        _lineageSearchService.searchAcrossLineage(urn, LineageDirection.valueOf(direction), entityList, input, maxHops,
            filter, sortCriterion, start, count), _entityService), "searchAcrossRelationships");
  }

  @Action(name = ACTION_LIST)
  @Nonnull
  @WithSpan
  public Task<ListResult> list(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    log.info("GET LIST RESULTS for {} with filter {}", entityName, filter);
    return RestliUtil.toTask(() -> validateListResult(
            toListResult(_entitySearchService.filter(entityName, filter, sortCriterion, start, count)), _entityService),
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
    return RestliUtil.toTask(
        () -> validateBrowseResult(_entitySearchService.browse(entityName, path, filter, start, limit), _entityService),
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
          _systemMetadataService.findByRegistry(finalRegistryName, finalRegistryVersion.toString(), false, 0,
              ESUtils.MAX_RESULT_SIZE);
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

  /**
   * Deletes all data related to an individual urn(entity).
   * @param urnStr - the urn of the entity.
   * @param aspectName - the optional aspect name if only want to delete the aspect (applicable only for timeseries aspects).
   * @param startTimeMills - the optional start time (applicable only for timeseries aspects).
   * @param endTimeMillis - the optional end time (applicable only for the timeseries aspects).
   * @return -  a DeleteEntityResponse object.
   * @throws URISyntaxException
   */
  @Action(name = ACTION_DELETE)
  @Nonnull
  @WithSpan
  public Task<DeleteEntityResponse> deleteEntity(@ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_ASPECT_NAME) @Optional String aspectName,
      @ActionParam(PARAM_START_TIME_MILLIS) @Optional Long startTimeMills,
      @ActionParam(PARAM_END_TIME_MILLIS) @Optional Long endTimeMillis) throws URISyntaxException {
    Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      // Find the timeseries aspects to delete. If aspectName is null, delete all.
      List<String> timeseriesAspectNames =
          EntitySpecUtils.getEntityTimeseriesAspectNames(_entityService.getEntityRegistry(), urn.getEntityType());
      if (aspectName != null && !timeseriesAspectNames.contains(aspectName)) {
        throw new UnsupportedOperationException(
            String.format("Not supported for non-timeseries aspect '{}'.", aspectName));
      }
      List<String> timeseriesAspectsToDelete =
          (aspectName == null) ? timeseriesAspectNames : ImmutableList.of(aspectName);

      DeleteEntityResponse response = new DeleteEntityResponse();
      if (aspectName == null) {
        RollbackRunResult result = _entityService.deleteUrn(urn);
        response.setRows(result.getRowsDeletedFromEntityDeletion());
      }
      Long numTimeseriesDocsDeleted =
          deleteTimeseriesAspects(urn, startTimeMills, endTimeMillis, timeseriesAspectsToDelete);
      log.info("Total number of timeseries aspect docs deleted: {}", numTimeseriesDocsDeleted);

      response.setUrn(urnStr);
      response.setTimeseriesRows(numTimeseriesDocsDeleted);

      return response;
    }, MetricRegistry.name(this.getClass(), "delete"));
  }

  /**
   * Deletes the set of timeseries aspect values for the specified aspects that are associated with the given
   * entity urn between startTimeMillis and endTimeMillis.
   * @param urn The entity urn whose timeseries aspect values need to be deleted.
   * @param startTimeMillis The start time in milliseconds from when the aspect values need to be deleted.
   *                        If this is null, the deletion starts from the oldest value.
   * @param endTimeMillis The end time in milliseconds up to when the aspect values need to be deleted.
   *                      If this is null, the deletion will go till the most recent value.
   * @param aspectsToDelete - The list of aspect names whose values need to be deleted.
   * @return The total number of documents deleted.
   */
  private Long deleteTimeseriesAspects(@Nonnull Urn urn, @Nullable Long startTimeMillis, @Nullable Long endTimeMillis,
      @Nonnull List<String> aspectsToDelete) {
    long totalNumberOfDocsDeleted = 0;
    // Construct the filter.
    List<Criterion> criteria = new ArrayList<>();
    criteria.add(QueryUtils.newCriterion("urn", urn.toString()));
    if (startTimeMillis != null) {
      criteria.add(
          QueryUtils.newCriterion(ES_FILED_TIMESTAMP, startTimeMillis.toString(), Condition.GREATER_THAN_OR_EQUAL_TO));
    }
    if (endTimeMillis != null) {
      criteria.add(
          QueryUtils.newCriterion(ES_FILED_TIMESTAMP, endTimeMillis.toString(), Condition.LESS_THAN_OR_EQUAL_TO));
    }
    final Filter filter = QueryUtils.getFilterFromCriteria(criteria);

    // Delete all the timeseries aspects by the filter.
    final String entityType = urn.getEntityType();
    for (final String aspect : aspectsToDelete) {
      DeleteAspectValuesResult result = _timeseriesAspectService.deleteAspectValues(entityType, aspect, filter);
      totalNumberOfDocsDeleted += result.getNumDocsDeleted();

      log.debug("Number of timeseries docs deleted for entity:{}, aspect:{}, urn:{}, startTime:{}, endTime:{}={}",
          entityType, aspect, urn, startTimeMillis, endTimeMillis, result.getNumDocsDeleted());
    }
    return totalNumberOfDocsDeleted;
  }

  @Action(name = "deleteReferences")
  @Nonnull
  @WithSpan
  public Task<DeleteReferencesResponse> deleteReferencesTo(@ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam("dryRun") @Optional Boolean dry) throws URISyntaxException {
    boolean dryRun = dry != null ? dry : false;

    Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> _deleteEntityService.deleteReferencesTo(urn, dryRun),
        MetricRegistry.name(this.getClass(), "deleteReferences"));
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

  @Action(name = ACTION_FILTER)
  @Nonnull
  @WithSpan
  public Task<SearchResult> filter(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_FILTER) Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    log.info("FILTER RESULTS for {} with filter {}", entityName, filter);
    return RestliUtil.toTask(
        () -> validateSearchResult(_entitySearchService.filter(entityName, filter, sortCriterion, start, count),
            _entityService), MetricRegistry.name(this.getClass(), "search"));
  }

  @Action(name = ACTION_EXISTS)
  @Nonnull
  @WithSpan
  public Task<Boolean> exists(@ActionParam(PARAM_URN) @Nonnull String urnStr) throws URISyntaxException {
    log.info("EXISTS for {}", urnStr);
    Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> _entityService.exists(urn), MetricRegistry.name(this.getClass(), "exists"));
  }
}
