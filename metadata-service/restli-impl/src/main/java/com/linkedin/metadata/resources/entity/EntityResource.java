package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.resources.ResourceUtils.validateOrThrow;
import static com.linkedin.metadata.resources.ResourceUtils.validateOrWarn;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_AUTOCOMPLETE;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_BROWSE;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_GET_BROWSE_PATHS;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_INGEST;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_ASPECTS;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_FIELD;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_FILTER;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_INPUT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_LIMIT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_PATH;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_QUERY;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_SORT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_START;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_URN;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiCollection(name = "entities", namespace = "com.linkedin.entity")
public class EntityResource extends CollectionResourceTaskTemplate<String, Entity> {

  private static final String ACTION_SEARCH = "search";
  private static final String ACTION_BATCH_INGEST = "batchIngest";
  private static final String ACTION_LIST_URNS = "listUrns";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ENTITIES = "entities";
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_VALUE = "value";
  private static final String SYSTEM_METADATA = "systemMetadata";

  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<Entity> get(@Nonnull String urnStr, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames)
      throws URISyntaxException {
    log.info("GET {}", urnStr);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtil.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
      final Entity entity = _entityService.getEntity(urn, projectedAspects);
      if (entity == null) {
        throw RestliUtil.resourceNotFoundException();
      } else {
        validateOrWarn(entity);
      }
      return entity;
    }, MetricRegistry.name(this.getClass(), "get"));
  }

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<String, Entity>> batchGet(@Nonnull Set<String> urnStrs,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    log.info("BATCH GET {}", urnStrs.toString());
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
          .peek(entry -> validateOrWarn(entry.getValue()))
          .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
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
    validateOrThrow(entity, HttpStatus.S_422_UNPROCESSABLE_ENTITY);

    Timer.Context preIngestTimer = MetricUtils.timer(this.getClass(), "preIngest").time();
    SystemMetadata systemMetadata = populateDefaultFieldsIfEmpty(providedSystemMetadata);

    final Set<String> projectedAspects = new HashSet<>(Arrays.asList("browsePaths"));
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(entity.getValue());
    final Urn urn = com.linkedin.metadata.dao.utils.ModelUtils.getUrnFromSnapshot(snapshotRecord);

    log.info("INGEST urn {} with system metadata {}", urn.toString(), systemMetadata.toString());

    final Entity browsePathEntity = _entityService.getEntity(urn, projectedAspects);
    BrowsePathUtils.addBrowsePathIfNotExists(entity.getValue(), browsePathEntity);

    // TODO Correctly audit ingestions.
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(Constants.UNKNOWN_ACTOR));

    preIngestTimer.stop();

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
      validateOrThrow(entity, HttpStatus.S_422_UNPROCESSABLE_ENTITY);
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
    return RestliUtil.toTask(() -> _searchService.search(entityName, input, filter, sortCriterion, start, count),
        MetricRegistry.name(this.getClass(), "search"));
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Nonnull
  @WithSpan
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_QUERY) @Nonnull String query, @ActionParam(PARAM_FIELD) @Optional @Nullable String field,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_LIMIT) int limit) {

    return RestliUtil.toTask(() -> _searchService.autoComplete(entityName, query, field, filter, limit),
        MetricRegistry.name(this.getClass(), "autocomplete"));
  }

  @Action(name = ACTION_BROWSE)
  @Nonnull
  @WithSpan
  public Task<BrowseResult> browse(@ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_PATH) @Nonnull String path, @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_START) int start, @ActionParam(PARAM_LIMIT) int limit) {

    log.info("GET BROWSE RESULTS for {} at path {}", entityName, path);
    return RestliUtil.toTask(() -> _searchService.browse(entityName, path, filter, start, limit),
        MetricRegistry.name(this.getClass(), "browse"));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Nonnull
  @WithSpan
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = PARAM_URN, typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    log.info("GET BROWSE PATHS for {}", urn.toString());
    return RestliUtil.toTask(() -> new StringArray(_searchService.getBrowsePaths(urnToEntityName(urn), urn)),
        MetricRegistry.name(this.getClass(), "getBrowsePaths"));
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
    return RestliUtil.toTask(() -> _searchService.docCount(entityName));
  }

  @Action(name = "batchGetTotalEntityCount")
  @Nonnull
  @WithSpan
  public Task<LongMap> batchGetTotalEntityCount(@ActionParam(PARAM_ENTITIES) @Nonnull String[] entityNames) {
    return RestliUtil.toTask(() -> new LongMap(
        Arrays.stream(entityNames).collect(Collectors.toMap(Function.identity(), _searchService::docCount))));
  }

  @Action(name = ACTION_LIST_URNS)
  @Nonnull
  @WithSpan
  public Task<ListUrnsResult> listUrns(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count
  ) throws URISyntaxException {
    log.info("LIST URNS for {} with start {} and count {}", entityName, start, count);
    return RestliUtil.toTask(() -> _entityService.listUrns(entityName, start, count), "listUrns");
  }
}
