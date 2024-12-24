package com.linkedin.metadata.resources.entity;

import static com.datahub.authorization.AuthUtil.*;
import static com.linkedin.metadata.authorization.ApiGroup.COUNTS;
import static com.linkedin.metadata.authorization.ApiGroup.LINEAGE;
import static com.linkedin.metadata.authorization.ApiGroup.TIMESERIES;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.EXISTS;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.entity.validation.ValidationApiUtils.validateOrThrow;
import static com.linkedin.metadata.entity.validation.ValidationUtils.*;
import static com.linkedin.metadata.resources.restli.RestliConstants.*;
import static com.linkedin.metadata.search.utils.SearchUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.validateAndConvert;
import static com.linkedin.metadata.utils.PegasusUtils.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.generateSystemMetadataIfEmpty;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;

import com.linkedin.metadata.resources.restli.RestliUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.services.RestrictedService;
import com.linkedin.data.template.SetMode;
import io.datahubproject.metadata.context.OperationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.authorization.PoliciesConfig;
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
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageScrollResult;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.ScrollResult;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.artifact.versioning.ComparableVersion;

/** Single unified resource for fetching, updating, searching, & browsing DataHub entities */
@Slf4j
@RestLiCollection(name = "entities", namespace = "com.linkedin.entity")
public class EntityResource extends CollectionResourceTaskTemplate<String, Entity> {

  private static final String ACTION_SEARCH = "search";
  private static final String ACTION_LIST = "list";
  private static final String ACTION_SEARCH_ACROSS_ENTITIES = "searchAcrossEntities";
  private static final String ACTION_SEARCH_ACROSS_LINEAGE = "searchAcrossLineage";
  private static final String ACTION_SCROLL_ACROSS_ENTITIES = "scrollAcrossEntities";
  private static final String ACTION_SCROLL_ACROSS_LINEAGE = "scrollAcrossLineage";
  private static final String ACTION_BATCH_INGEST = "batchIngest";
  private static final String ACTION_LIST_URNS = "listUrns";
  private static final String ACTION_APPLY_RETENTION = "applyRetention";
  private static final String ACTION_FILTER = "filter";
  private static final String ACTION_DELETE = "delete";
  private static final String ACTION_EXISTS = "exists";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ENTITIES = "entities";
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_FULLTEXT = "fulltext";
  private static final String PARAM_VALUE = "value";
  private static final String PARAM_ASPECT_NAME = "aspectName";
  private static final String PARAM_START_TIME_MILLIS = "startTimeMillis";
  private static final String PARAM_END_TIME_MILLIS = "endTimeMillis";
  private static final String PARAM_URN = "urn";
  private static final String PARAM_INCLUDE_SOFT_DELETE = "includeSoftDelete";
  private static final String SYSTEM_METADATA = "systemMetadata";
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final Integer ELASTIC_MAX_PAGE_SIZE = 10000;
  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService<?> entityService;

  @Inject
  @Named("searchService")
  private SearchService searchService;

  @Inject
  @Named("entitySearchService")
  private EntitySearchService entitySearchService;

  @Inject
  @Named("systemMetadataService")
  private SystemMetadataService systemMetadataService;

  @Inject
  @Named("relationshipSearchService")
  private LineageSearchService lineageSearchService;

  @Inject
  @Named("kafkaEventProducer")
  private EventProducer eventProducer;

  @Inject
  @Named("graphService")
  private GraphService graphService;

  @Inject
  @Named("deleteEntityService")
  private DeleteEntityService deleteEntityService;

  @Inject
  @Named("timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @Inject
  @Named("authorizerChain")
  private Authorizer authorizer;

  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  @Inject
  @Named("restrictedService")
  private RestrictedService restrictedService;

  /** Retrieves the value for an entity that is made up of latest versions of specified aspects. */
  @RestMethod.Get
  @Nonnull
  @WithSpan
  public Task<AnyRecord> get(
      @Nonnull String urnStr, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames)
      throws URISyntaxException {
    log.info("GET {}", urnStr);
    final Urn urn = Urn.createFromString(urnStr);

    Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "restrictedService", urn.getEntityType()), authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            READ,
            List.of(urn))) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity " + urn);
    }

    return RestliUtils.toTask(
        () -> {
          final Set<String> projectedAspects =
              aspectNames == null
                  ? Collections.emptySet()
                  : new HashSet<>(Arrays.asList(aspectNames));
          final Entity entity = entityService.getEntity(opContext, urn, projectedAspects, true);
          if (entity == null) {
            throw RestliUtils.resourceNotFoundException(String.format("Did not find %s", urnStr));
          }
          return new AnyRecord(entity.data());
        },
        MetricRegistry.name(this.getClass(), "get"));
  }

  @RestMethod.BatchGet
  @Nonnull
  @WithSpan
  public Task<Map<String, AnyRecord>> batchGet(
      @Nonnull Set<String> urnStrs,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames)
      throws URISyntaxException {
    log.info("BATCH GET {}", urnStrs);
    final Set<Urn> urns = new HashSet<>();
    for (final String urnStr : urnStrs) {
      urns.add(Urn.createFromString(urnStr));
    }

    Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "batchGet", urnStrs), authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            READ,
            urns)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entities: " + urnStrs);
    }

    return RestliUtils.toTask(
        () -> {
          final Set<String> projectedAspects =
              aspectNames == null
                  ? Collections.emptySet()
                  : new HashSet<>(Arrays.asList(aspectNames));
          return entityService.getEntities(opContext, urns, projectedAspects, true).entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> entry.getKey().toString(),
                      entry -> new AnyRecord(entry.getValue().data())));
        },
        MetricRegistry.name(this.getClass(), "batchGet"));
  }

  @Action(name = ACTION_INGEST)
  @Nonnull
  @WithSpan
  public Task<Void> ingest(
      @ActionParam(PARAM_ENTITY) @Nonnull Entity entity,
      @ActionParam(SYSTEM_METADATA) @Optional @Nullable SystemMetadata providedSystemMetadata)
      throws URISyntaxException {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    final Urn urn = com.datahub.util.ModelUtils.getUrnFromSnapshotUnion(entity.getValue());
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(actorUrnStr, getContext(),
                    ACTION_INGEST, urn.getEntityType()), authorizer, authentication, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            CREATE,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User " + actorUrnStr + " is unauthorized to edit entity " + urn);
    }

    try {
      validateOrThrow(entity);
    } catch (ValidationException e) {
      throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, e);
    }

    SystemMetadata systemMetadata = generateSystemMetadataIfEmpty(providedSystemMetadata);

    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    // variables referenced in lambdas are required to be final
    final SystemMetadata finalSystemMetadata = systemMetadata;
    return RestliUtils.toTask(
        () -> {
          entityService.ingestEntity(opContext, entity, auditStamp, finalSystemMetadata);
          return null;
        },
        MetricRegistry.name(this.getClass(), "ingest"));
  }

  @Action(name = ACTION_BATCH_INGEST)
  @Nonnull
  @WithSpan
  public Task<Void> batchIngest(
      @ActionParam(PARAM_ENTITIES) @Nonnull Entity[] entities,
      @ActionParam(SYSTEM_METADATA) @Optional @Nullable SystemMetadata[] systemMetadataList)
      throws URISyntaxException {

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    List<Urn> urns = Arrays.stream(entities)
            .map(Entity::getValue)
            .map(com.datahub.util.ModelUtils::getUrnFromSnapshotUnion).collect(Collectors.toList());
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(actorUrnStr,
                    getContext(), ACTION_BATCH_INGEST, urns.stream().map(Urn::getEntityType).collect(Collectors.toList())),
            authorizer, authentication, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            CREATE, urns)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User " + actorUrnStr +  " is unauthorized to edit entities.");
    }

    for (Entity entity : entities) {
      try {
        validateOrThrow(entity);
      } catch (ValidationException e) {
        throw new RestLiServiceException(HttpStatus.S_422_UNPROCESSABLE_ENTITY, e);
      }
    }

    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(actorUrnStr));

    if (systemMetadataList == null) {
      systemMetadataList = new SystemMetadata[entities.length];
    }

    if (entities.length != systemMetadataList.length) {
      throw RestliUtils.invalidArgumentsException("entities and systemMetadata length must match");
    }

    final List<SystemMetadata> finalSystemMetadataList =
        Arrays.stream(systemMetadataList)
            .map(SystemMetadataUtils::generateSystemMetadataIfEmpty)
            .collect(Collectors.toList());

    return RestliUtils.toTask(
        () -> {
          entityService.ingestEntities(opContext,
              Arrays.asList(entities), auditStamp, finalSystemMetadataList);
          return null;
        },
        MetricRegistry.name(this.getClass(), "batchIngest"));
  }

  @Action(name = ACTION_SEARCH)
  @Nonnull
  @WithSpan
  public Task<SearchResult> search(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_INPUT) @Nonnull String input,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count,
      @Optional @Deprecated @Nullable @ActionParam(PARAM_FULLTEXT) Boolean fulltext,
      @Optional @Nullable @ActionParam(PARAM_SEARCH_FLAGS) SearchFlags searchFlags) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(systemOperationContext,
                    RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(), ACTION_SEARCH, entityName), authorizer, auth, true)
            .withSearchFlags(flags -> searchFlags != null ? searchFlags : new SearchFlags().setFulltext(Boolean.TRUE.equals(fulltext)));


    if (!AuthUtil.isAPIAuthorizedEntityType(
            opContext,
             READ,
            entityName)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);

    log.info("GET SEARCH RESULTS for {} with query {}", entityName, input);
    // TODO - change it to use _searchService once we are confident on it's latency
    return RestliUtils.toTask(
        () -> {
          final SearchResult result;
          // This API is not used by the frontend for search bars so we default to structured
          result =
              entitySearchService.search(opContext,
                  List.of(entityName), input, validateAndConvert(filter), sortCriterionList, start, count);

          if (!isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized get entity.");
          }

          return validateSearchResult(opContext, result, entityService);
        },
        MetricRegistry.name(this.getClass(), "search"));
  }

  @Action(name = ACTION_SEARCH_ACROSS_ENTITIES)
  @Nonnull
  @WithSpan
  public Task<SearchResult> searchAcrossEntities(
      @ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Nonnull String input,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count,
      @ActionParam(PARAM_SEARCH_FLAGS) @Optional SearchFlags searchFlags) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_SEARCH_ACROSS_ENTITIES, entities), authorizer, auth, true)
            .withSearchFlags(flags -> searchFlags != null ? searchFlags : new SearchFlags().setFulltext(true));

    List<String> entityList = searchService.getEntitiesToSearch(opContext, entities == null ? Collections.emptyList() : Arrays.asList(entities), count);
    if (!isAPIAuthorizedEntityType(
            opContext,
            READ,
            entityList)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);

    log.info("GET SEARCH RESULTS ACROSS ENTITIES for {} with query {}", entityList, input);
    return RestliUtils.toTask(
        () -> {
          SearchResult result = searchService.searchAcrossEntities(opContext, entityList, input, validateAndConvert(filter), sortCriterionList, start, count);
          if (!isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized get entity.");
          }

          return validateSearchResult(opContext, result, entityService);
        });
  }

  private List<SortCriterion> getSortCriteria(@Nullable SortCriterion[] sortCriteria, @Nullable SortCriterion sortCriterion) {
    List<SortCriterion> sortCriterionList;
    if (sortCriteria != null) {
      sortCriterionList = Arrays.asList(sortCriteria);
    } else if (sortCriterion != null) {
      sortCriterionList = Collections.singletonList(sortCriterion);
    } else {
      sortCriterionList = Collections.emptyList();
    }
    return sortCriterionList;
  }

  @Action(name = ACTION_SCROLL_ACROSS_ENTITIES)
  @Nonnull
  @WithSpan
  public Task<ScrollResult> scrollAcrossEntities(
      @ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Nonnull String input,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_SCROLL_ID) @Optional @Nullable String scrollId,
      @ActionParam(PARAM_KEEP_ALIVE) String keepAlive,
      @ActionParam(PARAM_COUNT) int count,
      @ActionParam(PARAM_SEARCH_FLAGS) @Optional SearchFlags searchFlags) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_SCROLL_ACROSS_ENTITIES, entities), authorizer, auth, true)
            .withSearchFlags(flags -> searchFlags != null ? searchFlags : new SearchFlags().setFulltext(true));

    List<String> entityList = searchService.getEntitiesToSearch(opContext, entities == null ? Collections.emptyList() : Arrays.asList(entities), count);
    if (!isAPIAuthorizedEntityType(
            opContext,
            READ, entityList)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);

    log.info(
        "GET SCROLL RESULTS ACROSS ENTITIES for {} with query {} and scroll ID: {}",
        entityList,
        input,
        scrollId);

    return RestliUtils.toTask(
        () -> {
          ScrollResult result = searchService.scrollAcrossEntities(
                  opContext,
                  entityList,
                  input,
                  validateAndConvert(filter),
                  sortCriterionList,
                  scrollId,
                  keepAlive,
                  count);
          if (!isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized get entity.");
          }

          return validateScrollResult(opContext, result, entityService);
        },
        "scrollAcrossEntities");
  }

  @Action(name = ACTION_SEARCH_ACROSS_LINEAGE)
  @Nonnull
  @WithSpan
  public Task<LineageSearchResult> searchAcrossLineage(
      @ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_DIRECTION) String direction,
      @ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Optional @Nullable String input,
      @ActionParam(PARAM_MAX_HOPS) @Optional @Nullable Integer maxHops,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count,
      @ActionParam(PARAM_START_TIME_MILLIS) @Optional @Nullable Long startTimeMillis,
      @ActionParam(PARAM_END_TIME_MILLIS) @Optional @Nullable Long endTimeMillis,
      @Optional @Nullable @ActionParam(PARAM_SEARCH_FLAGS) SearchFlags searchFlags)
      throws URISyntaxException {
    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_SEARCH_ACROSS_LINEAGE, entities), authorizer, auth, true)
            .withSearchFlags(flags -> (searchFlags != null ? searchFlags : new SearchFlags().setFulltext(true))
                    .setIncludeRestricted(true))
            .withLineageFlags(flags -> flags.setStartTimeMillis(startTimeMillis, SetMode.REMOVE_IF_NULL)
                    .setEndTimeMillis(endTimeMillis, SetMode.REMOVE_IF_NULL));

    if (!isAPIAuthorized(
            opContext,
            LINEAGE, READ)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);

    Urn urn = Urn.createFromString(urnStr);
    List<String> entityList = entities == null ? Collections.emptyList() : Arrays.asList(entities);
    log.info(
        "GET SEARCH RESULTS ACROSS RELATIONSHIPS for source urn {}, direction {}, entities {} with query {}",
        urnStr,
        direction,
        entityList,
        input);
    return RestliUtils.toTask(
        () -> validateLineageSearchResult(opContext, lineageSearchService.searchAcrossLineage(
                  opContext,
                  urn,
                  LineageDirection.valueOf(direction),
                  entityList,
                  input,
                  maxHops,
                  validateAndConvert(filter),
                  sortCriterionList,
                  start,
                  count),
            entityService),
        "searchAcrossRelationships");
  }

  @Action(name = ACTION_SCROLL_ACROSS_LINEAGE)
  @Nonnull
  @WithSpan
  public Task<LineageScrollResult> scrollAcrossLineage(
      @ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_DIRECTION) String direction,
      @ActionParam(PARAM_ENTITIES) @Optional @Nullable String[] entities,
      @ActionParam(PARAM_INPUT) @Optional @Nullable String input,
      @ActionParam(PARAM_MAX_HOPS) @Optional @Nullable Integer maxHops,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_SCROLL_ID) @Optional @Nullable String scrollId,
      @ActionParam(PARAM_KEEP_ALIVE) String keepAlive,
      @ActionParam(PARAM_COUNT) int count,
      @ActionParam(PARAM_START_TIME_MILLIS) @Optional @Nullable Long startTimeMillis,
      @ActionParam(PARAM_END_TIME_MILLIS) @Optional @Nullable Long endTimeMillis,
      @ActionParam(PARAM_SEARCH_FLAGS) @Optional @Nullable SearchFlags searchFlags)
      throws URISyntaxException {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(), ACTION_SCROLL_ACROSS_LINEAGE, entities),
                    authorizer, auth, true)
            .withSearchFlags(flags -> (searchFlags != null ? searchFlags : new SearchFlags().setSkipCache(true))
                    .setIncludeRestricted(true))
            .withLineageFlags(flags -> flags.setStartTimeMillis(startTimeMillis, SetMode.REMOVE_IF_NULL)
                    .setEndTimeMillis(endTimeMillis, SetMode.REMOVE_IF_NULL));

    if (!isAPIAuthorized(
            opContext,
            LINEAGE, READ)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    Urn urn = Urn.createFromString(urnStr);
    List<String> entityList = entities == null ? Collections.emptyList() : Arrays.asList(entities);
    log.info(
        "GET SCROLL RESULTS ACROSS RELATIONSHIPS for source urn {}, direction {}, entities {} with query {}",
        urnStr,
        direction,
        entityList,
        input);

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);

    return RestliUtils.toTask(
        () ->
            validateLineageScrollResult(opContext,
                lineageSearchService.scrollAcrossLineage(
                        opContext,
                    urn,
                    LineageDirection.valueOf(direction),
                    entityList,
                    input,
                    maxHops,
                    validateAndConvert(filter),
                    sortCriterionList,
                    scrollId,
                    keepAlive,
                    count),
                entityService),
        "scrollAcrossLineage");
  }

  @Action(name = ACTION_LIST)
  @Nonnull
  @WithSpan
  public Task<ListResult> list(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_LIST, entityName), authorizer, auth, true)
            .withSearchFlags(flags -> new SearchFlags().setFulltext(false));

    if (!AuthUtil.isAPIAuthorizedEntityType(
            opContext,
            READ, entityName)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);

    final Filter finalFilter = validateAndConvert(filter);
    log.info("GET LIST RESULTS for {} with filter {}", entityName, finalFilter);
    return RestliUtils.toTask(
        () -> {
            SearchResult result = entitySearchService.filter(opContext, entityName, finalFilter, sortCriterionList, start, count);
          if (!AuthUtil.isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized get entity.");
          }
            return validateListResult(opContext,
                toListResult(result), entityService);
          },
        MetricRegistry.name(this.getClass(), "filter"));
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Nonnull
  @WithSpan
  public Task<AutoCompleteResult> autocomplete(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Optional @Nullable String field,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit,
      @ActionParam(PARAM_SEARCH_FLAGS) @Optional @Nullable SearchFlags searchFlags) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_AUTOCOMPLETE, entityName), authorizer, auth, true)
            .withSearchFlags(flags -> searchFlags != null ? searchFlags : flags);

    if (!AuthUtil.isAPIAuthorizedEntityType(
            opContext,
             READ, entityName)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    return RestliUtils.toTask(
        () -> {
          AutoCompleteResult result = entitySearchService.autoComplete(opContext, entityName, query, field, filter, limit);
          if (!isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized get entity.");
          }
          return result; },
        MetricRegistry.name(this.getClass(), "autocomplete"));
  }

  @Action(name = ACTION_BROWSE)
  @Nonnull
  @WithSpan
  public Task<BrowseResult> browse(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit,
      @ActionParam(PARAM_SEARCH_FLAGS) @Optional @Nullable SearchFlags searchFlags) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
                    systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                            ACTION_BROWSE, entityName), authorizer, auth, true)
            .withSearchFlags(flags -> searchFlags != null ? searchFlags : flags);

    if (!AuthUtil.isAPIAuthorizedEntityType(
            opContext,
             READ, entityName)) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    log.info("GET BROWSE RESULTS for {} at path {}", entityName, path);
    return RestliUtils.toTask(
        () -> {
          BrowseResult result = entitySearchService.browse(opContext, entityName, path, filter, start, limit);
          if (!isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized get entity.");
          }
          return validateBrowseResult(opContext,
                result,
                  entityService);
            },
        MetricRegistry.name(this.getClass(), "browse"));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Nonnull
  @WithSpan
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = PARAM_URN, typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_GET_BROWSE_PATHS, urn.getEntityType()), authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
             READ,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity: " + urn);
    }
    log.info("GET BROWSE PATHS for {}", urn);

    return RestliUtils.toTask(
        () -> new StringArray(entitySearchService.getBrowsePaths(opContext, urnToEntityName(urn), urn)),
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
  public Task<RollbackResponse> deleteEntities(
      @ActionParam("registryId") @Optional String registryId,
      @ActionParam("dryRun") @Optional Boolean dryRun) {
    String registryName = null;
    ComparableVersion registryVersion = new ComparableVersion("0.0.0-dev");

    if (registryId != null) {
      try {
        registryName = registryId.split(":")[0];
        registryVersion = new ComparableVersion(registryId.split(":")[1]);
      } catch (Exception e) {
        throw new RestLiServiceException(
            HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Failed to parse registry id: " + registryId,
            e);
      }
    }
    String finalRegistryName = registryName;
    ComparableVersion finalRegistryVersion = registryVersion;
    String finalRegistryName1 = registryName;
    ComparableVersion finalRegistryVersion1 = registryVersion;
    return RestliUtils.toTask(
        () -> {
          RollbackResponse response = new RollbackResponse();
          List<AspectRowSummary> aspectRowsToDelete =
              systemMetadataService.findByRegistry(
                  finalRegistryName,
                  finalRegistryVersion.toString(),
                  false,
                  0,
                  ESUtils.MAX_RESULT_SIZE);
          log.info("found {} rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
          response.setAspectsAffected(aspectRowsToDelete.size());
          Set<String> urns =
              aspectRowsToDelete.stream()
                  .collect(Collectors.groupingBy(AspectRowSummary::getUrn))
                  .keySet();

          final Authentication auth = AuthenticationContext.getAuthentication();
          OperationContext opContext = OperationContext.asSession(
                  systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                          "deleteAll", urns), authorizer, auth, true);

          if (!isAPIAuthorizedEntityUrns(
                  opContext,
                  DELETE,
                  urns.stream().map(UrnUtils::getUrn).collect(Collectors.toSet()))) {
            throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to delete entities.");
          }

          response.setEntitiesAffected(urns.size());
          response.setEntitiesDeleted(
              aspectRowsToDelete.stream().filter(AspectRowSummary::isKeyAspect).count());
          response.setAspectRowSummaries(
              new AspectRowSummaryArray(
                  aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size()))));
          if ((dryRun == null) || (!dryRun)) {
            Map<String, String> conditions = new HashMap();
            conditions.put("registryName", finalRegistryName1);
            conditions.put("registryVersion", finalRegistryVersion1.toString());
            entityService.rollbackWithConditions(opContext, aspectRowsToDelete, conditions, false);
          }
          return response;
        },
        MetricRegistry.name(this.getClass(), "deleteAll"));
  }

  /**
   * Deletes all data related to an individual urn(entity).
   *
   * @param urnStr - the urn of the entity.
   * @param aspectName - the optional aspect name if only want to delete the aspect (applicable only
   *     for timeseries aspects).
   * @param startTimeMills - the optional start time (applicable only for timeseries aspects).
   * @param endTimeMillis - the optional end time (applicable only for the timeseries aspects).
   * @return - a DeleteEntityResponse object.
   * @throws URISyntaxException
   */
  @Action(name = ACTION_DELETE)
  @Nonnull
  @WithSpan
  public Task<DeleteEntityResponse> deleteEntity(
      @ActionParam(PARAM_URN) @Nonnull String urnStr,
      @ActionParam(PARAM_ASPECT_NAME) @Optional String aspectName,
      @ActionParam(PARAM_START_TIME_MILLIS) @Optional Long startTimeMills,
      @ActionParam(PARAM_END_TIME_MILLIS) @Optional Long endTimeMillis)
      throws URISyntaxException {
    Urn urn = Urn.createFromString(urnStr);

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_DELETE, urn.getEntityType()), authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            DELETE,
            List.of(urn))) {
      throw new RestLiServiceException(
              HttpStatus.S_403_FORBIDDEN, "User is unauthorized to delete entity: " + urnStr);
    }

    return RestliUtils.toTask(
        () -> {
          // Find the timeseries aspects to delete. If aspectName is null, delete all.
          List<String> timeseriesAspectNames =
              EntitySpecUtils.getEntityTimeseriesAspectNames(
                  opContext.getEntityRegistry(), urn.getEntityType());
          if (aspectName != null && !timeseriesAspectNames.contains(aspectName)) {
            throw new UnsupportedOperationException(
                String.format("Not supported for non-timeseries aspect %s.", aspectName));
          }
          List<String> timeseriesAspectsToDelete =
              (aspectName == null) ? timeseriesAspectNames : ImmutableList.of(aspectName);

          DeleteEntityResponse response = new DeleteEntityResponse();
          if (aspectName == null) {
            RollbackRunResult result = entityService.deleteUrn(opContext, urn);
            response.setRows(result.getRowsDeletedFromEntityDeletion());
          }
          Long numTimeseriesDocsDeleted =
              deleteTimeseriesAspects(
                  urn, startTimeMills, endTimeMillis, timeseriesAspectsToDelete);
          log.info("Total number of timeseries aspect docs deleted: {}", numTimeseriesDocsDeleted);

          response.setUrn(urnStr);
          response.setTimeseriesRows(numTimeseriesDocsDeleted);

          return response;
        },
        MetricRegistry.name(this.getClass(), "delete"));
  }

  /**
   * Deletes the set of timeseries aspect values for the specified aspects that are associated with
   * the given entity urn between startTimeMillis and endTimeMillis.
   *
   * @param urn The entity urn whose timeseries aspect values need to be deleted.
   * @param startTimeMillis The start time in milliseconds from when the aspect values need to be
   *     deleted. If this is null, the deletion starts from the oldest value.
   * @param endTimeMillis The end time in milliseconds up to when the aspect values need to be
   *     deleted. If this is null, the deletion will go till the most recent value.
   * @param aspectsToDelete - The list of aspect names whose values need to be deleted.
   * @return The total number of documents deleted.
   */
  private Long deleteTimeseriesAspects(
      @Nonnull Urn urn,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nonnull List<String> aspectsToDelete) {
    long totalNumberOfDocsDeleted = 0;

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "deleteTimeseriesAspects", urn.getEntityType()), authorizer, auth, true);

    if (!isAPIAuthorizedUrns(
            opContext,
            TIMESERIES, DELETE,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to delete entity " + urn);
    }

    // Construct the filter.
    List<Criterion> criteria = new ArrayList<>();
    criteria.add(CriterionUtils.buildCriterion("urn", Condition.EQUAL, urn.toString()));
    if (startTimeMillis != null) {
      criteria.add(
              CriterionUtils.buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTimeMillis.toString()));
    }
    if (endTimeMillis != null) {
      criteria.add(
              CriterionUtils.buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.LESS_THAN_OR_EQUAL_TO, endTimeMillis.toString()));
    }
    final Filter filter = QueryUtils.getFilterFromCriteria(criteria);

    // Delete all the timeseries aspects by the filter.
    final String entityType = urn.getEntityType();
    for (final String aspect : aspectsToDelete) {
      DeleteAspectValuesResult result =
          timeseriesAspectService.deleteAspectValues(opContext, entityType, aspect, filter);
      totalNumberOfDocsDeleted += result.getNumDocsDeleted();

      log.debug(
          "Number of timeseries docs deleted for entity:{}, aspect:{}, urn:{}, startTime:{}, endTime:{}={}",
          entityType,
          aspect,
          urn,
          startTimeMillis,
          endTimeMillis,
          result.getNumDocsDeleted());
    }
    return totalNumberOfDocsDeleted;
  }

  @Action(name = "deleteReferences")
  @Nonnull
  @WithSpan
  public Task<DeleteReferencesResponse> deleteReferencesTo(
      @ActionParam(PARAM_URN) @Nonnull String urnStr, @ActionParam("dryRun") @Optional Boolean dry)
      throws URISyntaxException {
    boolean dryRun = dry != null ? dry : false;

    Urn urn = Urn.createFromString(urnStr);

    final Authentication auth = AuthenticationContext.getAuthentication();
    final OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "deleteReferences", urn.getEntityType()), authorizer, auth, true);

    if (!isAPIAuthorizedEntityUrns(
            opContext,
            DELETE,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to delete entity " + urnStr);
    }

    return RestliUtils.toTask(
        () -> deleteEntityService.deleteReferencesTo(opContext, urn, dryRun),
        MetricRegistry.name(this.getClass(), "deleteReferences"));
  }

  /*
  Used to enable writes in GMS after data migration is complete
   */
  @Action(name = "setWritable")
  @Nonnull
  @WithSpan
  public Task<Void> setWriteable(
      @ActionParam(PARAM_VALUE) @Optional("true") @Nonnull Boolean value) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "setWriteable"), authorizer, auth, true);

    if (!isAPIOperationsAuthorized(
            opContext,
            PoliciesConfig.SET_WRITEABLE_PRIVILEGE)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to enable and disable write mode.");
    }
    log.info("setting entity resource to be writable");
    return RestliUtils.toTask(
        () -> {
          entityService.setWritable(value);
          return null;
        });
  }

  @Action(name = "getTotalEntityCount")
  @Nonnull
  @WithSpan
  public Task<Long> getTotalEntityCount(@ActionParam(PARAM_ENTITY) @Nonnull String entityName) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "getTotalEntityCount", entityName), authorizer, auth, true);

    if (!isAPIAuthorized(
            opContext,
            COUNTS, READ)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity counts.");
    }

    return RestliUtils.toTask(() -> entitySearchService.docCount(opContext, entityName));
  }

  @Action(name = "batchGetTotalEntityCount")
  @Nonnull
  @WithSpan
  public Task<LongMap> batchGetTotalEntityCount(
      @ActionParam(PARAM_ENTITIES) @Nonnull String[] entityNames) {
    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    "batchGetTotalEntityCount", entityNames), authorizer, auth, true);

    if (!isAPIAuthorized(
            opContext,
            COUNTS, READ)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity counts.");
    }

    return RestliUtils.toTask(
        () -> new LongMap(searchService.docCountPerEntity(opContext, Arrays.asList(entityNames))));
  }

  @Action(name = ACTION_LIST_URNS)
  @Nonnull
  @WithSpan
  public Task<ListUrnsResult> listUrns(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count)
      throws URISyntaxException {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_LIST_URNS, entityName), authorizer, auth, true);

    if (!AuthUtil.isAPIAuthorizedEntityType(
            opContext,
            READ, entityName)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    log.info("LIST URNS for {} with start {} and count {}", entityName, start, count);
    return RestliUtils.toTask(() -> {
      ListUrnsResult result = entityService.listUrns(opContext, entityName, start, count);
      if (!isAPIAuthorizedEntityUrns(
              opContext,
              READ, result.getEntities())) {
        throw new RestLiServiceException(
                HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity counts.");
      }
      return result;
      }, "listUrns");
  }

  @Action(name = ACTION_APPLY_RETENTION)
  @Nonnull
  @WithSpan
  public Task<String> applyRetention(
      @ActionParam(PARAM_START) @Optional @Nullable Integer start,
      @ActionParam(PARAM_COUNT) @Optional @Nullable Integer count,
      @ActionParam("attemptWithVersion") @Optional @Nullable Integer attemptWithVersion,
      @ActionParam(PARAM_ASPECT_NAME) @Optional @Nullable String aspectName,
      @ActionParam(PARAM_URN) @Optional @Nullable String urn) {

    EntitySpec resourceSpec = null;
    if (StringUtils.isNotBlank(urn)) {
      Urn resource = UrnUtils.getUrn(urn);
      resourceSpec = new EntitySpec(resource.getEntityType(), resource.toString());
    }

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_APPLY_RETENTION, resourceSpec.getType()), authorizer, auth, true);

    if (!isAPIOperationsAuthorized(
            opContext,
            PoliciesConfig.APPLY_RETENTION_PRIVILEGE,
            resourceSpec)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to apply retention.");
    }

    return RestliUtils.toTask(
        () -> entityService.batchApplyRetention(opContext, start, count, attemptWithVersion, aspectName, urn),
        ACTION_APPLY_RETENTION);
  }

  @Action(name = ACTION_FILTER)
  @Nonnull
  @WithSpan
  public Task<SearchResult> filter(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_FILTER) Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_SORT_CRITERIA) @Optional @Nullable SortCriterion[] sortCriteria,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_FILTER, entityName), authorizer, auth, true);

    if (!AuthUtil.isAPIAuthorizedEntityType(
            opContext,
            READ, entityName)) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized to search.");
    }

    List<SortCriterion> sortCriterionList = getSortCriteria(sortCriteria, sortCriterion);
    log.info("FILTER RESULTS for {} with filter {}", entityName, filter);
    return RestliUtils.toTask(
        () -> {
          SearchResult result = entitySearchService.filter(opContext.withSearchFlags(flags -> flags.setFulltext(true)),
                  entityName, filter, sortCriterionList, start, count);
          if (!isAPIAuthorizedResult(
                  opContext,
                  result)) {
            throw new RestLiServiceException(
                    HttpStatus.S_403_FORBIDDEN, "User is unauthorized to get entity counts.");
          }
            return validateSearchResult(opContext,
                result,
                    entityService);},
        MetricRegistry.name(this.getClass(), "search"));
  }

  @Action(name = ACTION_EXISTS)
  @Nonnull
  @WithSpan
  public Task<Boolean> exists(@ActionParam(PARAM_URN) @Nonnull String urnStr, @ActionParam(PARAM_INCLUDE_SOFT_DELETE) @Nullable @Optional Boolean includeSoftDelete)
      throws URISyntaxException {
    Urn urn = UrnUtils.getUrn(urnStr);

    final Authentication auth = AuthenticationContext.getAuthentication();
    OperationContext opContext = OperationContext.asSession(
            systemOperationContext, RequestContext.builder().buildRestli(auth.getActor().toUrnStr(), getContext(),
                    ACTION_EXISTS, urn.getEntityType()), authorizer, auth, true);
    if (!isAPIAuthorizedEntityUrns(
            opContext,
            EXISTS,
            List.of(urn))) {
      throw new RestLiServiceException(
          HttpStatus.S_403_FORBIDDEN, "User is unauthorized check entity existence: " + urnStr);
    }

    log.info("EXISTS for {}", urnStr);
    final boolean includeRemoved = includeSoftDelete == null || includeSoftDelete;
    return RestliUtils.toTask(
        () -> entityService.exists(opContext, urn, includeRemoved), MetricRegistry.name(this.getClass(), "exists"));
  }
}
