package com.linkedin.metadata.resources.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.utils.BrowsePathUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.PegasusUtils.urnToEntityName;
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


/**
 * Single unified resource for fetching, updating, searching, & browsing DataHub entities
 */
@Slf4j
@RestLiCollection(name = "entities", namespace = "com.linkedin.entity")
public class EntityResource extends CollectionResourceTaskTemplate<String, Entity> {

  private static final String ACTION_SEARCH = "search";
  private static final String ACTION_BATCH_INGEST = "batchIngest";
  private static final String PARAM_ENTITY = "entity";
  private static final String PARAM_ENTITIES = "entities";
  private static final String PARAM_COUNT = "count";
  private static final String PARAM_VALUE = "value";

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
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
  public Task<Entity> get(@Nonnull String urnStr, @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames)
      throws URISyntaxException {
    log.info("GET {}", urnStr);
    final Urn urn = Urn.createFromString(urnStr);
    return RestliUtils.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
      final Entity entity = _entityService.getEntity(urn, projectedAspects);
      if (entity == null) {
        throw RestliUtils.resourceNotFoundException();
      }
      return entity;
    });
  }

  @RestMethod.BatchGet
  @Nonnull
  public Task<Map<String, Entity>> batchGet(
      @Nonnull Set<String> urnStrs,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) throws URISyntaxException {
    log.info("BATCH GET {}", urnStrs.toString());
    final Set<Urn> urns = new HashSet<>();
    for (final String urnStr : urnStrs) {
      urns.add(Urn.createFromString(urnStr));
    }
    return RestliUtils.toTask(() -> {
      final Set<String> projectedAspects =
          aspectNames == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(aspectNames));
      return _entityService.getEntities(urns, projectedAspects)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(entry -> entry.getKey().toString(), Map.Entry::getValue));
    });
  }

  @Action(name = ACTION_INGEST)
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_ENTITY) @Nonnull Entity entity) throws URISyntaxException {
    final Set<String> projectedAspects = new HashSet<>(Arrays.asList("browsePaths"));
    final RecordTemplate snapshotRecord = RecordUtils.getSelectedRecordTemplateFromUnion(entity.getValue());
    final Urn urn = com.linkedin.metadata.dao.utils.ModelUtils.getUrnFromSnapshot(snapshotRecord);

    log.info("INGEST urn {}", urn.toString());

    final Entity browsePathEntity = _entityService.getEntity(urn, projectedAspects);
    BrowsePathUtils.addBrowsePathIfNotExists(entity.getValue(), browsePathEntity);

    // TODO Correctly audit ingestions.
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));
    return RestliUtils.toTask(() -> {
      _entityService.ingestEntity(entity, auditStamp);
      return null;
    });
  }

  @Action(name = ACTION_BATCH_INGEST)
  @Nonnull
  public Task<Void> batchIngest(@ActionParam(PARAM_ENTITIES) @Nonnull Entity[] entities) throws URISyntaxException {
    final AuditStamp auditStamp =
        new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));
    return RestliUtils.toTask(() -> {
      _entityService.ingestEntities(Arrays.asList(entities), auditStamp);
      return null;
    });
  }

  @Action(name = ACTION_SEARCH)
  @Nonnull
  public Task<SearchResult> search(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_INPUT) @Nonnull String input,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_COUNT) int count) {

    log.info("GET SEARCH RESULTS for {} with query {}", entityName, input);
    return RestliUtils.toTask(() -> _searchService.search(entityName, input, filter, sortCriterion, start, count));
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Optional @Nullable String field,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {

    return RestliUtils.toTask(() -> _searchService.autoComplete(entityName, query, field, filter, limit));
  }

  @Action(name = ACTION_BROWSE)
  @Nonnull
  public Task<BrowseResult> browse(
      @ActionParam(PARAM_ENTITY) @Nonnull String entityName,
      @ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit) {

    log.info("GET BROWSE RESULTS for {} at path {}", entityName, path);
    return RestliUtils.toTask(() -> _searchService.browse(entityName, path, filter, start, limit));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Nonnull
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = PARAM_URN, typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    log.info("GET BROWSE PATHS for {}", urn.toString());
    return RestliUtils.toTask(() -> new StringArray(_searchService.getBrowsePaths(urnToEntityName(urn), urn)));
  }

  /*
  Used to enable writes in GMS after data migration is complete
   */
  @Action(name = "setWritable")
  @Nonnull
  public Task<Void> setWriteable(@ActionParam(PARAM_VALUE) @Optional("true") @Nonnull Boolean value) {
    log.info("setting entity resource to be writable");
    return RestliUtils.toTask(() -> {
      _entityService.setWritable(value);
      return null;
    });
  }
}
