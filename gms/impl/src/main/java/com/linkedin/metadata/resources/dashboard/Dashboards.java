package com.linkedin.metadata.resources.dashboard;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.Dashboard;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.dashboard.DashboardKey;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseBrowsableEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.DashboardDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.BrowseUtil;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
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

import static com.linkedin.metadata.restli.RestliConstants.ACTION_AUTOCOMPLETE;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_BACKFILL_WITH_URNS;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_BROWSE;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_GET_BROWSE_PATHS;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_GET_SNAPSHOT;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_INGEST;
import static com.linkedin.metadata.restli.RestliConstants.FINDER_SEARCH;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_ASPECTS;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_FIELD;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_FILTER;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_INPUT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_LIMIT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_PATH;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_QUERY;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_SNAPSHOT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_SORT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_START;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_URN;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_URNS;


/**
 * Deprecated! Use {@link EntityResource} instead.
 */
@Deprecated
@RestLiCollection(name = "dashboards", namespace = "com.linkedin.dashboard", keyName = "key")
public class Dashboards extends BaseBrowsableEntityResource<
    // @formatter:off
    ComplexResourceKey<DashboardKey, EmptyRecord>,
    Dashboard,
    DashboardUrn,
    DashboardSnapshot,
    DashboardAspect,
    DashboardDocument> {
  // @formatter:on

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final Clock _clock = Clock.systemUTC();

  public Dashboards() {
    super(DashboardSnapshot.class, DashboardAspect.class, DashboardUrn.class);
  }

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  @Nonnull
  @Override
  protected BaseSearchDAO<DashboardDocument> getSearchDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DashboardAspect, DashboardUrn> getLocalDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected BaseBrowseDAO getBrowseDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected DashboardUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DashboardUrn.createFromString(urnString);
  }

  @Nonnull
  @Override
  protected DashboardUrn toUrn(@Nonnull ComplexResourceKey<DashboardKey, EmptyRecord> key) {
    return new DashboardUrn(key.getKey().getTool(), key.getKey().getDashboardId());
  }

  @Nonnull
  @Override
  protected ComplexResourceKey<DashboardKey, EmptyRecord> toKey(@Nonnull DashboardUrn urn) {
    return new ComplexResourceKey<>(
        new DashboardKey().setTool(urn.getDashboardToolEntity()).setDashboardId(urn.getDashboardIdEntity()),
        new EmptyRecord());
  }

  @Nonnull
  @Override
  protected Dashboard toValue(@Nonnull DashboardSnapshot snapshot) {
    final Dashboard value = new Dashboard().setUrn(snapshot.getUrn())
        .setTool(snapshot.getUrn().getDashboardToolEntity())
        .setDashboardId(snapshot.getUrn().getDashboardIdEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof DashboardInfo) {
        DashboardInfo info = DashboardInfo.class.cast(aspect);
        value.setInfo(info);
      } else if (aspect instanceof Ownership) {
        Ownership ownership = Ownership.class.cast(aspect);
        value.setOwnership(ownership);
      } else if (aspect instanceof Status) {
        Status status = Status.class.cast(aspect);
        value.setStatus(status);
      } else if (aspect instanceof GlobalTags) {
        value.setGlobalTags(GlobalTags.class.cast(aspect));
      }
    });

    return value;
  }

  @Nonnull
  @Override
  protected DashboardSnapshot toSnapshot(@Nonnull Dashboard dashboard, @Nonnull DashboardUrn urn) {
    final List<DashboardAspect> aspects = new ArrayList<>();
    if (dashboard.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getInfo()));
    }
    if (dashboard.hasOwnership()) {
      aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getOwnership()));
    }
    if (dashboard.hasStatus()) {
      aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getStatus()));
    }
    if (dashboard.hasGlobalTags()) {
      aspects.add(ModelUtils.newAspectUnion(DashboardAspect.class, dashboard.getGlobalTags()));
    }
    return ModelUtils.newSnapshot(DashboardSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<Dashboard> get(@Nonnull ComplexResourceKey<DashboardKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames)
            .stream()
            .map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity =
          _entityService.getEntity(new DashboardUrn(key.getKey().getTool(), key.getKey().getDashboardId()),
              projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getDashboardSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DashboardKey, EmptyRecord>, Dashboard>> batchGet(
      @Nonnull Set<ComplexResourceKey<DashboardKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames)
            .stream()
            .map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Map<ComplexResourceKey<DashboardKey, EmptyRecord>, Dashboard> entities = new HashMap<>();
      for (final ComplexResourceKey<DashboardKey, EmptyRecord> key : keys) {
        final Entity entity =
            _entityService.getEntity(new DashboardUrn(key.getKey().getTool(), key.getKey().getDashboardId()),
                projectedAspects);

        if (entity != null) {
          entities.put(key, toValue(entity.getValue().getDashboardSnapshot()));
        }
      }
      return entities;
    });
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<Dashboard>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames)
            .stream()
            .map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
      final SortCriterion searchSortCriterion =
          sortCriterion != null ? sortCriterion : new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING);
      final SearchResult filterResults =
          _searchService.filter("dashboard", searchFilter, searchSortCriterion, pagingContext.getStart(),
              pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(filterResults.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(entity.keySet()
          .stream()
          .map(urn -> toValue(entity.get(urn).getValue().getDashboardSnapshot()))
          .collect(Collectors.toList()), filterResults.getNumEntities(),
          filterResults.getMetadata().setUrns(new UrnArray(urns))).getElements();
    });
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<Dashboard, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames)
            .stream()
            .map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final SearchResult searchResult =
          _searchService.search("dashboard", input, filter, sortCriterion, pagingContext.getStart(),
              pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(entity.keySet()
          .stream()
          .map(urn -> toValue(entity.get(urn).getValue().getDashboardSnapshot()))
          .collect(Collectors.toList()), searchResult.getNumEntities(),
          searchResult.getMetadata().setUrns(new UrnArray(urns)));
    });
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Override
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(() -> _searchService.autoComplete("dashboard", query, field, filter, limit));
  }

  @Action(name = ACTION_BROWSE)
  @Override
  @Nonnull
  public Task<BrowseResult> browse(@ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(
        () -> BrowseUtil.convertToLegacyResult(_searchService.browse("dashboard", path, filter, start, limit)));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Override
  @Nonnull
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    return RestliUtils.toTask(() -> new StringArray(_searchService.getBrowsePaths("dashboard", urn)));
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DashboardSnapshot snapshot) {
    return RestliUtils.toTask(() -> {
      try {
        final AuditStamp auditStamp =
            new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));
        _entityService.ingestEntity(new Entity().setValue(Snapshot.create(snapshot)), auditStamp);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Failed to create Audit Urn");
      }
      return null;
    });
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DashboardSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames)
            .stream()
            .map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity;
      try {
        entity = _entityService.getEntity(Urn.createFromString(urnString), projectedAspects);

        if (entity != null) {
          return entity.getValue().getDashboardSnapshot();
        }
        throw RestliUtils.resourceNotFoundException();
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to convert urnString %s into an Urn", urnString));
      }
    });
  }

  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Override
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.backfill(urns, aspectNames);
  }
}
