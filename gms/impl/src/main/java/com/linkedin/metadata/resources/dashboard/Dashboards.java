package com.linkedin.metadata.resources.dashboard;

import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.Dashboard;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.dashboard.DashboardKey;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.DashboardDocument;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;


@RestLiCollection(name = "dashboards", namespace = "com.linkedin.dashboard", keyName = "key")
public class Dashboards extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<DashboardKey, EmptyRecord>,
    Dashboard,
    DashboardUrn,
    DashboardSnapshot,
    DashboardAspect,
    DashboardDocument> {
  // @formatter:on

  public Dashboards() {
    super(DashboardSnapshot.class, DashboardAspect.class, DashboardUrn.class);
  }

  @Inject
  @Named("dashboardDAO")
  private BaseLocalDAO<DashboardAspect, DashboardUrn> _localDAO;

  @Inject
  @Named("dashboardSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  @Nonnull
  @Override
  protected BaseSearchDAO<DashboardDocument> getSearchDAO() {
    return _esSearchDAO;
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DashboardAspect, DashboardUrn> getLocalDAO() {
    return _localDAO;
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
        new DashboardKey()
            .setTool(urn.getDashboardToolEntity())
            .setDashboardId(urn.getDashboardIdEntity()),
        new EmptyRecord());
  }

  @Nonnull
  @Override
  protected Dashboard toValue(@Nonnull DashboardSnapshot snapshot) {
    final Dashboard value = new Dashboard()
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
    return ModelUtils.newSnapshot(DashboardSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<Dashboard> get(@Nonnull ComplexResourceKey<DashboardKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DashboardKey, EmptyRecord>, Dashboard>> batchGet(
      @Nonnull Set<ComplexResourceKey<DashboardKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<Dashboard>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<Dashboard, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return super.search(input, aspectNames, filter, sortCriterion, pagingContext);
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Override
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    return super.autocomplete(query, field, filter, limit);
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DashboardSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DashboardSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.getSnapshot(urnString, aspectNames);
  }

  @Action(name = ACTION_BACKFILL_WITH_URNS)
  @Override
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URNS) @Nonnull String[] urns,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.backfill(urns, aspectNames);
  }
}
