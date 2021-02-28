package com.linkedin.metadata.resources.dashboard;

import com.linkedin.chart.ChartInfo;
import com.linkedin.chart.ChartQuery;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.Chart;
import com.linkedin.dashboard.ChartKey;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseBrowsableEntityResource;
import com.linkedin.metadata.search.ChartDocument;
import com.linkedin.metadata.snapshot.ChartSnapshot;
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


@RestLiCollection(name = "charts", namespace = "com.linkedin.chart", keyName = "key")
public class Charts extends BaseBrowsableEntityResource<
    // @formatter:off
    ComplexResourceKey<ChartKey, EmptyRecord>,
    Chart,
    ChartUrn,
    ChartSnapshot,
    ChartAspect,
    ChartDocument> {
  // @formatter:on

  public Charts() {
    super(ChartSnapshot.class, ChartAspect.class, ChartUrn.class);
  }

  @Inject
  @Named("chartDAO")
  private BaseLocalDAO<ChartAspect, ChartUrn> _localDAO;

  @Inject
  @Named("chartSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  @Inject
  @Named("chartBrowseDao")
  private BaseBrowseDAO _browseDAO;

  @Nonnull
  @Override
  protected BaseSearchDAO<ChartDocument> getSearchDAO() {
    return _esSearchDAO;
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<ChartAspect, ChartUrn> getLocalDAO() {
    return _localDAO;
  }

  @Nonnull
  @Override
  protected BaseBrowseDAO getBrowseDAO() {
    return _browseDAO;
  }

  @Nonnull
  @Override
  protected ChartUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return ChartUrn.createFromString(urnString);
  }

  @Nonnull
  @Override
  protected ChartUrn toUrn(@Nonnull ComplexResourceKey<ChartKey, EmptyRecord> key) {
    return new ChartUrn(key.getKey().getTool(), key.getKey().getChartId());
  }

  @Nonnull
  @Override
  protected ComplexResourceKey<ChartKey, EmptyRecord> toKey(@Nonnull ChartUrn urn) {
    return new ComplexResourceKey<>(
        new ChartKey()
            .setTool(urn.getDashboardToolEntity())
            .setChartId(urn.getChartIdEntity()),
        new EmptyRecord());
  }

  @Nonnull
  @Override
  protected Chart toValue(@Nonnull ChartSnapshot snapshot) {
    final Chart value = new Chart()
        .setUrn(snapshot.getUrn())
        .setTool(snapshot.getUrn().getDashboardToolEntity())
        .setChartId(snapshot.getUrn().getChartIdEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof ChartInfo) {
        ChartInfo info = ChartInfo.class.cast(aspect);
        value.setInfo(info);
      } else if (aspect instanceof ChartQuery) {
        ChartQuery query = ChartQuery.class.cast(aspect);
        value.setQuery(query);
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
  protected ChartSnapshot toSnapshot(@Nonnull Chart chart, @Nonnull ChartUrn urn) {
    final List<ChartAspect> aspects = new ArrayList<>();
    if (chart.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(ChartAspect.class, chart.getInfo()));
    }
    if (chart.hasQuery()) {
      aspects.add(ModelUtils.newAspectUnion(ChartAspect.class, chart.getQuery()));
    }
    if (chart.hasOwnership()) {
      aspects.add(ModelUtils.newAspectUnion(ChartAspect.class, chart.getOwnership()));
    }
    if (chart.hasStatus()) {
      aspects.add(ModelUtils.newAspectUnion(ChartAspect.class, chart.getStatus()));
    }
    return ModelUtils.newSnapshot(ChartSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<Chart> get(@Nonnull ComplexResourceKey<ChartKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<ChartKey, EmptyRecord>, Chart>> batchGet(
      @Nonnull Set<ComplexResourceKey<ChartKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<Chart>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<Chart, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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

  @Action(name = ACTION_BROWSE)
  @Override
  @Nonnull
  public Task<BrowseResult> browse(@ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit) {
    return super.browse(path, filter, start, limit);
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Override
  @Nonnull
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    return super.getBrowsePaths(urn);
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull ChartSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<ChartSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
