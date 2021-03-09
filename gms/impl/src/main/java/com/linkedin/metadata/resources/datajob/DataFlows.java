package com.linkedin.metadata.resources.dashboard;

import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datajob.DataFlow;
import com.linkedin.datajob.DataFlowKey;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.DataFlowAspect;
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
import com.linkedin.metadata.search.DataFlowDocument;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;
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


@RestLiCollection(name = "dataFlows", namespace = "com.linkedin.dataflow", keyName = "key")
public class DataFlows extends BaseBrowsableEntityResource<
    // @formatter:off
    ComplexResourceKey<DataFlowKey, EmptyRecord>,
    DataFlow,
    DataFlowUrn,
    DataFlowSnapshot,
    DataFlowAspect,
    DataFlowDocument> {
  // @formatter:on

  public DataFlows() {
    super(DataFlowSnapshot.class, DataFlowAspect.class, DataFlowUrn.class);
  }

  @Inject
  @Named("dataFlowDAO")
  private BaseLocalDAO<DataFlowAspect, DataFlowUrn> _localDAO;

  @Inject
  @Named("dataFlowSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  @Inject
  @Named("dataFlowBrowseDao")
  private BaseBrowseDAO _browseDAO;

  @Nonnull
  @Override
  protected BaseSearchDAO<DataFlowDocument> getSearchDAO() {
    return _esSearchDAO;
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DataFlowAspect, DataFlowUrn> getLocalDAO() {
    return _localDAO;
  }

  @Nonnull
  @Override
  protected BaseBrowseDAO getBrowseDAO() {
    return _browseDAO;
  }

  @Nonnull
  @Override
  protected DataFlowUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DataFlowUrn.createFromString(urnString);
  }

  @Nonnull
  @Override
  protected DataFlowUrn toUrn(@Nonnull ComplexResourceKey<DataFlowKey, EmptyRecord> key) {
    return new DataFlowUrn(key.getKey().getOrchestrator(), key.getKey().getFlowId(), key.getKey().getCluster());
  }

  @Nonnull
  @Override
  protected ComplexResourceKey<DataFlowKey, EmptyRecord> toKey(@Nonnull DataFlowUrn urn) {
    return new ComplexResourceKey<>(
        new DataFlowKey()
            .setOrchestrator(urn.getOrchestratorEntity())
            .setFlowId(urn.getFlowIdEntity())
            .setCluster(urn.getClusterEntity()),
        new EmptyRecord());
  }

  @Nonnull
  @Override
  protected DataFlow toValue(@Nonnull DataFlowSnapshot snapshot) {
    final DataFlow value = new DataFlow()
        .setUrn(snapshot.getUrn())
        .setOrchestrator(snapshot.getUrn().getOrchestratorEntity())
        .setFlowId(snapshot.getUrn().getFlowIdEntity())
        .setCluster(snapshot.getUrn().getClusterEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof DataFlowInfo) {
        DataFlowInfo info = DataFlowInfo.class.cast(aspect);
        value.setInfo(info);
      } else if (aspect instanceof Ownership) {
        Ownership ownership = Ownership.class.cast(aspect);
        value.setOwnership(ownership);
      }
    });

    return value;
  }

  @Nonnull
  @Override
  protected DataFlowSnapshot toSnapshot(@Nonnull DataFlow dataFlow, @Nonnull DataFlowUrn urn) {
    final List<DataFlowAspect> aspects = new ArrayList<>();
    if (dataFlow.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(DataFlowAspect.class, dataFlow.getInfo()));
    }
    if (dataFlow.hasOwnership()) {
      aspects.add(ModelUtils.newAspectUnion(DataFlowAspect.class, dataFlow.getOwnership()));
    }
    return ModelUtils.newSnapshot(DataFlowSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<DataFlow> get(@Nonnull ComplexResourceKey<DataFlowKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DataFlowKey, EmptyRecord>, DataFlow>> batchGet(
      @Nonnull Set<ComplexResourceKey<DataFlowKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<DataFlow>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<DataFlow, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DataFlowSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DataFlowSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
