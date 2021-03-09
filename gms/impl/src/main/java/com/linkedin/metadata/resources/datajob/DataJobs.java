package com.linkedin.metadata.resources.dashboard;

import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datajob.DataJob;
import com.linkedin.datajob.DataJobKey;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.DataJobAspect;
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
import com.linkedin.metadata.search.DataJobDocument;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
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


@RestLiCollection(name = "dataJobs", namespace = "com.linkedin.datajob", keyName = "key")
public class DataJobs extends BaseBrowsableEntityResource<
    // @formatter:off
    ComplexResourceKey<DataJobKey, EmptyRecord>,
    DataJob,
    DataJobUrn,
    DataJobSnapshot,
    DataJobAspect,
    DataJobDocument> {
  // @formatter:on

  public DataJobs() {
    super(DataJobSnapshot.class, DataJobAspect.class, DataJobUrn.class);
  }

  @Inject
  @Named("dataJobDAO")
  private BaseLocalDAO<DataJobAspect, DataJobUrn> _localDAO;

  @Inject
  @Named("dataJobSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  @Inject
  @Named("dataJobBrowseDao")
  private BaseBrowseDAO _browseDAO;

  @Nonnull
  @Override
  protected BaseSearchDAO<DataJobDocument> getSearchDAO() {
    return _esSearchDAO;
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DataJobAspect, DataJobUrn> getLocalDAO() {
    return _localDAO;
  }

  @Nonnull
  @Override
  protected BaseBrowseDAO getBrowseDAO() {
    return _browseDAO;
  }

  @Nonnull
  @Override
  protected DataJobUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DataJobUrn.createFromString(urnString);
  }

  @Nonnull
  @Override
  protected DataJobUrn toUrn(@Nonnull ComplexResourceKey<DataJobKey, EmptyRecord> key) {
    return new DataJobUrn(key.getKey().getDataFlow(), key.getKey().getJobId());
  }

  @Nonnull
  @Override
  protected ComplexResourceKey<DataJobKey, EmptyRecord> toKey(@Nonnull DataJobUrn urn) {
    return new ComplexResourceKey<>(
        new DataJobKey()
            .setDataFlow(urn.getFlowEntity())
            .setJobId(urn.getJobIdEntity()),
        new EmptyRecord());
  }

  @Nonnull
  @Override
  protected DataJob toValue(@Nonnull DataJobSnapshot snapshot) {
    final DataJob value = new DataJob()
        .setUrn(snapshot.getUrn())
        .setDataFlow(snapshot.getUrn().getFlowEntity())
        .setJobId(snapshot.getUrn().getJobIdEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof DataJobInfo) {
        DataJobInfo info = DataJobInfo.class.cast(aspect);
        value.setInfo(info);
      } else if (aspect instanceof DataJobInputOutput) {
        DataJobInputOutput inputOutput = DataJobInputOutput.class.cast(aspect);
        value.setInputOutput(inputOutput);
      }
      else if (aspect instanceof Ownership) {
        Ownership ownership = Ownership.class.cast(aspect);
        value.setOwnership(ownership);
      }
    });

    return value;
  }

  @Nonnull
  @Override
  protected DataJobSnapshot toSnapshot(@Nonnull DataJob dataJob, @Nonnull DataJobUrn urn) {
    final List<DataJobAspect> aspects = new ArrayList<>();
    if (dataJob.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getInfo()));
    }
    if (dataJob.hasInputOutput()) {
      aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getInputOutput()));
    }
    if (dataJob.hasOwnership()) {
      aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getOwnership()));
    }
    return ModelUtils.newSnapshot(DataJobSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<DataJob> get(@Nonnull ComplexResourceKey<DataJobKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DataJobKey, EmptyRecord>, DataJob>> batchGet(
      @Nonnull Set<ComplexResourceKey<DataJobKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<DataJob>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<DataJob, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DataJobSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DataJobSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
