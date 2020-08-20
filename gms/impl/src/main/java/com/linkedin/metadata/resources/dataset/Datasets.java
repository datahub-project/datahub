package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.Status;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BaseBrowsableEntityResource;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
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

@RestLiCollection(name = "datasets", namespace = "com.linkedin.dataset", keyName = "dataset")
public final class Datasets extends BaseBrowsableEntityResource<
    // @formatter:off
        DatasetKey,
        Dataset,
        DatasetUrn,
        DatasetSnapshot,
        DatasetAspect,
        DatasetDocument> {
    // @formatter:on

  public Datasets() {
    super(DatasetSnapshot.class, DatasetAspect.class);
  }

  @Inject
  @Named("datasetDao")
  private BaseLocalDAO _localDAO;

  @Inject
  @Named("datasetSearchDao")
  private BaseSearchDAO _searchDAO;

  @Inject
  @Named("datasetBrowseDao")
  private BaseBrowseDAO _browseDAO;

  @Override
  @Nonnull
  protected BaseLocalDAO getLocalDAO() {
    return _localDAO;
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    return _searchDAO;
  }

  @Override
  @Nonnull
  protected BaseBrowseDAO getBrowseDAO() {
    return _browseDAO;
  }

  @Nonnull
  @Override
  protected DatasetUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DatasetUrn.createFromString(urnString);
  }

  @Override
  @Nonnull
  protected DatasetUrn toUrn(@Nonnull DatasetKey key) {
    return new DatasetUrn(key.getPlatform(), key.getName(), key.getOrigin());
  }

  @Override
  @Nonnull
  protected DatasetKey toKey(@Nonnull DatasetUrn urn) {
    return new DatasetKey()
        .setPlatform(urn.getPlatformEntity())
        .setName(urn.getDatasetNameEntity())
        .setOrigin(urn.getOriginEntity());
  }

  @Override
  @Nonnull
  protected Dataset toValue(@Nonnull DatasetSnapshot snapshot) {
    final Dataset value = new Dataset()
        .setPlatform(snapshot.getUrn().getPlatformEntity())
        .setName(snapshot.getUrn().getDatasetNameEntity())
        .setOrigin(snapshot.getUrn().getOriginEntity())
        .setUrn(snapshot.getUrn());

    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof DatasetProperties) {
        DatasetProperties datasetProperties = DatasetProperties.class.cast(aspect);
        value.setProperties(datasetProperties.getCustomProperties());
        value.setTags(datasetProperties.getTags());
        if (datasetProperties.hasUri()) {
          value.setUri(datasetProperties.getUri());
        }
        if (datasetProperties.hasDescription()) {
          value.setDescription(datasetProperties.getDescription());
        }
      } else if (aspect instanceof DatasetDeprecation) {
        value.setDeprecation(DatasetDeprecation.class.cast(aspect));
      } else if (aspect instanceof Status) {
        value.setRemoved(Status.class.cast(aspect).isRemoved());
      }
    });
    return value;
  }

  @Override
  @Nonnull
  protected DatasetSnapshot toSnapshot(@Nonnull Dataset dataset, @Nonnull DatasetUrn datasetUrn) {
    final List<DatasetAspect> aspects = new ArrayList<>();
    if (dataset.hasProperties()) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, getDatasetPropertiesAspect(dataset)));
    }
    if (dataset.hasDeprecation()) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getDeprecation()));
    }

    aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, new Status().setRemoved(dataset.isRemoved())));
    return ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn, aspects);
  }

  @Nonnull
  private DatasetProperties getDatasetPropertiesAspect(@Nonnull Dataset dataset) {
    final DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setDescription(dataset.getDescription());
    datasetProperties.setTags(dataset.getTags());
    if (dataset.hasUri()) {
      datasetProperties.setUri(dataset.getUri());
    }
    if (dataset.hasPlatform()) {
      datasetProperties.setCustomProperties(dataset.getProperties());
    }
    return datasetProperties;
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<Dataset> get(@Nonnull ComplexResourceKey<DatasetKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DatasetKey, EmptyRecord>, Dataset>> batchGet(
      @Nonnull Set<ComplexResourceKey<DatasetKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<Dataset, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DatasetSnapshot snapshot) {
    return super.ingest(snapshot);
  }
  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DatasetSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.getSnapshot(urnString, aspectNames);
  }
  @Action(name = ACTION_BACKFILL)
  @Override
  @Nonnull
  public Task<String[]> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.backfill(urnString, aspectNames);
  }
}
