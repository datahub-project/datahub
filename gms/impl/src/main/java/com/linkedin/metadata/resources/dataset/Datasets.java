package com.linkedin.metadata.resources.dataset;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.IndexFilter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseBrowsableEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.DatasetDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
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
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaMetadata;
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
import static com.linkedin.metadata.restli.RestliConstants.ACTION_BACKFILL;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_BROWSE;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_GET_BROWSE_PATHS;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_GET_SNAPSHOT;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_INGEST;
import static com.linkedin.metadata.restli.RestliConstants.FINDER_FILTER;
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

/**
 * Deprecated! Use {@link EntityResource} instead.
 */
@Deprecated
@RestLiCollection(name = "datasets", namespace = "com.linkedin.dataset", keyName = "dataset")
public final class Datasets extends BaseBrowsableEntityResource<
    // @formatter:off
        ComplexResourceKey<DatasetKey, EmptyRecord>,
        Dataset,
        DatasetUrn,
        DatasetSnapshot,
        DatasetAspect,
        DatasetDocument> {
    // @formatter:on

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final Clock _clock = Clock.systemUTC();

  public Datasets() {
    super(DatasetSnapshot.class, DatasetAspect.class, DatasetUrn.class);
  }

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  @Override
  @Nonnull
  protected BaseLocalDAO getLocalDAO() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  protected BaseBrowseDAO getBrowseDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected DatasetUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DatasetUrn.createFromString(urnString);
  }

  @Override
  @Nonnull
  protected DatasetUrn toUrn(@Nonnull ComplexResourceKey<DatasetKey, EmptyRecord> key) {
    return new DatasetUrn(key.getKey().getPlatform(), key.getKey().getName(), key.getKey().getOrigin());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<DatasetKey, EmptyRecord> toKey(@Nonnull DatasetUrn urn) {
    return new ComplexResourceKey<>(
        new DatasetKey()
            .setPlatform(urn.getPlatformEntity())
            .setName(urn.getDatasetNameEntity())
            .setOrigin(urn.getOriginEntity()),
        new EmptyRecord());
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
        final DatasetProperties datasetProperties = (DatasetProperties) aspect;
        value.setProperties(datasetProperties.getCustomProperties());
        value.setTags(datasetProperties.getTags());
        if (datasetProperties.getUri() != null) {
          value.setUri(datasetProperties.getUri());
        }
        if (datasetProperties.getDescription() != null) {
          value.setDescription(datasetProperties.getDescription());
        }
        if (datasetProperties.getExternalUrl() != null) {
          value.setExternalUrl(datasetProperties.getExternalUrl());
        }
      } else if (aspect instanceof DatasetDeprecation) {
        value.setDeprecation((DatasetDeprecation) aspect);
      } else if (aspect instanceof InstitutionalMemory) {
        value.setInstitutionalMemory((InstitutionalMemory) aspect);
      } else if (aspect instanceof Ownership) {
        value.setOwnership((Ownership) aspect);
      } else if (aspect instanceof SchemaMetadata) {
        value.setSchemaMetadata((SchemaMetadata) aspect);
      } else if (aspect instanceof Status) {
        value.setStatus((Status) aspect);
        value.setRemoved(((Status) aspect).isRemoved());
      } else if (aspect instanceof UpstreamLineage) {
        value.setUpstreamLineage((UpstreamLineage) aspect);
      } else if (aspect instanceof GlobalTags) {
        value.setGlobalTags(GlobalTags.class.cast(aspect));
      } else if (aspect instanceof EditableSchemaMetadata) {
        value.setEditableSchemaMetadata(EditableSchemaMetadata.class.cast(aspect));
      } else if (aspect instanceof EditableDatasetProperties) {
        value.setEditableProperties(EditableDatasetProperties.class.cast(aspect));
      } else if (aspect instanceof GlossaryTerms) {
        value.setGlossaryTerms((GlossaryTerms) aspect);
      }
  });
    return value;
  }

  @Override
  @Nonnull
  protected DatasetSnapshot toSnapshot(@Nonnull Dataset dataset, @Nonnull DatasetUrn datasetUrn) {
    final List<DatasetAspect> aspects = new ArrayList<>();
    if (dataset.getProperties() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, getDatasetPropertiesAspect(dataset)));
    }
    if (dataset.getDeprecation() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getDeprecation()));
    }
    if (dataset.getInstitutionalMemory() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getInstitutionalMemory()));
    }
    if (dataset.getOwnership() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getOwnership()));
    }
    if (dataset.getSchemaMetadata() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getSchemaMetadata()));
    }
    if (dataset.getStatus() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getStatus()));
    }
    if (dataset.getUpstreamLineage() != null) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getUpstreamLineage()));
    }
    if (dataset.hasRemoved()) {
      aspects.add(DatasetAspect.create(new Status().setRemoved(dataset.isRemoved())));
    }
    if (dataset.hasGlossaryTerms()) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getGlossaryTerms()));
    }
    if (dataset.hasGlobalTags()) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getGlobalTags()));
    }
    if (dataset.hasEditableSchemaMetadata()) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getEditableSchemaMetadata()));
    }
    if (dataset.hasEditableProperties()) {
      aspects.add(ModelUtils.newAspectUnion(DatasetAspect.class, dataset.getEditableProperties()));
    }
    return ModelUtils.newSnapshot(DatasetSnapshot.class, datasetUrn, aspects);
  }

  @Nonnull
  private DatasetProperties getDatasetPropertiesAspect(@Nonnull Dataset dataset) {
    final DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setDescription(dataset.getDescription());
    datasetProperties.setTags(dataset.getTags());
    if (dataset.getUri() != null)  {
      datasetProperties.setUri(dataset.getUri());
    }
    if (dataset.getProperties() != null) {
      datasetProperties.setCustomProperties(dataset.getProperties());
    }
    if (dataset.getExternalUrl() != null) {
      datasetProperties.setExternalUrl(dataset.getExternalUrl());
    }
    return datasetProperties;
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<Dataset> get(@Nonnull ComplexResourceKey<DatasetKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity = _entityService.getEntity(new DatasetUrn(
          key.getKey().getPlatform(),
          key.getKey().getName(),
          key.getKey().getOrigin()), projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getDatasetSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DatasetKey, EmptyRecord>, Dataset>> batchGet(
      @Nonnull Set<ComplexResourceKey<DatasetKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Map<ComplexResourceKey<DatasetKey, EmptyRecord>, Dataset> entities = new HashMap<>();
      for (final ComplexResourceKey<DatasetKey, EmptyRecord> key : keys) {
        final Entity entity = _entityService.getEntity(
            new DatasetUrn(key.getKey().getPlatform(), key.getKey().getName(), key.getKey().getOrigin()),
            projectedAspects);
        if (entity != null) {
          entities.put(key, toValue(entity.getValue().getDatasetSnapshot()));
        }
      }
      return entities;
    });
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<Dataset, SearchResultMetadata>> search(
      @QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final SearchResult searchResult = _searchService.search(
          "dataset",
          input,
          filter,
          sortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getDatasetSnapshot())).collect(Collectors.toList()),
          searchResult.getNumEntities(),
          searchResult.getMetadata().setUrns(new UrnArray(urns))
      );
    });
  }

  /**
   * Retrieves the values for multiple entities obtained after filtering urns from local secondary index. Here the value is
   * made up of latest versions of specified aspects. If no aspects are provided, value model will not contain any metadata aspect.
   *
   * <p>If no filter conditions are provided, then it returns values of given entity type.
   *
   * @param indexFilter {@link IndexFilter} that defines the filter conditions
   * @param aspectNames list of aspects to be returned in the VALUE model
   * @param lastUrn last urn of the previous fetched page. For the first page, this should be set as NULL
   * @param pagingContext {@link PagingContext} defining the paging parameters of the request
   * @return list of values
   */
  @Finder(FINDER_FILTER)
  @Override
  @Nonnull
  public Task<List<Dataset>> filter(
      @QueryParam(PARAM_FILTER) @Optional @Nullable IndexFilter indexFilter,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_URN) @Optional @Nullable String lastUrn,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    throw new UnsupportedOperationException();
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Override
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(() ->
        _searchService.autoComplete(
          "dataset",
          query,
          field,
          filter,
          limit)
    );
  }

  @Action(name = ACTION_BROWSE)
  @Override
  @Nonnull
  public Task<BrowseResult> browse(@ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(
        () -> BrowseUtil.convertToLegacyResult(_searchService.browse("dataset", path, filter, start, limit))
    );
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Override
  @Nonnull
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    return RestliUtils.toTask(() ->
        new StringArray(_searchService.getBrowsePaths(
            "dataset",
            urn))
    );
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DatasetSnapshot snapshot) {
    return RestliUtils.toTask(() -> {
          try {
            final AuditStamp auditStamp =
                new AuditStamp().setTime(_clock.millis()).setActor(Urn.createFromString(DEFAULT_ACTOR));
            _entityService.ingestEntity(new Entity().setValue(Snapshot.create(snapshot)), auditStamp);
          } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to create Audit Urn", e);
          }
          return null;
    });
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DatasetSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      try {
        final Entity entity = _entityService.getEntity(
            Urn.createFromString(urnString), projectedAspects);

        if (entity != null) {
          return entity.getValue().getDatasetSnapshot();
        }
        throw RestliUtils.resourceNotFoundException();
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to convert urnString %s into an Urn", urnString));
      }
    });
  }

  @Action(name = ACTION_BACKFILL)
  @Override
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.backfill(urnString, aspectNames);
  }
}
