package com.linkedin.metadata.resources.datajob;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datajob.DataJob;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.DataJobKey;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.DataJobAspect;
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
import com.linkedin.metadata.search.DataJobDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
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

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final Clock _clock = Clock.systemUTC();

  public DataJobs() {
    super(DataJobSnapshot.class, DataJobAspect.class, DataJobUrn.class);
  }

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  @Nonnull
  @Override
  protected BaseSearchDAO<DataJobDocument> getSearchDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DataJobAspect, DataJobUrn> getLocalDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected BaseBrowseDAO getBrowseDAO() {
    throw new UnsupportedOperationException();
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
    if (dataJob.hasStatus()) {
      aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getStatus()));
    }
    if (dataJob.hasGlobalTags()) {
      aspects.add(ModelUtils.newAspectUnion(DataJobAspect.class, dataJob.getGlobalTags()));
    }
    return ModelUtils.newSnapshot(DataJobSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<DataJob> get(@Nonnull ComplexResourceKey<DataJobKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity = _entityService.getEntity(new DataJobUrn(
          key.getKey().getDataFlow(),
          key.getKey().getJobId()), projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getDataJobSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<DataJobKey, EmptyRecord>, DataJob>> batchGet(
      @Nonnull Set<ComplexResourceKey<DataJobKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Map<ComplexResourceKey<DataJobKey, EmptyRecord>, DataJob> entities = new HashMap<>();
      for (final ComplexResourceKey<DataJobKey, EmptyRecord> key : keys) {
        final Entity entity = _entityService.getEntity(
            new DataJobUrn(key.getKey().getDataFlow(), key.getKey().getJobId()),
            projectedAspects);

        if (entity != null) {
          entities.put(key, toValue(entity.getValue().getDataJobSnapshot()));
        }
      }
      return entities;
    });
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<DataJob>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Filter searchFilter = filter != null ? filter : QueryUtils.EMPTY_FILTER;
      final SortCriterion searchSortCriterion = sortCriterion != null ? sortCriterion
          : new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING);
      final SearchResult filterResults = _searchService.filter(
          "dataJob",
          searchFilter,
          searchSortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(filterResults.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getDataJobSnapshot())).collect(
              Collectors.toList()),
          filterResults.getNumEntities(),
          filterResults.getMetadata().setUrns(new UrnArray(urns))
      ).getElements();
    });
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<DataJob, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final SearchResult searchResult = _searchService.search(
          "dataJob",
          input,
          filter,
          sortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getDataJobSnapshot())).collect(
              Collectors.toList()),
          searchResult.getNumEntities(),
          searchResult.getMetadata().setUrns(new UrnArray(urns))
      );
    });  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Override
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(() ->
        _searchService.autoComplete(
            "dataJob",
            query,
            field,
            filter,
            limit)
    );  }

  @Action(name = ACTION_BROWSE)
  @Override
  @Nonnull
  public Task<BrowseResult> browse(@ActionParam(PARAM_PATH) @Nonnull String path,
      @ActionParam(PARAM_FILTER) @Optional @Nullable Filter filter, @ActionParam(PARAM_START) int start,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(
        () -> BrowseUtil.convertToLegacyResult(_searchService.browse("dataJob", path, filter, start, limit)));
  }

  @Action(name = ACTION_GET_BROWSE_PATHS)
  @Override
  @Nonnull
  public Task<StringArray> getBrowsePaths(
      @ActionParam(value = "urn", typeref = com.linkedin.common.Urn.class) @Nonnull Urn urn) {
    return RestliUtils.toTask(() -> new StringArray(_searchService.getBrowsePaths("dataJob", urn)));
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull DataJobSnapshot snapshot) {
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
  public Task<DataJobSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity;
      try {
        entity = _entityService.getEntity(
            Urn.createFromString(urnString), projectedAspects);

        if (entity != null) {
          return entity.getValue().getDataJobSnapshot();
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
