package com.linkedin.metadata.resources.identity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserResourceKey;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
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

import static com.linkedin.metadata.restli.RestliConstants.*;

/**
 * Deprecated! Use {@link EntityResource} instead.
 */
@Deprecated
@RestLiCollection(name = "corpUsers", namespace = "com.linkedin.identity", keyName = "corpUser")
public final class CorpUsers extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<CorpUserResourceKey, EmptyRecord>,
    CorpUser,
    CorpuserUrn,
    CorpUserSnapshot,
    CorpUserAspect,
    CorpUserInfoDocument> {
    // @formatter:on

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  public CorpUsers() {
    super(CorpUserSnapshot.class, CorpUserAspect.class);
  }

  @Override
  @Nonnull
  protected BaseLocalDAO getLocalDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected CorpuserUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return CorpuserUrn.createFromUrn(Urn.createFromString(urnString));
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  protected CorpuserUrn toUrn(@Nonnull ComplexResourceKey<CorpUserResourceKey, EmptyRecord> key) {
    return new CorpuserUrn(key.getKey().getName());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<CorpUserResourceKey, EmptyRecord> toKey(@Nonnull CorpuserUrn urn) {
    return new ComplexResourceKey<>(new CorpUserResourceKey().setName(urn.getUsernameEntity()), new EmptyRecord());
  }

  @Override
  @Nonnull
  protected CorpUser toValue(@Nonnull CorpUserSnapshot snapshot) {
    final CorpUser value = new CorpUser().setUsername(snapshot.getUrn().getUsernameEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof CorpUserInfo) {
        value.setInfo(CorpUserInfo.class.cast(aspect));
      } else if (aspect instanceof CorpUserEditableInfo) {
        value.setEditableInfo(CorpUserEditableInfo.class.cast(aspect));
      } else if (aspect instanceof GlobalTags) {
        value.setGlobalTags(GlobalTags.class.cast(aspect));
      }
    });
    return value;
  }

  @Override
  @Nonnull
  protected CorpUserSnapshot toSnapshot(@Nonnull CorpUser corpUser, @Nonnull CorpuserUrn corpuserUrn) {
    final List<CorpUserAspect> aspects = new ArrayList<>();
    if (corpUser.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(CorpUserAspect.class, corpUser.getInfo()));
    }
    if (corpUser.hasEditableInfo()) {
      aspects.add(ModelUtils.newAspectUnion(CorpUserAspect.class, corpUser.getEditableInfo()));
    }
    if (corpUser.hasGlobalTags()) {
      aspects.add(ModelUtils.newAspectUnion(CorpUserAspect.class, corpUser.getGlobalTags()));
    }
    return ModelUtils.newSnapshot(CorpUserSnapshot.class, corpuserUrn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<CorpUser> get(@Nonnull ComplexResourceKey<CorpUserResourceKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity = _entityService.getEntity(new CorpuserUrn(
          key.getKey().getName()), projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getCorpUserSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<CorpUserResourceKey, EmptyRecord>, CorpUser>> batchGet(
      @Nonnull Set<ComplexResourceKey<CorpUserResourceKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Map<ComplexResourceKey<CorpUserResourceKey, EmptyRecord>, CorpUser> entities = new HashMap<>();
      for (final ComplexResourceKey<CorpUserResourceKey, EmptyRecord> key : keys) {
        final Entity entity = _entityService.getEntity(
            new CorpuserUrn(key.getKey().getName()),
            projectedAspects);
        if (entity != null) {
          entities.put(key, toValue(entity.getValue().getCorpUserSnapshot()));
        }
      }
      return entities;
    });
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<CorpUser>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
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
          "corpUser",
          searchFilter,
          searchSortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(filterResults.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getCorpUserSnapshot())).collect(
              Collectors.toList()),
          filterResults.getNumEntities(),
          filterResults.getMetadata().setUrns(new UrnArray(urns))
      ).getElements();
    });
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<CorpUser, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));

    return RestliUtils.toTask(() -> {

      final SearchResult searchResult = _searchService.search(
          "corpUser",
          input,
          filter,
          sortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getCorpUserSnapshot())).collect(
              Collectors.toList()),
          searchResult.getNumEntities(),
          searchResult.getMetadata().setUrns(new UrnArray(urns))
      );
    });
  }

  @Action(name = ACTION_AUTOCOMPLETE)
  @Override
  @Nonnull
  public Task<AutoCompleteResult> autocomplete(@ActionParam(PARAM_QUERY) @Nonnull String query,
      @ActionParam(PARAM_FIELD) @Nullable String field, @ActionParam(PARAM_FILTER) @Nullable Filter filter,
      @ActionParam(PARAM_LIMIT) int limit) {
    return RestliUtils.toTask(() ->
        _searchService.autoComplete(
            "corpUser",
            query,
            field,
            filter,
            limit)
    );  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull CorpUserSnapshot snapshot) {
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
  public Task<CorpUserSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
          return entity.getValue().getCorpUserSnapshot();
        }
        throw RestliUtils.resourceNotFoundException();
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to convert urnString %s into an Urn", urnString));
      }
    });  }

  @Action(name = ACTION_BACKFILL)
  @Override
  @Nonnull
  public Task<BackfillResult> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.backfill(urnString, aspectNames);
  }
}
