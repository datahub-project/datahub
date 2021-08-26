package com.linkedin.metadata.resources.identity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.identity.CorpGroup;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpGroupResourceKey;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.CorpGroupDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
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
@RestLiCollection(name = "corpGroups", namespace = "com.linkedin.identity", keyName = "corpGroup")
public final class CorpGroups extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<CorpGroupResourceKey, EmptyRecord>,
    CorpGroup,
    CorpGroupUrn,
    CorpGroupSnapshot,
    CorpGroupAspect,
    CorpGroupDocument> {
    // @formatter:on

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final Clock _clock = Clock.systemUTC();

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  @Inject
  @Named("searchService")
  private SearchService _searchService;

  public CorpGroups() {
    super(CorpGroupSnapshot.class, CorpGroupAspect.class);
  }

  @Override
  @Nonnull
  protected BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> getLocalDAO() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  protected CorpGroupUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return CorpGroupUrn.deserialize(urnString);
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  protected CorpGroupUrn toUrn(@Nonnull ComplexResourceKey<CorpGroupResourceKey, EmptyRecord> corpGroupKey) {
    return new CorpGroupUrn(corpGroupKey.getKey().getName());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<CorpGroupResourceKey, EmptyRecord> toKey(@Nonnull CorpGroupUrn urn) {
    return new ComplexResourceKey<>(new CorpGroupResourceKey().setName(urn.getGroupNameEntity()), new EmptyRecord());
  }

  @Override
  @Nonnull
  protected CorpGroup toValue(@Nonnull CorpGroupSnapshot snapshot) {
    final CorpGroup value = new CorpGroup().setName(snapshot.getUrn().getGroupNameEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof CorpGroupInfo) {
        value.setInfo(CorpGroupInfo.class.cast(aspect));
      }
    });
    return value;
  }

  @Override
  @Nonnull
  protected CorpGroupSnapshot toSnapshot(@Nonnull CorpGroup corpGroup, @Nonnull CorpGroupUrn urn) {
    final List<CorpGroupAspect> aspects = new ArrayList<>();
    if (corpGroup.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(CorpGroupAspect.class, corpGroup.getInfo()));
    }
    return ModelUtils.newSnapshot(CorpGroupSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<CorpGroup> get(@Nonnull ComplexResourceKey<CorpGroupResourceKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity = _entityService.getEntity(new CorpGroupUrn(
          key.getKey().getName()), projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getCorpGroupSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<CorpGroupResourceKey, EmptyRecord>, CorpGroup>> batchGet(
      @Nonnull Set<ComplexResourceKey<CorpGroupResourceKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Map<ComplexResourceKey<CorpGroupResourceKey, EmptyRecord>, CorpGroup> entities = new HashMap<>();
      for (final ComplexResourceKey<CorpGroupResourceKey, EmptyRecord> key : keys) {
        final Entity entity = _entityService.getEntity(
            new CorpGroupUrn(key.getKey().getName()),
            projectedAspects);
        if (entity != null) {
          entities.put(key, toValue(entity.getValue().getCorpGroupSnapshot()));
        }
      }
      return entities;
    });
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<CorpGroup, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final SearchResult searchResult = _searchService.search(
          "corpGroup",
          input,
          filter,
          sortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getCorpGroupSnapshot())).collect(
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
            "corpGroup",
            query,
            field,
            filter,
            limit)
    );  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull CorpGroupSnapshot snapshot) {
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
  public Task<CorpGroupSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
          return entity.getValue().getCorpGroupSnapshot();
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
