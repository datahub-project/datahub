package com.linkedin.metadata.resources.glossary;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.glossary.GlossaryTerm;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.common.Ownership;
import com.linkedin.glossary.GlossaryTermKey;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.GlossaryTermAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.query.SortOrder;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseBrowsableEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.search.GlossaryTermInfoDocument;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.metadata.query.Filter;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.metadata.query.BrowseResult;
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
import com.linkedin.data.template.StringArray;

import static com.linkedin.metadata.restli.RestliConstants.*;

/**
 * Deprecated! Use {@link EntityResource} instead.
 */
@Deprecated
@RestLiCollection(name = "glossaryTerms", namespace = "com.linkedin.glossary", keyName = "glossaryTerm")
public final class GlossaryTerms extends BaseBrowsableEntityResource<
    // @formatter:off
    ComplexResourceKey<GlossaryTermKey, EmptyRecord>,
    GlossaryTerm,
    GlossaryTermUrn,
    GlossaryTermSnapshot,
    GlossaryTermAspect,
    GlossaryTermInfoDocument> {
    // @formatter:on

  private static final String DEFAULT_ACTOR = "urn:li:principal:UNKNOWN";
  private final Clock _clock = Clock.systemUTC();

  public GlossaryTerms() {
    super(GlossaryTermSnapshot.class, GlossaryTermAspect.class);
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
  protected GlossaryTermUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return GlossaryTermUrn.createFromUrn(Urn.createFromString(urnString));
  }

  @Override
  @Nonnull
  protected GlossaryTermUrn toUrn(@Nonnull ComplexResourceKey<GlossaryTermKey, EmptyRecord> key) {
    return new GlossaryTermUrn(key.getKey().getName());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<GlossaryTermKey, EmptyRecord> toKey(@Nonnull GlossaryTermUrn urn) {
    return new ComplexResourceKey<>(new GlossaryTermKey().setName(urn.getNameEntity()), new EmptyRecord());
  }

  @Override
  @Nonnull
  protected GlossaryTerm toValue(@Nonnull GlossaryTermSnapshot snapshot) {
    final GlossaryTerm value = new GlossaryTerm()
            .setUrn(snapshot.getUrn());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof GlossaryTermInfo) {
        value.setGlossaryTermInfo(GlossaryTermInfo.class.cast(aspect));
      }
      if (aspect instanceof Ownership) {
        value.setOwnership(Ownership.class.cast(aspect));
      }
    });
    return value;
  }

  @Override
  @Nonnull
  protected GlossaryTermSnapshot toSnapshot(@Nonnull GlossaryTerm glossaryTerm, @Nonnull GlossaryTermUrn glossaryTermUrn) {
    final List<GlossaryTermAspect> aspects = new ArrayList<>();
    if (glossaryTerm.hasGlossaryTermInfo()) {
      aspects.add(ModelUtils.newAspectUnion(GlossaryTermAspect.class, glossaryTerm.getGlossaryTermInfo()));
    }
    if (glossaryTerm.hasOwnership()) {
      aspects.add(ModelUtils.newAspectUnion(GlossaryTermAspect.class, glossaryTerm.getOwnership()));
    }
    return ModelUtils.newSnapshot(GlossaryTermSnapshot.class, glossaryTermUrn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<GlossaryTerm> get(@Nonnull ComplexResourceKey<GlossaryTermKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity = _entityService.getEntity(new GlossaryTermUrn(
          key.getKey().getName()), projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getGlossaryTermSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<GlossaryTermKey, EmptyRecord>, GlossaryTerm>> batchGet(
      @Nonnull Set<ComplexResourceKey<GlossaryTermKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final Map<ComplexResourceKey<GlossaryTermKey, EmptyRecord>, GlossaryTerm> entities = new HashMap<>();
      for (final ComplexResourceKey<GlossaryTermKey, EmptyRecord> key : keys) {
        final Entity entity = _entityService.getEntity(
            new GlossaryTermUrn(key.getKey().getName()),
            projectedAspects);

        if (entity != null) {
          entities.put(key, toValue(entity.getValue().getGlossaryTermSnapshot()));
        }
      }
      return entities;
    });  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<GlossaryTerm>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
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
          "glossaryTerm",
          searchFilter,
          searchSortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(filterResults.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getGlossaryTermSnapshot())).collect(
              Collectors.toList()),
          filterResults.getNumEntities(),
          filterResults.getMetadata().setUrns(new UrnArray(urns))
      ).getElements();
    });
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull GlossaryTermSnapshot snapshot) {
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

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<GlossaryTerm, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
                                                                       @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
                                                                       @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
                                                                       @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion,
                                                                       @PagingContextParam @Nonnull PagingContext pagingContext) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {

      final SearchResult searchResult = _searchService.search(
          "glossaryTerm",
          input,
          filter,
          sortCriterion,
          pagingContext.getStart(),
          pagingContext.getCount());

      final Set<Urn> urns = new HashSet<>(searchResult.getEntities());
      final Map<Urn, Entity> entity = _entityService.getEntities(urns, projectedAspects);

      return new CollectionResult<>(
          entity.keySet().stream().map(urn -> toValue(entity.get(urn).getValue().getGlossaryTermSnapshot())).collect(
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
            "glossaryTerm",
            query,
            field,
            filter,
            limit)
    );  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<GlossaryTermSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
          return entity.getValue().getGlossaryTermSnapshot();
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
}
