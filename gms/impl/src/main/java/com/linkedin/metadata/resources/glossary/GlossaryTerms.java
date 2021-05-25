package com.linkedin.metadata.resources.glossary;

import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.glossary.GlossaryTerm;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.common.Ownership;
import com.linkedin.glossary.GlossaryTermKey;
import com.linkedin.metadata.aspect.GlossaryTermAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.BaseBrowseDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseBrowsableEntityResource;
import com.linkedin.metadata.search.GlossaryTermInfoDocument;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import com.linkedin.data.template.StringArray;

import static com.linkedin.metadata.restli.RestliConstants.*;

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

  public GlossaryTerms() {
    super(GlossaryTermSnapshot.class, GlossaryTermAspect.class);
  }

  @Inject
  @Named("glossaryTermDao")
  private BaseLocalDAO<GlossaryTermAspect, GlossaryTermUrn> _localDAO;

  @Inject
  @Named("glossaryTermSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  @Inject
  @Named("glossaryTermBrowseDao")
  private BaseBrowseDAO _browseDAO;

  @Override
  @Nonnull
  protected BaseLocalDAO getLocalDAO() {
    return _localDAO;
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    return _esSearchDAO;
  }

  @Override
  @Nonnull
  protected BaseBrowseDAO getBrowseDAO() {
    return _browseDAO;
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
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<GlossaryTermKey, EmptyRecord>, GlossaryTerm>> batchGet(
      @Nonnull Set<ComplexResourceKey<GlossaryTermKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<GlossaryTerm>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull GlossaryTermSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<GlossaryTerm, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<GlossaryTermSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.getSnapshot(urnString, aspectNames);
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
