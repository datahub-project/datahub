package com.linkedin.metadata.resources.glossary;

import com.linkedin.glossary.GlossaryNode;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryNodeKey;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.GlossaryNodeAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.GlossaryNodeInfoDocument;
import com.linkedin.metadata.snapshot.GlossaryNodeSnapshot;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linkedin.metadata.restli.RestliConstants.ACTION_AUTOCOMPLETE;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_BACKFILL;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_GET_SNAPSHOT;
import static com.linkedin.metadata.restli.RestliConstants.ACTION_INGEST;
import static com.linkedin.metadata.restli.RestliConstants.FINDER_SEARCH;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_ASPECTS;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_FIELD;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_FILTER;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_INPUT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_LIMIT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_QUERY;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_SNAPSHOT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_SORT;
import static com.linkedin.metadata.restli.RestliConstants.PARAM_URN;

@RestLiCollection(name = "glossaryNodes", namespace = "com.linkedin.glossary", keyName = "glossaryNode")
public final class GlossaryNodes extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<GlossaryNodeKey, EmptyRecord>,
    GlossaryNode,
    GlossaryNodeUrn,
    GlossaryNodeSnapshot,
    GlossaryNodeAspect,
    GlossaryNodeInfoDocument> {
    // @formatter:on

  public GlossaryNodes() {
    super(GlossaryNodeSnapshot.class, GlossaryNodeAspect.class);
  }

  @Inject
  @Named("glossaryNodeDao")
  private BaseLocalDAO<GlossaryNodeAspect, GlossaryNodeUrn> _localDAO;

  @Inject
  @Named("glossaryNodeSearchDAO")
  private BaseSearchDAO _esSearchDAO;

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

  @Nonnull
  @Override
  protected GlossaryNodeUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return GlossaryNodeUrn.createFromUrn(Urn.createFromString(urnString));
  }

  @Override
  @Nonnull
  protected GlossaryNodeUrn toUrn(@Nonnull ComplexResourceKey<GlossaryNodeKey, EmptyRecord> key) {
    return new GlossaryNodeUrn(key.getKey().getName());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<GlossaryNodeKey, EmptyRecord> toKey(@Nonnull GlossaryNodeUrn urn) {
    return new ComplexResourceKey<>(new GlossaryNodeKey().setName(urn.getNameEntity()), new EmptyRecord());
  }

  @Override
  @Nonnull
  protected GlossaryNode toValue(@Nonnull GlossaryNodeSnapshot snapshot) {
    final GlossaryNode value = new GlossaryNode()
            .setUrn(snapshot.getUrn());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof GlossaryNodeInfo) {
        value.setGlossaryNodeInfo(GlossaryNodeInfo.class.cast(aspect));
      }
      if (aspect instanceof Ownership) {
        value.setOwnership(Ownership.class.cast(aspect));
      }
    });
    return value;
  }

  @Override
  @Nonnull
  protected GlossaryNodeSnapshot toSnapshot(@Nonnull GlossaryNode glossaryNode, @Nonnull GlossaryNodeUrn glossaryNodeUrn) {
    final List<GlossaryNodeAspect> aspects = new ArrayList<>();
    if (glossaryNode.hasGlossaryNodeInfo()) {
      aspects.add(ModelUtils.newAspectUnion(GlossaryNodeAspect.class, glossaryNode.getGlossaryNodeInfo()));
    }
    if (glossaryNode.hasOwnership()) {
      aspects.add(ModelUtils.newAspectUnion(GlossaryNodeAspect.class, glossaryNode.getOwnership()));
    }
    return ModelUtils.newSnapshot(GlossaryNodeSnapshot.class, glossaryNodeUrn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<GlossaryNode> get(@Nonnull ComplexResourceKey<GlossaryNodeKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<GlossaryNodeKey, EmptyRecord>, GlossaryNode>> batchGet(
      @Nonnull Set<ComplexResourceKey<GlossaryNodeKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<GlossaryNode>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull GlossaryNodeSnapshot snapshot) {
    System.out.println("Snapshot :: " + snapshot);
    return super.ingest(snapshot);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<GlossaryNode, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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
  public Task<GlossaryNodeSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
}
