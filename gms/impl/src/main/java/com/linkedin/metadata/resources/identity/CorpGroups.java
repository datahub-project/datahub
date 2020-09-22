package com.linkedin.metadata.resources.identity;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroup;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.CorpGroupDocument;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
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


@RestLiCollection(name = "corpGroups", namespace = "com.linkedin.identity", keyName = "corpGroup")
public final class CorpGroups extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<CorpGroupKey, EmptyRecord>,
    CorpGroup,
    CorpGroupUrn,
    CorpGroupSnapshot,
    CorpGroupAspect,
    CorpGroupDocument> {
    // @formatter:on

  @Inject
  @Named("corpGroupDao")
  private BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> _localDAO;

  @Inject
  @Named("corpGroupSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  public CorpGroups() {
    super(CorpGroupSnapshot.class, CorpGroupAspect.class);
  }

  @Override
  @Nonnull
  protected BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> getLocalDAO() {
    return _localDAO;
  }

  @Override
  @Nonnull
  protected CorpGroupUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return CorpGroupUrn.deserialize(urnString);
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    return _esSearchDAO;
  }

  @Override
  @Nonnull
  protected CorpGroupUrn toUrn(@Nonnull ComplexResourceKey<CorpGroupKey, EmptyRecord> corpGroupKey) {
    return new CorpGroupUrn(corpGroupKey.getKey().getName());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<CorpGroupKey, EmptyRecord> toKey(@Nonnull CorpGroupUrn urn) {
    return new ComplexResourceKey<>(new CorpGroupKey().setName(urn.getGroupNameEntity()), new EmptyRecord());
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
  public Task<CorpGroup> get(@Nonnull ComplexResourceKey<CorpGroupKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<CorpGroupKey, EmptyRecord>, CorpGroup>> batchGet(
      @Nonnull Set<ComplexResourceKey<CorpGroupKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<CorpGroup, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull CorpGroupSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<CorpGroupSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
