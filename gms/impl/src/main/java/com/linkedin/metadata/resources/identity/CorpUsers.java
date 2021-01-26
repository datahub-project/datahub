package com.linkedin.metadata.resources.identity;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserKey;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
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

@RestLiCollection(name = "corpUsers", namespace = "com.linkedin.identity", keyName = "corpUser")
public final class CorpUsers extends BaseSearchableEntityResource<
    // @formatter:off
    ComplexResourceKey<CorpUserKey, EmptyRecord>,
    CorpUser,
    CorpuserUrn,
    CorpUserSnapshot,
    CorpUserAspect,
    CorpUserInfoDocument> {
    // @formatter:on

  @Inject
  @Named("corpUserDao")
  private BaseLocalDAO<CorpUserAspect, CorpuserUrn> _localDAO;

  @Inject
  @Named("corpUserSearchDAO")
  private BaseSearchDAO _esSearchDAO;

  public CorpUsers() {
    super(CorpUserSnapshot.class, CorpUserAspect.class);
  }

  @Override
  @Nonnull
  protected BaseLocalDAO getLocalDAO() {
    return _localDAO;
  }

  @Nonnull
  @Override
  protected CorpuserUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return CorpuserUrn.createFromUrn(Urn.createFromString(urnString));
  }

  @Override
  @Nonnull
  protected BaseSearchDAO getSearchDAO() {
    return _esSearchDAO;
  }

  @Override
  @Nonnull
  protected CorpuserUrn toUrn(@Nonnull ComplexResourceKey<CorpUserKey, EmptyRecord> key) {
    return new CorpuserUrn(key.getKey().getName());
  }

  @Override
  @Nonnull
  protected ComplexResourceKey<CorpUserKey, EmptyRecord> toKey(@Nonnull CorpuserUrn urn) {
    return new ComplexResourceKey<>(new CorpUserKey().setName(urn.getUsernameEntity()), new EmptyRecord());
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
    return ModelUtils.newSnapshot(CorpUserSnapshot.class, corpuserUrn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<CorpUser> get(@Nonnull ComplexResourceKey<CorpUserKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<CorpUserKey, EmptyRecord>, CorpUser>> batchGet(
      @Nonnull Set<ComplexResourceKey<CorpUserKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<List<CorpUser>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext,
      @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames,
      @QueryParam(PARAM_FILTER) @Optional @Nullable Filter filter,
      @QueryParam(PARAM_SORT) @Optional @Nullable SortCriterion sortCriterion) {
    return super.getAll(pagingContext, aspectNames, filter, sortCriterion);
  }

  @Finder(FINDER_SEARCH)
  @Override
  @Nonnull
  public Task<CollectionResult<CorpUser, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull CorpUserSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<CorpUserSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
