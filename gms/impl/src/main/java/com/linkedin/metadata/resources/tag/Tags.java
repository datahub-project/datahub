package com.linkedin.metadata.resources.tag;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.TagAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.BaseSearchDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.metadata.restli.BackfillResult;
import com.linkedin.metadata.restli.BaseSearchableEntityResource;
import com.linkedin.metadata.search.TagDocument;
import com.linkedin.metadata.snapshot.TagSnapshot;
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
import com.linkedin.tag.Tag;
import com.linkedin.tag.TagKey;
import com.linkedin.tag.TagProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;

@RestLiCollection(name = "tags", namespace = "com.linkedin.tag", keyName = "tag")
public final class Tags extends BaseSearchableEntityResource<
        // @formatter:off
        ComplexResourceKey<TagKey, EmptyRecord>,
        Tag,
        TagUrn,
        TagSnapshot,
        TagAspect,
        TagDocument
        > {
    // @formatter:on

    @Inject
    @Named("tagDAO")
    private BaseLocalDAO<TagAspect, TagUrn> _localDAO;

    @Inject
    @Named("tagSearchDAO")
    private BaseSearchDAO _esSearchDAO;

    public Tags() {
        super(TagSnapshot.class, TagAspect.class);
    }

    @Override
    @Nonnull
    protected BaseLocalDAO getLocalDAO() {
        return _localDAO;
    }

    @Nonnull
    @Override
    protected BaseSearchDAO<TagDocument> getSearchDAO() {
        return _esSearchDAO;
    }

    @Nonnull
    @Override
    protected TagUrn createUrnFromString(@Nonnull String urnString) throws Exception {
        return TagUrn.createFromUrn(Urn.createFromString(urnString));
    }


    @Override
    @Nonnull
    protected TagUrn toUrn(@Nonnull ComplexResourceKey<TagKey, EmptyRecord> key) {
        return new TagUrn(key.getKey().getName());
    }

    @Override
    @Nonnull
    protected ComplexResourceKey<TagKey, EmptyRecord> toKey(@Nonnull TagUrn urn) {
        return new ComplexResourceKey<>(new TagKey().setName(urn.getName()), new EmptyRecord());
    }

    @Override
    @Nonnull
    protected Tag toValue(@Nonnull TagSnapshot snapshot) {
        final Tag value = new Tag().setName(snapshot.getUrn().getName());
        ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
            if (aspect instanceof TagProperties) {
                value.setDescription(TagProperties.class.cast(aspect).getDescription());
                value.setName(TagProperties.class.cast(aspect).getName());
            } else if (aspect instanceof Ownership) {
                value.setOwnership((Ownership) aspect);
            }
        });
        return value;
    }

    @Override
    @Nonnull
    protected TagSnapshot toSnapshot(@Nonnull Tag tag, @Nonnull TagUrn tagUrn) {
        final List<TagAspect> aspects = new ArrayList<>();
        if (tag.hasDescription()) {
            TagProperties tagProperties = new TagProperties();
            tagProperties.setDescription((tag.getDescription()));
            tagProperties.setName((tag.getName()));
            aspects.add(ModelUtils.newAspectUnion(TagAspect.class, tagProperties));
        }
        if (tag.hasOwnership()) {
            aspects.add(ModelUtils.newAspectUnion(TagAspect.class, tag.getOwnership()));
        }
        return ModelUtils.newSnapshot(TagSnapshot.class, tagUrn, aspects);
    }

    @RestMethod.Get
    @Override
    @Nonnull
    public Task<Tag> get(@Nonnull ComplexResourceKey<TagKey, EmptyRecord> key,
                              @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.get(key, aspectNames);
    }

    @RestMethod.BatchGet
    @Override
    @Nonnull
    public Task<Map<ComplexResourceKey<TagKey, EmptyRecord>, Tag>> batchGet(
            @Nonnull Set<ComplexResourceKey<TagKey, EmptyRecord>> keys,
            @QueryParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
        return super.batchGet(keys, aspectNames);
    }

    @RestMethod.GetAll
    @Nonnull
    public Task<List<Tag>> getAll(@PagingContextParam @Nonnull PagingContext pagingContext) {
        return super.getAll(pagingContext);
    }

    @Finder(FINDER_SEARCH)
    @Override
    @Nonnull
    public Task<CollectionResult<Tag, SearchResultMetadata>> search(@QueryParam(PARAM_INPUT) @Nonnull String input,
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
    public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull TagSnapshot snapshot) {
        return super.ingest(snapshot);
    }

    @Action(name = ACTION_GET_SNAPSHOT)
    @Override
    @Nonnull
    public Task<TagSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
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
