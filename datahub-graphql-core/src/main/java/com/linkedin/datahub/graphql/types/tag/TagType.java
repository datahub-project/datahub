package com.linkedin.datahub.graphql.types.tag;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.generated.TagUpdate;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagSnapshotMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagUpdateSnapshotMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchResult;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.snapshot.TagSnapshot;
import com.linkedin.r2.RemoteInvocationException;

import graphql.execution.DataFetcherResult;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TagType implements com.linkedin.datahub.graphql.types.SearchableEntityType<Tag>, MutableType<TagUpdate> {

    private static final Set<String> FACET_FIELDS = Collections.emptySet();

    private final EntityClient _tagClient;

    public TagType(final EntityClient tagClient) {
        _tagClient = tagClient;
    }

    @Override
    public Class<Tag> objectClass() {
        return Tag.class;
    }

    @Override
    public EntityType type() {
        return EntityType.TAG;
    }

    @Override
    public Class<TagUpdate> inputClass() {
        return TagUpdate.class;
    }

    @Override
    public List<DataFetcherResult<Tag>> batchLoad(final List<String> urns, final QueryContext context) {

        final List<TagUrn> tagUrns = urns.stream()
                .map(this::getTagUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> tagMap = _tagClient.batchGet(tagUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<Entity> gmsResults = new ArrayList<>();
            for (TagUrn urn : tagUrns) {
                gmsResults.add(tagMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsTag -> gmsTag == null ? null
                        : DataFetcherResult.<Tag>newResult()
                            .data(TagSnapshotMapper.map(gmsTag.getValue().getTagSnapshot()))
                            .localContext(AspectExtractor.extractAspects(gmsTag.getValue().getTagSnapshot()))
                            .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Tags", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _tagClient.search("tag", query, facetFilters, start, count);
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _tagClient.autoComplete("tag", query, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }


    @Override
    public Tag update(@Nonnull TagUpdate input, @Nonnull QueryContext context) throws Exception {
        // TODO: Verify that updater is owner.
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final TagSnapshot tagSnapshot = TagUpdateSnapshotMapper.map(input, actor);
        final Snapshot snapshot = Snapshot.create(tagSnapshot);
        try {
            Entity entity = new Entity();
            entity.setValue(snapshot);
            _tagClient.update(entity);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context).getData();
    }

    private TagUrn getTagUrn(final String urnStr) {
        try {
            return TagUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve tag with urn %s, invalid urn", urnStr));
        }
    }
}
