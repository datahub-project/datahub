package com.linkedin.datahub.graphql.types.tag;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.SetMode;
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
import com.linkedin.datahub.graphql.types.mappers.SearchResultsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagUpdateMapper;
import com.linkedin.metadata.configs.TagSearchConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.tag.client.Tags;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TagType implements com.linkedin.datahub.graphql.types.SearchableEntityType<Tag>, MutableType<TagUpdate> {

    private static final String DEFAULT_AUTO_COMPLETE_FIELD = "name";
    private static final TagSearchConfig TAG_SEARCH_CONFIG = new TagSearchConfig();

    private final Tags _tagClient;

    public TagType(final Tags tagClient) {
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
    public List<Tag> batchLoad(final List<String> urns, final QueryContext context) {

        final List<TagUrn> tagUrns = urns.stream()
                .map(this::getTagUrn)
                .collect(Collectors.toList());

        try {
            final Map<TagUrn, com.linkedin.tag.Tag> tagMap = _tagClient.batchGet(tagUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));

            final List<com.linkedin.tag.Tag> gmsResults = new ArrayList<>();
            for (TagUrn urn : tagUrns) {
                gmsResults.add(tagMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsTag -> gmsTag == null ? null : TagMapper.map(gmsTag))
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
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, TAG_SEARCH_CONFIG.getFacetFields());
        final CollectionResponse<com.linkedin.tag.Tag> searchResult = _tagClient.search(query, null, facetFilters, null, start, count);
        return SearchResultsMapper.map(searchResult, TagMapper::map);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, TAG_SEARCH_CONFIG.getFacetFields());
        final AutoCompleteResult result = _tagClient.autocomplete(query, field, facetFilters, limit);
        return AutoCompleteResultsMapper.map(result);
    }


    @Override
    public Tag update(@Nonnull TagUpdate input, @Nonnull QueryContext context) throws Exception {
        // TODO: Verify that updater is owner.
        final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
        final com.linkedin.tag.Tag partialTag = TagUpdateMapper.map(input, actor);

        // Create Audit Stamp
        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        if (partialTag.hasOwnership()) {
            partialTag.getOwnership().setLastModified(auditStamp);
        } else {
            final Ownership ownership = new Ownership();
            final Owner owner = new Owner();
            owner.setOwner(actor);
            owner.setType(OwnershipType.DATAOWNER);
            owner.setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE));

            ownership.setOwners(new OwnerArray(owner));
            ownership.setLastModified(auditStamp);
            partialTag.setOwnership(ownership);
        }

        partialTag.setLastModified(auditStamp);

        try {
            _tagClient.update(TagUrn.createFromString(input.getUrn()), partialTag);
        } catch (RemoteInvocationException e) {
            throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
        }

        return load(input.getUrn(), context);
    }

    private TagUrn getTagUrn(final String urnStr) {
        try {
            return TagUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve tag with urn %s, invalid urn", urnStr));
        }
    }
}
