package com.linkedin.datahub.graphql.types.tag;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.TagUpdateInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagSnapshotMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.TagUpdateInputSnapshotMapper;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
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

public class TagType implements com.linkedin.datahub.graphql.types.SearchableEntityType<Tag>, MutableType<TagUpdateInput> {

    private static final Set<String> FACET_FIELDS = Collections.emptySet();

    private final EntityClient _entityClient;

    public TagType(final EntityClient entityClient) {
        _entityClient = entityClient;
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
    public Class<TagUpdateInput> inputClass() {
        return TagUpdateInput.class;
    }

    @Override
    public List<DataFetcherResult<Tag>> batchLoad(final List<String> urns, final QueryContext context) {

        final List<TagUrn> tagUrns = urns.stream()
                .map(this::getTagUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> tagMap = _entityClient.batchGet(tagUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()),
                context.getActor());

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
        final SearchResult searchResult = _entityClient.search("tag", query, facetFilters, start, count, context.getActor());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _entityClient.autoComplete("tag", query, facetFilters, limit, context.getActor());
        return AutoCompleteResultsMapper.map(result);
    }


    @Override
    public Tag update(@Nonnull String urn, @Nonnull TagUpdateInput input, @Nonnull QueryContext context) throws Exception {
        if (isAuthorized(input, context)) {
            final CorpuserUrn actor = CorpuserUrn.createFromString(context.getActor());
            final TagSnapshot tagSnapshot = TagUpdateInputSnapshotMapper.map(input, actor);
            final Snapshot snapshot = Snapshot.create(tagSnapshot);
            try {
                Entity entity = new Entity();
                entity.setValue(snapshot);
                _entityClient.update(entity, context.getActor());
            } catch (RemoteInvocationException e) {
                throw new RuntimeException(String.format("Failed to write entity with urn %s", urn), e);
            }

            return load(urn, context).getData();
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    private TagUrn getTagUrn(final String urnStr) {
        try {
            return TagUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve tag with urn %s, invalid urn", urnStr));
        }
    }

    private boolean isAuthorized(@Nonnull TagUpdateInput update, @Nonnull QueryContext context) {
        // Decide whether the current principal should be allowed to update the Dataset.
        final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(update);
        return AuthorizationUtils.isAuthorized(
            context.getAuthorizer(),
            context.getActor(),
            PoliciesConfig.DATASET_PRIVILEGES.getResourceType(),
            update.getUrn(),
            orPrivilegeGroups);
    }

    private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final TagUpdateInput updateInput) {

        final ConjunctivePrivilegeGroup allPrivilegesGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
        ));

        List<String> specificPrivileges = new ArrayList<>();
        if (updateInput.getOwnership() != null) {
            specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_OWNERS_PRIVILEGE.getType());
        }
        if (updateInput.getDescription() != null || updateInput.getName() != null) {
            specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType());
        }
        final ConjunctivePrivilegeGroup specificPrivilegeGroup = new ConjunctivePrivilegeGroup(specificPrivileges);

        // If you either have all entity privileges, or have the specific privileges required, you are authorized.
        return new DisjunctivePrivilegeGroup(ImmutableList.of(
            allPrivilegesGroup,
            specificPrivilegeGroup
        ));
    }
}
