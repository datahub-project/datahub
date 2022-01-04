package com.linkedin.datahub.graphql.types.glossary;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermSnapshotMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermUpdateInputSnapshotMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.extractor.AspectExtractor;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;

import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;

public class GlossaryTermType implements SearchableEntityType<GlossaryTerm>, BrowsableEntityType<GlossaryTerm>, MutableType<GlossaryTermUpdateInput> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("");

    private final EntityClient _entityClient;

    public GlossaryTermType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<GlossaryTerm> objectClass() {
        return GlossaryTerm.class;
    }

    @Override
    public EntityType type() {
        return EntityType.GLOSSARY_TERM;
    }

    @Override
    public List<DataFetcherResult<GlossaryTerm>> batchLoad(final List<String> urns, final QueryContext context) {
        final List<GlossaryTermUrn> glossaryTermUrns = urns.stream()
                .map(GlossaryTermUtils::getGlossaryTermUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, Entity> glossaryTermMap = _entityClient.batchGet(glossaryTermUrns
                    .stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()),
                context.getAuthentication());

            final List<Entity> gmsResults = new ArrayList<>();
            for (GlossaryTermUrn urn : glossaryTermUrns) {
                gmsResults.add(glossaryTermMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsGlossaryTerm ->
                        gmsGlossaryTerm == null ? null
                            : DataFetcherResult.<GlossaryTerm>newResult()
                                .data(GlossaryTermSnapshotMapper.map(gmsGlossaryTerm.getValue().getGlossaryTermSnapshot()))
                                .localContext(AspectExtractor.extractAspects(gmsGlossaryTerm.getValue().getGlossaryTermSnapshot()))
                                .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load GlossaryTerms", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _entityClient.search(
            "glossaryTerm", query, facetFilters, start, count, context.getAuthentication());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _entityClient.autoComplete(
            "glossaryTerm", query, facetFilters, limit, context.getAuthentication());
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _entityClient.browse(
                "glossaryTerm",
                pathStr,
                facetFilters,
                start,
                count,
            context.getAuthentication());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _entityClient.getBrowsePaths(GlossaryTermUtils.getGlossaryTermUrn(urn), context.getAuthentication());
        return BrowsePathsMapper.map(result);
    }

    @Override
    public Class<GlossaryTermUpdateInput> inputClass() {
        return GlossaryTermUpdateInput.class;
    }

    @Override
    public GlossaryTerm update(@Nonnull String urn, @Nonnull GlossaryTermUpdateInput input, @Nonnull QueryContext context) throws Exception {
        if (isAuthorized(urn, input, context)) {
            final CorpuserUrn actor = CorpuserUrn.createFromString(context.getAuthentication().getActor().toUrnStr());
            final GlossaryTermSnapshot glossaryTermSnapshot = GlossaryTermUpdateInputSnapshotMapper.map(input, actor);
            final Snapshot snapshot = Snapshot.create(glossaryTermSnapshot);

            try {
                Entity entity = new Entity();
                entity.setValue(snapshot);
                _entityClient.update(entity, context.getAuthentication());
            } catch (RemoteInvocationException e) {
                throw new RuntimeException(String.format("Failed to write entity with urn %s", input.getUrn()), e);
            }

            return load(urn, context).getData();
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your Data workbench support.");
    }

    private boolean isAuthorized(@Nonnull String urn, @Nonnull GlossaryTermUpdateInput update, @Nonnull QueryContext context) {
        // Decide whether the current principal should be allowed to update the Dataset.
        final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(update);
        return AuthorizationUtils.isAuthorized(
                context.getAuthorizer(),
                context.getAuthentication().getActor().toUrnStr(),
                PoliciesConfig.GLOSSARY_TERM_PRIVILEGES.getResourceType(),
                urn,
                orPrivilegeGroups);
    }

    private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final GlossaryTermUpdateInput updateInput) {

        final ConjunctivePrivilegeGroup allPrivilegesGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
        ));

        List<String> specificPrivileges = new ArrayList<>();

        if (updateInput.getEditableSchemaMetadata() != null) {
            specificPrivileges.add(PoliciesConfig.EDIT_GLOSSARY_TERM_COL_TAGS_PRIVILEGE.getType());
            specificPrivileges.add(PoliciesConfig.EDIT_GLOSSARY_TERM_COL_DESCRIPTION_PRIVILEGE.getType());
        }

        final ConjunctivePrivilegeGroup specificPrivilegeGroup = new ConjunctivePrivilegeGroup(specificPrivileges);

        // If you either have all entity privileges, or have the specific privileges required, you are authorized.
        return new DisjunctivePrivilegeGroup(ImmutableList.of(
                allPrivilegesGroup,
                specificPrivilegeGroup
        ));
    }

}
