package com.linkedin.datahub.graphql.types.glossary;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.Constants.*;
import static com.linkedin.metadata.Constants.*;

public class GlossaryTermType implements SearchableEntityType<GlossaryTerm, String>,
                                         BrowsableEntityType<GlossaryTerm, String> {

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("");

    private static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
        GLOSSARY_TERM_KEY_ASPECT_NAME,
        GLOSSARY_TERM_INFO_ASPECT_NAME,
        GLOSSARY_RELATED_TERM_ASPECT_NAME,
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        OWNERSHIP_ASPECT_NAME,
        STATUS_ASPECT_NAME,
        BROWSE_PATHS_ASPECT_NAME,
        DEPRECATION_ASPECT_NAME
    );

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
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public List<DataFetcherResult<GlossaryTerm>> batchLoad(final List<String> urns, final QueryContext context) {
        final List<Urn> glossaryTermUrns = urns.stream()
                .map(UrnUtils::getUrn)
                .collect(Collectors.toList());

        try {
            final Map<Urn, EntityResponse> glossaryTermMap = _entityClient.batchGetV2(GLOSSARY_TERM_ENTITY_NAME,
                new HashSet<>(glossaryTermUrns), ASPECTS_TO_RESOLVE, context.getAuthentication());

            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : glossaryTermUrns) {
                gmsResults.add(glossaryTermMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsGlossaryTerm ->
                        gmsGlossaryTerm == null ? null
                            : DataFetcherResult.<GlossaryTerm>newResult()
                                .data(GlossaryTermMapper.map(gmsGlossaryTerm))
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

}
