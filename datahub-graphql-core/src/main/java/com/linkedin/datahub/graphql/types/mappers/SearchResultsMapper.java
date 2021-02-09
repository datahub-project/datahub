package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.AggregationMetadata;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.restli.common.CollectionResponse;

import javax.annotation.Nonnull;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SearchResultsMapper<T extends RecordTemplate, E extends Entity> {

    public static <T extends RecordTemplate, E extends Entity> SearchResults map(
            @Nonnull final CollectionResponse<T> results,
            @Nonnull final Function<T, E> elementMapper) {
        return new SearchResultsMapper<T, E>().apply(results, elementMapper);
    }

    public SearchResults apply(@Nonnull final CollectionResponse<T> input,
                               @Nonnull final Function<T, E> elementMapper) {
        final SearchResults result = new SearchResults();

        if (!input.hasPaging()) {
            throw new RuntimeException("Invalid search response received. Unable to find paging details.");
        }
        result.setStart(input.getPaging().getStart());
        result.setCount(input.getPaging().getCount());
        result.setTotal(input.getPaging().getTotal());
        result.setEntities(input.getElements().stream().map(elementMapper::apply).collect(Collectors.toList()));

        final SearchResultMetadata searchResultMetadata = new SearchResultMetadata(input.getMetadataRaw());
        result.setFacets(searchResultMetadata.getSearchResultMetadatas().stream().map(this::mapFacet).collect(Collectors.toList()));

        return result;
    }

    private FacetMetadata mapFacet(com.linkedin.metadata.query.AggregationMetadata aggregationMetadata) {
        final FacetMetadata facetMetadata = new FacetMetadata();
        facetMetadata.setField(aggregationMetadata.getName());
        facetMetadata.setAggregations(aggregationMetadata.getAggregations()
                .entrySet()
                .stream()
                .map(entry -> new AggregationMetadata(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
        return facetMetadata;
    }
}
