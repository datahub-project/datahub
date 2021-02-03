package com.linkedin.datahub.graphql.resolvers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ResolverUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ResolverUtils() { }

    @Nonnull
    public static <T> T bindArgument(Object argument, Class<T> clazz) {
        return MAPPER.convertValue(argument, clazz);
    }

    /**
     * Returns the string with the forward slash escaped
     * More details on reserved characters in Elasticsearch can be found at,
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
     */
    @Nonnull
    public static String escapeForwardSlash(@Nonnull String input) {
        if (input.contains("/")) {
            input = input.replace("/", "\\\\/");
        }
        return input;
    }

    @Nonnull
    public static Map<String, String> buildFacetFilters(@Nullable List<FacetFilterInput> facetFilterInputs,
                                                        @Nonnull Set<String> validFacetFields) {
        if (facetFilterInputs == null) {
            return Collections.emptyMap();
        }

        final Map<String, String> facetFilters = new HashMap<>();

        facetFilterInputs.forEach(facetFilterInput -> {
            if (!validFacetFields.contains(facetFilterInput.getField())) {
                throw new ValidationException(String.format("Unrecognized facet with name %s provided", facetFilterInput.getField()));
            }
            facetFilters.put(facetFilterInput.getField(), facetFilterInput.getValue());
        });

        return facetFilters;
    }
}
