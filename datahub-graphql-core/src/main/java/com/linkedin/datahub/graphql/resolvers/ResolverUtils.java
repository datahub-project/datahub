package com.linkedin.datahub.graphql.resolvers;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;

import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.search.utils.ESUtils;
import graphql.schema.DataFetchingEnvironment;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResolverUtils {

    private static final Set<String> KEYWORD_EXCLUDED_FILTERS = ImmutableSet.of(
        "runId"
    );
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger _logger = LoggerFactory.getLogger(ResolverUtils.class.getName());

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
    public static Authentication getAuthentication(DataFetchingEnvironment environment) {
        return ((QueryContext) environment.getContext()).getAuthentication();
    }

    /**
     * @apiNote DO NOT use this method if the facet filters do not include `.keyword` suffix to ensure
     * that it is matched against a keyword filter in ElasticSearch.
     *
     * @param facetFilterInputs The list of facet filters inputs
     * @param validFacetFields  The set of valid fields against which to filter for.
     * @return A map of filter definitions to be used in ElasticSearch.
     */
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

    @Nullable
    public static Filter buildFilter(@Nullable List<FacetFilterInput> facetFilterInputs) {
        if (facetFilterInputs == null || facetFilterInputs.isEmpty()) {
            return null;
        }
        return new Filter().setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(facetFilterInputs.stream()
            .map(filter -> new Criterion().setField(getFilterField(filter.getField())).setValue(filter.getValue()))
            .collect(Collectors.toList())))));
    }

    private static String getFilterField(final String originalField) {
        if (KEYWORD_EXCLUDED_FILTERS.contains(originalField)) {
            return originalField;
        }
        return originalField + ESUtils.KEYWORD_SUFFIX;
    }
}
