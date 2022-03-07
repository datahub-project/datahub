package com.linkedin.datahub.graphql.resolvers;

import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
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
            .map(filter -> new Criterion().setField(filter.getField() + ESUtils.KEYWORD_SUFFIX).setValue(filter.getValue()))
            .collect(Collectors.toList())))));
    }
}
