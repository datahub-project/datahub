package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.metadata.query.AutoCompleteResult;

import javax.annotation.Nonnull;


public class AutoCompleteResultsMapper implements ModelMapper<AutoCompleteResult, AutoCompleteResults> {

    public static final AutoCompleteResultsMapper INSTANCE = new AutoCompleteResultsMapper();

    public static AutoCompleteResults map(@Nonnull final AutoCompleteResult results) {
        return INSTANCE.apply(results);
    }

    @Override
    public AutoCompleteResults apply(@Nonnull final AutoCompleteResult input) {
        final AutoCompleteResults result = new AutoCompleteResults();
        result.setQuery(input.getQuery());
        result.setSuggestions(input.getSuggestions());
        return result;
    }
}
