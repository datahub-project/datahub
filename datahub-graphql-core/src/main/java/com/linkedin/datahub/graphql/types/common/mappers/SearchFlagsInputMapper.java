package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.query.GroupingCriterionArray;
import com.linkedin.metadata.query.GroupingSpec;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps GraphQL SearchFlags to Pegasus
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class SearchFlagsInputMapper
    implements ModelMapper<SearchFlags, com.linkedin.metadata.query.SearchFlags> {

  public static final SearchFlagsInputMapper INSTANCE = new SearchFlagsInputMapper();

  public static com.linkedin.metadata.query.SearchFlags map(
      @Nullable QueryContext context, @Nonnull final SearchFlags searchFlags) {
    return INSTANCE.apply(context, searchFlags);
  }

  @Override
  public com.linkedin.metadata.query.SearchFlags apply(
      @Nullable QueryContext context, @Nonnull final SearchFlags searchFlags) {
    com.linkedin.metadata.query.SearchFlags result = new com.linkedin.metadata.query.SearchFlags();
    if (searchFlags.getFulltext() != null) {
      result.setFulltext(searchFlags.getFulltext());
    } else {
      result.setFulltext(true);
    }
    if (searchFlags.getSkipCache() != null) {
      result.setSkipCache(searchFlags.getSkipCache());
    }
    if (searchFlags.getMaxAggValues() != null) {
      result.setMaxAggValues(searchFlags.getMaxAggValues());
    }
    if (searchFlags.getSkipHighlighting() != null) {
      result.setSkipHighlighting(searchFlags.getSkipHighlighting());
    }
    if (searchFlags.getSkipAggregates() != null) {
      result.setSkipAggregates(searchFlags.getSkipAggregates());
    }
    if (searchFlags.getGetSuggestions() != null) {
      result.setGetSuggestions(searchFlags.getGetSuggestions());
    }
    if (searchFlags.getIncludeSoftDeleted() != null) {
      result.setIncludeSoftDeleted(searchFlags.getIncludeSoftDeleted());
    }
    if (searchFlags.getIncludeRestricted() != null) {
      result.setIncludeRestricted(searchFlags.getIncludeRestricted());
    }
    if (searchFlags.getGroupingSpec() != null
        && searchFlags.getGroupingSpec().getGroupingCriteria() != null) {
      result.setGroupingSpec(
          new GroupingSpec()
              .setGroupingCriteria(
                  new GroupingCriterionArray(
                      searchFlags.getGroupingSpec().getGroupingCriteria().stream()
                          .map(c -> GroupingCriterionInputMapper.map(context, c))
                          .collect(Collectors.toList()))));
    }
    if (searchFlags.getCustomHighlightingFields() != null) {
      result.setCustomHighlightingFields(
          new StringArray(searchFlags.getCustomHighlightingFields()));
    }
    if (searchFlags.getFilterNonLatestVersions() != null) {
      result.setFilterNonLatestVersions(searchFlags.getFilterNonLatestVersions());
    }
    return result;
  }
}
