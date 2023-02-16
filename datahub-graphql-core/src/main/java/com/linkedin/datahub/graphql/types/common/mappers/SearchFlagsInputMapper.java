package com.linkedin.datahub.graphql.types.common.mappers;


import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps GraphQL SearchFlags to Pegasus
 *
 * To be replaced by auto-generated mappers implementations
 */
public class SearchFlagsInputMapper implements ModelMapper<SearchFlags, com.linkedin.metadata.query.SearchFlags> {

  public static final SearchFlagsInputMapper INSTANCE = new SearchFlagsInputMapper();

  public static com.linkedin.metadata.query.SearchFlags map(@Nonnull final SearchFlags searchFlags) {
    return INSTANCE.apply(searchFlags);
  }

  @Override
  public com.linkedin.metadata.query.SearchFlags apply(@Nonnull final SearchFlags searchFlags) {
    com.linkedin.metadata.query.SearchFlags result = new com.linkedin.metadata.query.SearchFlags();
    if (searchFlags.getFulltext() != null) {
      result.setFulltext(searchFlags.getFulltext());
    }
    if (searchFlags.getSkipCache() != null) {
      result.setSkipCache(searchFlags.getSkipCache());
    }
    if (searchFlags.getMaxAggValues() != null) {
      result.setMaxAggValues(searchFlags.getMaxAggValues());
    }
    return result;
  }
}
