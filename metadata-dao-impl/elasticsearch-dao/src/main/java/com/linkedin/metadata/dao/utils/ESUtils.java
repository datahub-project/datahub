package com.linkedin.metadata.dao.utils;

import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nonnull;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


public class ESUtils {

  private ESUtils() {

  }

  /**
   * Constructs the filter query given filter map
   *
   * Multiple values can be selected for a filter, and it is currently modeled as string separated by comma
   *
   * @param requestMap the search request map with fields and its values
   * @return built filters
   */
  @Nonnull
  public static BoolQueryBuilder buildFilterQuery(@Nonnull Map<String, String> requestMap) {
    BoolQueryBuilder boolFilter = new BoolQueryBuilder();
    for (Map.Entry<String, String> entry : requestMap.entrySet()) {
      BoolQueryBuilder filters = new BoolQueryBuilder();
      Arrays.stream(entry.getValue().split(","))
          .forEach(elem -> filters.should(QueryBuilders.matchQuery(entry.getKey(), elem)));
      boolFilter.must(filters);
    }
    return boolFilter;
  }
}
