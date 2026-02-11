package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SearchRequestOptions {
  @Builder.Default String input = "*";
  @Nullable Filter filter;
  @Builder.Default List<SortCriterion> sortCriteria = Collections.emptyList();
  @Nullable SearchDocFieldFetchConfig fieldFetchConfig;
  @Nullable String pitId;
  @Nullable String keepAlive;
  @Builder.Default int from = 0;
  @Nullable Integer size;
  @Builder.Default List<String> facets = Collections.emptyList();
  @Nullable Object[] searchAfter;
  @Nullable Predicate predicate;
}
