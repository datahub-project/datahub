package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

/** Query plus optional Search V3 template request and extraction metadata. */
public record SearchQueryPlan(
    @Nonnull QueryBuilder query, @Nullable RenderedSearchTemplate template) {

  @Nonnull
  public static SearchQueryPlan legacy(@Nonnull QueryBuilder query) {
    return new SearchQueryPlan(query, null);
  }
}
