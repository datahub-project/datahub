package com.linkedin.metadata.search.elasticsearch.query.filter;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

public class QueryFilterRewriteChain {
  public static final QueryFilterRewriteChain EMPTY = new QueryFilterRewriteChain(List.of());
  private final List<QueryFilterRewriter> filterRewriters;

  public static QueryFilterRewriteChain of(@Nonnull QueryFilterRewriter... filters) {
    return new QueryFilterRewriteChain(Arrays.stream(filters).collect(Collectors.toList()));
  }

  public QueryFilterRewriteChain(List<QueryFilterRewriter> filterRewriters) {
    this.filterRewriters = filterRewriters;
  }

  public <T extends QueryBuilder> T rewrite(
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriterContext rewriterContext,
      @Nullable T filterQuery) {
    for (QueryFilterRewriter queryFilterRewriter : filterRewriters) {
      filterQuery = queryFilterRewriter.rewrite(opContext, rewriterContext, filterQuery);
    }
    return filterQuery;
  }
}
