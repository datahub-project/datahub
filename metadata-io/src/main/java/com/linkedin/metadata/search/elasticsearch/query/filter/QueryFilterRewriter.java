package com.linkedin.metadata.search.elasticsearch.query.filter;

import com.linkedin.metadata.query.SearchFlags;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

public interface QueryFilterRewriter {

  <T extends QueryBuilder> T rewrite(
      @Nonnull OperationContext opContext,
      @Nonnull QueryFilterRewriterContext rewriterContext,
      @Nullable T filterQuery);

  @Nonnull
  Set<String> getRewriterFieldNames();

  @Nonnull
  Set<QueryFilterRewriterSearchType> getRewriterSearchTypes();

  default boolean isQueryTimeEnabled(
      @Nonnull QueryFilterRewriterContext queryFilterRewriterContext) {
    return isQueryTimeEnabled(
        queryFilterRewriterContext.getSearchType(), queryFilterRewriterContext.getSearchFlags());
  }

  default boolean isQueryTimeEnabled(
      @Nonnull QueryFilterRewriterSearchType rewriteSearchType, @Nullable SearchFlags searchFlags) {
    return getRewriterSearchTypes().contains(rewriteSearchType) && searchFlags == null
        || searchFlags.isRewriteQuery() == null
        || Boolean.TRUE.equals(searchFlags.isRewriteQuery());
  }
}
