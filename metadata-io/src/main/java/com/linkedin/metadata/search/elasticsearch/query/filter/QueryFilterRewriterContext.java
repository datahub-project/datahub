package com.linkedin.metadata.search.elasticsearch.query.filter;

import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.AUTOCOMPLETE;
import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.FULLTEXT_SEARCH;
import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.STRUCTURED_SEARCH;
import static com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriterSearchType.TIMESERIES;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.index.query.QueryBuilder;

@Builder
@Getter
public class QueryFilterRewriterContext {
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  @Nonnull private final QueryFilterRewriterSearchType searchType;
  @Nullable private final Condition condition;
  @Nullable private final SearchFlags searchFlags;

  public <T extends QueryBuilder> T rewrite(
      @Nonnull OperationContext opContext, @Nullable T filterQuery) {
    return queryFilterRewriteChain.rewrite(opContext, this, filterQuery);
  }

  public static class QueryFilterRewriterContextBuilder {
    private QueryFilterRewriterContext build() {
      return null;
    }

    public QueryFilterRewriterContext build(boolean isTimeseries) {
      if (this.searchType == null) {
        if (isTimeseries) {
          this.searchType = TIMESERIES;
        } else if (this.searchFlags != null) {
          this.searchType = this.searchFlags.isFulltext() ? FULLTEXT_SEARCH : STRUCTURED_SEARCH;
        } else {
          this.searchType = AUTOCOMPLETE;
        }
      }

      return new QueryFilterRewriterContext(
          this.queryFilterRewriteChain, this.searchType, this.condition, this.searchFlags);
    }
  }
}
