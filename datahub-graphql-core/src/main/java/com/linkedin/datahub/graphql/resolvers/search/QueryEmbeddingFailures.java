package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.metadata.search.QueryEmbeddingException;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.function.FailableFunction;

/**
 * A search a vector rung may serve: semantic search through the semantic search template, or
 * keyword search through a template the caller names, such as {@code semantic}. A query the rung
 * could not embed is an error naming why, or, when the caller set {@code
 * SearchFlags.keywordFallbackOnEmbeddingFailure}, the keyword search results for the same request,
 * marked with that reason. Similarity embeds nothing, so it never falls back.
 */
@Slf4j
public final class QueryEmbeddingFailures {

  private QueryEmbeddingFailures() {}

  /**
   * Runs {@code search} with {@code searching}. A query it could not embed fails typed, or, when
   * the caller opted in, is answered by {@code keywordSearch} without the template the caller
   * named, which may be the vector template that just failed. Any other failure propagates as it
   * was.
   */
  @Nonnull
  public static SearchResult searchWithKeywordFallback(
      @Nonnull final OperationContext searching,
      @Nonnull
          final FailableFunction<OperationContext, SearchResult, RemoteInvocationException> search,
      @Nonnull
          final FailableFunction<OperationContext, SearchResult, RemoteInvocationException>
              keywordSearch)
      throws RemoteInvocationException {
    try {
      return search.apply(searching);
    } catch (RuntimeException e) {
      final QueryEmbeddingException failure =
          ExceptionUtils.throwableOfType(e, QueryEmbeddingException.class);
      if (failure == null) {
        throw e;
      }
      if (!Boolean.TRUE.equals(
          searching.getSearchContext().getSearchFlags().isKeywordFallbackOnEmbeddingFailure())) {
        throw new DataHubGraphQLException(failure.getMessage(), errorCode(failure), failure);
      }
      log.warn(
          "Search could not embed the query ({}); serving keyword results, as the caller opted in",
          failure.getReason());
      log.debug("The query embedding failure behind the keyword fallback", failure);
      SearchTemplateMetrics.keywordFallback(searching, reasonTag(failure));
      final SearchResult result;
      try {
        result =
            keywordSearch.apply(
                searching.withSearchFlags(
                    flags -> {
                      flags.removeSearchTemplate();
                      return flags;
                    }));
      } catch (RemoteInvocationException | RuntimeException keywordFailure) {
        // The fallback's own failure sets the code: the embedding's code may invite a retry, which
        // would fail the same way. The message and the suppressed failure keep the embedding's
        // reason for the log.
        keywordFailure.addSuppressed(failure);
        throw new DataHubGraphQLException(
            failure.getMessage()
                + ", and the keyword fallback failed: "
                + keywordFailure.getMessage(),
            ExceptionUtils.throwableOfType(keywordFailure, IllegalArgumentException.class) != null
                ? DataHubGraphQLErrorCode.BAD_REQUEST
                : DataHubGraphQLErrorCode.SERVER_ERROR,
            keywordFailure);
      }
      result.getMetadata().setKeywordFallback(failure.getReason());
      return result;
    }
  }

  /**
   * A query embedding that ran out of time is an exceeded deadline, and one the embedding service
   * throttled or budgeted out is a transient unavailability. Any other failure stays a server
   * error, since it may not clear on a retry.
   */
  @Nonnull
  public static DataHubGraphQLErrorCode errorCode(@Nonnull final QueryEmbeddingException failure) {
    return switch (failure.getReason()) {
      case QueryEmbeddingException.TIMEOUT -> DataHubGraphQLErrorCode.DEADLINE_EXCEEDED;
      case QueryEmbeddingException.REJECTED,
          QueryEmbeddingException.RESERVE_EXHAUSTED,
          QueryEmbeddingException.BUDGET_EXHAUSTED ->
          DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE;
      default -> DataHubGraphQLErrorCode.SERVER_ERROR;
    };
  }

  /**
   * The keyword fallback metric's reason tag. An embedding service can set any reason, so one
   * outside the codes {@link QueryEmbeddingException} names counts as {@code failed}, which keeps
   * the tag's values bounded.
   */
  private static String reasonTag(final QueryEmbeddingException failure) {
    return switch (failure.getReason()) {
      case QueryEmbeddingException.TIMEOUT,
          QueryEmbeddingException.REJECTED,
          QueryEmbeddingException.RESERVE_EXHAUSTED,
          QueryEmbeddingException.BUDGET_EXHAUSTED ->
          failure.getReason();
      default -> QueryEmbeddingException.FAILED;
    };
  }
}
