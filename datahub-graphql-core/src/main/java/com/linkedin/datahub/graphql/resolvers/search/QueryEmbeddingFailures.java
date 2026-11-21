package com.linkedin.datahub.graphql.resolvers.search;

import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.exception.QueryEmbeddingErrorCodes;
import com.linkedin.metadata.search.QueryEmbeddingException;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateNoAnchorException;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.function.FailableFunction;

/**
 * A search a vector rung may serve: semantic search through the semantic search template, or
 * keyword search through a template the caller names, such as {@code semantic}. A query the rung
 * could not embed is an error naming why, or, when the caller set {@code
 * SearchFlags.keywordFallbackOnEmbeddingFailure}, the keyword search results for the same request,
 * marked with that reason. Similarity embeds nothing, so it never falls back, and an anchor it
 * cannot use is a bad request.
 */
@Slf4j
public final class QueryEmbeddingFailures {

  private QueryEmbeddingFailures() {}

  /**
   * Runs {@code search} with {@code searching}. A similarity anchor the request cannot use is a bad
   * request. A query it could not embed fails typed, or, when the caller opted in, is answered by
   * {@code keywordSearch} without the template the caller named, which may be the vector template
   * that just failed. Any other failure propagates as it was.
   *
   * @param keywordSearch null when only {@code search} can serve the request: a keyword search that
   *     named no template would run the same vector rung again, which a slot or route chose
   */
  @Nonnull
  public static SearchResult searchWithKeywordFallback(
      @Nonnull final OperationContext searching,
      @Nonnull
          final FailableFunction<OperationContext, SearchResult, RemoteInvocationException> search,
      @Nullable
          final FailableFunction<OperationContext, SearchResult, RemoteInvocationException>
              keywordSearch)
      throws RemoteInvocationException {
    try {
      return search.apply(searching);
    } catch (RuntimeException e) {
      // The request, not the service, is at fault; the message does not say whether the entity
      // exists
      final SearchTemplateNoAnchorException noAnchor =
          ExceptionUtils.throwableOfType(e, SearchTemplateNoAnchorException.class);
      if (noAnchor != null) {
        throw new DataHubGraphQLException(
            noAnchor.getMessage(), DataHubGraphQLErrorCode.BAD_REQUEST, noAnchor);
      }
      final QueryEmbeddingException failure =
          ExceptionUtils.throwableOfType(e, QueryEmbeddingException.class);
      if (failure == null) {
        throw e;
      }
      if (keywordSearch == null
          || !Boolean.TRUE.equals(
              searching
                  .getSearchContext()
                  .getSearchFlags()
                  .isKeywordFallbackOnEmbeddingFailure())) {
        throw typed(failure);
      }
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
        // Without the caller's template the same vector rung ran again, as it does when a slot or
        // route selected it, so the embedding's own error stands
        if (ExceptionUtils.throwableOfType(keywordFailure, QueryEmbeddingException.class) != null) {
          final DataHubGraphQLException typed = typed(failure);
          typed.addSuppressed(keywordFailure);
          throw typed;
        }
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
      // The keyword fallback counter tracks each fallback, so this stays at debug: an embedding
      // outage would otherwise log a line per search
      log.debug(
          "Search could not embed the query ({}); served keyword results, as the caller opted in",
          failure.getReason(),
          failure);
      SearchTemplateMetrics.keywordFallback(searching, reasonTag(failure));
      result.getMetadata().setKeywordFallback(reasonTag(failure));
      return result;
    }
  }

  @Nonnull
  private static DataHubGraphQLException typed(@Nonnull final QueryEmbeddingException failure) {
    return new DataHubGraphQLException(
        failure.getMessage(), QueryEmbeddingErrorCodes.of(failure), failure);
  }

  /**
   * The reason a keyword fallback reports, as its metric tag and as {@code
   * SearchResultMetadata.keywordFallback}. An embedding service can set any reason, so one outside
   * the codes {@link QueryEmbeddingException} names counts as {@code failed}, which keeps both to
   * the documented codes.
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
