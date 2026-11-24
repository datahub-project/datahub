package com.linkedin.metadata.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.OptionalInt;
import javax.annotation.Nonnull;
import org.apache.http.ConnectionClosedException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.ResponseException;

/**
 * Shared classification of transient vs permanent search-transport failures for V3 index
 * operations, so read-time hydration and write-time index-existence checks agree on what a
 * redelivery can recover. Transient failures escalate to {@link RetryableIndexingException} (the
 * MAE transports then redeliver instead of committing); everything else stays permanent and is
 * committed at-most-once.
 *
 * <p>Transient means the request never got a usable answer from a reachable, healthy server: HTTP
 * 429 (too many requests) and 5xx (server-side), plus connection resets/refused/closed, missing
 * responses, and socket timeouts — the common Apache-client transport failures. Explicitly NOT
 * transient: 4xx client errors (auth, mapping, malformed request), which are configuration failures
 * a retry would only repeat until the bounded attempts are exhausted and the write is lost anyway.
 */
public final class SearchIndexFailureClassifier {

  private SearchIndexFailureClassifier() {}

  public static boolean isRetryableTransportFailure(@Nonnull final Throwable failure) {
    final OptionalInt httpStatus = findHttpStatus(failure);
    if (httpStatus.isPresent()) {
      return isRetryableHttpStatus(httpStatus.getAsInt());
    }
    return hasCause(failure, ConnectException.class)
        || hasCause(failure, ConnectTimeoutException.class)
        || hasCause(failure, SocketTimeoutException.class)
        || hasCause(failure, NoHttpResponseException.class)
        || hasCause(failure, ConnectionClosedException.class)
        || hasRetryableSocketFailure(failure);
  }

  /**
   * True when an HTTP status denotes a transient failure a redelivery can recover: 429 (too many
   * requests / write-queue backpressure) or any 5xx (server-side). 4xx client errors (auth,
   * mapping, malformed request) are permanent and stay non-retryable. Shared by the whole-request
   * classifier above and the item-level bulk classifier so both halves agree on what a redelivery
   * can recover.
   */
  public static boolean isRetryableHttpStatus(final int status) {
    return status == 429 || (status >= 500 && status <= 599);
  }

  public static OptionalInt findHttpStatus(@Nonnull final Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof OpenSearchStatusException statusException) {
        return OptionalInt.of(statusException.status().getStatus());
      }
      if (current instanceof ElasticsearchException elasticsearchException) {
        return OptionalInt.of(elasticsearchException.status());
      }
      if (current instanceof ResponseException responseException
          && responseException.getResponse() != null) {
        return OptionalInt.of(responseException.getResponse().getStatusLine().getStatusCode());
      }
      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
    }
    return OptionalInt.empty();
  }

  private static boolean hasRetryableSocketFailure(@Nonnull final Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SocketException && current.getMessage() != null) {
        final String message = current.getMessage().toLowerCase();
        if (message.contains("connection reset") || message.contains("connection refused")) {
          return true;
        }
      }
      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean hasCause(
      @Nonnull final Throwable failure, @Nonnull final Class<? extends Throwable> causeType) {
    Throwable current = failure;
    while (current != null) {
      if (causeType.isInstance(current)) {
        return true;
      }
      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
    }
    return false;
  }
}
