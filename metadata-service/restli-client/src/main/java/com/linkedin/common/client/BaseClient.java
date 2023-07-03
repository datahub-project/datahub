package com.linkedin.common.client;

import com.datahub.authentication.Authentication;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.r2.RemoteInvocationException;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;


@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;
  protected final BackoffPolicy _backoffPolicy;
  protected final int _retryCount;

  protected BaseClient(@Nonnull Client restliClient, BackoffPolicy backoffPolicy, int retryCount) {
    _client = Objects.requireNonNull(restliClient);
    _backoffPolicy = backoffPolicy;
    _retryCount = retryCount;
  }

  protected <T> Response<T> sendClientRequest(final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder) throws RemoteInvocationException {
    return sendClientRequest(requestBuilder, null);
  }

  /**
   * TODO: Remove unused "actor" parameter. Actor is now implied by the systemClientId + systemClientSecret.
   */
  @SneakyThrows
  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder,
      @Nullable final Authentication authentication) throws RemoteInvocationException {
    if (authentication != null) {
      requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authentication.getCredentials());
    }

    int attemptCount = 0;

    while (attemptCount < _retryCount) {
      try {
        return _client.sendRequest(requestBuilder.build()).getResponse();
      } catch (Exception ex) {
        MetricUtils.counter(BaseClient.class, "exception" + MetricUtils.DELIMITER + ex.getClass().getName().toLowerCase()).inc();

        if (attemptCount == _retryCount - 1) {
          throw ex;
        } else {
          attemptCount = attemptCount + 1;
          Thread.sleep(_backoffPolicy.nextBackoff(attemptCount, ex) * 1000);
        }
      }
    }

    throw new IllegalStateException();
  }

  @Override
  public void close() {
    _client.shutdown(new FutureCallback<>());
  }
}
