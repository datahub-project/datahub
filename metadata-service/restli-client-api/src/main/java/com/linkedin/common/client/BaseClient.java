package com.linkedin.common.client;

import com.datahub.authentication.Authentication;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;

@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client client;
  protected final EntityClientConfig entityClientConfig;

  protected static final Set<String> NON_RETRYABLE =
      Set.of("com.linkedin.data.template.RequiredFieldNotPresentException");

  protected BaseClient(@Nonnull Client restliClient, EntityClientConfig entityClientConfig) {
    client = Objects.requireNonNull(restliClient);
    this.entityClientConfig = entityClientConfig;
  }

  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder)
      throws RemoteInvocationException {
    return sendClientRequest(requestBuilder, null);
  }

  /**
   * TODO: Remove unused "actor" parameter. Actor is now implied by the systemClientId +
   * systemClientSecret.
   */
  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder,
      @Nullable final Authentication authentication)
      throws RemoteInvocationException {
    if (authentication != null) {
      requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authentication.getCredentials());
    }

    int attemptCount = 0;

    while (attemptCount < entityClientConfig.getRetryCount() + 1) {
      try {
        return client.sendRequest(requestBuilder.build()).getResponse();
      } catch (Throwable ex) {
        MetricUtils.counter(
                BaseClient.class,
                "exception" + MetricUtils.DELIMITER + ex.getClass().getName().toLowerCase())
            .inc();

        final boolean skipRetry =
            NON_RETRYABLE.contains(ex.getClass().getCanonicalName())
                || (ex.getCause() != null
                    && NON_RETRYABLE.contains(ex.getCause().getClass().getCanonicalName()));

        if (attemptCount == entityClientConfig.getRetryCount() || skipRetry) {
          throw ex;
        } else {
          attemptCount = attemptCount + 1;
          try {
            Thread.sleep(
                entityClientConfig.getBackoffPolicy().nextBackoff(attemptCount, ex) * 1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    throw new IllegalStateException("No entityClient call executed.");
  }

  @Override
  public void close() {
    client.shutdown(new FutureCallback<>());
  }
}
