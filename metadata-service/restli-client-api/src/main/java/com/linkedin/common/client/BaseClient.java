package com.linkedin.common.client;

import com.datahub.authentication.Authentication;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.client.restli.RestliRequestContextResolver;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;

@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client client;
  protected final EntityClientConfig entityClientConfig;

  /**
   * Resolver that applies every registered {@link
   * com.linkedin.common.client.restli.RestliRequestContextEnricher} to outbound requests. Pulled
   * out as its own collaborator so deployment-specific decoration (routing headers, region headers,
   * tracing tokens) lands on every Restli call without each subclass having to opt in.
   *
   * <p>When no enrichers are registered the resolver is a pass-through and the call path is
   * unchanged. Deployments that need outbound decoration register enrichers that stamp headers the
   * receiving GMS can act on.
   */
  protected final RestliRequestContextResolver restliRequestContextResolver;

  protected static final Set<String> NON_RETRYABLE =
      Set.of("com.linkedin.data.template.RequiredFieldNotPresentException");

  /**
   * Construct a {@link BaseClient} with no outbound enrichers — convenience for tests and paths
   * that never need request decoration. Production code should prefer the constructor that takes a
   * {@link RestliRequestContextResolver} so any registered enrichers' headers land on every
   * outbound call.
   */
  protected BaseClient(@Nonnull Client restliClient, EntityClientConfig entityClientConfig) {
    this(
        restliClient,
        entityClientConfig,
        new RestliRequestContextResolver(Collections.emptyList()));
  }

  protected BaseClient(
      @Nonnull Client restliClient,
      EntityClientConfig entityClientConfig,
      @Nonnull RestliRequestContextResolver restliRequestContextResolver) {
    client = Objects.requireNonNull(restliClient);
    this.entityClientConfig = entityClientConfig;
    this.restliRequestContextResolver = Objects.requireNonNull(restliRequestContextResolver);
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
      @Nonnull OperationContext opContext)
      throws RemoteInvocationException {
    Authentication authentication = opContext.getAuthentication();
    if (authentication != null) {
      requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authentication.getCredentials());
    }

    // Single chokepoint for deployment-specific outbound header decoration. Pass-through when no
    // enrichers are registered; otherwise each registered enricher stamps its headers (e.g. a
    // routing header the receiving GMS can recover) here.
    restliRequestContextResolver.resolve(requestBuilder, opContext);

    int attemptCount = 0;

    while (attemptCount < entityClientConfig.getRetryCount() + 1) {
      try {
        return client.sendRequest(requestBuilder.build()).getResponse();
      } catch (Throwable ex) {
        opContext
            .getMetricUtils()
            .ifPresent(
                metricUtils ->
                    metricUtils.increment(
                        BaseClient.class,
                        "exception" + MetricUtils.DELIMITER + ex.getClass().getName().toLowerCase(),
                        1));

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
