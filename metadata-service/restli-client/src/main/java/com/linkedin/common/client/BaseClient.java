package com.linkedin.common.client;

import com.datahub.authentication.Authentication;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;


@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;

  protected BaseClient(@Nonnull Client restliClient) {
    _client = Objects.requireNonNull(restliClient);
  }

  protected <T> Response<T> sendClientRequest(final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder) throws RemoteInvocationException {
    return sendClientRequest(requestBuilder, null);
  }

  /**
   * TODO: Remove unused "actor" parameter. Actor is now implied by the systemClientId + systemClientSecret.
   */
  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder,
      @Nonnull final Authentication authentication) throws RemoteInvocationException {
    requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, authentication.getCredentials());
    return _client.sendRequest(requestBuilder.build()).getResponse();
  }

  @Override
  public void close() {
    if (_client != null) {
      _client.shutdown(new FutureCallback<>());
    }
  }
}
