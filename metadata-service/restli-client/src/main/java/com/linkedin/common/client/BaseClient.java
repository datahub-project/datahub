package com.linkedin.common.client;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.metadata.Constants;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;


@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;
  private final String _systemCredentials;

  protected BaseClient(@Nonnull Client restliClient) {
    _client = restliClient;
    // TODO: Take system credentials or get them from global configs
    _systemCredentials = "Basic __datahub_frontend:YouKnowNothing";
  }

  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder,
      final String actor) throws RemoteInvocationException {
    // Making system internal call on behalf of a user. Append system credentials.
    requestBuilder.addHeader(Constants.ACTOR_HEADER_NAME, actor); // The actor who initiated this request
    requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, this._systemCredentials);
    return _client.sendRequest(requestBuilder.build()).getResponse();
  }

  @Override
  public void close() {
    if (_client != null) {
      _client.shutdown(new FutureCallback<>());
    }
  }
}
