package com.linkedin.common.client;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiResponseException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;

  protected BaseClient(@Nonnull Client restliClient) {
    _client = restliClient;
  }

  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder,
      final String actor) throws RemoteInvocationException {
    try {
      // Hack there should be a better way to inject headers on all requests.
      // Actor = CorpUserUrn associated with the user. Should do minimal corp user urn to username conversion.
      requestBuilder.addHeader("actor", actor);
      return _client.sendRequest(requestBuilder.build()).getResponse();
    } catch (RemoteInvocationException e) {
      if (e instanceof RestLiResponseException) {
        RestLiResponseException restliException = (RestLiResponseException) e;
        if (restliException.getStatus() == 404) {
          log.error("ERROR: Your datahub-frontend instance version is ahead of your gms instance. "
              + "Please update your gms to the latest Datahub release");
          System.exit(1);
        }
      }
      throw e;
    }
  }

  @Override
  public void close() {
    if (_client != null) {
      _client.shutdown(new FutureCallback<>());
    }
  }
}
