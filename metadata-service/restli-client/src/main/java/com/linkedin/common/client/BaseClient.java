package com.linkedin.common.client;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.r2.RemoteInvocationException;
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

  protected <T> Response<T> sendClientRequest(Request<T> request) throws RemoteInvocationException {
    try {
      request.getHeaders().put("actor", "my-custom-actor");
      return _client.sendRequest(request).getResponse();
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
