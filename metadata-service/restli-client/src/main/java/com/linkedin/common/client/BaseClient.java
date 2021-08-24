package com.linkedin.common.client;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.restli.client.Client;
import javax.annotation.Nonnull;


public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;

  protected BaseClient(@Nonnull Client restliClient) {
    _client = restliClient;
  }

  @Override
  public void close() {
    if (_client != null) {
      _client.shutdown(new FutureCallback<>());
    }
  }
}
