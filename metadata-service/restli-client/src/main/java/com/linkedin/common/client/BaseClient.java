package com.linkedin.common.client;

import com.datahub.authentication.AuthenticationConstants;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.AbstractRequestBuilder;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;


@Slf4j
public abstract class BaseClient implements AutoCloseable {

  protected final Client _client;
  private final String _systemClientId;
  private final String _systemSecret;

  protected BaseClient(@Nonnull Client restliClient, @Nullable String systemClientId, @Nullable String systemClientSecret) {
    _client = Objects.requireNonNull(restliClient);
    _systemClientId = systemClientId == null ? "unknown" : systemClientId;
    _systemSecret = systemClientSecret == null ? "unknown" : systemClientSecret;
  }

  protected <T> Response<T> sendClientRequest(
      final AbstractRequestBuilder<?, ?, ? extends Request<T>> requestBuilder,
      final String actor) throws RemoteInvocationException {
    // Add an authorization header for system internal call on behalf of a user.
    requestBuilder.addHeader(HttpHeaders.AUTHORIZATION, String.format("Basic %s:%s", _systemClientId, _systemSecret));

    // Add a delegation header indicating the original actor who initiated the request.
    // TODO: Auth Part 2: Propagate the underlying actor privileges instead of the legacy actor urn header. (authentication info)
    requestBuilder.addHeader(AuthenticationConstants.DELEGATED_FOR_ACTOR_HEADER_NAME, actor);
    return _client.sendRequest(requestBuilder.build()).getResponse();
  }

  @Override
  public void close() {
    if (_client != null) {
      _client.shutdown(new FutureCallback<>());
    }
  }
}
