package com.datahub.authentication;

import java.util.Map;


/**
 * Interface implemented by components responsible for authenticating an inbound
 * request with DataHub.
 */
public interface Authenticator {

  /**
   * Initialize the Authenticator. Invoked once at boot time.
   * @param config deploy-time config provided to the authenticator.
   */
  void init(final Map<String, Object> config);

  /**
   * Authenticate a request provided an instance of {@link AuthenticationContext}
   * corresponding to the current request.
   */
  AuthenticationResult authenticate(final AuthenticationContext context);
}
