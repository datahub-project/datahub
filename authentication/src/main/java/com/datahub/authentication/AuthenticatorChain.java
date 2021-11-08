package com.datahub.authentication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.datahub.authentication.Constants.*;


/**
 * A chain of {@link Authenticator}s executed in series on receiving an inbound request.
 */
public class AuthenticatorChain {

  private static final AuthenticationResult SUCCESS_AUTHENTICATION_RESULT = new AuthenticationResult(
    AuthenticationResult.Type.SUCCESS,
      new Authentication(
          "",
          "urn:li:corpuser:datahub", // TODO Fix this.
          null,
          Collections.emptySet(),
          Collections.emptyMap())
  );

  private final Map<String, Object> config;
  private final List<Authenticator> authenticators = new ArrayList<>();

  public AuthenticatorChain(final Map<String, Object> config) {
    // TODO: Figure out config format.
    this.config = config;
  }

  /**
   * Registers a new {@link Authenticator} at the end of the authentication chain.
   *
   * @param authenticator the authenticator to register
   */
  public void register(final Authenticator authenticator) {
    if (authenticator == null) {
      throw new IllegalArgumentException("authenticator must not be null!");
    }
    try {
      authenticator.init(this.config);
      authenticators.add(authenticator);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to register authenticator %s!",
          authenticator.getClass().getCanonicalName()), e);
    }
  }

  /**
   * Executes a set of {@link Authenticator}s and returns the first succesful authentication result.
   */
  public AuthenticationResult authenticate(final AuthenticationContext context) {
    AuthenticationResult result = SUCCESS_AUTHENTICATION_RESULT;
    for (final Authenticator authenticator : this.authenticators) {
      try {
        result = authenticator.authenticate(context);
        if (AuthenticationResult.Type.SUCCESS.equals(result.type())) {
          // Short circuit on success.
          return result;
        }
      } catch (Exception e) {
        // THIS IS DANERGOUS - IF ANY AUTHENTICATOR THROWS THE WHOLE CHAIN IS DESTROYED.
        // todo add logging.
        // log.error(e);
        return FAILURE_AUTHENTICATION_RESULT;
      }
    }
    return result;
  }
}
