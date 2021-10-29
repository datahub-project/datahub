package com.datahub.authentication.authenticators;

import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.Authenticator;
import java.util.Collections;
import java.util.Map;

import static com.datahub.authentication.Constants.*;


/**
 * Simply authenticates a request against a known identity for DataHub frontend.
 * A safer approach would be to generate a token for the frontend using the private GMS
 * secret and then use that for subsequent communications.
 *
 * Warning: Do not share the DataHub Frontend Token. Rotate it on a normal cadence.
 */
public class DataHubFrontendAuthenticator implements Authenticator {

  private String sharedSecret;
  private String serviceUrn;

  @Override
  public void init(final Map<String, Object> config) {
    this.sharedSecret = (String) config.getOrDefault("shared_secret", "YouKnowNothing");
    this.serviceUrn = (String) config.getOrDefault("service_urn", "urn:li:corpuser:__datahub_frontend");
  }

  @Override
  public AuthenticationResult authenticate(AuthenticationContext context) {
    final String authorizationHeader = context.headers().get("authorization"); // Case insensitive
    if (authorizationHeader != null) {
      String token;
      if (authorizationHeader.startsWith("Bearer ") || authorizationHeader.startsWith("bearer ")) {
        token = authorizationHeader.substring(7);
        if (this.sharedSecret.equals(token)) {
          return new AuthenticationResult(
              AuthenticationResult.Type.SUCCESS,
              this.serviceUrn,
              Collections.emptySet(),
              Collections.emptyMap());
        }
      }
    }
    return FAILURE_AUTHENTICATION_RESULT;
  }
}