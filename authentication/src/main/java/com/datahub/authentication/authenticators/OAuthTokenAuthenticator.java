package com.datahub.authentication.authenticators;

import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.Authenticator;
import java.util.Map;


/**
 * Configurable authenticator for verifying access tokens provided in the
 * Authorization header.
 */
public class OAuthTokenAuthenticator implements Authenticator {
  @Override
  public void init(final Map<String, Object> config) {
    // Go fetch a JWKS public key & store.
  }

  @Override
  public AuthenticationResult authenticate(final AuthenticationContext context) {
    // Verify the authorization header against the incoming JWT. Extract the proper username claim.
    return null;
  }
}
