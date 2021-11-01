package com.datahub.authentication.authenticators;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.Authenticator;
import java.util.Collections;
import java.util.Map;

import static com.datahub.authentication.Constants.*;


/**
 * Authenticator that verifies system internal callers, such as the metadata-service itself OR datahub-frontend,
 * using HTTP Basic Authentication.
 *
 * This makes use of a single "system client id" and "system shared secret" which each
 * component in the system is configured to provide.
 */
public class DataHubSystemAuthenticator implements Authenticator {

  private String systemClientId;
  private String systemSecret;

  @Override
  public void init(final Map<String, Object> config) {
    this.systemClientId = (String) config.getOrDefault("system_secret", SYSTEM_ACTOR);
    this.systemSecret = (String) config.getOrDefault("system_secret", "YouKnowNothing");
  }

  @Override
  public AuthenticationResult authenticate(AuthenticationContext context) {
    final String authorizationHeader = context.headers().get("Authorization"); // Case insensitive
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Basic ") || authorizationHeader.startsWith("basic ")) {
        String credentials = authorizationHeader.substring(6);
        String[] splitCredentials = credentials.split(":");
        if (splitCredentials.length == 2
            && this.systemClientId.equals(splitCredentials[0])
            && this.systemSecret.equals(splitCredentials[1])
        ) {
          final String delegatedActorUrn = context.headers().get("X-DataHub-Actor"); // If this request was made internally.
          return new AuthenticationResult(
              AuthenticationResult.Type.SUCCESS,
              new Authentication(
                  authorizationHeader,
                  this.systemClientId,
                  delegatedActorUrn,
                  Collections.emptySet(),
                  Collections.emptyMap()
              )
          );
        }
      } else {
        return FAILURE_AUTHENTICATION_RESULT; // TODO: Qualify this further.
      }
    }
    return FAILURE_AUTHENTICATION_RESULT;
  }
}
