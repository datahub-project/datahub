package com.datahub.authentication.authenticators;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static com.datahub.authentication.Constants.*;


/**
 * Authenticator that verifies system internal callers, such as the metadata-service itself OR datahub-frontend,
 * using HTTP Basic Authentication.
 *
 * This makes use of a single "system client id" and "system shared secret" which each
 * component in the system is configured to provide.
 */
public class DataHubSystemAuthenticator implements Authenticator {

  /**
   * Configs required to authenticate an internal system caller, such as datahub-frontend server.
   */
  private static final String SYSTEM_CLIENT_ID_CONFIG = "systemClientId";
  private static final String SYSTEM_CLIENT_SECRET_CONFIG = "systemClientSecret";

  private String systemClientId;
  private String systemSecret;

  @Override
  public void init(@Nonnull final Map<String, Object> config) {
    Objects.requireNonNull(config);
    this.systemClientId = (String) config.get(SYSTEM_CLIENT_ID_CONFIG);
    this.systemSecret = (String) config.get(SYSTEM_CLIENT_SECRET_CONFIG);
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationContext context) throws AuthenticationException {
    Objects.requireNonNull(context);
    final String authorizationHeader = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME);
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Basic ") || authorizationHeader.startsWith("basic ")) {
        String credentials = authorizationHeader.substring(6);
        String[] splitCredentials = credentials.split(":");
        if (splitCredentials.length == 2
            && this.systemClientId.equals(splitCredentials[0])
            && this.systemSecret.equals(splitCredentials[1])
        ) {
          // If this request was made internally, there may be a delegated id.
          final Actor maybeDelegatedForActor = getDelegatedForActor(context);
          return new Authentication(
              new Actor(ActorType.CORP_USER, this.systemClientId), // todo: replace this with service actor type once those urns exist.
              authorizationHeader,
              Collections.emptySet(),
              Collections.emptyMap(),
              maybeDelegatedForActor
          );
        }
      } else {
        throw new AuthenticationException("Authorization header is missing 'Basic' prefix.");
      }
    }
    throw new AuthenticationException("Authorization header is missing 'Basic' prefix.");
  }

  private Actor getDelegatedForActor(final AuthenticationContext context) {
    Actor delegatedForActor = null;
    if (context.getRequestHeaders().containsKey("X-DataHub-Delegated-For-Id") && context.getRequestHeaders().containsKey("X-DataHub-Delegated-For-Type")) {
      final ActorType actorType = ActorType.valueOf(context.getRequestHeaders().get("X-DataHub-Delegated-For-Id"));
      final String actorId = context.getRequestHeaders().get("X-DataHub-Delegated-For-Id");
      delegatedForActor = new Actor(actorType, actorId);
    }
    return delegatedForActor;
  }
}
