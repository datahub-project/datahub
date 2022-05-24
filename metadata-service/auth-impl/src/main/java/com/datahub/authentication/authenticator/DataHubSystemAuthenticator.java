package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * Authenticator that verifies system internal callers, such as the metadata-service itself OR datahub-frontend,
 * using HTTP Basic Authentication.
 *
 * This makes use of a single "system client id" and "system shared secret" which each
 * component in the system is configured to provide.
 *
 * This authenticator also looks for a "delegated actor urn" which can be provided by system callers using the 'X-DataHub-Actor'
 * header. This indicates that the system is making a request on behalf of an end-user with the specified URN. In the future, we intend
 * to additionally pass the original credentials provided by the Actor along with the request, along with the Actor Type in the event that
 * service principals are added as a new entity type.
 *
 * This authenticator requires the following configurations:
 *
 *  - systemClientId: an identifier for internal system callers, provided in the Authorization header via Basic Authentication.
 *  - systemClientSecret: a shared secret used to authenticate internal system callers
 *
 */
@Slf4j
public class DataHubSystemAuthenticator implements Authenticator {

  private String systemClientId;
  private String systemClientSecret;

  @Override
  public void init(@Nonnull final Map<String, Object> config, @Nullable final AuthenticatorContext context) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    this.systemClientId = Objects.requireNonNull((String) config.get(SYSTEM_CLIENT_ID_CONFIG),
        String.format("Missing required config %s", SYSTEM_CLIENT_ID_CONFIG));
    this.systemClientSecret = Objects.requireNonNull((String) config.get(SYSTEM_CLIENT_SECRET_CONFIG),
        String.format("Missing required config %s", SYSTEM_CLIENT_SECRET_CONFIG));
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest context) throws AuthenticationException {
    Objects.requireNonNull(context);
    final String authorizationHeader = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME);
    if (authorizationHeader != null) {
      if (authorizationHeader.startsWith("Basic ") || authorizationHeader.startsWith("basic ")) {

        String credentials = authorizationHeader.substring(6);
        String[] splitCredentials = credentials.split(":");

        if (splitCredentials.length == 2
            && this.systemClientId.equals(splitCredentials[0])
            && this.systemClientSecret.equals(splitCredentials[1])
        ) {
          // If this request was made internally, there may be a delegated id.
          return new Authentication(
              new Actor(ActorType.USER, this.systemClientId), // todo: replace this with service actor type once they exist.
              authorizationHeader,
              Collections.emptyMap()
          );
        } else {
          throw new AuthenticationException("Provided credentials do not match known system client id & client secret. Check your configuration values...");
        }
      } else {
        throw new AuthenticationException("Authorization header is missing 'Basic' prefix.");
      }
    }
    throw new AuthenticationException("Authorization header is missing Authorization header.");
  }
}
