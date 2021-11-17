package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * This Authenticator is used as a no-op to simply convert the X-DataHub-Actor header into a valid Authentication. It is
 * mainly here for backwards compatibility with deployments that do not have Metadata Service Authentication enabled.
 *
 * Notice that this authenticator should generally be avoided in production.
 */
@Slf4j
public class NoOpAuthenticator implements Authenticator {

  @Override
  public void init(@Nonnull final Map<String, Object> config) {
    // Pass
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticatorContext context) throws AuthenticationException {
    Objects.requireNonNull(context);
    String delegatedDataHubActorUrn = context.getRequestHeaders().get(DELEGATED_FOR_ACTOR_HEADER_NAME);

    if (delegatedDataHubActorUrn == null || "".equals(delegatedDataHubActorUrn)) {
      log.debug(String.format("Found no X-DataHub-Actor header provided with the request. Falling back to %s", Constants.UNKNOWN_ACTOR));
      delegatedDataHubActorUrn = Constants.UNKNOWN_ACTOR;
    }

    return new Authentication(
        // When authentication is disabled, assume everyone is a normal user.
        new Actor(ActorType.USER, getActorIdFromUrn(delegatedDataHubActorUrn)),
        "", // No Credentials provided.
        delegatedDataHubActorUrn,
        Collections.emptyMap()
    );
  }

  private String getActorIdFromUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr).getId();
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Failed to parse urn %s", urnStr));
    }
  }
}
