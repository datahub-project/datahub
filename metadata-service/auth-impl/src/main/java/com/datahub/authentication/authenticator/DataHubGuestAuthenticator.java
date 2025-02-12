package com.datahub.authentication.authenticator;

import static com.datahub.authentication.AuthenticationConstants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.plugins.auth.authentication.Authenticator;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Authenticator that enables Guest users (when configured), bypassing authentication. This still
 * requires a specific user to be configured and designated as the guest user. A User who has not
 * authenticated will be treated as the guest user and all the guest user permissions will apply
 *
 * <p>This authenticator requires the name of the guest user that must be implicitly used as the
 * logged in user.
 */
@Slf4j
public class DataHubGuestAuthenticator implements Authenticator {

  public static final String GUEST_USER = "guestUser";
  String guestUser = null;

  @Override
  public void init(final Map<String, Object> config, final AuthenticatorContext context) {
    if (config != null && config.containsKey(GUEST_USER)) {
      guestUser = (String) config.get(GUEST_USER);
    }
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest context)
      throws AuthenticationException {
    if (guestUser == null) {
      return null; // Guest user is not enabled, so fall through the chain.
    }
    // TODO: Also ensure either the user is the guest user or not specified at all
    return new Authentication(new Actor(ActorType.USER, guestUser), "guestUser");
  }
}
