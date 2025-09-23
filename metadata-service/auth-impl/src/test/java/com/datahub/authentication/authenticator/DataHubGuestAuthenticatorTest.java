package com.datahub.authentication.authenticator;

import static com.datahub.authentication.AuthenticationConstants.*;
import static com.datahub.authentication.authenticator.DataHubGuestAuthenticator.*;
import static com.datahub.authentication.token.TokenClaims.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.testng.annotations.Test;

public class DataHubGuestAuthenticatorTest {
  @Test
  public void testInit() {
    final DataHubGuestAuthenticator authenticator = new DataHubGuestAuthenticator();
    AuthenticatorContext authenticatorContext = new AuthenticatorContext(Collections.emptyMap());
    authenticator.init(null, authenticatorContext);
    assertEquals(authenticator.guestUser, DEFAULT_GUEST_USER);
    assertFalse(authenticator.enabled);

    authenticator.init(Collections.emptyMap(), authenticatorContext);
    assertEquals(authenticator.guestUser, DEFAULT_GUEST_USER);
    assertFalse(authenticator.enabled);

    // Correct configs provided.
    authenticator.init(
        ImmutableMap.of(GUEST_USER, "publicUser", ENABLED, "false"), authenticatorContext);
    assertEquals(authenticator.guestUser, "publicUser");
    assertFalse(authenticator.enabled);

    // Correct configs provided.
    authenticator.init(ImmutableMap.of(GUEST_USER, "guest", ENABLED, "true"), authenticatorContext);
    assertEquals(authenticator.guestUser, "guest");
    assertTrue(authenticator.enabled);
  }

  @Test
  public void testGuestUserDisabled() {
    final DataHubGuestAuthenticator authenticator = new DataHubGuestAuthenticator();

    authenticator.init(Collections.emptyMap(), new AuthenticatorContext(Collections.emptyMap()));

    final AuthenticationRequest context = new AuthenticationRequest(Collections.emptyMap());
    assertThrows(() -> authenticator.authenticate(context));
  }

  @Test
  public void testGuestUserEnabled() throws AuthenticationException {
    final DataHubGuestAuthenticator authenticator = new DataHubGuestAuthenticator();

    authenticator.init(
        ImmutableMap.of(GUEST_USER, "guest", ENABLED, "true"),
        new AuthenticatorContext(Collections.emptyMap()));

    final AuthenticationRequest context = new AuthenticationRequest(Collections.emptyMap());
    Authentication authentication = authenticator.authenticate(context);
    assertNotNull(authentication);
    assertEquals(authentication.getActor().getId(), "guest");
  }
}
