package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.token.TokenType;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datahub.authentication.AuthenticationConstants.*;
import static com.datahub.authentication.authenticator.DataHubGuestAuthenticator.*;
import static com.datahub.authentication.token.TokenClaims.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class DataHubGuestAuthenticatorTest {
  @Test
  public void testInit() {
    final DataHubGuestAuthenticator authenticator = new DataHubGuestAuthenticator();
    AuthenticatorContext authenticatorContext =
        new AuthenticatorContext(
            Collections.emptyMap());
    authenticator.init(null, authenticatorContext);
    assertNull(authenticator.guestUser);
    assertFalse(authenticator.enabled);

    authenticator.init(Collections.emptyMap(), authenticatorContext);
    assertNull(authenticator.guestUser);
    assertFalse(authenticator.enabled);

    // Correct configs provided.
    authenticator.init(
        ImmutableMap.of(GUEST_USER, "guest", ENABLED, "false"),
        authenticatorContext);
    assertEquals(authenticator.guestUser, "guest");
    assertFalse(authenticator.enabled);

    // Correct configs provided.
    authenticator.init(
        ImmutableMap.of(GUEST_USER, "guest", ENABLED, "true"),
        authenticatorContext);
    assertEquals(authenticator.guestUser, "guest");
    assertTrue(authenticator.enabled);
  }

  @Test
  public void testGuestUserDisabled() {
    final DataHubGuestAuthenticator authenticator = new DataHubGuestAuthenticator();

    authenticator.init(
        Collections.emptyMap(),
        new AuthenticatorContext(Collections.emptyMap()));

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
