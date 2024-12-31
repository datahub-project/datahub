package com.datahub.authentication.authenticator;

import static com.datahub.authentication.AuthenticationConstants.*;
import static org.testng.Assert.*;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.testng.annotations.Test;

public class DataHubSystemAuthenticatorTest {

  private static final String TEST_CLIENT_ID = "clientId";
  private static final String TEST_CLIENT_SECRET = "clientSecret";

  @Test
  public void testInit() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    assertThrows(() -> authenticator.init(null, null));
    assertThrows(() -> authenticator.init(Collections.emptyMap(), null));
    assertThrows(
        () -> authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID), null));
    assertThrows(
        () ->
            authenticator.init(
                ImmutableMap.of(SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET), null));

    // Correct configs provided.
    authenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            TEST_CLIENT_ID,
            SYSTEM_CLIENT_SECRET_CONFIG,
            TEST_CLIENT_SECRET),
        null);
  }

  @Test
  public void testAuthenticateFailureMissingAuthorizationHeader() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            TEST_CLIENT_ID,
            SYSTEM_CLIENT_SECRET_CONFIG,
            TEST_CLIENT_SECRET),
        null);

    final AuthenticationRequest context = new AuthenticationRequest(Collections.emptyMap());
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailureMissingBasicCredentials() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            TEST_CLIENT_ID,
            SYSTEM_CLIENT_SECRET_CONFIG,
            TEST_CLIENT_SECRET),
        null);

    final AuthenticationRequest context =
        new AuthenticationRequest(
            ImmutableMap.of(
                AUTHORIZATION_HEADER_NAME, "Bearer something") // Missing basic authentication.
            );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailureMismatchingCredentials() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            TEST_CLIENT_ID,
            SYSTEM_CLIENT_SECRET_CONFIG,
            TEST_CLIENT_SECRET),
        null);

    final AuthenticationRequest context =
        new AuthenticationRequest(
            ImmutableMap.of(
                AUTHORIZATION_HEADER_NAME,
                "Basic incorrectId:incorrectSecret") // Incorrect authentication
            );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateSuccessNoDelegatedActor() throws Exception {

    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            TEST_CLIENT_ID,
            SYSTEM_CLIENT_SECRET_CONFIG,
            TEST_CLIENT_SECRET),
        null);

    final String authorizationHeaderValue =
        String.format("Basic %s:%s", TEST_CLIENT_ID, TEST_CLIENT_SECRET);
    final AuthenticationRequest context =
        new AuthenticationRequest(
            ImmutableMap.of(AUTHORIZATION_HEADER_NAME, authorizationHeaderValue));

    final Authentication authentication = authenticator.authenticate(context);

    // Validate the resulting authentication object
    assertNotNull(authentication);
    assertEquals(authentication.getActor().getType(), ActorType.USER);
    assertEquals(authentication.getActor().getId(), TEST_CLIENT_ID);
    assertEquals(authentication.getCredentials(), authorizationHeaderValue);
    assertEquals(authentication.getClaims(), Collections.emptyMap());
  }

  @Test
  public void testAuthenticateSuccessDelegatedActor() throws Exception {

    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(
        ImmutableMap.of(
            SYSTEM_CLIENT_ID_CONFIG,
            TEST_CLIENT_ID,
            SYSTEM_CLIENT_SECRET_CONFIG,
            TEST_CLIENT_SECRET),
        null);

    final String authorizationHeaderValue =
        String.format("Basic %s:%s", TEST_CLIENT_ID, TEST_CLIENT_SECRET);
    final AuthenticationRequest context =
        new AuthenticationRequest(
            ImmutableMap.of(
                AUTHORIZATION_HEADER_NAME,
                authorizationHeaderValue,
                LEGACY_X_DATAHUB_ACTOR_HEADER,
                "urn:li:corpuser:datahub"));

    final Authentication authentication = authenticator.authenticate(context);

    // Validate the resulting authentication object
    assertNotNull(authentication);
    assertEquals(authentication.getActor().getType(), ActorType.USER);
    assertEquals(authentication.getActor().getId(), TEST_CLIENT_ID);
    assertEquals(authentication.getCredentials(), authorizationHeaderValue);
    assertEquals(authentication.getClaims(), Collections.emptyMap());
  }
}
