package com.datahub.authentication.authenticator;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;;
import com.datahub.authentication.AuthenticatorContext;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Optional;
import org.testng.annotations.Test;

import static com.datahub.authentication.AuthenticationConstants.*;
import static org.testng.Assert.*;

public class DataHubSystemAuthenticatorTest {

  private static final String TEST_CLIENT_ID = "clientId";
  private static final String TEST_CLIENT_SECRET = "clientSecret";

  @Test
  public void testInit() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    assertThrows(() -> authenticator.init(null));
    assertThrows(() -> authenticator.init(Collections.emptyMap()));
    assertThrows(() -> authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID)));
    assertThrows(() -> authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET)));

    // Correct configs provided.
    authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID, SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET));
  }

  @Test
  public void testAuthenticateFailure_missingAuthorizationHeader() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID, SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET));

    final AuthenticatorContext context = new AuthenticatorContext(Collections.emptyMap());
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailure_missingBasicCredentials() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID, SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET));

    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Bearer something") // Missing basic authentication.
    );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailure_mismatchingCredentials() {
    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID, SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET));

    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Basic incorrectId:incorrectSecret") // Incorrect authentication
    );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateSuccess_noDelegatedActor() throws Exception {

    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID, SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET));

    final String authorizationHeaderValue = String.format("Basic %s:%s", TEST_CLIENT_ID, TEST_CLIENT_SECRET);
    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, authorizationHeaderValue)
    );

    final Authentication authentication = authenticator.authenticate(context);

    // Validate the resulting authentication object
    assertNotNull(authentication);
    assertEquals(authentication.getAuthenticatedActor().getType(), ActorType.USER);
    assertEquals(authentication.getAuthenticatedActor().getId(), TEST_CLIENT_ID);
    assertEquals(authentication.getCredentials(), authorizationHeaderValue);
    assertEquals(authentication.getDelegatedForActorUrn(), Optional.empty());
    assertEquals(authentication.getClaims(), Collections.emptyMap());
  }

  @Test
  public void testAuthenticateSuccess_delegatedActor() throws Exception {

    final DataHubSystemAuthenticator authenticator = new DataHubSystemAuthenticator();
    authenticator.init(ImmutableMap.of(SYSTEM_CLIENT_ID_CONFIG, TEST_CLIENT_ID, SYSTEM_CLIENT_SECRET_CONFIG, TEST_CLIENT_SECRET));

    final String authorizationHeaderValue = String.format("Basic %s:%s", TEST_CLIENT_ID, TEST_CLIENT_SECRET);
    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(
            AUTHORIZATION_HEADER_NAME, authorizationHeaderValue,
            DELEGATED_FOR_ACTOR_HEADER_NAME, "urn:li:corpuser:datahub")
    );

    final Authentication authentication = authenticator.authenticate(context);

    // Validate the resulting authentication object
    assertNotNull(authentication);
    assertEquals(authentication.getAuthenticatedActor().getType(), ActorType.USER);
    assertEquals(authentication.getAuthenticatedActor().getId(), TEST_CLIENT_ID);
    assertEquals(authentication.getCredentials(), authorizationHeaderValue);
    assertEquals(authentication.getDelegatedForActorUrn(), Optional.of("urn:li:corpuser:datahub"));
    assertEquals(authentication.getClaims(), Collections.emptyMap());
  }
}