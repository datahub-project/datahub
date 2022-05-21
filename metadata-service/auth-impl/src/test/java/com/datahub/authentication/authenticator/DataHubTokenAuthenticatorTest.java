package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.token.TokenType;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import java.util.Collections;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datahub.authentication.AuthenticationConstants.*;
import static com.datahub.authentication.authenticator.DataHubTokenAuthenticator.*;
import static com.datahub.authentication.token.TokenClaims.*;
import static org.testng.Assert.*;


public class DataHubTokenAuthenticatorTest {

  private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";
  private static final String TEST_SALTING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI93=";

  final EntityService mockService = Mockito.mock(EntityService.class);

  @Test
  public void testInit() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
    assertThrows(() -> authenticator.init(null, null));
    assertThrows(() -> authenticator.init(Collections.emptyMap(), null));
    assertThrows(() -> authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SIGNING_ALG_CONFIG_NAME, "UNSUPPORTED_ALG"), null));
    assertThrows(() -> authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SIGNING_ALG_CONFIG_NAME, "HS256"), null));

    // Correct configs provided.
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALTING_KEY_CONFIG_NAME,
            TEST_SALTING_KEY, SIGNING_ALG_CONFIG_NAME, "HS256"),
        ImmutableMap.of(ENTITY_SERVICE, mockService));
  }

  @Test
  public void testAuthenticateFailureMissingAuthorizationHeader() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();

    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALTING_KEY_CONFIG_NAME,
            TEST_SALTING_KEY, SIGNING_ALG_CONFIG_NAME, "HS256"),
        ImmutableMap.of(ENTITY_SERVICE, mockService));

    final AuthenticatorContext context = new AuthenticatorContext(Collections.emptyMap());
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailureMissingBearerCredentials() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();

    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALTING_KEY_CONFIG_NAME,
            TEST_SALTING_KEY, SIGNING_ALG_CONFIG_NAME, "HS256"),
        ImmutableMap.of(ENTITY_SERVICE, mockService));

    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Basic username:password")
    );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateFailureInvalidToken() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();

    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALTING_KEY_CONFIG_NAME,
            TEST_SALTING_KEY, SIGNING_ALG_CONFIG_NAME, "HS256"),
        ImmutableMap.of(ENTITY_SERVICE, mockService));

    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Bearer someRandomToken")
    );
    assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
  }

  @Test
  public void testAuthenticateSuccess() throws Exception {
    Mockito.when(mockService.exists(Mockito.any(Urn.class))).thenReturn(true);

    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
    authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALTING_KEY_CONFIG_NAME,
            TEST_SALTING_KEY, SIGNING_ALG_CONFIG_NAME, "HS256"),
        ImmutableMap.of(ENTITY_SERVICE, mockService));

    final String validToken = authenticator._statefulTokenService.generateAccessToken(
        TokenType.PERSONAL,
        new Actor(ActorType.USER, "datahub")
    );

    final String authorizationHeaderValue = String.format("Bearer %s", validToken);
    final AuthenticatorContext context = new AuthenticatorContext(
        ImmutableMap.of(AUTHORIZATION_HEADER_NAME, authorizationHeaderValue)
    );

    final Authentication authentication = authenticator.authenticate(context);

    // Validate the resulting authentication object
    assertNotNull(authentication);
    assertEquals(authentication.getActor().getType(), ActorType.USER);
    assertEquals(authentication.getActor().getId(), "datahub");
    assertEquals(authentication.getCredentials(), authorizationHeaderValue);

    Map<String, Object> claimsMap = authentication.getClaims();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 2);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "PERSONAL");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }
}
