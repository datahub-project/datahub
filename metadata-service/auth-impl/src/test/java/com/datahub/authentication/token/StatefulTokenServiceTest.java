package com.datahub.authentication.token;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import java.util.Date;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datahub.authentication.token.TokenClaims.*;
import static org.testng.Assert.*;


public class StatefulTokenServiceTest {

  private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";
  private static final String TEST_SALTING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI95=";

  final EntityService mockService = Mockito.mock(EntityService.class);

  @Test
  public void testConstructor() {
    assertThrows(() -> new StatefulTokenService(null, null, null, null, null));
    assertThrows(() -> new StatefulTokenService(TEST_SIGNING_KEY, null, null, null, null));
    assertThrows(() -> new StatefulTokenService(TEST_SIGNING_KEY, "UNSUPPORTED_ALG", null, null, null));

    // Succeeds:
    new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALTING_KEY);
  }

  @Test
  public void testGenerateAccessTokenPersonalToken() throws Exception {
    StatefulTokenService tokenService = new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALTING_KEY);
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = tokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.TWO);
    assertEquals(claims.getTokenType(), TokenType.PERSONAL);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertTrue(claims.getExpirationInMs() > System.currentTimeMillis());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 2);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "PERSONAL");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }

  @Test
  public void testGenerateAccessTokenSessionToken() throws Exception {
    StatefulTokenService tokenService = new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALTING_KEY);
    String token = tokenService.generateAccessToken(TokenType.SESSION, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = tokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.TWO);
    assertEquals(claims.getTokenType(), TokenType.SESSION);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertTrue(claims.getExpirationInMs() > System.currentTimeMillis());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 2);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "SESSION");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }

  @Test
  public void testValidateAccessTokenFailsDueToExpiration() {
    StatefulTokenService tokenService = new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALTING_KEY);
    // Generate token that expires immediately.
    Date date = new Date();
    //This method returns the time in millis
    long createdAtInMs = date.getTime();
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"), 0L,
        createdAtInMs, "token", "", "urn:li:corpuser:datahub");
    assertNotNull(token);

    // Validation should fail.
    assertThrows(TokenExpiredException.class, () -> tokenService.validateAccessToken(token));
  }

  @Test
  public void testValidateAccessTokenFailsDueToManipulation() {
    StatefulTokenService tokenService = new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALTING_KEY);
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Change single character
    String changedToken = token.substring(1);

    // Validation should fail.
    assertThrows(TokenException.class, () -> tokenService.validateAccessToken(changedToken));
  }

  @Test
  public void generateRevokeToken() throws TokenException {
    Mockito.when(mockService.exists(Mockito.any(Urn.class))).thenReturn(true);
    final RollbackRunResult result = new RollbackRunResult(ImmutableList.of(), 0);
    Mockito.when(mockService.deleteUrn(Mockito.any(Urn.class))).thenReturn(result);

    StatefulTokenService tokenService = new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALTING_KEY);
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));

    // Revoke token
    tokenService.revokeAccessToken(tokenService.hash(token));

    // Validation should fail.
    assertThrows(TokenException.class, () -> tokenService.validateAccessToken(token));
  }
}
