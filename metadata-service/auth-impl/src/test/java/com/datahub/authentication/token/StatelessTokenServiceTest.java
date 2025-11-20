package com.datahub.authentication.token;

import static com.datahub.authentication.token.TokenClaims.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.authenticator.DataHubTokenAuthenticator;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.key.CorpUserKey;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class StatelessTokenServiceTest {

  private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @Test
  public void testConstructor() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
    assertThrows(() -> new StatelessTokenService(null, null, null));
    assertThrows(() -> new StatelessTokenService(opContext, null, null, null));
    assertThrows(() -> new StatelessTokenService(opContext, TEST_SIGNING_KEY, null, null));
    assertThrows(
        () -> new StatelessTokenService(opContext, TEST_SIGNING_KEY, "UNSUPPORTED_ALG", null));

    // Succeeds:
    new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256", null);
  }

  @Test
  public void testGenerateAccessTokenPersonalToken() throws Exception {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = statelessTokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.ONE);
    assertEquals(claims.getTokenType(), TokenType.PERSONAL);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertTrue(claims.getExpirationInMs() > System.currentTimeMillis());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 1);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "PERSONAL");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }

  @Test
  public void testGenerateAccessTokenPersonalTokenEternal() throws Exception {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"), null);
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = statelessTokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.ONE);
    assertEquals(claims.getTokenType(), TokenType.PERSONAL);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertNull(claims.getExpirationInMs());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 1);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "PERSONAL");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }

  @Test
  public void testGenerateAccessTokenSessionToken() throws Exception {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.SESSION, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = statelessTokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.ONE);
    assertEquals(claims.getTokenType(), TokenType.SESSION);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertTrue(claims.getExpirationInMs() > System.currentTimeMillis());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 1);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "SESSION");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }

  @Test
  public void testValidateAccessTokenFailsDueToExpiration() {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    // Generate token that expires immediately.
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"), 0L);
    assertNotNull(token);

    // Validation should fail.
    assertThrows(
        TokenExpiredException.class, () -> statelessTokenService.validateAccessToken(token));
  }

  @Test
  public void testValidateAccessTokenFailsDueToManipulation() {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Change single character
    String changedToken = token.substring(1);

    // Validation should fail.
    assertThrows(
        TokenException.class, () -> statelessTokenService.validateAccessToken(changedToken));
  }

  @Test
  public void testValidateAccessTokenFailsDueToNoneAlgorithm() {
    // Token with none algorithm type.
    String badToken =
        "eyJhbGciOiJub25lIn0.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6Il9fZGF0YWh1Yl9zeXN0ZW0iL"
            + "CJ0eXBlIjoiU0VTU0lPTiIsInZlcnNpb24iOiIxIiwianRpIjoiN2VmOTkzYjQtMjBiOC00Y2Y5LTljNm"
            + "YtMTE2NjNjZWVmOTQzIiwic3ViIjoiZGF0YWh1YiIsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.";
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");
    // Validation should fail.
    assertThrows(TokenException.class, () -> statelessTokenService.validateAccessToken(badToken));
  }

  @Test
  public void testValidateAccessTokenFailsDueToUnsupportedSigningAlgorithm() throws Exception {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(opContext, TEST_SIGNING_KEY, "HS256");

    Map<String, Object> claims = new HashMap<>();
    claims.put(
        TOKEN_VERSION_CLAIM_NAME,
        String.valueOf(TokenVersion.ONE.numericValue)); // Hardcode version 1 for now.
    claims.put(TOKEN_TYPE_CLAIM_NAME, "SESSION");
    claims.put(ACTOR_TYPE_CLAIM_NAME, "USER");
    claims.put(ACTOR_ID_CLAIM_NAME, "__datahub_system");

    final JwtBuilder builder =
        Jwts.builder()
            .addClaims(claims)
            .setId(UUID.randomUUID().toString())
            .setIssuer("datahub-metadata-service")
            .setSubject("datahub");
    builder.setExpiration(new Date(System.currentTimeMillis() + 60));

    final String testSigningKey = "TLHLdPSivAwIjXP4MT4TtlitsEGkOKjQGNnqsprisfghpU8g";
    byte[] apiKeySecretBytes = testSigningKey.getBytes(StandardCharsets.UTF_8);
    final Key signingKey =
        new SecretKeySpec(apiKeySecretBytes, SignatureAlgorithm.HS384.getJcaName());
    final String badToken = builder.signWith(signingKey, SignatureAlgorithm.HS384).compact();

    // Validation should fail.
    assertThrows(TokenException.class, () -> statelessTokenService.validateAccessToken(badToken));
  }

  @Test
  public void testValidateAccessTokenSystemActorAlwaysActive() throws Exception {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    StatelessTokenService statelessTokenService =
        new StatelessTokenService(mockContext, TEST_SIGNING_KEY, "HS256");

    // Generate a token for system actor
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.SESSION, new Actor(ActorType.USER, "__datahub_system"));
    assertNotNull(token);

    // System actor should always be active, regardless of aspect retriever response
    // No need to mock anything - system actor bypasses all checks
    TokenClaims claims = statelessTokenService.validateAccessToken(token);
    assertEquals(claims.getActorId(), "__datahub_system");
  }

  @Test
  public void testValidateAccessTokenFailsDueToHardDeletedUser() throws Exception {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    StatelessTokenService statelessTokenService =
        new StatelessTokenService(mockContext, TEST_SIGNING_KEY, "HS256");

    // Generate a valid token
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "deleteduser"));
    assertNotNull(token);

    // Mock to return empty aspect map - user has no CorpUserKey aspect (hard deleted)
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(Mockito.any(), Mockito.any()))
        .thenReturn(Map.of(UrnUtils.getUrn("urn:li:corpuser:deleteduser"), Collections.emptyMap()));

    // Validation should fail due to missing CorpUserKey aspect
    TokenException exception =
        expectThrows(TokenException.class, () -> statelessTokenService.validateAccessToken(token));
    assertEquals(exception.getMessage(), "Actor is not active");
  }

  @Test
  public void testValidateAccessTokenFailsDueToRemovedStatus() throws Exception {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    StatelessTokenService statelessTokenService =
        new StatelessTokenService(mockContext, TEST_SIGNING_KEY, "HS256");

    // Generate a valid token
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "removeduser"));
    assertNotNull(token);

    // Create a removed status
    Status removedStatus = new Status().setRemoved(true);
    CorpUserKey corpUserKey = new CorpUserKey().setUsername("removeduser");

    // Mock to return removed status
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:removeduser");
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(Mockito.any(), Mockito.any()))
        .thenReturn(
            Map.of(
                userUrn,
                Map.of(
                    "status", new Aspect(removedStatus.data()),
                    "corpUserKey", new Aspect(corpUserKey.data()))));

    // Validation should fail due to removed status
    TokenException exception =
        expectThrows(TokenException.class, () -> statelessTokenService.validateAccessToken(token));
    assertEquals(exception.getMessage(), "Actor is not active");
  }

  @Test
  public void testValidateAccessTokenFailsDueToSuspendedStatus() throws Exception {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    StatelessTokenService statelessTokenService =
        new StatelessTokenService(mockContext, TEST_SIGNING_KEY, "HS256");

    // Generate a valid token
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "suspendeduser"));
    assertNotNull(token);

    // Create a suspended corp user status
    Status activeStatus = new Status().setRemoved(false);
    CorpUserStatus suspendedStatus = new CorpUserStatus().setStatus("SUSPENDED");
    CorpUserKey corpUserKey = new CorpUserKey().setUsername("suspendeduser");

    // Mock to return suspended status
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:suspendeduser");
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(Mockito.any(), Mockito.any()))
        .thenReturn(
            Map.of(
                userUrn,
                Map.of(
                    "status", new Aspect(activeStatus.data()),
                    "corpUserStatus", new Aspect(suspendedStatus.data()),
                    "corpUserKey", new Aspect(corpUserKey.data()))));

    // Validation should fail due to suspended status
    TokenException exception =
        expectThrows(TokenException.class, () -> statelessTokenService.validateAccessToken(token));
    assertEquals(exception.getMessage(), "Actor is not active");
  }

  @Test
  public void testValidateAccessTokenSucceedsForActiveUser() throws Exception {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    StatelessTokenService statelessTokenService =
        new StatelessTokenService(mockContext, TEST_SIGNING_KEY, "HS256");

    // Generate a valid token
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "activeuser"));
    assertNotNull(token);

    // Create active user aspects
    Status activeStatus = new Status().setRemoved(false);
    CorpUserStatus activeUserStatus = new CorpUserStatus().setStatus("ACTIVE");
    CorpUserKey corpUserKey = new CorpUserKey().setUsername("activeuser");

    // Mock to return active user
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:activeuser");
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(Mockito.any(), Mockito.any()))
        .thenReturn(
            Map.of(
                userUrn,
                Map.of(
                    "status", new Aspect(activeStatus.data()),
                    "corpUserStatus", new Aspect(activeUserStatus.data()),
                    "corpUserKey", new Aspect(corpUserKey.data()))));

    // Validation should succeed
    TokenClaims claims = statelessTokenService.validateAccessToken(token);
    assertEquals(claims.getActorId(), "activeuser");
  }

  @Test
  public void testValidateAccessTokenSucceedsWithMissingOptionalAspects() throws Exception {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    OperationContext mockContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    StatelessTokenService statelessTokenService =
        new StatelessTokenService(mockContext, TEST_SIGNING_KEY, "HS256");

    // Generate a valid token
    String token =
        statelessTokenService.generateAccessToken(
            TokenType.PERSONAL, new Actor(ActorType.USER, "minimaluser"));
    assertNotNull(token);

    // Only provide the required corpUserKey aspect - status and corpUserStatus will use defaults
    CorpUserKey corpUserKey = new CorpUserKey().setUsername("minimaluser");

    // Mock to return only corpUserKey
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:minimaluser");
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(Mockito.any(), Mockito.any()))
        .thenReturn(Map.of(userUrn, Map.of("corpUserKey", new Aspect(corpUserKey.data()))));

    // Validation should succeed - missing aspects use defaults (not removed, not suspended)
    TokenClaims claims = statelessTokenService.validateAccessToken(token);
    assertEquals(claims.getActorId(), "minimaluser");
  }
}
