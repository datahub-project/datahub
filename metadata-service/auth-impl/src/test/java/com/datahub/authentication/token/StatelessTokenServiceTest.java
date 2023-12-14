package com.datahub.authentication.token;

import static com.datahub.authentication.token.TokenClaims.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.authenticator.DataHubTokenAuthenticator;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.testng.annotations.Test;

public class StatelessTokenServiceTest {

  private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";

  @Test
  public void testConstructor() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
    assertThrows(() -> new StatelessTokenService(null, null, null));
    assertThrows(() -> new StatelessTokenService(TEST_SIGNING_KEY, null, null));
    assertThrows(() -> new StatelessTokenService(TEST_SIGNING_KEY, "UNSUPPORTED_ALG", null));

    // Succeeds:
    new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
    new StatelessTokenService(TEST_SIGNING_KEY, "HS256", null);
  }

  @Test
  public void testGenerateAccessTokenPersonalToken() throws Exception {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
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
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
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
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
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
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
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
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
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
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");
    // Validation should fail.
    assertThrows(TokenException.class, () -> statelessTokenService.validateAccessToken(badToken));
  }

  @Test
  public void testValidateAccessTokenFailsDueToUnsupportedSigningAlgorithm() throws Exception {
    StatelessTokenService statelessTokenService =
        new StatelessTokenService(TEST_SIGNING_KEY, "HS256");

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
}
