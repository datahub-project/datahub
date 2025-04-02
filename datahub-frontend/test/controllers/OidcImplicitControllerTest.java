package controllers;

import static auth.sso.SsoConfigs.OIDC_ENABLED_CONFIG_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static play.test.Helpers.contentAsString;

import client.AuthServiceClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.oidc.client.OidcClient;
import play.libs.Json;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;

public class OidcImplicitControllerTest {

  private OidcImplicitController controller;
  private AuthServiceClient mockAuthClient;
  private OidcClient mockOidcClient;
  private Config config;

  private RSAKey rsaKey;
  private JWSSigner signer;
  private JWKSet jwkSet;

  private static final String TEST_USER_EMAIL = "testUser@myCompany.com";
  private static final String TEST_TOKEN = "fake-test-token";
  private static final String CLIENT_ID = "datahub-client";
  private static final String ISSUER = "http://localhost:9001/testIssuer";

  @BeforeEach
  public void setup() throws Exception {
    // Generate RSA key pair for signing tokens
    rsaKey = new RSAKeyGenerator(2048).keyID("testkey").algorithm(JWSAlgorithm.RS256).generate();

    // Create signer with private key
    signer = new RSASSASigner(rsaKey);

    // Create JWKS with just the public key information
    jwkSet = new JWKSet(rsaKey.toPublicJWK());

    // Create configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.baseUrl", "http://localhost:9002");
    configMap.put(OIDC_ENABLED_CONFIG_PATH, true);
    configMap.put("auth.oidc.implicit.enabled", true);
    configMap.put("auth.oidc.clientId", CLIENT_ID);
    configMap.put("auth.oidc.implicit.clientIssuer", ISSUER);
    configMap.put("auth.oidc.implicit.jwksJson", jwkSet.toString());
    configMap.put("auth.verbose.logging", true);
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.authCookieSameSite", "Lax");
    configMap.put("auth.cookie.authCookieSecure", false);

    config = ConfigFactory.parseMap(configMap);

    // Mock dependencies
    mockAuthClient = mock(AuthServiceClient.class);
    mockOidcClient = mock(OidcClient.class);

    // Set up mocks
    when(mockAuthClient.generateSessionTokenForUser(anyString())).thenReturn(TEST_TOKEN);

    // Create controller with mocks
    controller = new OidcImplicitController(config);
    controller.authClient = mockAuthClient;
    controller.oidcClient = mockOidcClient;
  }

  @Test
  public void testExchangeTokenForSession() throws Exception {
    // Create a test ID token
    JWTClaimsSet idTokenClaims =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .issuer(ISSUER)
            .audience(CLIENT_ID)
            .expirationTime(
                new Date(System.currentTimeMillis() + 60 * 60 * 1000)) // 1 hour in the future
            .issueTime(new Date())
            .claim("email", TEST_USER_EMAIL)
            .build();

    SignedJWT idToken =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build(),
            idTokenClaims);

    // Sign the token
    idToken.sign(signer);

    // Create request body
    ObjectNode requestBody = Json.newObject();
    requestBody.put("id_token", idToken.serialize());
    requestBody.put("access_token", "fake-access-token");

    // Create HTTP request
    Http.RequestBuilder request = Helpers.fakeRequest().method("POST").bodyJson(requestBody);

    // Call the controller method
    Result result = controller.exchangeTokenForSession(request.build());

    // Verify response
    assertEquals(200, result.status());

    // Check cookies
    Optional<Http.Cookie> actorCookie = result.cookie("actor");
    assertNotNull(actorCookie);
    assertEquals("urn:li:corpuser:" + TEST_USER_EMAIL, actorCookie.get().value());

    // Check session
    Http.Session session = result.session();
    assertNotNull(session);
    assertEquals(TEST_TOKEN, session.get("token").get());
    assertEquals("urn:li:corpuser:" + TEST_USER_EMAIL, session.get("actor").get());
  }

  @Test
  public void testInvalidToken() throws Exception {
    // Create a test ID token with invalid issuer
    JWTClaimsSet idTokenClaims =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .issuer("https://wrong-issuer.com")
            .audience(CLIENT_ID)
            .expirationTime(new Date(System.currentTimeMillis() + 60 * 60 * 1000))
            .issueTime(new Date())
            .claim("email", TEST_USER_EMAIL)
            .build();

    SignedJWT idToken =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build(),
            idTokenClaims);

    // Sign the token
    idToken.sign(signer);

    // Create request body
    ObjectNode requestBody = Json.newObject();
    requestBody.put("id_token", idToken.serialize());
    requestBody.put("access_token", "fake-access-token");

    // Create HTTP request
    Http.RequestBuilder request = Helpers.fakeRequest().method("POST").bodyJson(requestBody);

    // Call the controller method
    Result result = controller.exchangeTokenForSession(request.build());

    // Verify response
    assertEquals(400, result.status());

    // Check error message
    String content = contentAsString(result);
    JsonNode jsonNode = Json.parse(content);
    assertNotNull(jsonNode.get("message"));
  }

  @Test
  public void testMissingTokens() {
    // Create request body without tokens
    ObjectNode requestBody = Json.newObject();

    // Create HTTP request
    Http.RequestBuilder request = Helpers.fakeRequest().method("POST").bodyJson(requestBody);

    // Call the controller method
    Result result = controller.exchangeTokenForSession(request.build());

    // Verify response
    assertEquals(400, result.status());

    // Check error message
    String content = contentAsString(result);
    JsonNode jsonNode = Json.parse(content);
    assertEquals("ID token and access token are required", jsonNode.get("message").asText());
  }

  @Test
  public void testExpiredToken() throws Exception {
    // Create a test ID token that's expired beyond the clock skew allowance
    // Typical clock skew is 30 seconds, so expire it 60 seconds ago to be safe
    JWTClaimsSet idTokenClaims =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .issuer(ISSUER)
            .audience(CLIENT_ID)
            .expirationTime(
                new Date(System.currentTimeMillis() - 60 * 1000)) // 60 seconds in the past
            .issueTime(new Date(System.currentTimeMillis() - 120 * 1000)) // 2 minutes in the past
            .claim("email", TEST_USER_EMAIL)
            .build();

    SignedJWT idToken =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build(),
            idTokenClaims);

    // Sign the token
    idToken.sign(signer);

    // Create request body
    ObjectNode requestBody = Json.newObject();
    requestBody.put("id_token", idToken.serialize());
    requestBody.put("access_token", "fake-access-token");

    // Create HTTP request
    Http.RequestBuilder request = Helpers.fakeRequest().method("POST").bodyJson(requestBody);

    // Call the controller method
    Result result = controller.exchangeTokenForSession(request.build());

    // Verify response
    assertEquals(400, result.status());

    // Check error message
    String content = contentAsString(result);
    JsonNode jsonNode = Json.parse(content);
    assertNotNull(jsonNode.get("message"));
  }

  @Test
  public void testMissingJwks() throws Exception {
    // Create configuration with JWKS explicitly set to null
    Map<String, Object> configMapNoJwks = new HashMap<>();
    configMapNoJwks.put("auth.baseUrl", "http://localhost:9002");
    configMapNoJwks.put(OIDC_ENABLED_CONFIG_PATH, true);
    configMapNoJwks.put("auth.oidc.implicit.enabled", true);
    configMapNoJwks.put("auth.oidc.clientId", CLIENT_ID);
    configMapNoJwks.put("auth.oidc.implicit.clientIssuer", ISSUER);
    // Intentionally not setting jwksJson
    configMapNoJwks.put("auth.verbose.logging", true);
    configMapNoJwks.put("auth.cookie.ttlInHours", 24);
    configMapNoJwks.put("auth.cookie.authCookieSameSite", "Lax");
    configMapNoJwks.put("auth.cookie.authCookieSecure", false);

    Config configNoJwks = ConfigFactory.parseMap(configMapNoJwks);

    // Create a controller with missing JWKS
    OidcImplicitController controllerNoJwks = new OidcImplicitController(configNoJwks);
    controllerNoJwks.authClient = mockAuthClient;
    controllerNoJwks.oidcClient = mockOidcClient;

    // Create a valid ID token
    JWTClaimsSet idTokenClaims =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .issuer(ISSUER)
            .audience(CLIENT_ID)
            .expirationTime(new Date(System.currentTimeMillis() + 60 * 60 * 1000))
            .issueTime(new Date())
            .claim("email", TEST_USER_EMAIL)
            .build();

    SignedJWT idToken =
        new SignedJWT(
            new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKey.getKeyID()).build(),
            idTokenClaims);

    // Sign the token
    idToken.sign(signer);

    // Create request body
    ObjectNode requestBody = Json.newObject();
    requestBody.put("id_token", idToken.serialize());
    requestBody.put("access_token", "fake-access-token");

    // Create HTTP request
    Http.RequestBuilder request = Helpers.fakeRequest().method("POST").bodyJson(requestBody);

    // Call the controller method
    Result result = controllerNoJwks.exchangeTokenForSession(request.build());

    // Verify response - should be 400 Bad Request due to missing JWKS
    assertEquals(400, result.status());

    // Check error message
    String content = contentAsString(result);
    JsonNode jsonNode = Json.parse(content);
    assertEquals("ID token validation failed", jsonNode.get("message").asText());
  }
}
