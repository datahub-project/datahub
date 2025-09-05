package com.datahub.authentication.token;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import java.math.BigInteger;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.HashSet;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubOAuthSigningKeyResolverTest {

  private static final String TEST_ISSUER = "https://auth.example.com";
  private static final String TEST_JWKS_URI = "https://auth.example.com/.well-known/jwks.json";
  private static final String TEST_ALGORITHM = "RS256";
  private static final String TEST_KEY_ID = "test-key-id";

  private DataHubOAuthSigningKeyResolver resolver;
  private HttpClient mockHttpClient;
  private HttpResponse<String> mockHttpResponse;
  private JwsHeader<?> mockJwsHeader;
  private Claims mockClaims;

  @BeforeMethod
  public void setUp() {
    HashSet<String> trustedIssuers = new HashSet<>();
    trustedIssuers.add(TEST_ISSUER);

    mockHttpClient = mock(HttpClient.class);
    mockHttpResponse = mock(HttpResponse.class);
    mockJwsHeader = mock(JwsHeader.class);
    mockClaims = mock(Claims.class);

    // Use the new constructor that accepts HttpClient for testing
    resolver =
        new DataHubOAuthSigningKeyResolver(
            trustedIssuers, TEST_JWKS_URI, TEST_ALGORITHM, mockHttpClient);
  }

  @Test
  public void testResolveSigningKeySuccess() throws Exception {
    // Arrange
    when(mockClaims.getIssuer()).thenReturn(TEST_ISSUER);
    when(mockJwsHeader.getKeyId()).thenReturn(TEST_KEY_ID);

    String jwksResponse = createValidJwksResponse();
    when(mockHttpResponse.body()).thenReturn(jwksResponse);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);

    // Act
    Key result = resolver.resolveSigningKey(mockJwsHeader, mockClaims);

    // Assert
    assertNotNull(result);
    assertEquals(result.getAlgorithm(), "RSA");
  }

  @Test
  public void testResolveSigningKeyInvalidIssuer() {
    // Arrange
    when(mockClaims.getIssuer()).thenReturn("https://malicious.com");
    when(mockJwsHeader.getKeyId()).thenReturn(TEST_KEY_ID);

    // Act & Assert
    try {
      resolver.resolveSigningKey(mockJwsHeader, mockClaims);
      assertNotNull(null, "Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      assertEquals(
          e.getMessage(), "Unable to resolve signing key: Invalid issuer: https://malicious.com");
    }
  }

  @Test
  public void testResolveSigningKeyKeyNotFoundInJwks() throws Exception {
    // Arrange
    when(mockClaims.getIssuer()).thenReturn(TEST_ISSUER);
    when(mockJwsHeader.getKeyId()).thenReturn("missing_key_id");

    String jwksResponse = createValidJwksResponse();
    when(mockHttpResponse.body()).thenReturn(jwksResponse);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);

    // Act & Assert
    try {
      resolver.resolveSigningKey(mockJwsHeader, mockClaims);
      assertNotNull(null, "Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      assertEquals(
          e.getMessage(),
          "Unable to resolve signing key: No matching key found in JWKS for kid=missing_key_id");
    }
  }

  @Test
  public void testResolveSigningKeyUnsupportedKeyType() throws Exception {
    // Arrange
    when(mockClaims.getIssuer()).thenReturn(TEST_ISSUER);
    when(mockJwsHeader.getKeyId()).thenReturn(TEST_KEY_ID);

    String jwksResponse = createJwksResponseWithUnsupportedKeyType();
    when(mockHttpResponse.body()).thenReturn(jwksResponse);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);

    // Act & Assert
    try {
      resolver.resolveSigningKey(mockJwsHeader, mockClaims);
      assertNotNull(null, "Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(), "Unable to resolve signing key: Unsupported key type: EC");
    }
  }

  @Test
  public void testConstructorWithValidParameters() {
    // Arrange
    HashSet<String> issuers = new HashSet<>();
    issuers.add("https://example.com");

    // Act
    DataHubOAuthSigningKeyResolver testResolver =
        new DataHubOAuthSigningKeyResolver(issuers, "https://example.com/jwks", "RS256");

    // Assert
    assertNotNull(testResolver);
  }

  private String createValidJwksResponse() throws Exception {
    // Create a valid JWKS response with a real RSA public key
    JSONObject jwks = new JSONObject();
    JSONObject key = new JSONObject();

    // Sample RSA public key components (these are safe test values)
    String modulus =
        "0vx7agoebGcQSuuPiLJXZptN9nndrQmbPFRP_gdHzfK3kczjmpsYRIFpqRYwtCAG3KOUKnp7EIbmgZN7I1l"
            + "_jBmjmfsGZHqG6dMwL3EwwU7rEUGXZRe0YJ_GWZjEK1HXf3rPCNjkOBYKjSJPnFjDPpK1"
            + "_XLIpLqYD8pj4Y-7E5uVa5E8kJvOPllGd4wGLJE6UjqQJ3NbPKHNYGZOdx9J9bL8YJbM"
            + "YGJK3l3c6CmjnSjZRh";
    String exponent = "AQAB";

    key.put("kty", "RSA");
    key.put("kid", TEST_KEY_ID);
    key.put("use", "sig");
    key.put("alg", TEST_ALGORITHM);
    key.put("n", modulus);
    key.put("e", exponent);

    jwks.put("keys", new Object[] {key});

    return jwks.toString();
  }

  private String createJwksResponseWithUnsupportedKeyType() {
    // Create a JWKS response with an unsupported key type (EC instead of RSA)
    JSONObject jwks = new JSONObject();
    JSONObject key = new JSONObject();

    key.put("kty", "EC");
    key.put("kid", TEST_KEY_ID);
    key.put("use", "sig");
    key.put("alg", "ES256");
    key.put("crv", "P-256");
    key.put("x", "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4");
    key.put("y", "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM");

    jwks.put("keys", new Object[] {key});

    return jwks.toString();
  }

  @Test
  public void testRSAPublicKeyGeneration() throws Exception {
    // Test helper method to generate RSA public key
    String modulus =
        "0vx7agoebGcQSuuPiLJXZptN9nndrQmbPFRP_gdHzfK3kczjmpsYRIFpqRYwtCAG3KOUKnp7EIbmgZN7I1l"
            + "_jBmjmfsGZHqG6dMwL3EwwU7rEUGXZRe0YJ_GWZjEK1HXf3rPCNjkOBYKjSJPnFjDPpK1"
            + "_XLIpLqYD8pj4Y-7E5uVa5E8kJvOPllGd4wGLJE6UjqQJ3NbPKHNYGZOdx9J9bL8YJbM"
            + "YGJK3l3c6CmjnSjZRh";
    String exponent = "AQAB";

    // Decode base64url
    byte[] modulusBytes = Base64.getUrlDecoder().decode(modulus);
    byte[] exponentBytes = Base64.getUrlDecoder().decode(exponent);

    BigInteger modulusBigInt = new BigInteger(1, modulusBytes);
    BigInteger exponentBigInt = new BigInteger(1, exponentBytes);

    RSAPublicKeySpec keySpec = new RSAPublicKeySpec(modulusBigInt, exponentBigInt);
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    PublicKey publicKey = keyFactory.generatePublic(keySpec);

    assertNotNull(publicKey);
    assertEquals(publicKey.getAlgorithm(), "RSA");
  }
}
