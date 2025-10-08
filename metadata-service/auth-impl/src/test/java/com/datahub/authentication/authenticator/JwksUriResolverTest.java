package com.datahub.authentication.authenticator;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class JwksUriResolverTest {

  @Test
  public void testDeriveJwksUriFallbackWithStandardFormat() {
    // Arrange
    String discoveryUri = "https://auth.example.com";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithTrailingSlash() {
    // Arrange
    String discoveryUri = "https://auth.example.com/";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithDiscoveryEndpoint() {
    // Arrange
    String discoveryUri = "https://auth.example.com/.well-known/openid-configuration";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithDiscoveryEndpointAndTrailingSlash() {
    // Arrange
    String discoveryUri = "https://auth.example.com/.well-known/openid-configuration/";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithPath() {
    // Arrange
    String discoveryUri = "https://auth.example.com/oauth2/v1";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/oauth2/v1/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithPathAndDiscoveryEndpoint() {
    // Arrange
    String discoveryUri = "https://auth.example.com/oauth2/v1/.well-known/openid-configuration";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/oauth2/v1/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithWhitespace() {
    // Arrange
    String discoveryUri = "  https://auth.example.com  ";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithLocalhost() {
    // Arrange
    String discoveryUri = "http://localhost:8080";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "http://localhost:8080/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithPort() {
    // Arrange
    String discoveryUri = "https://auth.example.com:443/oauth";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "https://auth.example.com:443/oauth/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFromDiscoveryUriWithUnreachableEndpoint() {
    // Arrange
    String discoveryUri = "https://nonexistent.example.com";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);

    // Assert - Should fallback to standard pattern
    assertEquals(jwksUri, "https://nonexistent.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFromDiscoveryUriWithInvalidUrl() {
    // Arrange
    String discoveryUri = "not-a-valid-url";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);

    // Assert - Should fallback to standard pattern
    assertEquals(jwksUri, "not-a-valid-url/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFromDiscoveryUriAppendsDiscoveryPath() {
    // Test that when discovery document fetch fails, it tries the right URL format
    // Arrange
    String discoveryUri = "https://auth.example.com/oauth2";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);

    // Assert - Should fallback to standard pattern since we can't reach the endpoint
    assertEquals(jwksUri, "https://auth.example.com/oauth2/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFromDiscoveryUriWithFullDiscoveryPath() {
    // Arrange
    String discoveryUri = "https://nonexistent.example.com/.well-known/openid-configuration";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);

    // Assert - Should fallback to standard pattern
    assertEquals(jwksUri, "https://nonexistent.example.com/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFromDiscoveryUriWithEmptyString() {
    // Arrange
    String discoveryUri = "";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);

    // Assert - Should fallback gracefully
    assertEquals(jwksUri, "/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFromDiscoveryUriWithNull() {
    // Arrange
    String discoveryUri = null;

    // Act & Assert - Should handle gracefully
    assertThrows(
        NullPointerException.class,
        () -> {
          JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);
        });
  }

  @Test
  public void testDeriveJwksUriFallbackWithNull() {
    // Arrange
    String discoveryUri = null;

    // Act & Assert - Should handle gracefully
    assertThrows(
        NullPointerException.class,
        () -> {
          JwksUriResolver.deriveJwksUriFallback(discoveryUri);
        });
  }

  @Test
  public void testDeriveJwksUriFallbackWithOnlySlash() {
    // Arrange
    String discoveryUri = "/";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert
    assertEquals(jwksUri, "/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackCaseInsensitive() {
    // Test case sensitivity - OIDC endpoints should maintain case
    // Arrange
    String discoveryUri = "https://Auth.Example.Com/OAuth2";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert - Should preserve case
    assertEquals(jwksUri, "https://Auth.Example.Com/OAuth2/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithMultipleTrailingSlashes() {
    // Arrange
    String discoveryUri = "https://auth.example.com///";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert - Only removes one trailing slash
    assertEquals(jwksUri, "https://auth.example.com///.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithQueryParams() {
    // Arrange - discovery URI with query parameters
    String discoveryUri = "https://auth.example.com?tenant=example";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert - Should preserve query parameters
    assertEquals(jwksUri, "https://auth.example.com?tenant=example/.well-known/jwks.json");
  }

  @Test
  public void testDeriveJwksUriFallbackWithFragment() {
    // Arrange - discovery URI with fragment
    String discoveryUri = "https://auth.example.com#section";

    // Act
    String jwksUri = JwksUriResolver.deriveJwksUriFallback(discoveryUri);

    // Assert - Should preserve fragment
    assertEquals(jwksUri, "https://auth.example.com#section/.well-known/jwks.json");
  }

  // Integration test to verify the pattern matching works correctly
  @Test
  public void testDiscoveryEndpointDetectionPattern() {
    // Test various formats to ensure the discovery endpoint pattern is detected correctly
    String[] testCases = {
      "https://example.com/.well-known/openid-configuration",
      "https://example.com/oauth/.well-known/openid-configuration",
      "https://example.com/auth/realms/master/.well-known/openid-configuration",
      "http://localhost:8080/.well-known/openid-configuration"
    };

    String[] expectedResults = {
      "https://example.com/.well-known/jwks.json",
      "https://example.com/oauth/.well-known/jwks.json",
      "https://example.com/auth/realms/master/.well-known/jwks.json",
      "http://localhost:8080/.well-known/jwks.json"
    };

    for (int i = 0; i < testCases.length; i++) {
      String result = JwksUriResolver.deriveJwksUriFallback(testCases[i]);
      assertEquals(result, expectedResults[i], "Failed for input: " + testCases[i]);
    }
  }
}
