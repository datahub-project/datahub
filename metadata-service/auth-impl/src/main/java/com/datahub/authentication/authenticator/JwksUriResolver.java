package com.datahub.authentication.authenticator;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for resolving JWKS URIs from OAuth/OIDC discovery endpoints. Handles both standard
 * OIDC discovery and fallback scenarios.
 */
@Slf4j
public class JwksUriResolver {

  /**
   * Derives JWKS URI from a discovery URI by fetching the discovery document. Falls back to
   * standard patterns if discovery document is unavailable.
   *
   * @param discoveryUri The OIDC discovery URI or base URL
   * @return The JWKS URI for fetching signing keys
   */
  public static String deriveJwksUriFromDiscoveryUri(String discoveryUri) {
    try {
      // Handle different formats of discovery URIs
      String discoveryEndpoint = discoveryUri.trim();

      // Remove trailing slash
      if (discoveryEndpoint.endsWith("/")) {
        discoveryEndpoint = discoveryEndpoint.substring(0, discoveryEndpoint.length() - 1);
      }

      // If it's not a full discovery endpoint, construct one
      if (!discoveryEndpoint.endsWith("/.well-known/openid-configuration")) {
        discoveryEndpoint = discoveryEndpoint + "/.well-known/openid-configuration";
      }

      log.debug("Fetching discovery document from: {}", discoveryEndpoint);

      // Fetch the discovery document to get the actual JWKS URI
      java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
      java.net.http.HttpRequest request =
          java.net.http.HttpRequest.newBuilder()
              .uri(java.net.URI.create(discoveryEndpoint))
              .build();

      java.net.http.HttpResponse<String> response =
          client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        log.warn(
            "Failed to fetch discovery document from {}, status: {}",
            discoveryEndpoint,
            response.statusCode());
        // Fallback to standard pattern
        return deriveJwksUriFallback(discoveryUri);
      }

      // Parse the discovery document
      org.json.JSONObject discoveryDoc = new org.json.JSONObject(response.body());

      if (discoveryDoc.has("jwks_uri")) {
        String jwksUri = discoveryDoc.getString("jwks_uri");
        log.debug("Found JWKS URI in discovery document: {}", jwksUri);
        return jwksUri;
      } else {
        log.warn("No jwks_uri found in discovery document from {}", discoveryEndpoint);
        return deriveJwksUriFallback(discoveryUri);
      }

    } catch (Exception e) {
      log.error("Failed to fetch discovery document from {}: {}", discoveryUri, e.getMessage());
      log.debug("Discovery document fetch error details", e);
      // Fallback to standard pattern
      return deriveJwksUriFallback(discoveryUri);
    }
  }

  /**
   * Provides a fallback JWKS URI using standard OIDC patterns when discovery is unavailable.
   *
   * @param discoveryUri The original discovery URI or base URL
   * @return Fallback JWKS URI using standard patterns
   */
  public static String deriveJwksUriFallback(String discoveryUri) {
    // Fallback to standard OIDC pattern when discovery document is unavailable
    String baseUri = discoveryUri.trim();

    // Remove trailing slash
    if (baseUri.endsWith("/")) {
      baseUri = baseUri.substring(0, baseUri.length() - 1);
    }

    // If it's a full discovery endpoint, derive base
    if (baseUri.endsWith("/.well-known/openid-configuration")) {
      baseUri = baseUri.replace("/.well-known/openid-configuration", "");
    }

    // Standard OIDC JWKS endpoint
    String fallbackUri = baseUri + "/.well-known/jwks.json";
    log.debug("Using fallback JWKS URI: {}", fallbackUri);
    return fallbackUri;
  }
}
