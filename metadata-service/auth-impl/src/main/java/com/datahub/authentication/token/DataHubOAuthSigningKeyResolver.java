package com.datahub.authentication.token;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.SigningKeyResolverAdapter;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Key;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.HashSet;
import org.json.JSONArray;
import org.json.JSONObject;

/** Resolves signing keys from OAuth2 / OIDC JWKS endpoints. */
public class DataHubOAuthSigningKeyResolver extends SigningKeyResolverAdapter {

  private final HttpClient client;
  private final HashSet<String> trustedIssuers;
  private final String jwksUri;
  private final String algorithm;

  public DataHubOAuthSigningKeyResolver(
      HashSet<String> trustedIssuers, String jwksUri, String algorithm) {
    this(trustedIssuers, jwksUri, algorithm, HttpClient.newHttpClient());
  }

  // Constructor for testing with custom HttpClient
  public DataHubOAuthSigningKeyResolver(
      HashSet<String> trustedIssuers, String jwksUri, String algorithm, HttpClient httpClient) {
    this.trustedIssuers = trustedIssuers;
    this.jwksUri = jwksUri;
    this.algorithm = algorithm;
    this.client = httpClient;
  }

  @Override
  public Key resolveSigningKey(JwsHeader jwsHeader, Claims claims) {
    try {
      if (!trustedIssuers.contains(claims.getIssuer())) {
        throw new RuntimeException("Invalid issuer: " + claims.getIssuer());
      }

      String keyId = jwsHeader.getKeyId();
      return loadPublicKey(jwksUri, keyId);
    } catch (Exception e) {
      throw new RuntimeException("Unable to resolve signing key: " + e.getMessage(), e);
    }
  }

  private PublicKey loadPublicKey(String jwksUri, String keyId) throws Exception {
    HttpRequest request = HttpRequest.newBuilder().uri(URI.create(jwksUri)).build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    var body = new JSONObject(response.body());

    JSONArray keys = body.getJSONArray("keys");

    for (int i = 0; i < keys.length(); i++) {
      var token = keys.getJSONObject(i);
      if (keyId.equals(token.getString("kid"))) {
        return getPublicKey(token);
      }
    }
    throw new Exception("No matching key found in JWKS for kid=" + keyId);
  }

  private PublicKey getPublicKey(JSONObject token) throws Exception {
    if (!"RSA".equals(token.getString("kty"))) {
      throw new Exception("Unsupported key type: " + token.getString("kty"));
    }
    KeyFactory kf = KeyFactory.getInstance("RSA");
    BigInteger modulus = new BigInteger(1, Base64.getUrlDecoder().decode(token.getString("n")));
    BigInteger exponent = new BigInteger(1, Base64.getUrlDecoder().decode(token.getString("e")));
    return kf.generatePublic(new RSAPublicKeySpec(modulus, exponent));
  }
}
