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
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashSet;
import org.json.JSONArray;
import org.json.JSONObject;

public class DataHubJwtSigningKeyResolver extends SigningKeyResolverAdapter {

  public HttpClient client;
  HashSet<String> trustedIssuers;

  String publicKey;

  String algorithm;

  public DataHubJwtSigningKeyResolver(HashSet<String> list, String publicKey, String algorithm) {
    this.trustedIssuers = list;
    this.publicKey = publicKey;
    this.algorithm = algorithm;
    client = HttpClient.newHttpClient();
  }

  /** inspect the header or claims, lookup and return the signing key */
  @Override
  public Key resolveSigningKey(JwsHeader jwsHeader, Claims claims) {

    PublicKey key = null;

    try {

      if (!trustedIssuers.contains(claims.getIssuer())) {
        throw new Exception("Invalid issuer");
      }

      if (publicKey != null) {
        // Use public key from configuration for signature verification.
        key = generatePublicKey(this.algorithm, this.publicKey);
      } else {
        // Get public key from issuer for signature verification
        String keyId = jwsHeader.getKeyId();
        key = loadPublicKey(claims.getIssuer(), keyId);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return key;
  }

  /** Get public keys from issuer and filter public key for token signature based on token keyId. */
  private PublicKey loadPublicKey(String issuer, String keyId) throws Exception {

    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(issuer + "/protocol/openid-connect/certs")).build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    var body = new JSONObject(response.body());
    JSONArray result = (JSONArray) body.get("keys");

    for (int i = 0; i < result.length(); i++) {
      var token = (JSONObject) result.get(i);
      if (keyId.equals(token.get("kid"))) {
        return getPublicKey(token);
      }
    }
    throw new Exception("The public key is missing for provided issuer");
  }

  /**
   * Generate public key based on token algorithem and public token received from issuer. Supported
   * algo RSA
   */
  private PublicKey getPublicKey(JSONObject token) throws Exception {
    PublicKey publicKey = null;

    switch (token.get("kty").toString()) {
      case "RSA":
        try {
          KeyFactory kf = KeyFactory.getInstance("RSA");
          BigInteger modulus =
              new BigInteger(1, Base64.getUrlDecoder().decode(token.get("n").toString()));
          BigInteger exponent =
              new BigInteger(1, Base64.getUrlDecoder().decode(token.get("e").toString()));
          publicKey = kf.generatePublic(new RSAPublicKeySpec(modulus, exponent));
        } catch (InvalidKeySpecException e) {
          throw new InvalidKeySpecException("Invalid public key", e);
        } catch (NoSuchAlgorithmException e) {
          throw new NoSuchAlgorithmException("Invalid algorithm to generate key", e);
        }
        break;
      default:
        throw new Exception("The key type of " + token.get("alg") + " is not supported");
    }

    return publicKey;
  }

  /** Generate public Key based on algorithem and 64 encoded public key. Supported algo RSA */
  private PublicKey generatePublicKey(String alg, String key) throws Exception {

    PublicKey publicKey = null;

    switch (this.algorithm) {
      case "RSA":
        try {
          key.replace(" ", "");
          byte[] decode = Base64.getDecoder().decode(key);
          X509EncodedKeySpec keySpecX509 = new X509EncodedKeySpec(decode);
          KeyFactory keyFactory = KeyFactory.getInstance("RSA");
          publicKey = (RSAPublicKey) keyFactory.generatePublic(keySpecX509);
        } catch (Exception e) {
          throw new NoSuchAlgorithmException("Unable to generate public key: ", e);
        }
        break;
      default:
        throw new Exception("The key type of " + alg + " is not supported");
    }
    return publicKey;
  }
}
