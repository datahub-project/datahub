package com.datahub.authentication.token;

import static org.testng.AssertJUnit.*;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import java.math.BigInteger;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashSet;
import org.json.JSONObject;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataHubJwtSigningKeyResolverTest {

  @InjectMocks private DataHubJwtSigningKeyResolver resolver;

  @Test
  public void testResolveSigningKeyWithPublicKey() throws Exception {
    String publicKey =
        "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1SU1LfVLPHCozMxH2Mo4lgOEePzNm0tRgeLezV6ffAt0gunVTLw7onLRnrq0/IzW7yWR7Q"
            + "krmBL7jTKEn5u+qKhbwKfBstIs+bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemMghRniWaoLcyehkd3qqGElvW/VDL5AaWTg0nLVkjRo9z+40R"
            + "QzuVaE8AkAFmxZzow3x+VJYKdjykkJ0iT9wCS0DRTXu269V264Vf/3jvredZiKRkgwlL9xNAwxXFg0x/XFw005UWVRIkdgcKWTjpBP2dPwVZ4WWC+"
            + "9aGVd+Gyn1o0CLelf4rEjGoXbAAEgAqeGUxrcIlbjXfbcmwIDAQAB";
    HashSet<String> trustedIssuers = new HashSet<>();
    trustedIssuers.add("https://example.com");
    resolver = new DataHubJwtSigningKeyResolver(trustedIssuers, publicKey, "RSA");
    Key expectedKey = generatePublicKey("RSA", publicKey);
    JwsHeader mockJwsHeader = Mockito.mock(JwsHeader.class);

    Claims mockClaims = Mockito.mock(Claims.class);
    Mockito.when(mockClaims.getIssuer()).thenReturn("https://example.com");
    Key actualKey = resolver.resolveSigningKey(mockJwsHeader, mockClaims);
    assertNotNull(actualKey);
    assertEquals(expectedKey.getAlgorithm(), actualKey.getAlgorithm());
    assertArrayEquals(expectedKey.getEncoded(), actualKey.getEncoded());
  }

  @Test
  void testResolveSigningKeyWithRemotePublicKey() throws Exception {
    HttpClient httpClient = Mockito.mock(HttpClient.class);
    HttpResponse<String> httpResponse = Mockito.mock(HttpResponse.class);

    Mockito.when(httpResponse.statusCode()).thenReturn(200);
    JSONObject token =
        new JSONObject(
            "{\"kty\": \"RSA\", \"kid\": \"test_key\", \"n\": \"ueXyoaxgWhMTLwkowaskhiV85rbN9n_nLft8CxFUY3nbMpNybAWsWuhJ4SYLT4U-GbKdL-h-NYgBXKn"
                + "GK1ieG6qSC25T3hWXTb3cNe73ZQUcZSivAV2tZouPYcb1XKSyKd-PsK8NsCpq1NHsJsrXSKq-7YCaf4MxIUaFXSZTE7ZNC0fPVqYH71jnyOU9FA_KJm0IC-x_Bs2g"
                + "Ak3Eq1_6pZ_0VeYpczv82LACAUzi1vuU1gbbZLNHHl4DHwWb98eI1aCbWHNMux70Ba4aREOdKOWrxZ066W_NKUVtPY_njW66NvgBujxqHD2EQUc87KPAL6rYOH"
                + "0hWWPEzencGdYj2w\", \"e\": \"AQAB\"}");
    PublicKey expectedKey = getPublicKey(token);

    String responseJson =
        "{\"keys\": [{\"kty\": \"RSA\", \"kid\": \"test_key\", \"n\": \"ueXyoaxgWhMTLwkowaskhiV85rbN9n_nLft8CxFUY3nbMpNybAWsWuhJ4SYLT4U-GbKdL-h-NYgB"
            + "XKnGK1ieG6qSC25T3hWXTb3cNe73ZQUcZSivAV2tZouPYcb1XKSyKd-PsK8NsCpq1NHsJsrXSKq-7YCaf4MxIUaFXSZTE7ZNC0fPVqYH71jny"
            + "OU9FA_KJm0IC-x_Bs2gAk3Eq1_6pZ_0VeYpczv82LACAUzi1vuU1gbbZLNHHl4DHwWb98eI1aCbWHNMux70Ba4aREOdKOWrxZ066W_N"
            + "KUVtPY_njW66NvgBujxqHD2EQUc87KPAL6rYOH0hWWPEzencGdYj2w\", \"e\": \"AQAB\"}]}";
    Mockito.when(httpResponse.body()).thenReturn(responseJson);

    Mockito.when(
            httpClient.send(
                Mockito.any(HttpRequest.class), Mockito.any(HttpResponse.BodyHandler.class)))
        .thenReturn(httpResponse);
    HashSet<String> trustedIssuers = new HashSet<>();
    trustedIssuers.add("https://example.com");
    DataHubJwtSigningKeyResolver resolver =
        new DataHubJwtSigningKeyResolver(trustedIssuers, null, "RSA");
    resolver.client = httpClient;
    JwsHeader mockJwsHeader = Mockito.mock(JwsHeader.class);
    Mockito.when(mockJwsHeader.getKeyId()).thenReturn("test_key");
    Claims mockClaims = Mockito.mock(Claims.class);
    Mockito.when(mockClaims.getIssuer()).thenReturn("https://example.com");
    Key actualKey = resolver.resolveSigningKey(mockJwsHeader, mockClaims);

    assertEquals(expectedKey, actualKey);
  }

  @Test(expectedExceptions = Exception.class)
  void testInvalidIssuer() throws Exception {

    HashSet<String> trustedIssuers = new HashSet<>();
    DataHubJwtSigningKeyResolver resolver =
        new DataHubJwtSigningKeyResolver(trustedIssuers, null, "RSA");
    JwsHeader mockJwsHeader = Mockito.mock(JwsHeader.class);
    Claims mockClaims = Mockito.mock(Claims.class);
    resolver.resolveSigningKey(mockJwsHeader, mockClaims);
  }

  private PublicKey generatePublicKey(String alg, String key) throws Exception {

    PublicKey publicKey = null;

    if (alg.equals("RSA")) {
      try {
        key.replace(" ", "");
        byte[] decode = Base64.getDecoder().decode(key);
        X509EncodedKeySpec keySpecX509 = new X509EncodedKeySpec(decode);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        publicKey = keyFactory.generatePublic(keySpecX509);
      } catch (Exception e) {
        throw new NoSuchAlgorithmException("Unable to generate public key: ", e);
      }
    } else {
      throw new Exception("The key type of " + alg + " is not supported");
    }
    return publicKey;
  }

  private PublicKey getPublicKey(JSONObject token) throws Exception {
    PublicKey publicKey = null;

    if (token.get("kty").toString().equals("RSA")) {
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
    } else {
      throw new Exception("The key type of " + token.get("alg") + " is not supported");
    }

    return publicKey;
  }
}
