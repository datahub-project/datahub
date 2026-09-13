package io.datahub.ownership.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the RSA public key used to verify a Keycloak (or any OIDC) RS256 JWT by fetching the
 * issuer's JWKS endpoint and matching the token's {@code kid} header.
 *
 * <p>Keys are cached by {@code kid}. On a cache miss (e.g. after Keycloak rotates signing keys) the
 * JWKS is re-fetched at most once per {@link #refreshCooldown} window, so a flood of tokens with an
 * unknown {@code kid} cannot hammer the JWKS endpoint.
 *
 * <p>Only RSA keys / RS* algorithms are accepted; a non-RSA algorithm (e.g. the HS256 used by
 * DataHub's own tokens) makes {@link #resolveSigningKey} throw, which the authenticator treats as
 * "not my token" and returns null so the chain falls through.
 */
public class JwksSigningKeyResolver extends SigningKeyResolverAdapter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();

    private final String jwksUri;
    private final HttpClient httpClient;
    private final Duration refreshCooldown;
    private final Map<String, PublicKey> keysByKid = new ConcurrentHashMap<>();
    private volatile long lastRefreshEpochMs = 0L;

    public JwksSigningKeyResolver(String jwksUri) {
        this(jwksUri, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                Duration.ofMinutes(5));
    }

    public JwksSigningKeyResolver(String jwksUri, HttpClient httpClient, Duration refreshCooldown) {
        this.jwksUri = jwksUri;
        this.httpClient = httpClient;
        this.refreshCooldown = refreshCooldown;
    }

    @Override
    public Key resolveSigningKey(JwsHeader header, Claims claims) {
        String alg = header.getAlgorithm();
        if (alg == null || !alg.startsWith("RS")) {
            // Not an RSA-signed token (e.g. DataHub's HS256 token) — reject so the chain continues.
            throw new IllegalArgumentException("Unsupported JWS algorithm for Keycloak auth: " + alg);
        }
        String kid = header.getKeyId();
        if (kid == null) {
            throw new IllegalArgumentException("JWT is missing the 'kid' header");
        }
        PublicKey key = keysByKid.get(kid);
        if (key == null) {
            refreshIfAllowed();
            key = keysByKid.get(kid);
        }
        if (key == null) {
            throw new IllegalStateException("No JWKS key found for kid " + kid);
        }
        return key;
    }

    private synchronized void refreshIfAllowed() {
        long now = System.currentTimeMillis();
        if (now - lastRefreshEpochMs < refreshCooldown.toMillis() && !keysByKid.isEmpty()) {
            return;
        }
        lastRefreshEpochMs = now;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(jwksUri))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                return;
            }
            JsonNode keys = MAPPER.readTree(resp.body()).path("keys");
            for (JsonNode jwk : keys) {
                if (!"RSA".equals(jwk.path("kty").asText())) {
                    continue;
                }
                String kid = jwk.path("kid").asText(null);
                String n = jwk.path("n").asText(null);
                String e = jwk.path("e").asText(null);
                if (kid == null || n == null || e == null) {
                    continue;
                }
                keysByKid.put(kid, buildRsaKey(n, e));
            }
        } catch (Exception ex) {
            // Swallow — resolveSigningKey will throw "no key" and the token is rejected.
        }
    }

    private static PublicKey buildRsaKey(String nB64Url, String eB64Url) throws Exception {
        BigInteger modulus = new BigInteger(1, URL_DECODER.decode(nB64Url));
        BigInteger exponent = new BigInteger(1, URL_DECODER.decode(eB64Url));
        return KeyFactory.getInstance("RSA").generatePublic(new RSAPublicKeySpec(modulus, exponent));
    }
}
