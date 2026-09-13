package io.datahub.ownership.auth;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationRequest;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SigningKeyResolverAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class KeycloakJwtAuthenticatorTest {

    private static KeyPair rsa;
    private static KeyPair otherRsa;
    private KeycloakJwtAuthenticator auth;

    static {
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
            g.initialize(2048);
            rsa = g.generateKeyPair();
            otherRsa = g.generateKeyPair();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Resolver that always returns our trusted public key (stands in for the JWKS endpoint). */
    private SigningKeyResolverAdapter resolverFor(KeyPair kp) {
        return new SigningKeyResolverAdapter() {
            @Override
            public Key resolveSigningKey(JwsHeader header, Claims claims) {
                if (!String.valueOf(header.getAlgorithm()).startsWith("RS")) {
                    throw new IllegalArgumentException("non-RSA");
                }
                return kp.getPublic();
            }
        };
    }

    @BeforeEach
    void setUp() {
        auth = new KeycloakJwtAuthenticator();
        auth.configureForTest(resolverFor(rsa), "email",
                Set.of("https://kc/realms/demo"), Set.of());
    }

    private String jwt(KeyPair signer, String email, String issuer, Date exp) {
        return Jwts.builder()
                .setHeaderParam("kid", "test-kid")
                .setIssuer(issuer)
                .setSubject("subject-id")
                .claim("email", email)
                .setExpiration(exp)
                .signWith(signer.getPrivate(), SignatureAlgorithm.RS256)
                .compact();
    }

    private AuthenticationRequest bearer(String token) {
        return new AuthenticationRequest(Map.of("Authorization", "Bearer " + token));
    }

    @Test
    void authenticatesValidTokenToCorpuserFromEmail() throws Exception {
        String token = jwt(rsa, "alice@corp.com", "https://kc/realms/demo",
                new Date(System.currentTimeMillis() + 60_000));

        Authentication result = auth.authenticate(bearer(token));

        assertThat(result).isNotNull();
        assertThat(result.getActor().getType()).isEqualTo(ActorType.USER);
        assertThat(result.getActor().getId()).isEqualTo("alice@corp.com");
        assertThat(result.getActor().toUrnStr()).isEqualTo("urn:li:corpuser:alice@corp.com");
    }

    @Test
    void rejectsExpiredToken() throws Exception {
        String token = jwt(rsa, "alice@corp.com", "https://kc/realms/demo",
                new Date(System.currentTimeMillis() - 60_000));
        assertThat(auth.authenticate(bearer(token))).isNull();
    }

    @Test
    void rejectsUntrustedIssuer() throws Exception {
        String token = jwt(rsa, "alice@corp.com", "https://evil/realms/x",
                new Date(System.currentTimeMillis() + 60_000));
        assertThat(auth.authenticate(bearer(token))).isNull();
    }

    @Test
    void rejectsWrongSigningKey() throws Exception {
        // Signed by a key the resolver does not trust -> signature verification fails.
        String token = jwt(otherRsa, "alice@corp.com", "https://kc/realms/demo",
                new Date(System.currentTimeMillis() + 60_000));
        assertThat(auth.authenticate(bearer(token))).isNull();
    }

    @Test
    void rejectsMissingEmailClaim() throws Exception {
        String token = Jwts.builder()
                .setHeaderParam("kid", "test-kid")
                .setIssuer("https://kc/realms/demo")
                .setSubject("subject-id")
                .setExpiration(new Date(System.currentTimeMillis() + 60_000))
                .signWith(rsa.getPrivate(), SignatureAlgorithm.RS256)
                .compact();
        assertThat(auth.authenticate(bearer(token))).isNull();
    }

    @Test
    void fallsThroughForHs256DataHubStyleToken() throws Exception {
        // A DataHub-style HS256 token must NOT be accepted here (resolver rejects non-RSA) so the
        // chain falls through to DataHubTokenAuthenticator. authenticate returns null.
        byte[] secret = "0123456789012345678901234567890123456789".getBytes();
        String hs = Jwts.builder()
                .setSubject("datahub")
                .claim("email", "datahub@corp.com")
                .signWith(io.jsonwebtoken.security.Keys.hmacShaKeyFor(secret), SignatureAlgorithm.HS256)
                .compact();
        assertThat(auth.authenticate(bearer(hs))).isNull();
    }

    @Test
    void returnsNullWhenNoBearerHeader() throws Exception {
        assertThat(auth.authenticate(new AuthenticationRequest(Map.of()))).isNull();
        assertThat(auth.authenticate(new AuthenticationRequest(Map.of("Authorization", "Basic abc")))).isNull();
    }
}
