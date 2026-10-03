package io.datahub.ownership.auth;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.plugins.auth.authentication.Authenticator;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Authenticates a plain (Keycloak / OIDC) RS256 JWT and maps a configured claim (default
 * {@code email}) to a DataHub corpuser. This lets DataHub accept tokens minted by an external IdP
 * in addition to its own access tokens.
 *
 * <p>It deliberately {@code return}s {@code null} for anything it cannot positively authenticate
 * (missing/blank bearer, non-RSA signature, unknown {@code kid}, expired, wrong issuer/audience,
 * missing user claim). The {@code AuthenticatorChain} treats a null as "not me, try the next
 * authenticator", so DataHub's own {@code DataHubTokenAuthenticator} continues to handle DataHub
 * tokens unchanged.
 *
 * <p>Config keys (passed via {@code init}):
 * <ul>
 *   <li>{@code jwksUri} (required) — the IdP JWKS endpoint, e.g.
 *       {@code https://kc/realms/<realm>/protocol/openid-connect/certs}</li>
 *   <li>{@code trustedIssuers} (optional, comma-separated) — if set, the token {@code iss} must match one</li>
 *   <li>{@code allowedAudiences} (optional, comma-separated) — if set, the token {@code aud} must intersect</li>
 *   <li>{@code userClaim} (optional, default {@code email}) — the claim whose value becomes the corpuser id</li>
 * </ul>
 */
public class KeycloakJwtAuthenticator implements Authenticator {

    private static final Logger log = LoggerFactory.getLogger(KeycloakJwtAuthenticator.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private String userClaim = "email";
    private Set<String> trustedIssuers = Collections.emptySet();
    private Set<String> allowedAudiences = Collections.emptySet();
    private SigningKeyResolver keyResolver;

    @Override
    public void init(@Nonnull Map<String, Object> config, @Nullable AuthenticatorContext context) {
        String jwksUri = asString(config.get("jwksUri"));
        if (jwksUri == null || jwksUri.isBlank()) {
            throw new IllegalArgumentException("KeycloakJwtAuthenticator requires a 'jwksUri' config value");
        }
        this.keyResolver = new JwksSigningKeyResolver(jwksUri);
        this.trustedIssuers = csvToSet(asString(config.get("trustedIssuers")));
        this.allowedAudiences = csvToSet(asString(config.get("allowedAudiences")));
        String configuredClaim = asString(config.get("userClaim"));
        if (configuredClaim != null && !configuredClaim.isBlank()) {
            this.userClaim = configuredClaim.trim();
        }
        log.info("KeycloakJwtAuthenticator initialized (jwksUri={}, userClaim={}, trustedIssuers={}, allowedAudiences={})",
                jwksUri, userClaim, trustedIssuers, allowedAudiences);
    }

    /** Test seam: inject a resolver backed by a known key instead of a live JWKS endpoint. */
    void configureForTest(SigningKeyResolver keyResolver, String userClaim,
                          Set<String> trustedIssuers, Set<String> allowedAudiences) {
        this.keyResolver = keyResolver;
        this.userClaim = userClaim;
        this.trustedIssuers = trustedIssuers;
        this.allowedAudiences = allowedAudiences;
    }

    @Override
    @Nullable
    public Authentication authenticate(@Nonnull AuthenticationRequest request) throws AuthenticationException {
        String header = request.getRequestHeaders().get(AUTHORIZATION_HEADER);
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            return null;
        }
        String token = header.substring(BEARER_PREFIX.length()).trim();
        if (token.isEmpty()) {
            return null;
        }

        final Claims claims;
        try {
            Jws<Claims> jws = Jwts.parserBuilder()
                    .setSigningKeyResolver(keyResolver)
                    .build()
                    .parseClaimsJws(token);
            claims = jws.getBody();
        } catch (Exception e) {
            // Bad signature, expired, not an RSA/Keycloak token, etc. Defer to other authenticators.
            log.debug("KeycloakJwtAuthenticator could not validate token: {}", e.getMessage());
            return null;
        }

        if (!trustedIssuers.isEmpty() && !trustedIssuers.contains(claims.getIssuer())) {
            log.debug("Rejecting JWT: issuer {} not in trustedIssuers", claims.getIssuer());
            return null;
        }
        if (!allowedAudiences.isEmpty() && audiences(claims).stream().noneMatch(allowedAudiences::contains)) {
            log.debug("Rejecting JWT: audience {} not in allowedAudiences", audiences(claims));
            return null;
        }

        String user = claims.get(userClaim, String.class);
        if (user == null || user.isBlank()) {
            log.debug("Rejecting JWT: user claim '{}' is missing or blank", userClaim);
            return null;
        }

        Map<String, Object> claimMap = new LinkedHashMap<>(claims);
        return new Authentication(new Actor(ActorType.USER, user.trim()), BEARER_PREFIX + token, claimMap);
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> audiences(Claims claims) {
        Object aud = claims.get("aud");
        if (aud == null) {
            return Collections.emptySet();
        }
        if (aud instanceof String s) {
            return Set.of(s);
        }
        if (aud instanceof Collection<?> c) {
            return ((Collection<Object>) c).stream().map(String::valueOf).collect(Collectors.toSet());
        }
        return Set.of(String.valueOf(aud));
    }

    private static String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static Set<String> csvToSet(String csv) {
        if (csv == null || csv.isBlank()) {
            return Collections.emptySet();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(HashSet::new));
    }

    // Visible for assertions in tests.
    String getUserClaim() {
        return userClaim;
    }

    List<String> getTrustedIssuers() {
        return List.copyOf(trustedIssuers);
    }
}
