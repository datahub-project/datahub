package com.datahub.authentication.authenticator;

import static com.datahub.authentication.AuthenticationConstants.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.token.DataHubOAuthSigningKeyResolver;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.linkedin.common.OriginType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ServiceAccountService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.OAuthProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Authenticator that validates OAuth2 / OIDC JWT tokens using a unified OAuth provider approach.
 * Supports both static configuration (application.yaml) and dynamic configuration (GlobalSettings).
 * Primary use case is to authenticate service accounts via OAuth tokens (not individual users).
 *
 * <p><strong>Static Configuration (application.yaml):</strong>
 *
 * <ul>
 *   <li>trustedIssuers: List of trusted JWT issuers (must match the 'iss' claim in JWT tokens)
 *   <li>allowedAudiences: List of allowed JWT audiences (must match the 'aud' claim in JWT tokens)
 *   <li>jwksUri: URI to fetch JWT signing keys, OR discoveryUri to auto-derive JWKS URI
 *   <li>Optional: userIdClaim (defaults to "sub")
 *   <li>Optional: algorithm (defaults to "RS256")
 * </ul>
 *
 * <p><strong>Dynamic Configuration (GlobalSettings):</strong> OAuth providers can also be
 * configured dynamically through GlobalSettings.oauth.providers. Dynamic providers are refreshed
 * automatically every minute.
 *
 * <p><strong>Unified Provider Chain:</strong> The authenticator maintains a unified list of OAuth
 * providers (static + dynamic) and validates tokens by finding the first matching provider based on
 * issuer and audience claims.
 *
 * <p>This authenticator creates service account users automatically in DataHub when they first
 * authenticate, with proper SubTypes (SERVICE) and Origin aspects.
 */
@Slf4j
public class DataHubOAuthAuthenticator implements Authenticator {

  static final String USER_ID_PREFIX = "__oauth_";
  static final String DEFAULT_USER_CLAIM = "sub";

  // Configuration fields
  private EntityService<?> entityService;
  private OperationContext systemOperationContext;
  private String userIdClaim;
  private String algorithm;
  private boolean isConfigured = false;

  // Service dependencies
  private ServiceAccountService serviceAccountService;
  private OAuthConfigurationFetcher configurationFetcher;

  @Override
  public void init(
      @Nonnull final Map<String, Object> config, @Nullable final AuthenticatorContext context) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    Objects.requireNonNull(context, "Context parameter cannot be null");

    // Check if OAuth authentication is enabled
    boolean enabled = Boolean.parseBoolean(config.getOrDefault("enabled", "false").toString());
    if (!enabled) {
      log.info("OAuth authentication is disabled via configuration. Skipping initialization.");
      this.isConfigured = false;
      return;
    }

    // Get EntityService from context
    if (!context.data().containsKey(ENTITY_SERVICE)) {
      throw new IllegalArgumentException(
          "Unable to initialize DataHubOAuthAuthenticator, entity service reference not found.");
    }
    final Object entityServiceObj = context.data().get(ENTITY_SERVICE);
    if (!(entityServiceObj instanceof EntityService)) {
      throw new RuntimeException(
          "Unable to initialize DataHubOAuthAuthenticator, entity service reference is not of type: "
              + "EntityService.class, found: "
              + entityServiceObj.getClass());
    }
    this.entityService = (EntityService<?>) entityServiceObj;

    // Get system operation context
    if (!context.data().containsKey("systemOperationContext")) {
      throw new IllegalArgumentException(
          "Unable to initialize DataHubOAuthAuthenticator, system operation context not found.");
    }
    final Object systemOpContextObj = context.data().get("systemOperationContext");
    if (!(systemOpContextObj instanceof OperationContext)) {
      throw new RuntimeException(
          "Unable to initialize DataHubOAuthAuthenticator, system operation context is not of type: "
              + "OperationContext.class, found: "
              + systemOpContextObj.getClass());
    }
    this.systemOperationContext = (OperationContext) systemOpContextObj;

    // Initialize services
    this.serviceAccountService = new ServiceAccountService();
    this.configurationFetcher = new OAuthConfigurationFetcher();

    // Load static configuration
    loadStaticConfiguration(config);
  }

  private void loadStaticConfiguration(@Nonnull final Map<String, Object> config) {
    try {
      log.debug("Loading OAuth settings from static configuration");

      // Load basic settings
      this.userIdClaim = (String) config.getOrDefault("userIdClaim", DEFAULT_USER_CLAIM);
      this.algorithm = (String) config.getOrDefault("algorithm", "RS256");

      // Initialize configuration fetcher
      this.configurationFetcher.initialize(config, entityService, systemOperationContext);

      // Validate final configuration
      this.isConfigured = this.configurationFetcher.isConfigured();

      if (this.isConfigured) {
        List<OAuthProvider> providers = this.configurationFetcher.getCachedConfiguration();
        log.info(
            "OAuth authenticator successfully configured with {} OAuth provider(s)",
            providers.size());
        for (OAuthProvider provider : providers) {
          log.debug(
              "OAuth Provider - Name: {}, Issuer: {}, Audience: {}, JWKS URI: {}",
              provider.getName(),
              provider.getIssuer(),
              provider.getAudience(),
              provider.getJwksUri());
        }
      } else {
        log.warn(
            "OAuth authenticator configuration incomplete. Please provide trustedIssuers, allowedAudiences, "
                + "and either jwksUri or discoveryUri in application.yaml, or configure OAuth providers in GlobalSettings.");
      }

    } catch (Exception e) {
      log.error("Failed to load OAuth static configuration", e);
      this.isConfigured = false;
    }
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticationRequest context)
      throws AuthenticationException {
    Objects.requireNonNull(context);

    // Check if the authenticator is properly configured
    if (!isConfigured) {
      throw new AuthenticationException(
          "OAuth authenticator is not configured. Please configure either SSO settings in GlobalSettings or provide static configuration.");
    }

    try {
      String jwtToken = context.getRequestHeaders().get(AUTHORIZATION_HEADER_NAME);

      log.info("Request headers are: {}", context.getRequestHeaders());

      if (jwtToken == null
          || (!jwtToken.startsWith("Bearer ") && !jwtToken.startsWith("bearer "))) {
        throw new AuthenticationException("Invalid Authorization header");
      }

      String token = getToken(jwtToken);

      // Parse JWT to extract issuer and audience (without signature verification)
      String[] tokenParts = token.split("\\.");
      if (tokenParts.length != 3) {
        throw new AuthenticationException("Invalid JWT token format");
      }

      String payload = new String(java.util.Base64.getUrlDecoder().decode(tokenParts[1]));
      com.fasterxml.jackson.databind.JsonNode payloadJson =
          new com.fasterxml.jackson.databind.ObjectMapper().readTree(payload);

      String issuer = payloadJson.has("iss") ? payloadJson.get("iss").asText() : null;
      if (issuer == null) {
        throw new AuthenticationException("Missing issuer claim in JWT token");
      }

      // Get audience(s) from token
      List<String> audiences = new ArrayList<>();
      if (payloadJson.has("aud")) {
        com.fasterxml.jackson.databind.JsonNode audNode = payloadJson.get("aud");
        if (audNode.isArray()) {
          audNode.forEach(node -> audiences.add(node.asText()));
        } else {
          audiences.add(audNode.asText());
        }
      }

      if (audiences.isEmpty()) {
        throw new AuthenticationException("Missing audience claim in JWT token");
      }

      // Find matching OAuth provider from configuration fetcher
      OAuthProvider matchingProvider = configurationFetcher.findMatchingProvider(issuer, audiences);
      if (matchingProvider == null) {
        throw new AuthenticationException(
            "No configured OAuth provider matches token issuer '"
                + issuer
                + "' and audiences "
                + audiences);
      }

      log.debug(
          "Using OAuth provider '{}' to validate token from issuer '{}'",
          matchingProvider.getName(),
          issuer);

      // Validate JWT signature using the matching provider's JWKS
      HashSet<String> trustedIssuers = new HashSet<>();
      trustedIssuers.add(issuer);

      Jws<Claims> claims =
          Jwts.parserBuilder()
              .setSigningKeyResolver(
                  new DataHubOAuthSigningKeyResolver(
                      trustedIssuers, matchingProvider.getJwksUri(), algorithm))
              .build()
              .parseClaimsJws(token);

      Claims body = claims.getBody();

      // Extract subject (userIdClaim)
      final String subject = body.get(userIdClaim, String.class);
      if (subject == null) {
        throw new AuthenticationException("Missing required claim: " + userIdClaim);
      }

      // Build unique service account user ID
      final String userId = serviceAccountService.buildServiceUserUrn(issuer, subject);

      // Ensure service account exists in DataHub (create if needed)
      serviceAccountService.ensureServiceAccountExists(
          userId, issuer, subject, entityService, systemOperationContext);

      // TODO: distinguish USER vs SERVICE based on scope or custom claim
      ActorType actorType = ActorType.USER;
      return new Authentication(new Actor(actorType, userId), jwtToken);
    } catch (Exception e) {
      throw new AuthenticationException("OAuth token validation failed: " + e.getMessage());
    }
  }

  private String getToken(String jwtToken) {
    var tokenArray = jwtToken.split(" ");
    return tokenArray.length == 1 ? tokenArray[0] : tokenArray[1];
  }

  /** Delegation method for test compatibility. */
  private String buildServiceUserUrn(Claims body) {
    String issuer = body.getIssuer();
    String subject = body.get(userIdClaim, String.class);
    return serviceAccountService.buildServiceUserUrn(issuer, subject);
  }

  /** Delegation method for test compatibility. */
  private void ensureServiceAccountExists(@Nonnull String userId, @Nonnull Claims claims) {
    String issuer = claims.getIssuer();
    String subject = claims.get(userIdClaim, String.class);
    serviceAccountService.ensureServiceAccountExists(
        userId, issuer, subject, entityService, systemOperationContext);
  }

  /** Delegation method for test compatibility. */
  private List<MetadataChangeProposal> createServiceAccountAspects(
      @Nonnull CorpuserUrn userUrn, @Nonnull Claims claims) {
    String issuer = claims.getIssuer();
    String subject = claims.get(userIdClaim, String.class);
    String displayName = String.format("Service Account: %s @ %s", subject, issuer);
    return serviceAccountService.createServiceAccountAspects(
        userUrn, displayName, OriginType.EXTERNAL, issuer);
  }

  /**
   * Returns the current OAuth provider configuration. This method is provided for backward
   * compatibility with existing tests.
   *
   * @return List of OAuth providers
   */
  public List<OAuthProvider> getOAuthProviders() {
    return configurationFetcher != null
        ? configurationFetcher.getCachedConfiguration()
        : new ArrayList<>();
  }

  /** Cleanup method to shutdown the scheduler when the authenticator is destroyed. */
  public void destroy() {
    if (this.configurationFetcher != null) {
      this.configurationFetcher.destroy();
    }
  }
}
