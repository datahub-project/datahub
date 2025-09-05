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
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthProvider;
import com.linkedin.settings.global.OAuthSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  // Unified OAuth provider configuration (static + dynamic)
  private volatile List<OAuthProvider> oauthProviders = new ArrayList<>();
  private ScheduledExecutorService scheduler;
  private static final int REFRESH_INTERVAL_MINUTES = 1;

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

    // Load static configuration
    loadStaticConfiguration(config);
  }

  private void loadStaticConfiguration(@Nonnull final Map<String, Object> config) {
    try {
      log.debug("Loading OAuth settings from static configuration");

      // Load basic settings
      this.userIdClaim = (String) config.getOrDefault("userIdClaim", DEFAULT_USER_CLAIM);
      this.algorithm = (String) config.getOrDefault("algorithm", "RS256");

      // Initialize unified provider list with static configuration
      this.oauthProviders = new ArrayList<>();
      createStaticOAuthProviders(config);

      // Load initial dynamic configuration from GlobalSettings and merge
      loadDynamicConfiguration();

      // Set up scheduled refresh of dynamic configuration
      setupDynamicConfigurationRefresh();

      // Validate final configuration
      this.isConfigured = validateConfiguration();

      if (this.isConfigured) {
        log.info(
            "OAuth authenticator successfully configured with {} OAuth provider(s)",
            this.oauthProviders.size());
        for (OAuthProvider provider : this.oauthProviders) {
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

  private void createStaticOAuthProviders(@Nonnull final Map<String, Object> config) {
    // Parse static configuration and convert to OAuthProvider objects

    // Load trusted issuers
    List<String> trustedIssuers = new ArrayList<>();
    if (config.containsKey("trustedIssuers")) {
      String issuersStr = (String) config.get("trustedIssuers");
      if (issuersStr != null && !issuersStr.trim().isEmpty()) {
        trustedIssuers = Arrays.asList(issuersStr.split(","));
      }
    }

    // Load allowed audiences
    List<String> allowedAudiences = new ArrayList<>();
    if (config.containsKey("allowedAudiences")) {
      String audiencesStr = (String) config.get("allowedAudiences");
      if (audiencesStr != null && !audiencesStr.trim().isEmpty()) {
        allowedAudiences = Arrays.asList(audiencesStr.split(","));
      }
    }

    // Load JWKS URI - either directly or derive from discoveryUri
    String jwksUri = (String) config.get("jwksUri");
    if (jwksUri == null || jwksUri.trim().isEmpty()) {
      String discoveryUri = (String) config.get("discoveryUri");
      if (discoveryUri != null && !discoveryUri.trim().isEmpty()) {
        jwksUri = deriveJwksUriFromDiscoveryUri(discoveryUri);
      }
    }

    // Only create static providers if we have sufficient configuration
    if (!trustedIssuers.isEmpty()
        && !allowedAudiences.isEmpty()
        && jwksUri != null
        && !jwksUri.trim().isEmpty()) {
      // Create a provider for each issuer/audience combination
      // For simplicity, we'll assume each issuer can work with each audience
      for (String issuer : trustedIssuers) {
        for (String audience : allowedAudiences) {
          OAuthProvider provider = new OAuthProvider();
          provider.setName("static_" + issuer.replaceAll("[^a-zA-Z0-9]", "_"));
          provider.setIssuer(issuer.trim());
          provider.setAudience(audience.trim());
          provider.setJwksUri(jwksUri.trim());
          provider.data().put("enabled", Boolean.TRUE);
          this.oauthProviders.add(provider);
        }
      }
      log.info(
          "Created {} static OAuth provider(s) from configuration", this.oauthProviders.size());
    } else {
      log.debug("No valid static OAuth configuration found - static providers not created");
    }
  }

  private boolean validateConfiguration() {
    // Check if we have any enabled OAuth providers
    long enabledProviders =
        this.oauthProviders.stream()
            .filter(provider -> Boolean.TRUE.equals(provider.data().get("enabled")))
            .count();

    boolean valid = enabledProviders > 0;

    if (!valid) {
      log.debug(
          "Configuration validation failed: no enabled OAuth providers found (total providers: {})",
          this.oauthProviders.size());
    } else {
      log.debug(
          "Configuration validation passed: {} enabled OAuth provider(s) found", enabledProviders);
    }

    return valid;
  }

  private String deriveJwksUriFromDiscoveryUri(String discoveryUri) {
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

  private String deriveJwksUriFallback(String discoveryUri) {
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

      // Find matching OAuth provider from unified list
      OAuthProvider matchingProvider = findMatchingProvider(issuer, audiences);
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
      final String userId = buildServiceUserUrn(body);

      // Ensure service account exists in DataHub (create if needed)
      ensureServiceAccountExists(userId, body);

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

  /** Find a matching OAuth provider from the unified list based on issuer and audience. */
  private OAuthProvider findMatchingProvider(String issuer, List<String> audiences) {
    for (OAuthProvider provider : this.oauthProviders) {
      // Skip disabled providers
      if (!Boolean.TRUE.equals(provider.data().get("enabled"))) {
        continue;
      }

      // Check if issuer matches
      if (!issuer.equals(provider.getIssuer())) {
        continue;
      }

      // Check if any token audience matches the provider's audience
      String providerAudience = provider.getAudience();
      if (audiences.contains(providerAudience)) {
        log.debug(
            "Found matching provider '{}' for issuer '{}' and audience '{}'",
            provider.getName(),
            issuer,
            providerAudience);
        return provider;
      }
    }

    log.debug("No matching provider found for issuer '{}' and audiences {}", issuer, audiences);
    return null;
  }

  private String buildServiceUserUrn(Claims body) {
    // Structure of user id ensures uniqueness across IdPs, in case migrations happen.
    // It parses the iss and sub fields and combines them to create a unique user id.
    // Note that this is different than how corp users are mapped at login via OIDC,
    // so this only applies to service users created via OAuth tokens.
    String issuer = body.getIssuer();
    String subject = body.get(userIdClaim, String.class);
    String sanitizedIssuer = issuer.replaceAll("https?://", "").replaceAll("[^a-zA-Z0-9]", "_");
    return String.format("%s_%s_%s", USER_ID_PREFIX, sanitizedIssuer, subject);
  }

  /**
   * Ensures that a service account user exists in DataHub. If the user doesn't exist, creates a new
   * user with CorpUserInfo, SubTypes, and Origin aspects. This is treated as a side-effect -
   * authentication will still succeed even if user creation fails.
   *
   * @param userId The unique service account user ID
   * @param claims JWT claims containing issuer and subject information
   */
  private void ensureServiceAccountExists(@Nonnull String userId, @Nonnull Claims claims) {
    try {
      // Create corp user URN
      CorpuserUrn userUrn = new CorpuserUrn(userId);

      // Check if user already exists
      boolean userExists = entityService.exists(systemOperationContext, userUrn, false);

      if (userExists) {
        log.debug("Service account user already exists: {}", userUrn);
        return;
      }

      log.info("Creating new service account user: {}", userUrn);

      // Create the aspects for the new service account
      List<MetadataChangeProposal> aspectsToIngest = createServiceAccountAspects(userUrn, claims);

      // Ingest synchronously to ensure user is immediately available
      AspectsBatch aspectsBatch =
          AspectsBatchImpl.builder()
              .mcps(
                  aspectsToIngest,
                  createSystemAuditStamp(),
                  systemOperationContext.getRetrieverContext())
              .build(systemOperationContext);

      entityService.ingestAspects(systemOperationContext, aspectsBatch, false, true);

      log.info(
          "Successfully created service account user: {} from issuer: {}",
          userId,
          claims.getIssuer());

    } catch (Exception e) {
      // Don't fail authentication if user creation fails - treat as side-effect
      log.error(
          "Failed to create service account user: {} from issuer: {}. "
              + "Authentication will proceed but user may not be visible in DataHub UI. Error: {}",
          userId,
          claims.getIssuer(),
          e.getMessage());
    }
  }

  /**
   * Creates the required aspects for a new service account user.
   *
   * @param userUrn The URN of the user to create
   * @param claims JWT claims containing issuer and subject information
   * @return List of MetadataChangeProposal objects representing the aspects to ingest
   */
  private List<MetadataChangeProposal> createServiceAccountAspects(
      @Nonnull CorpuserUrn userUrn, @Nonnull Claims claims) {
    List<MetadataChangeProposal> aspects = new ArrayList<>();

    String issuer = claims.getIssuer();
    String subject = claims.get(userIdClaim, String.class);

    // 1. CorpUserInfo aspect - basic user information
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setActive(true);
    corpUserInfo.setDisplayName(String.format("Service Account: %s @ %s", subject, issuer));
    corpUserInfo.setTitle("OAuth Service Account");

    aspects.add(createMetadataChangeProposal(userUrn, CORP_USER_INFO_ASPECT_NAME, corpUserInfo));

    // 2. SubTypes aspect - mark as SERVICE
    SubTypes subTypes = new SubTypes();
    StringArray typeNames = new StringArray();
    typeNames.add("SERVICE");
    subTypes.setTypeNames(typeNames);

    aspects.add(createMetadataChangeProposal(userUrn, SUB_TYPES_ASPECT_NAME, subTypes));

    // 3. Origin aspect - mark as EXTERNAL with issuer information
    Origin origin = new Origin();
    origin.setType(OriginType.EXTERNAL);
    origin.setExternalType(issuer); // Full issuer URL as requested

    aspects.add(createMetadataChangeProposal(userUrn, ORIGIN_ASPECT_NAME, origin));

    return aspects;
  }

  /** Helper method to create a MetadataChangeProposal for an aspect. */
  private MetadataChangeProposal createMetadataChangeProposal(
      @Nonnull CorpuserUrn userUrn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(userUrn);
    mcp.setEntityType(userUrn.getEntityType());
    mcp.setAspectName(aspectName);
    mcp.setAspect(GenericRecordUtils.serializeAspect(aspect));
    mcp.setChangeType(ChangeType.UPSERT);

    return mcp;
  }

  /**
   * Creates an AuditStamp for system-level operations.
   *
   * @return AuditStamp with system context
   */
  private AuditStamp createSystemAuditStamp() {
    return new AuditStamp()
        .setTime(System.currentTimeMillis())
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  /** Load dynamic OAuth provider configuration from GlobalSettings. */
  private void loadDynamicConfiguration() {
    try {
      log.debug("Loading dynamic OAuth configuration from GlobalSettings");

      GlobalSettingsInfo globalSettings = getGlobalSettings();
      if (globalSettings == null || !globalSettings.hasOauth()) {
        log.debug("No OAuth settings found in GlobalSettings");
        removeDynamicProviders();
        return;
      }

      OAuthSettings oauthSettings = globalSettings.getOauth();
      if (!oauthSettings.hasProviders() || oauthSettings.getProviders().isEmpty()) {
        log.debug("No OAuth providers configured in GlobalSettings");
        removeDynamicProviders();
        return;
      }

      // Remove existing dynamic providers to refresh them
      removeDynamicProviders();

      // Add enabled dynamic providers to the unified list
      List<OAuthProvider> enabledProviders = new ArrayList<>();
      for (OAuthProvider provider : oauthSettings.getProviders()) {
        if (Boolean.TRUE.equals(provider.data().get("enabled"))) {
          enabledProviders.add(provider);
          this.oauthProviders.add(provider);
          log.debug(
              "Added dynamic OAuth provider: {} (issuer: {}, audience: {})",
              provider.getName(),
              provider.getIssuer(),
              provider.getAudience());
        } else {
          log.debug("Skipping disabled OAuth provider: {}", provider.getName());
        }
      }

      log.info(
          "Successfully loaded {} enabled OAuth provider(s) from GlobalSettings (total providers: {})",
          enabledProviders.size(),
          this.oauthProviders.size());

      // Re-validate configuration after dynamic config changes
      boolean previouslyConfigured = this.isConfigured;
      this.isConfigured = validateConfiguration();

      if (!previouslyConfigured && this.isConfigured) {
        log.info("OAuth authenticator now configured with dynamic providers");
      } else if (previouslyConfigured && !this.isConfigured) {
        log.warn(
            "OAuth authenticator configuration is no longer valid after dynamic configuration update");
      }

    } catch (Exception e) {
      log.error("Failed to load dynamic OAuth configuration from GlobalSettings", e);
      // On error, remove dynamic configuration to avoid stale data
      removeDynamicProviders();

      // Re-validate configuration after clearing dynamic config
      this.isConfigured = validateConfiguration();
    }
  }

  private void removeDynamicProviders() {
    // Remove any dynamic providers (those without "static_" prefix in name)
    this.oauthProviders.removeIf(provider -> !provider.getName().startsWith("static_"));
  }

  /** Set up scheduled refresh of dynamic OAuth configuration. */
  private void setupDynamicConfigurationRefresh() {
    if (this.scheduler != null) {
      this.scheduler.shutdown();
    }

    this.scheduler =
        Executors.newScheduledThreadPool(
            1,
            r -> {
              Thread t = new Thread(r, "oauth-config-refresher");
              t.setDaemon(true);
              return t;
            });

    this.scheduler.scheduleAtFixedRate(
        this::loadDynamicConfiguration,
        REFRESH_INTERVAL_MINUTES,
        REFRESH_INTERVAL_MINUTES,
        TimeUnit.MINUTES);

    log.info("Scheduled OAuth configuration refresh every {} minute(s)", REFRESH_INTERVAL_MINUTES);
  }

  /** Retrieve GlobalSettings from the entity service. */
  private GlobalSettingsInfo getGlobalSettings() {
    try {
      Object globalSettingsAspect =
          this.entityService.getLatestAspect(
              this.systemOperationContext, GLOBAL_SETTINGS_URN, GLOBAL_SETTINGS_INFO_ASPECT_NAME);

      if (globalSettingsAspect instanceof GlobalSettingsInfo) {
        return (GlobalSettingsInfo) globalSettingsAspect;
      }

      return null;
    } catch (Exception e) {
      log.warn("Failed to retrieve GlobalSettings", e);
      return null;
    }
  }

  /** Cleanup method to shutdown the scheduler when the authenticator is destroyed. */
  public void destroy() {
    if (this.scheduler != null && !this.scheduler.isShutdown()) {
      this.scheduler.shutdown();
      try {
        if (!this.scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          this.scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        this.scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
      log.info("OAuth configuration refresh scheduler shutdown");
    }
  }
}
