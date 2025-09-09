package com.datahub.authentication.authenticator;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthProvider;
import com.linkedin.settings.global.OAuthSettings;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for fetching and managing OAuth provider configurations from both static configuration
 * and dynamic GlobalSettings. Provides caching and background refresh capabilities.
 *
 * <p>This service maintains a unified list of OAuth providers from:
 *
 * <ul>
 *   <li>Static configuration from application.yaml
 *   <li>Dynamic configuration from GlobalSettings entity
 * </ul>
 *
 * <p>The service automatically refreshes dynamic configuration on a scheduled interval and provides
 * methods for both cached access and forced refresh.
 */
@Slf4j
public class OAuthConfigurationFetcher {

  private static final int REFRESH_INTERVAL_MINUTES = 1;

  // Configuration dependencies
  private EntityService<?> entityService;
  private OperationContext systemOperationContext;

  // Unified OAuth provider configuration (static + dynamic)
  private volatile List<OAuthProvider> oauthProviders = new ArrayList<>();
  private ScheduledExecutorService scheduler;
  private volatile boolean isConfigured = false;

  /**
   * Initializes the configuration fetcher with dependencies and loads initial configuration.
   *
   * @param staticConfig Static configuration from application.yaml
   * @param entityService EntityService for accessing GlobalSettings
   * @param systemOperationContext System operation context for entity operations
   */
  public void initialize(
      @Nonnull Map<String, Object> staticConfig,
      @Nonnull EntityService<?> entityService,
      @Nonnull OperationContext systemOperationContext) {

    this.entityService = entityService;
    this.systemOperationContext = systemOperationContext;

    // Initialize unified provider list with static configuration
    this.oauthProviders = new ArrayList<>();
    createStaticOAuthProviders(staticConfig);

    // Load initial dynamic configuration from GlobalSettings and merge
    loadDynamicConfiguration();

    // Set up scheduled refresh of dynamic configuration
    setupDynamicConfigurationRefresh();

    // Validate final configuration
    this.isConfigured = validateConfiguration();

    if (this.isConfigured) {
      log.info(
          "OAuth configuration fetcher initialized with {} OAuth provider(s)",
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
          "OAuth configuration incomplete. Please provide trustedIssuers, allowedAudiences, "
              + "and either jwksUri or discoveryUri in application.yaml, or configure OAuth providers in GlobalSettings.");
    }
  }

  /**
   * Returns the cached OAuth provider configuration.
   *
   * @return List of OAuth providers from both static and dynamic sources
   */
  public List<OAuthProvider> getCachedConfiguration() {
    return new ArrayList<>(this.oauthProviders);
  }

  /**
   * Forces a refresh of dynamic configuration from GlobalSettings and returns the updated
   * configuration.
   *
   * @return List of OAuth providers after forced refresh
   */
  public List<OAuthProvider> forceRefreshConfiguration() {
    log.debug("Forcing refresh of OAuth configuration");
    loadDynamicConfiguration();
    return getCachedConfiguration();
  }

  /**
   * Returns whether the configuration fetcher has valid OAuth providers configured.
   *
   * @return true if at least one enabled OAuth provider is configured
   */
  public boolean isConfigured() {
    return this.isConfigured;
  }

  /**
   * Finds a matching OAuth provider from the unified list based on issuer and audience.
   *
   * @param issuer The JWT issuer to match
   * @param audiences List of JWT audiences to match
   * @return Matching OAuth provider or null if no match found
   */
  @Nullable
  public OAuthProvider findMatchingProvider(
      @Nonnull String issuer, @Nonnull List<String> audiences) {
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

  /** Cleanup method to shutdown the scheduler when the fetcher is destroyed. */
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

  /**
   * Creates static OAuth providers from application.yaml configuration.
   *
   * @param config Static configuration map
   */
  private void createStaticOAuthProviders(@Nonnull Map<String, Object> config) {
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
        jwksUri = JwksUriResolver.deriveJwksUriFromDiscoveryUri(discoveryUri);
      }
    }

    // Only create static providers if we have sufficient configuration
    if (!trustedIssuers.isEmpty()
        && !allowedAudiences.isEmpty()
        && jwksUri != null
        && !jwksUri.trim().isEmpty()) {
      // Create a provider for each issuer/audience combination
      for (String issuer : trustedIssuers) {
        for (String audience : allowedAudiences) {
          OAuthProvider provider = new OAuthProvider();
          provider.setName("static_" + issuer.replaceAll("[^a-zA-Z0-9]", "_"));
          provider.setIssuer(issuer.trim());
          provider.setAudience(audience.trim());
          provider.setJwksUri(jwksUri.trim());
          provider.setEnabled(true);
          this.oauthProviders.add(provider);
        }
      }
      log.info(
          "Created {} static OAuth provider(s) from configuration", this.oauthProviders.size());
    } else {
      log.debug("No valid static OAuth configuration found - static providers not created");
    }
  }

  /** Loads dynamic OAuth provider configuration from GlobalSettings. */
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
        if (Boolean.TRUE.equals(provider.isEnabled())) {
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
        log.info("OAuth configuration fetcher now configured with dynamic providers");
      } else if (previouslyConfigured && !this.isConfigured) {
        log.warn("OAuth configuration is no longer valid after dynamic configuration update");
      }

    } catch (Exception e) {
      log.error("Failed to load dynamic OAuth configuration from GlobalSettings", e);
      // On error, remove dynamic configuration to avoid stale data
      removeDynamicProviders();

      // Re-validate configuration after clearing dynamic config
      this.isConfigured = validateConfiguration();
    }
  }

  /** Removes dynamic providers from the unified list. */
  private void removeDynamicProviders() {
    // Remove any dynamic providers (those without "static_" prefix in name)
    this.oauthProviders.removeIf(provider -> !provider.getName().startsWith("static_"));
  }

  /**
   * Validates the current OAuth provider configuration.
   *
   * @return true if at least one enabled provider is configured
   */
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

  /** Sets up scheduled refresh of dynamic OAuth configuration. */
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

  /**
   * Retrieves GlobalSettings from the entity service.
   *
   * @return GlobalSettingsInfo or null if not found
   */
  @Nullable
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
}
