package auth.sso;

import auth.sso.oidc.support.OidcSupportConfigs;
import auth.sso.oidc.support.OidcSupportProvider;
import com.datahub.authentication.Authentication;
import com.google.inject.Inject;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Manager class that stores & serves reference to a single support {@link SsoProvider} if one
 * exists. This extends SsoManager to reuse common functionality while providing support-specific
 * behavior.
 */
@Slf4j
public class SsoSupportManager extends SsoManager {

  @Inject
  public SsoSupportManager(
      com.typesafe.config.Config configs,
      Authentication authentication,
      String ssoSettingsRequestUrl,
      CloseableHttpClient httpClient) {
    super(configs, authentication, ssoSettingsRequestUrl, httpClient);
  }

  /**
   * Returns true if support SSO is enabled, meaning a non-null {@link SsoProvider} has been
   * provided to the manager.
   *
   * @return true if support SSO logic is enabled, false otherwise.
   */
  public boolean isSupportSsoEnabled() {
    // Support SSO can only be enabled via environment/config files, not dynamic settings
    return configs.hasPath("auth.oidc.support.enabled")
        && configs.getBoolean("auth.oidc.support.enabled");
  }

  @Override
  public boolean isSsoEnabled() {
    return isSupportSsoEnabled();
  }

  /**
   * Sets or replace a Support SsoProvider.
   *
   * @param provider the new {@link SsoProvider} to be used during support authentication.
   */
  public void setSupportSsoProvider(final SsoProvider<?> provider) {
    setSsoProvider(provider);
  }

  public void clearSupportSsoProvider() {
    clearSsoProvider();
  }

  /**
   * Gets the active support {@link SsoProvider} instance.
   *
   * @return the {@SsoProvider} that should be used during support authentication and on IdP
   *     callback, or null if support SSO is not enabled.
   */
  @Nullable
  public SsoProvider<?> getSupportSsoProvider() {
    return getSsoProvider();
  }

  public void initializeSupportSsoProvider() {
    // Check if support OIDC is enabled directly
    if (configs.hasPath("auth.oidc.support.enabled")
        && configs.getBoolean("auth.oidc.support.enabled")) {
      try {
        OidcSupportConfigs oidcSupportConfigs =
            new OidcSupportConfigs.Builder().from(configs).build();
        maybeUpdateOidcSupportProvider(oidcSupportConfigs);
      } catch (Exception e) {
        // Error-level logging since this is unexpected to fail if support SSO has been configured.
        log.error(
            String.format("Error building OidcSupportConfigs from static configs %s", configs), e);
      }
    } else {
      // Clear the Support SSO Provider since no support SSO is enabled.
      clearSupportSsoProvider();
    }
  }

  private void maybeUpdateOidcSupportProvider(OidcSupportConfigs oidcSupportConfigs) {
    SsoProvider existingSsoProvider = getSupportSsoProvider();
    if (existingSsoProvider instanceof OidcSupportProvider) {
      OidcSupportProvider existingOidcSupportProvider = (OidcSupportProvider) existingSsoProvider;
      // If the existing provider is an OIDC support provider and the configs are the same, do
      // nothing.
      if (existingOidcSupportProvider.configs().equals(oidcSupportConfigs)) {
        return;
      }
    }

    OidcSupportProvider oidcSupportProvider = new OidcSupportProvider(oidcSupportConfigs);
    setSupportSsoProvider(oidcSupportProvider);
  }
}
