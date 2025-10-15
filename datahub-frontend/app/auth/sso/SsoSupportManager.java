package auth.sso;

import auth.sso.oidc.support.OidcSupportConfigs;
import auth.sso.oidc.support.OidcSupportProvider;
import com.datahub.authentication.Authentication;
import com.google.inject.Inject;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import play.mvc.Http;

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
    if (configs.hasPath("auth.oidc.support.enabled")
        && configs.getBoolean("auth.oidc.support.enabled")) {
      return true;
    }
    refreshSupportSsoProvider();
    return getSsoProvider() != null;
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

    refreshSupportSsoProvider();
  }

  private void refreshSupportSsoProvider() {
    final Optional<String> maybeSsoSettingsJsonStr = getDynamicSsoSettings();
    if (maybeSsoSettingsJsonStr.isEmpty()) {
      return;
    }

    // If we receive a non-empty response, try to update the support SSO provider.
    final String ssoSettingsJsonStr = maybeSsoSettingsJsonStr.get();
    try {
      OidcSupportConfigs oidcSupportConfigs =
          new OidcSupportConfigs.Builder().from(configs, ssoSettingsJsonStr).build();
      maybeUpdateOidcSupportProvider(oidcSupportConfigs);
    } catch (Exception e) {
      log.error(
          String.format(
              "Error building OidcSupportConfigs from invalid json %s, reusing previous settings",
              ssoSettingsJsonStr),
          e);
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

  /** Call the Auth Service to get SSO settings */
  @Nonnull
  private Optional<String> getDynamicSsoSettings() {
    CloseableHttpResponse response = null;
    try {
      final HttpPost request = new HttpPost(ssoSettingsRequestUrl);

      // Build JSON request to verify credentials for a native user.
      request.setEntity(new StringEntity(""));

      // Add authorization header with DataHub frontend system id and secret.
      request.addHeader(Http.HeaderNames.AUTHORIZATION, authentication.getCredentials());

      response = httpClient.execute(request);
      final HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        // Successfully received the SSO settings
        return Optional.of(EntityUtils.toString(entity));
      } else {
        log.debug("No support SSO settings received from Auth Service, reusing previous settings");
      }
    } catch (Exception e) {
      log.warn("Failed to get support SSO settings due to exception, reusing previous settings", e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.warn("Failed to close http response", e);
      }
    }
    return Optional.empty();
  }
}
