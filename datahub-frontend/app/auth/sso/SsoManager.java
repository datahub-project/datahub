package auth.sso;

import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.OidcProvider;
import com.datahub.authentication.Authentication;
import java.util.Objects;
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
 * Singleton class that stores & serves reference to a single {@link SsoProvider} if one exists.
 * TODO: Refactor SsoManager to only accept SsoConfigs when initialized. See SsoConfigs TODO as
 * well.
 */
@Slf4j
public class SsoManager {

  private SsoProvider<?> _provider; // Only one active provider at a time.
  private final Authentication authentication; // Authentication used to fetch SSO settings from GMS
  private final String ssoSettingsRequestUrl; // SSO settings request URL.
  private final CloseableHttpClient httpClient; // HTTP client for making requests to GMS.
  private com.typesafe.config.Config configs;

  public SsoManager(
      com.typesafe.config.Config configs,
      Authentication authentication,
      String ssoSettingsRequestUrl,
      CloseableHttpClient httpClient) {
    this.configs = configs;
    this.authentication = Objects.requireNonNull(authentication, "authentication cannot be null");
    this.ssoSettingsRequestUrl =
        Objects.requireNonNull(ssoSettingsRequestUrl, "ssoSettingsRequestUrl cannot be null");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient cannot be null");
    _provider = null;
  }

  /**
   * Returns true if SSO is enabled, meaning a non-null {@link SsoProvider} has been provided to the
   * manager.
   *
   * @return true if SSO logic is enabled, false otherwise.
   */
  public boolean isSsoEnabled() {
    if (configs.hasPath("auth.oidc.enabled") && configs.getBoolean("auth.oidc.enabled")) {
      return true;
    }
    refreshSsoProvider();
    return _provider != null;
  }

  /**
   * Sets or replace a SsoProvider.
   *
   * @param provider the new {@link SsoProvider} to be used during authentication.
   */
  public void setSsoProvider(final SsoProvider<?> provider) {
    _provider = provider;
  }

  public void setConfigs(final com.typesafe.config.Config configs) {
    this.configs = configs;
  }

  public void clearSsoProvider() {
    _provider = null;
  }

  /**
   * Gets the active {@link SsoProvider} instance.
   *
   * @return the {@SsoProvider} that should be used during authentication and on IdP callback, or
   *     null if SSO is not enabled.
   */
  @Nullable
  public SsoProvider<?> getSsoProvider() {
    return _provider;
  }

  public void initializeSsoProvider() {
    SsoConfigs ssoConfigs = null;
    try {
      ssoConfigs = new SsoConfigs.Builder().from(configs).build();
    } catch (Exception e) {
      // Debug-level logging since this is expected to fail if SSO has not been configured.
      log.debug(String.format("Missing SSO settings in static configs %s", configs), e);
    }

    if (ssoConfigs != null && ssoConfigs.isOidcEnabled()) {
      try {
        OidcConfigs oidcConfigs = new OidcConfigs.Builder().from(configs).build();
        maybeUpdateOidcProvider(oidcConfigs);
      } catch (Exception e) {
        // Error-level logging since this is unexpected to fail if SSO has been configured.
        log.error(String.format("Error building OidcConfigs from static configs %s", configs), e);
      }
    } else {
      // Clear the SSO Provider since no SSO is enabled.
      clearSsoProvider();
    }

    refreshSsoProvider();
  }

  private void refreshSsoProvider() {
    final Optional<String> maybeSsoSettingsJsonStr = getDynamicSsoSettings();
    if (maybeSsoSettingsJsonStr.isEmpty()) {
      return;
    }

    // If we receive a non-empty response, try to update the SSO provider.
    final String ssoSettingsJsonStr = maybeSsoSettingsJsonStr.get();
    SsoConfigs ssoConfigs;
    try {
      ssoConfigs = new SsoConfigs.Builder().from(ssoSettingsJsonStr).build();
    } catch (Exception e) {
      log.error(
          String.format(
              "Error building SsoConfigs from invalid json %s, reusing previous settings",
              ssoSettingsJsonStr),
          e);
      return;
    }

    if (ssoConfigs != null && ssoConfigs.isOidcEnabled()) {
      try {
        OidcConfigs oidcConfigs =
            new OidcConfigs.Builder().from(configs, ssoSettingsJsonStr).build();
        maybeUpdateOidcProvider(oidcConfigs);
      } catch (Exception e) {
        log.error(
            String.format(
                "Error building OidcConfigs from invalid json %s, reusing previous settings",
                ssoSettingsJsonStr),
            e);
      }
    } else {
      // Clear the SSO Provider since no SSO is enabled.
      clearSsoProvider();
    }
  }

  private void maybeUpdateOidcProvider(OidcConfigs oidcConfigs) {
    SsoProvider existingSsoProvider = getSsoProvider();
    if (existingSsoProvider instanceof OidcProvider) {
      OidcProvider existingOidcProvider = (OidcProvider) existingSsoProvider;
      // If the existing provider is an OIDC provider and the configs are the same, do nothing.
      if (existingOidcProvider.configs().equals(oidcConfigs)) {
        return;
      }
    }

    OidcProvider oidcProvider = new OidcProvider(oidcConfigs);
    setSsoProvider(oidcProvider);
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
        log.debug("No SSO settings received from Auth Service, reusing previous settings");
      }
    } catch (Exception e) {
      log.warn("Failed to get SSO settings due to exception, reusing previous settings", e);
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
