package controllers;

import auth.sso.SsoManager;
import com.typesafe.config.Config;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.play.LogoutController;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;
import utils.BasePathUtils;

/** Responsible for handling logout logic with oidc providers */
@Slf4j
public class CentralLogoutController extends LogoutController {
  private static final String AUTH_URL_CONFIG_PATH = "/login";
  private static final String DEFAULT_BASE_URL_PATH = "/";

  @Inject private SsoManager ssoManager;
  @Inject private Config config;

  private final String basePath = getBasePath();

  public CentralLogoutController() {
    // Note: URLs will be set dynamically in executeLogout method
    // since we need config access for base path
    setLocalLogout(true);
    setCentralLogout(true);
  }

  /**
   * Gets the configured base path for DataHub.
   *
   * @return the normalized base path
   */
  @Nonnull
  private String getBasePath() {
    return BasePathUtils.normalizeBasePath(config.getString("datahub.basePath"));
  }

  /** logout() method should not be called if oidc is not enabled */
  public Result executeLogout(Http.Request request) {
    // Set dynamic URLs with proper base path
    String loginUrl = BasePathUtils.addBasePath(AUTH_URL_CONFIG_PATH, this.basePath);
    String logoutPattern = basePath.isEmpty() ? DEFAULT_BASE_URL_PATH + ".*" : this.basePath + "/.*";

    setDefaultUrl(loginUrl);
    setLogoutUrlPattern(logoutPattern);

    if (ssoManager.isSsoEnabled()) {
      try {
        return logout(request).toCompletableFuture().get().withNewSession();
      } catch (Exception e) {
        log.error(
            "Caught exception while attempting to perform SSO logout! It's likely that SSO integration is mis-configured.",
            e);

        String errorLoginUrl =
            String.format(
                "%s?error_msg=%s",
                loginUrl,
                URLEncoder.encode(
                    "Failed to sign out using Single Sign-On provider. Please contact your DataHub Administrator, "
                        + "or refer to server logs for more information.",
                    StandardCharsets.UTF_8));

        return redirect(errorLoginUrl).withNewSession();
      }
    }
    return Results.redirect(loginUrl).withNewSession();
  }
}
