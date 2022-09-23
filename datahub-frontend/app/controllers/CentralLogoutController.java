package controllers;

import com.typesafe.config.Config;
import java.net.URLEncoder;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.play.LogoutController;
import play.mvc.Result;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

/**
 * Responsible for handling logout logic with oidc providers
 */
@Slf4j
public class CentralLogoutController extends LogoutController {

  private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";
  private static final String DEFAULT_BASE_URL_PATH = "/";
  private static Boolean _isOidcEnabled = false;

  @Inject
  public CentralLogoutController(Config config) {

    String authBaseUrl = config.hasPath(AUTH_BASE_URL_CONFIG_PATH)
            ? config.getString(AUTH_BASE_URL_CONFIG_PATH)
            : DEFAULT_BASE_URL_PATH;

    _isOidcEnabled = config.hasPath("auth.oidc.enabled") && config.getBoolean("auth.oidc.enabled");

    setDefaultUrl(authBaseUrl);
    setLogoutUrlPattern(authBaseUrl + ".*");
    setLocalLogout(true);
    setCentralLogout(true);

  }

  /**
   * logout() method should not be called if oidc is not enabled
   */
  public Result executeLogout() throws ExecutionException, InterruptedException {
    if (_isOidcEnabled) {
      try {
        return logout().toCompletableFuture().get().withNewSession();
      } catch (Exception e) {
        log.error("Caught exception while attempting to perform SSO logout! It's likely that SSO integration is mis-configured.", e);
        return redirect(
            String.format("/login?error_msg=%s",
                URLEncoder.encode("Failed to sign out using Single Sign-On provider. Please contact your DataHub Administrator, "
                    + "or refer to server logs for more information.")));
      }
    }
    return redirect("/").withNewSession();
  }
}