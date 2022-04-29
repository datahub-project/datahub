package controllers;

import com.typesafe.config.Config;
import org.pac4j.play.LogoutController;
import play.mvc.Result;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

/**
 * Responsible for handling logout logic with oidc providers
 */
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
      return logout().toCompletableFuture().get();
    }
    return redirect("/");
  }
}