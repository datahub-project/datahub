package controllers;

import com.typesafe.config.Config;
import org.pac4j.play.LogoutController;
import play.Logger;
import play.mvc.Result;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

public class CentralLogoutController extends LogoutController {

  private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";
  private static final String DEFAULT_BASE_URL_PATH = "/";
  private static Boolean _isOidcEnabled = false;

  @Inject
  public CentralLogoutController(Config config) {

    String _authBaseUrl = config.hasPath(AUTH_BASE_URL_CONFIG_PATH)
            ? config.getString(AUTH_BASE_URL_CONFIG_PATH)
            : DEFAULT_BASE_URL_PATH;

    _isOidcEnabled = config.hasPath("auth.oidc.enabled") && config.getBoolean("auth.oidc.enabled");

    String baseUrl = _authBaseUrl;
    setDefaultUrl(baseUrl + "/login");
    setLogoutUrlPattern(baseUrl + "/.*");
    setLocalLogout(true);
    setCentralLogout(true);

  }

  public Result executeLogout() throws ExecutionException, InterruptedException {
    Logger.info("logout called");
    if (_isOidcEnabled) {
      return logout().toCompletableFuture().get();
    }
    return status(200);
  }
}