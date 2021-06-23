package controllers;

import com.typesafe.config.Config;
import org.pac4j.play.LogoutController;

import javax.inject.Inject;

public class CentralLogoutController extends LogoutController {

  private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";
  private static final String DEFAULT_BASE_URL_PATH = "/";

  @Inject
  public CentralLogoutController(Config config) {

    String _authBaseUrl = config.hasPath(AUTH_BASE_URL_CONFIG_PATH)
            ? config.getString(AUTH_BASE_URL_CONFIG_PATH)
            : DEFAULT_BASE_URL_PATH;

    String baseUrl = _authBaseUrl;
    setDefaultUrl(baseUrl + "/login");
    setLocalLogout(true);
    setCentralLogout(true);
    setLogoutUrlPattern(baseUrl + "/.*");
  }
}