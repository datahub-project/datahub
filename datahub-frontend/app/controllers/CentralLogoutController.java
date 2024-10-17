package controllers;

import com.typesafe.config.Config;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.play.LogoutController;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

/** Responsible for handling logout logic with oidc providers */
@Slf4j
public class CentralLogoutController extends LogoutController {
  private static final String AUTH_URL_CONFIG_PATH = "/login";
  private static final String DEFAULT_BASE_URL_PATH = "/";
  private static Boolean _isOidcEnabled = false;
  private static String _oidcDiscoveryUri;
  private static String _endSessionEndpoint;

  @Inject
  public CentralLogoutController(Config config) {
    _isOidcEnabled = config.hasPath("auth.oidc.enabled") && config.getBoolean("auth.oidc.enabled");
    _oidcDiscoveryUri = config.getString("auth.oidc.discoveryUri");

    setDefaultUrl(DEFAULT_BASE_URL_PATH);
    setLogoutUrlPattern(DEFAULT_BASE_URL_PATH + ".*");
    setLocalLogout(true);
    setCentralLogout(true);
  }

  /** logout() method should not be called if oidc is not enabled */
  public Result executeLogout(Http.Request request) {
    if (_isOidcEnabled) {
      try {
        fetchEndSessionEndpoint();

        if (_endSessionEndpoint != null && !_endSessionEndpoint.isEmpty()) {
          logout(request).toCompletableFuture().get();
          return Results.redirect(_endSessionEndpoint).withNewSession();
        } else {
          log.error("OIDC end_session_endpoint not found!");
        }
      } catch (Exception e) {
        log.error(
            "Caught exception while attempting to perform SSO logout! It's likely that SSO integration is mis-configured.",
            e);
        return redirect(
                String.format(
                    "/login?error_msg=%s",
                    URLEncoder.encode(
                        "Failed to sign out using Single Sign-On provider. Please contact your DataHub Administrator, "
                            + "or refer to server logs for more information.",
                        StandardCharsets.UTF_8)))
            .withNewSession();
      }
    }
    return Results.redirect(AUTH_URL_CONFIG_PATH).withNewSession();
  }

  private void fetchEndSessionEndpoint() throws Exception {
    try {
      HttpClient client = HttpClient.newHttpClient();
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(_oidcDiscoveryUri))
          .GET()
          .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == HttpURLConnection.HTTP_OK) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(response.body());
        _endSessionEndpoint = jsonNode.get("end_session_endpoint").asText();
      } else {
        log.error("Error accessing OIDC Discovery: " + response.statusCode());
      }
    } catch (Exception e) {
      log.error("Error fetching end_session_endpoint in OIDC Discovery", e);
      throw e;
    }
  }
}
