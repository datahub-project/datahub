package controllers;

import auth.CookieConfigs;
import auth.sso.SsoManager;
import auth.sso.SsoProvider;
import auth.sso.oidc.OidcCallbackLogic;
import client.AuthServiceClient;
import com.datahub.authentication.Authentication;
import com.linkedin.entity.client.SystemEntityClient;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.config.Config;
import org.pac4j.core.engine.CallbackLogic;
import org.pac4j.core.http.adapter.HttpActionAdapter;
import org.pac4j.play.CallbackController;
import org.pac4j.play.PlayWebContext;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

/**
 * A dedicated Controller for handling redirects to DataHub by 3rd-party Identity Providers after
 * off-platform authentication.
 *
 * <p>Handles a single "callback/{protocol}" route, where the protocol (ie. OIDC / SAML) determines
 * the handling logic to invoke.
 */
@Slf4j
public class SsoCallbackController extends CallbackController {

  private final SsoManager _ssoManager;

  @Inject
  public SsoCallbackController(
      @Nonnull SsoManager ssoManager,
      @Nonnull Authentication systemAuthentication,
      @Nonnull SystemEntityClient entityClient,
      @Nonnull AuthServiceClient authClient,
      @Nonnull com.typesafe.config.Config configs) {
    _ssoManager = ssoManager;
    setDefaultUrl("/"); // By default, redirects to Home Page on log in.
    setSaveInSession(false);
    setCallbackLogic(
        new SsoCallbackLogic(
            ssoManager,
            systemAuthentication,
            entityClient,
            authClient,
            new CookieConfigs(configs)));
  }

  public CompletionStage<Result> handleCallback(String protocol, Http.Request request) {
    if (shouldHandleCallback(protocol)) {
      log.debug(String.format("Handling SSO callback. Protocol: %s", protocol));
      return callback(request)
          .handle(
              (res, e) -> {
                if (e != null) {
                  log.error(
                      "Caught exception while attempting to handle SSO callback! It's likely that SSO integration is mis-configured.",
                      e);
                  return Results.redirect(
                          String.format(
                              "/login?error_msg=%s",
                              URLEncoder.encode(
                                  "Failed to sign in using Single Sign-On provider. Please try again, or contact your DataHub Administrator.",
                                  StandardCharsets.UTF_8)))
                      .discardingCookie("actor")
                      .withNewSession();
                }
                return res;
              });
    }
    return CompletableFuture.completedFuture(
        Results.internalServerError(
            String.format(
                "Failed to perform SSO callback. SSO is not enabled for protocol: %s", protocol)));
  }

  /** Logic responsible for delegating to protocol-specific callback logic. */
  public class SsoCallbackLogic implements CallbackLogic<Result, PlayWebContext> {

    private final OidcCallbackLogic _oidcCallbackLogic;

    SsoCallbackLogic(
        final SsoManager ssoManager,
        final Authentication systemAuthentication,
        final SystemEntityClient entityClient,
        final AuthServiceClient authClient,
        final CookieConfigs cookieConfigs) {
      _oidcCallbackLogic =
          new OidcCallbackLogic(
              ssoManager, systemAuthentication, entityClient, authClient, cookieConfigs);
    }

    @Override
    public Result perform(
        PlayWebContext context,
        Config config,
        HttpActionAdapter<Result, PlayWebContext> httpActionAdapter,
        String defaultUrl,
        Boolean saveInSession,
        Boolean multiProfile,
        Boolean renewSession,
        String defaultClient) {
      if (SsoProvider.SsoProtocol.OIDC.equals(_ssoManager.getSsoProvider().protocol())) {
        return _oidcCallbackLogic.perform(
            context,
            config,
            httpActionAdapter,
            defaultUrl,
            saveInSession,
            multiProfile,
            renewSession,
            defaultClient);
      }
      // Should never occur.
      throw new UnsupportedOperationException(
          "Failed to find matching SSO Provider. Only one supported is OIDC.");
    }
  }

  private boolean shouldHandleCallback(final String protocol) {
    return _ssoManager.isSsoEnabled()
        && _ssoManager.getSsoProvider().protocol().getCommonName().equals(protocol);
  }
}
