package controllers;

import auth.CookieConfigs;
import auth.sso.SsoManager;
import auth.sso.SsoProvider;
import auth.sso.oidc.OidcCallbackLogic;
import client.AuthServiceClient;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.pac4j.core.adapter.FrameworkAdapter;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.FrameworkParameters;
import org.pac4j.core.engine.CallbackLogic;
import org.pac4j.play.CallbackController;
import org.pac4j.play.context.PlayFrameworkParameters;
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

  private final SsoManager ssoManager;
  private final Config config;
  private final CallbackLogic callbackLogic;

  @Inject
  public SsoCallbackController(
      @Nonnull SsoManager ssoManager,
      @Named("systemOperationContext") @Nonnull OperationContext systemOperationContext,
      @Nonnull SystemEntityClient entityClient,
      @Nonnull AuthServiceClient authClient,
      @Nonnull Config config,
      @Nonnull com.typesafe.config.Config configs) {
    this.ssoManager = ssoManager;
    this.config = config;
    setDefaultUrl("/"); // By default, redirects to Home Page on log in.

    callbackLogic =
        new SsoCallbackLogic(
            ssoManager,
            systemOperationContext,
            entityClient,
            authClient,
            new CookieConfigs(configs));
  }

  @Override
  public CompletionStage<Result> callback(Http.Request request) {
    FrameworkAdapter.INSTANCE.applyDefaultSettingsIfUndefined(this.config);

    return CompletableFuture.supplyAsync(
        () -> {
          return (Result)
              callbackLogic.perform(
                  this.config,
                  getDefaultUrl(),
                  getRenewSession(),
                  getDefaultClient(),
                  new PlayFrameworkParameters(request));
        },
        this.ec.current());
  }

  public CompletionStage<Result> handleCallback(String protocol, Http.Request request) {
    if (shouldHandleCallback(protocol)) {
      log.debug(
          "Handling SSO callback. Protocol: {}",
          ssoManager.getSsoProvider().protocol().getCommonName());
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
  public class SsoCallbackLogic implements CallbackLogic {

    private final OidcCallbackLogic oidcCallbackLogic;

    SsoCallbackLogic(
        final SsoManager ssoManager,
        final OperationContext systemOperationContext,
        final SystemEntityClient entityClient,
        final AuthServiceClient authClient,
        final CookieConfigs cookieConfigs) {
      oidcCallbackLogic =
          new OidcCallbackLogic(
              ssoManager, systemOperationContext, entityClient, authClient, cookieConfigs);
    }

    @Override
    public Object perform(
        Config config,
        String inputDefaultUrl,
        Boolean inputRenewSession,
        String defaultClient,
        FrameworkParameters parameters) {
      if (SsoProvider.SsoProtocol.OIDC.equals(ssoManager.getSsoProvider().protocol())) {
        return oidcCallbackLogic.perform(
            config, inputDefaultUrl, inputRenewSession, defaultClient, parameters);
      }
      // Should never occur.
      throw new UnsupportedOperationException(
          "Failed to find matching SSO Provider. Only one supported is OIDC.");
    }
  }

  private boolean shouldHandleCallback(final String protocol) {
    if (!ssoManager.isSsoEnabled()) {
      return false;
    }
    updateConfig();
    return ssoManager.getSsoProvider().protocol().getCommonName().equals(protocol);
  }

  private void updateConfig() {
    final Clients clients = new Clients();
    final List<Client> clientList = new ArrayList<>();
    clientList.add(ssoManager.getSsoProvider().client());
    clients.setClients(clientList);
    config.setClients(clients);
  }
}
