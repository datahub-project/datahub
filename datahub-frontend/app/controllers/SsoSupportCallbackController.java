package controllers;

import auth.CookieConfigs;
import auth.sso.SsoProvider;
import auth.sso.SsoSupportManager;
import auth.sso.oidc.support.OidcSupportCallbackLogic;
import client.AuthServiceClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.utils.BasePathUtils;
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
 * off-platform authentication for support staff.
 *
 * <p>Handles a single "callback/oidc-support" route for support OIDC authentication.
 */
@Slf4j
public class SsoSupportCallbackController extends CallbackController {

  private final SsoSupportManager ssoSupportManager;
  private final Config config;
  private final CallbackLogic callbackLogic;
  private final com.typesafe.config.Config configs;

  @Inject
  public SsoSupportCallbackController(
      @Nonnull SsoSupportManager ssoSupportManager,
      @Named("systemOperationContext") @Nonnull OperationContext systemOperationContext,
      @Nonnull SystemEntityClient entityClient,
      @Nonnull AuthServiceClient authClient,
      @Nonnull Config config,
      @Nonnull com.typesafe.config.Config configs) {
    this.ssoSupportManager = ssoSupportManager;
    this.config = config;
    this.configs = configs;

    // Set default URL with proper base path - redirects to Home Page on log in
    String basePath = BasePathUtils.normalizeBasePath(configs.getString("datahub.basePath"));
    String homeUrl = basePath.isEmpty() ? "/" : basePath + "/";
    setDefaultUrl(homeUrl);

    log.info("Support Home URL: {}", getDefaultUrl());

    callbackLogic =
        new SsoSupportCallbackLogic(
            ssoSupportManager,
            systemOperationContext,
            entityClient,
            authClient,
            new CookieConfigs(configs),
            configs);
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

  public CompletionStage<Result> handleSupportCallback(String protocol, Http.Request request) {
    log.debug("handleSupportCallback called with protocol: {}", protocol);

    if (shouldHandleSupportCallback(protocol)) {
      log.debug(
          "Handling Support SSO callback. Protocol: {}",
          ssoSupportManager.getSupportSsoProvider().protocol().getCommonName());
      return callback(request)
          .handle(
              (res, e) -> {
                if (e != null) {
                  log.error(
                      "Caught exception while attempting to handle Support SSO callback! It's likely that Support SSO integration is mis-configured.",
                      e);
                  String basePath =
                      BasePathUtils.normalizeBasePath(configs.getString("datahub.basePath"));
                  String loginUrl = BasePathUtils.addBasePath("/login", basePath);
                  return Results.redirect(
                          String.format(
                              "%s?error_msg=%s",
                              loginUrl,
                              URLEncoder.encode(
                                  "Failed to sign in using Support Single Sign-On provider. Please try again, or contact your DataHub Administrator.",
                                  StandardCharsets.UTF_8)))
                      .discardingCookie("actor")
                      .withNewSession();
                }
                return res;
              });
    }

    log.warn("Support SSO callback not handled for protocol: {}", protocol);
    return CompletableFuture.completedFuture(
        Results.internalServerError(
            String.format(
                "Failed to perform Support SSO callback. Support SSO is not enabled for protocol: %s",
                protocol)));
  }

  /** Logic responsible for delegating to protocol-specific callback logic for support. */
  public class SsoSupportCallbackLogic implements CallbackLogic {

    private final OidcSupportCallbackLogic oidcSupportCallbackLogic;

    SsoSupportCallbackLogic(
        final SsoSupportManager ssoSupportManager,
        final OperationContext systemOperationContext,
        final SystemEntityClient entityClient,
        final AuthServiceClient authClient,
        final CookieConfigs cookieConfigs,
        final com.typesafe.config.Config config) {
      oidcSupportCallbackLogic =
          new OidcSupportCallbackLogic(
              ssoSupportManager,
              systemOperationContext,
              entityClient,
              authClient,
              cookieConfigs,
              config);
    }

    @Override
    public Object perform(
        Config config,
        String inputDefaultUrl,
        Boolean inputRenewSession,
        String defaultClient,
        FrameworkParameters parameters) {
      if (SsoProvider.SsoProtocol.OIDC.equals(
          ssoSupportManager.getSupportSsoProvider().protocol())) {
        return oidcSupportCallbackLogic.perform(
            config, inputDefaultUrl, inputRenewSession, defaultClient, parameters);
      }
      // Should never occur.
      throw new UnsupportedOperationException(
          "Failed to find matching Support SSO Provider. Only one supported is OIDC.");
    }
  }

  private boolean shouldHandleSupportCallback(final String protocol) {
    log.debug("shouldHandleSupportCallback called with protocol: {}", protocol);

    if (!ssoSupportManager.isSupportSsoEnabled()) {
      log.debug("Support SSO is not enabled");
      return false;
    }

    updateConfig();

    // Handle the specific case where protocol is "oidc" for support OIDC
    if ("oidc".equals(protocol)
        && SsoProvider.SsoProtocol.OIDC.equals(
            ssoSupportManager.getSupportSsoProvider().protocol())) {
      log.debug("Handling oidc protocol with OIDC provider");
      return true;
    }

    boolean matches =
        ssoSupportManager.getSupportSsoProvider().protocol().getCommonName().equals(protocol);
    log.debug(
        "Protocol match result: {} (protocol: {}, provider protocol: {})",
        matches,
        protocol,
        ssoSupportManager.getSupportSsoProvider().protocol().getCommonName());
    return matches;
  }

  private void updateConfig() {
    final Clients clients = new Clients();
    final List<Client> clientList = new ArrayList<>();
    clientList.add(ssoSupportManager.getSupportSsoProvider().client());
    clients.setClients(clientList);
    config.setClients(clients);
  }
}
