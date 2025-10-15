package controllers;

import static auth.AuthUtils.*;
import static org.pac4j.core.client.IndirectClient.ATTEMPTED_AUTHENTICATION_SUFFIX;
import static org.pac4j.play.store.PlayCookieSessionStore.*;

import auth.AuthUtils;
import auth.CookieConfigs;
import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.utils.BasePathUtils;
import com.typesafe.config.Config;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.RedirectException;
import org.pac4j.core.client.Client;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

@Slf4j
public class SupportAuthenticationController extends Controller {
  public static final String AUTH_VERBOSE_LOGGING = "auth.verbose.logging";
  private static final String AUTH_REDIRECT_URI_PARAM = "redirect_uri";
  private static final String ERROR_MESSAGE_URI_PARAM = "error_msg";
  private static final String SSO_DISABLED_ERROR_MESSAGE = "Support SSO is not configured";

  private static final String SSO_NO_REDIRECT_MESSAGE =
      "Support SSO is configured, however missing redirect from idp";

  private final CookieConfigs cookieConfigs;
  private final boolean verbose;
  private final Config config;
  private final String basePath;

  @Inject private org.pac4j.core.config.Config ssoConfig;

  @VisibleForTesting @Inject protected PlayCookieSessionStore playCookieSessionStore;

  @VisibleForTesting @Inject protected SsoSupportManager ssoSupportManager;

  @Inject AuthServiceClient authClient;

  @Inject
  public SupportAuthenticationController(@Nonnull Config configs) {
    this.config = configs;
    cookieConfigs = new CookieConfigs(configs);
    verbose = configs.hasPath(AUTH_VERBOSE_LOGGING) && configs.getBoolean(AUTH_VERBOSE_LOGGING);
    basePath = getBasePath();
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

  /**
   * Gets the login URL with proper base path for error redirects.
   *
   * @return the full login URL with base path
   */
  @Nonnull
  private String getLoginUrl() {
    return BasePathUtils.addBasePath("/login", this.basePath);
  }

  /**
   * Route used to perform support authentication via SSO.
   *
   * <p>If Support SSO is configured, this route will redirect to the identity provider. If not
   * configured, it will return an error.
   */
  @Nonnull
  public Result authenticateSupport(Http.Request request) {

    final Optional<String> maybeRedirectPath =
        Optional.ofNullable(request.getQueryString(AUTH_REDIRECT_URI_PARAM));
    String redirectPath = maybeRedirectPath.orElse("/");

    // If the redirect path is /logOut, we do not want to redirect to the logout page after login.
    if (redirectPath.equals("/logOut")) {
      redirectPath = BasePathUtils.addBasePath("/logOut", this.basePath);
    }

    try {
      URI redirectUri = new URI(redirectPath);
      if (redirectUri.getScheme() != null || redirectUri.getAuthority() != null) {
        throw new RedirectException(
            "Redirect location must be relative to the base url, cannot "
                + "redirect to other domains: "
                + redirectPath);
      }
    } catch (URISyntaxException | RedirectException e) {
      log.warn(e.getMessage());
      redirectPath = BasePathUtils.addBasePath("/", this.basePath);
    }

    if (AuthUtils.hasValidSessionCookie(request)) {
      return Results.redirect(redirectPath);
    }

    // If Support SSO is enabled, redirect to IdP if not authenticated.
    if (ssoSupportManager.isSupportSsoEnabled()) {
      return redirectToSupportIdentityProvider(request, redirectPath)
          .orElse(
              Results.redirect(
                  getLoginUrl()
                      + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_NO_REDIRECT_MESSAGE)));
    }

    // Support SSO is not configured - return error
    return Results.redirect(
        getLoginUrl()
            + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_DISABLED_ERROR_MESSAGE));
  }

  /** Redirect to the identity provider for support authentication. */
  @Nonnull
  public Result ssoSupport(Http.Request request) {
    if (ssoSupportManager.isSupportSsoEnabled()) {
      return redirectToSupportIdentityProvider(request, "/")
          .orElse(
              Results.redirect(
                  getLoginUrl()
                      + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_NO_REDIRECT_MESSAGE)));
    }
    return Results.redirect(
        getLoginUrl()
            + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_DISABLED_ERROR_MESSAGE));
  }

  @VisibleForTesting
  protected Result addRedirectCookie(Result result, CallContext ctx, String redirectPath) {
    // Set the originally requested path for post-auth redirection. We split off into a separate
    // cookie from the session
    // to reduce size of the session cookie
    FoundAction foundAction =
        new FoundAction(BasePathUtils.addBasePath(redirectPath, this.basePath));
    byte[] javaSerBytes =
        ((PlayCookieSessionStore) ctx.sessionStore()).getSerializer().serializeToBytes(foundAction);
    String serialized = Base64.getEncoder().encodeToString(compressBytes(javaSerBytes));
    Http.CookieBuilder redirectCookieBuilder =
        Http.Cookie.builder(REDIRECT_URL_COOKIE_NAME, serialized);
    redirectCookieBuilder.withPath(BasePathUtils.addBasePath("/", this.basePath));
    redirectCookieBuilder.withSecure(true);
    redirectCookieBuilder.withHttpOnly(true);
    redirectCookieBuilder.withMaxAge(Duration.ofSeconds(86400));
    redirectCookieBuilder.withSameSite(Http.Cookie.SameSite.NONE);

    return result.withCookies(redirectCookieBuilder.build());
  }

  private Optional<Result> redirectToSupportIdentityProvider(
      Http.RequestHeader request, String redirectPath) {
    CallContext ctx = buildCallContext(request);

    final Client client = ssoSupportManager.getSupportSsoProvider().client();
    configurePac4jSessionStore(ctx, client);
    try {
      final Optional<RedirectionAction> action = client.getRedirectionAction(ctx);
      final Optional<Result> maybeResult =
          action.map(act -> new PlayHttpActionAdapter().adapt(act, ctx.webContext()));
      return maybeResult.map(result -> addRedirectCookie(result, ctx, redirectPath));
    } catch (Exception e) {
      if (verbose) {
        log.error(
            "Caught exception while attempting to redirect to Support SSO identity provider! It's likely that Support SSO integration is mis-configured",
            e);
      } else {
        log.error(
            "Caught exception while attempting to redirect to Support SSO identity provider! It's likely that Support SSO integration is mis-configured");
      }
      return Optional.of(
          Results.redirect(
              String.format(
                  "%s?error_msg=%s",
                  getLoginUrl(),
                  URLEncoder.encode(
                      "Failed to redirect to Support Single Sign-On provider. Please contact your DataHub Administrator, "
                          + "or refer to server logs for more information.",
                      StandardCharsets.UTF_8))));
    }
  }

  @VisibleForTesting
  protected CallContext buildCallContext(Http.RequestHeader request) {
    // First create PlayWebContext from the request
    PlayWebContext webContext = new PlayWebContext(request);

    // Then create CallContext using the web context and session store
    return new CallContext(webContext, playCookieSessionStore);
  }

  private void configurePac4jSessionStore(CallContext ctx, Client client) {
    WebContext context = ctx.webContext();

    // This is to prevent previous login attempts from being cached.
    // We replicate the logic here, which is buried in the Pac4j client.
    Optional<Object> attempt =
        playCookieSessionStore.get(context, client.getName() + ATTEMPTED_AUTHENTICATION_SUFFIX);
    if (attempt.isPresent() && !"".equals(attempt.get())) {
      log.debug(
          "Found previous support login attempt. Removing it manually to prevent unexpected errors.");
      playCookieSessionStore.set(context, client.getName() + ATTEMPTED_AUTHENTICATION_SUFFIX, "");
    }
  }
}
