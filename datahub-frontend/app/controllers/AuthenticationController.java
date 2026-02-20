package controllers;

import static auth.AuthUtils.*;
import static org.pac4j.core.client.IndirectClient.ATTEMPTED_AUTHENTICATION_SUFFIX;
import static utils.FrontendConstants.FALLBACK_LOGIN;
import static utils.FrontendConstants.GUEST_LOGIN;
import static utils.FrontendConstants.PASSWORD_LOGIN;
import static utils.FrontendConstants.PASSWORD_RESET;
import static utils.FrontendConstants.SIGN_UP_LINK_LOGIN;

import auth.AuthUtils;
import auth.CookieConfigs;
import auth.GuestAuthenticationConfigs;
import auth.JAASConfigs;
import auth.NativeAuthenticationConfigs;
import auth.sso.SsoManager;
import client.AuthServiceClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.utils.BasePathUtils;
import com.typesafe.config.Config;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.RedirectException;
import org.pac4j.core.client.Client;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.http.PlayHttpActionAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.data.validation.Constraints;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;
import security.AuthenticationManager;
import utils.SerializationUtils;

public class AuthenticationController extends Controller {
  public static final String AUTH_VERBOSE_LOGGING = "auth.verbose.logging";
  private static final String AUTH_REDIRECT_URI_PARAM = "redirect_uri";
  private static final String ERROR_MESSAGE_URI_PARAM = "error_msg";
  private static final String SSO_DISABLED_ERROR_MESSAGE = "SSO is not configured";

  private static final String SSO_NO_REDIRECT_MESSAGE =
      "SSO is configured, however missing redirect from idp";

  private static final Logger logger =
      LoggerFactory.getLogger(AuthenticationController.class.getName());
  private final CookieConfigs cookieConfigs;
  private final JAASConfigs jaasConfigs;
  private final NativeAuthenticationConfigs nativeAuthenticationConfigs;
  private final GuestAuthenticationConfigs guestAuthenticationConfigs;

  private final boolean verbose;
  private final Config config;

  private final String basePath;

  @Inject private org.pac4j.core.config.Config ssoConfig;

  @VisibleForTesting @Inject protected SessionStore sessionStore;

  @VisibleForTesting @Inject protected SsoManager ssoManager;

  @Inject AuthServiceClient authClient;

  @Inject
  public AuthenticationController(@Nonnull Config configs) {
    this.config = configs;
    cookieConfigs = new CookieConfigs(configs);
    jaasConfigs = new JAASConfigs(configs);
    nativeAuthenticationConfigs = new NativeAuthenticationConfigs(configs);
    guestAuthenticationConfigs = new GuestAuthenticationConfigs(configs);
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
   * Gets the login URL with proper base path.
   *
   * @return the full login URL with base path
   */
  @Nonnull
  private String getLoginUrl() {
    return BasePathUtils.addBasePath(LOGIN_ROUTE, this.basePath);
  }

  /**
   * Route used to perform authentication, or redirect to log in if authentication fails.
   *
   * <p>If indirect SSO (eg. oidc) is configured, this route will redirect to the identity provider
   * (Indirect auth). If not, we will fall back to the default username / password login experience
   * (Direct auth).
   */
  @Nonnull
  public Result authenticate(Http.Request request) {

    // TODO: Call getAuthenticatedUser and then generate a session cookie for the UI if the user is
    // authenticated.

    final Optional<String> maybeRedirectPath =
        Optional.ofNullable(request.getQueryString(AUTH_REDIRECT_URI_PARAM));
    String redirectPath = maybeRedirectPath.orElse("/");
    // If the redirect path is /logOut, we do not want to redirect to the logout page after login.
    if (redirectPath.equals("/logOut")) {
      redirectPath = BasePathUtils.addBasePath("/logOut", this.basePath);
    }
    try {
      // Reject protocol-relative URLs (e.g. ///google.com) which browsers resolve to
      // https://host; the URI parser may not set scheme/authority for these.
      if (redirectPath.trim().startsWith("//")) {
        throw new RedirectException(
            "Redirect location must be relative to the base url, cannot "
                + "use protocol-relative URL: "
                + redirectPath);
      }
      URI redirectUri = new URI(redirectPath);
      if (redirectUri.getScheme() != null || redirectUri.getAuthority() != null) {
        throw new RedirectException(
            "Redirect location must be relative to the base url, cannot "
                + "redirect to other domains: "
                + redirectPath);
      }
    } catch (URISyntaxException | RedirectException e) {
      logger.warn(e.getMessage());
      redirectPath = BasePathUtils.addBasePath("/", this.basePath);
    }

    if (AuthUtils.hasValidSessionCookie(request)) {
      return Results.redirect(redirectPath);
    }

    if (guestAuthenticationConfigs.isGuestEnabled()
        && guestAuthenticationConfigs.getGuestPath().equals(redirectPath)) {
      final String accessToken =
          authClient.generateSessionTokenForUser(
              guestAuthenticationConfigs.getGuestUser(), GUEST_LOGIN);
      // We requested guest login by accessing {guestPath} URL. It is not really a target.
      redirectPath = BasePathUtils.addBasePath("/", this.basePath);
      CorpuserUrn guestUserUrn = new CorpuserUrn(guestAuthenticationConfigs.getGuestUser());
      return Results.redirect(redirectPath)
          .withSession(createSessionMap(guestUserUrn.toString(), accessToken))
          .withCookies(
              createActorCookie(
                  guestUserUrn.toString(),
                  cookieConfigs.getTtlInHours(),
                  cookieConfigs.getAuthCookieSameSite(),
                  cookieConfigs.getAuthCookieSecure()));
    }

    // 1. If SSO is enabled, redirect to IdP if not authenticated.
    if (ssoManager.isSsoEnabled()) {
      return redirectToIdentityProvider(request, redirectPath)
          .orElse(
              Results.redirect(
                  getLoginUrl() // will already have a basepath
                      + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_NO_REDIRECT_MESSAGE)));
    }

    // 2. If either JAAS auth or Native auth is enabled, fallback to it
    if (jaasConfigs.isJAASEnabled()
        || nativeAuthenticationConfigs.isNativeAuthenticationEnabled()) {
      return Results.redirect(
          getLoginUrl()
              + String.format("?%s=%s", AUTH_REDIRECT_URI_PARAM, encodeRedirectUri(redirectPath)));
    }

    // 3. If no auth enabled, fallback to using default user account & redirect.
    // Generate GMS session token, TODO:
    final String accessToken =
        authClient.generateSessionTokenForUser(DEFAULT_ACTOR_URN.getId(), FALLBACK_LOGIN);
    return Results.redirect(redirectPath)
        .withSession(createSessionMap(DEFAULT_ACTOR_URN.toString(), accessToken))
        .withCookies(
            createActorCookie(
                DEFAULT_ACTOR_URN.toString(),
                cookieConfigs.getTtlInHours(),
                cookieConfigs.getAuthCookieSameSite(),
                cookieConfigs.getAuthCookieSecure()));
  }

  /** Redirect to the identity provider for authentication. */
  @Nonnull
  public Result sso(Http.Request request) {
    if (ssoManager.isSsoEnabled()) {
      return redirectToIdentityProvider(request, "/")
          .orElse(
              Results.redirect(
                  getLoginUrl()
                      + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_NO_REDIRECT_MESSAGE)));
    }
    return Results.redirect(
        getLoginUrl()
            + String.format("?%s=%s", ERROR_MESSAGE_URI_PARAM, SSO_DISABLED_ERROR_MESSAGE));
  }

  /**
   * Log in a user based on a username + password.
   *
   * <p>TODO: Implement built-in support for LDAP auth. Currently dummy jaas authentication is the
   * default.
   */
  @Nonnull
  public Result logIn(Http.Request request) {
    boolean jaasEnabled = jaasConfigs.isJAASEnabled();
    logger.debug(String.format("Jaas authentication enabled: %b", jaasEnabled));
    boolean nativeAuthenticationEnabled =
        nativeAuthenticationConfigs.isNativeAuthenticationEnabled();
    logger.debug(String.format("Native authentication enabled: %b", nativeAuthenticationEnabled));
    boolean noAuthEnabled = !jaasEnabled && !nativeAuthenticationEnabled;
    if (noAuthEnabled) {
      String message = "Neither JAAS nor native authentication is enabled on the server.";
      final ObjectNode error = Json.newObject();
      error.put("message", message);
      return Results.badRequest(error);
    }

    final JsonNode json = request.body().asJson();
    final String username = json.findPath(USER_NAME).textValue();
    final String password = json.findPath(PASSWORD).textValue();

    if (StringUtils.isBlank(username)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "User name must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    JsonNode invalidCredsJson = Json.newObject().put("message", "Invalid Credentials");
    boolean loginSucceeded = tryLogin(username, password);

    if (!loginSucceeded) {
      logger.info("Login failed for user: {}", username);
      return Results.badRequest(invalidCredsJson);
    }

    final Urn actorUrn = new CorpuserUrn(username);
    logger.info("Login successful for user: {}, urn: {}", username, actorUrn);
    final String accessToken =
        authClient.generateSessionTokenForUser(actorUrn.getId(), PASSWORD_LOGIN);
    return createSession(actorUrn.toString(), accessToken);
  }

  /**
   * Sign up a native user based on a name, email, title, and password. The invite token must match
   * an existing invite token.
   */
  @Nonnull
  public Result signUp(Http.Request request) {
    boolean nativeAuthenticationEnabled =
        nativeAuthenticationConfigs.isNativeAuthenticationEnabled();
    logger.debug(String.format("Native authentication enabled: %b", nativeAuthenticationEnabled));
    if (!nativeAuthenticationEnabled) {
      String message = "Native authentication is not enabled on the server.";
      final ObjectNode error = Json.newObject();
      error.put("message", message);
      return Results.badRequest(error);
    }

    final JsonNode json = request.body().asJson();
    final String fullName = json.findPath(FULL_NAME).textValue();
    final String email = json.findPath(EMAIL).textValue();
    // Title is optional - pass null if blank
    final String title =
        StringUtils.isBlank(json.findPath(TITLE).textValue())
            ? null
            : json.findPath(TITLE).textValue();
    final String password = json.findPath(PASSWORD).textValue();
    final String inviteToken = json.findPath(INVITE_TOKEN).textValue();

    if (StringUtils.isBlank(fullName)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "Full name must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    if (StringUtils.isBlank(email)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "Email must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }
    if (nativeAuthenticationConfigs.isEnforceValidEmailEnabled()) {
      Constraints.EmailValidator emailValidator = new Constraints.EmailValidator();
      if (!emailValidator.isValid(email)) {
        JsonNode invalidCredsJson = Json.newObject().put("message", "Email must not be empty.");
        return Results.badRequest(invalidCredsJson);
      }
    }

    if (StringUtils.isBlank(password)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "Password must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    if (StringUtils.isBlank(inviteToken)) {
      JsonNode invalidCredsJson =
          Json.newObject().put("message", "Invite token must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    final Urn userUrn = new CorpuserUrn(email);
    final String userUrnString = userUrn.toString();
    authClient.signUp(userUrnString, fullName, email, title, password, inviteToken);
    logger.info("Signed up user {} using invite tokens", userUrnString);
    final String accessToken =
        authClient.generateSessionTokenForUser(userUrn.getId(), SIGN_UP_LINK_LOGIN);
    return createSession(userUrnString, accessToken);
  }

  /** Reset a native user's credentials based on a username, old password, and new password. */
  @Nonnull
  public Result resetNativeUserCredentials(Http.Request request) {
    boolean nativeAuthenticationEnabled =
        nativeAuthenticationConfigs.isNativeAuthenticationEnabled();
    logger.debug(String.format("Native authentication enabled: %b", nativeAuthenticationEnabled));
    if (!nativeAuthenticationEnabled) {
      String message = "Native authentication is not enabled on the server.";
      final ObjectNode error = Json.newObject();
      error.put("message", message);
      return badRequest(error);
    }

    final JsonNode json = request.body().asJson();
    final String email = json.findPath(EMAIL).textValue();
    final String password = json.findPath(PASSWORD).textValue();
    final String resetToken = json.findPath(RESET_TOKEN).textValue();

    if (StringUtils.isBlank(email)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "Email must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    if (StringUtils.isBlank(password)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "Password must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    if (StringUtils.isBlank(resetToken)) {
      JsonNode invalidCredsJson = Json.newObject().put("message", "Reset token must not be empty.");
      return Results.badRequest(invalidCredsJson);
    }

    final Urn userUrn = new CorpuserUrn(email);
    final String userUrnString = userUrn.toString();
    authClient.resetNativeUserCredentials(userUrnString, password, resetToken);
    final String accessToken =
        authClient.generateSessionTokenForUser(userUrn.getId(), PASSWORD_RESET);
    return createSession(userUrnString, accessToken);
  }

  @VisibleForTesting
  protected Result addRedirectCookie(Result result, CallContext ctx, String redirectPath) {
    // Set the originally requested path for post-auth redirection. We split off into a separate
    // cookie from the session
    // to reduce size of the session cookie
    FoundAction foundAction =
        new FoundAction(BasePathUtils.addBasePath(redirectPath, this.basePath));
    Http.CookieBuilder redirectCookieBuilder =
        Http.Cookie.builder(
            REDIRECT_URL_COOKIE_NAME, SerializationUtils.serializeFoundAction(foundAction));
    redirectCookieBuilder.withPath(BasePathUtils.addBasePath("/", this.basePath));
    redirectCookieBuilder.withSecure(true);
    redirectCookieBuilder.withHttpOnly(true);
    redirectCookieBuilder.withMaxAge(Duration.ofSeconds(86400));
    redirectCookieBuilder.withSameSite(Http.Cookie.SameSite.NONE);

    return result.withCookies(redirectCookieBuilder.build());
  }

  private Optional<Result> redirectToIdentityProvider(
      Http.RequestHeader request, String redirectPath) {
    CallContext ctx = buildCallContext(request);

    final Client client = ssoManager.getSsoProvider().client();
    configurePac4jSessionStore(ctx, client);
    try {
      final Optional<RedirectionAction> action = client.getRedirectionAction(ctx);
      final Optional<Result> maybeResult =
          action.map(act -> new PlayHttpActionAdapter().adapt(act, ctx.webContext()));
      return maybeResult.map(result -> addRedirectCookie(result, ctx, redirectPath));
    } catch (Exception e) {
      if (verbose) {
        logger.error(
            "Caught exception while attempting to redirect to SSO identity provider! It's likely that SSO integration is mis-configured",
            e);
      } else {
        logger.error(
            "Caught exception while attempting to redirect to SSO identity provider! It's likely that SSO integration is mis-configured");
      }
      return Optional.of(
          Results.redirect(
              String.format(
                  "%s?error_msg=%s",
                  getLoginUrl(),
                  URLEncoder.encode(
                      "Failed to redirect to Single Sign-On provider. Please contact your DataHub Administrator, "
                          + "or refer to server logs for more information.",
                      StandardCharsets.UTF_8))));
    }
  }

  private CallContext buildCallContext(Http.RequestHeader request) {
    // First create PlayWebContext from the request
    PlayWebContext webContext = new PlayWebContext(request);

    // Then create CallContext using the web context and session store
    return new CallContext(webContext, sessionStore);
  }

  private void configurePac4jSessionStore(CallContext ctx, Client client) {
    WebContext context = ctx.webContext();

    // This is to prevent previous login attempts from being cached.
    // We replicate the logic here, which is buried in the Pac4j client.
    Optional<Object> attempt =
        ctx.sessionStore().get(context, client.getName() + ATTEMPTED_AUTHENTICATION_SUFFIX);
    if (attempt.isPresent() && !"".equals(attempt.get())) {
      logger.debug(
          "Found previous login attempt. Removing it manually to prevent unexpected errors.");
      ctx.sessionStore().set(context, client.getName() + ATTEMPTED_AUTHENTICATION_SUFFIX, "");
    }
  }

  private String encodeRedirectUri(final String redirectUri) {
    return URLEncoder.encode(redirectUri, StandardCharsets.UTF_8);
  }

  private boolean tryLogin(String username, String password) {
    boolean loginSucceeded = false;

    // First try jaas login, if enabled
    if (jaasConfigs.isJAASEnabled()) {
      try {
        logger.debug("Attempting JAAS authentication for user: {}", username);
        AuthenticationManager.authenticateJaasUser(username, password);
        logger.debug("JAAS authentication successful. Login succeeded");
        loginSucceeded = true;
      } catch (Exception e) {
        if (verbose) {
          logger.debug("JAAS authentication error. Login failed", e);
        } else {
          logger.debug("JAAS authentication error. Login failed");
        }
      }
    }

    // If jaas login fails or is disabled, try native auth login
    if (nativeAuthenticationConfigs.isNativeAuthenticationEnabled() && !loginSucceeded) {
      final Urn userUrn = new CorpuserUrn(username);
      final String userUrnString = userUrn.toString();
      loginSucceeded =
          loginSucceeded || authClient.verifyNativeUserCredentials(userUrnString, password);
    }

    return loginSucceeded;
  }

  private Result createSession(String userUrnString, String accessToken) {
    return Results.ok()
        .withSession(createSessionMap(userUrnString, accessToken))
        .withCookies(
            createActorCookie(
                userUrnString,
                cookieConfigs.getTtlInHours(),
                cookieConfigs.getAuthCookieSameSite(),
                cookieConfigs.getAuthCookieSecure()));
  }
}
