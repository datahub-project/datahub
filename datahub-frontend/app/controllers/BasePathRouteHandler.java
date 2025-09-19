package controllers;

import com.typesafe.config.Config;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import play.inject.Injector;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import utils.BasePathUtils;

/**
 * Controller that handles routing with base path support. This controller acts as a wrapper around
 * existing controllers, checking if the request matches the configured base path before delegating.
 */
public class BasePathRouteHandler extends Controller {

  private final Config config;
  private final Injector injector;

  @Inject
  public BasePathRouteHandler(Config config, Injector injector) {
    this.config = config;
    this.injector = injector;
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
   * Checks if the current request matches the expected route pattern with base path support.
   * Returns false if the request doesn't match the base path configuration.
   */
  private boolean isValidBasePathRequest(Http.Request request, String routePattern) {
    String basePath = getBasePath();
    return BasePathUtils.matchesRoute(request.path(), routePattern, basePath);
  }

  /** Wrapper for authentication routes that validates base path. */
  public Result authenticateWithBasePath(Http.Request request) {
    if (!isValidBasePathRequest(request, "/authenticate")) {
      return notFound("Route not found");
    }
    return injector.instanceOf(AuthenticationController.class).authenticate(request);
  }

  public Result ssoWithBasePath(Http.Request request) {
    if (!isValidBasePathRequest(request, "/sso")) {
      return notFound("Route not found");
    }
    return injector.instanceOf(AuthenticationController.class).sso(request);
  }

  public Result logInWithBasePath(Http.Request request) {
    if (!isValidBasePathRequest(request, "/logIn")) {
      return notFound("Route not found");
    }
    return injector.instanceOf(AuthenticationController.class).logIn(request);
  }

  public Result signUpWithBasePath(Http.Request request) {
    if (!isValidBasePathRequest(request, "/signUp")) {
      return notFound("Route not found");
    }
    return injector.instanceOf(AuthenticationController.class).signUp(request);
  }

  public Result resetNativeUserCredentialsWithBasePath(Http.Request request) {
    if (!isValidBasePathRequest(request, "/resetNativeUserCredentials")) {
      return notFound("Route not found");
    }
    return injector.instanceOf(AuthenticationController.class).resetNativeUserCredentials(request);
  }

  public CompletionStage<Result> callbackWithBasePath(String protocol, Http.Request request) {
    if (!isValidBasePathRequest(request, "/callback/" + protocol)) {
      return CompletableFuture.completedFuture(notFound("Route not found"));
    }
    return injector.instanceOf(SsoCallbackController.class).handleCallback(protocol, request);
  }

  public Result logOutWithBasePath(Http.Request request) {
    if (!isValidBasePathRequest(request, "/logOut")) {
      return notFound("Route not found");
    }
    return injector.instanceOf(CentralLogoutController.class).executeLogout(request);
  }
}
